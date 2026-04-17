from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds

from pipeline.types import MetadataDict, SourceFileStates
from pipeline.utils.files import (
    describe_source_files,
    extract_source_file_year_months,
    list_source_files,
)
from pipeline.utils.metadata import try_read_metadata_file, write_metadata_file
from pipeline.utils.partitions import enrich_timestamp, find_missing_days, get_days_month_df
from pipeline.utils.schema import deserialize_schema, serialize_schema

if TYPE_CHECKING:
    from polars._typing import SchemaDict

BRONZE_PARTITION_SCHEMA = ds.partitioning(
    pa.schema(
        [
            ("year", pa.int64()),
            ("month", pa.int64()),
        ]
    ),
    flavor="hive",
)


def files_to_read(
    data_folder: str,
    metadata_folder_path: str,
    metadata_file_name: str,
    insur_type: str = "premium",
    dataset_type: str = "transaction",
    ext_type: str = ".json",
    time_column: str = "created_at",
) -> tuple[
    MetadataDict | None,
    SchemaDict | None,
    list[str],
    list[str],
    list[str],
    SourceFileStates,
]:
    source_files = list_source_files(
        directory_path=data_folder,
        insur_type=insur_type,
        dataset_type=dataset_type,
        ext_type=ext_type,
        time_column=time_column,
    )
    source_file_states = describe_source_files(source_files)
    new_files, ignored_duplicate_files = _dedupe_new_files_by_content(
        candidate_file_paths=source_files,
        source_file_states=source_file_states,
        known_content_digests=set(),
    )

    metadata_dict = try_read_metadata_file(metadata_folder_path, metadata_file_name)
    if metadata_dict is None:
        return None, None, new_files, new_files, ignored_duplicate_files, source_file_states

    old_schema = deserialize_schema(metadata_dict.get("old_schema"))
    old_file_states = _read_source_file_states(metadata_dict)

    if not source_files:
        return metadata_dict, old_schema, [], [], [], source_file_states

    if not old_file_states:
        return (
            metadata_dict,
            old_schema,
            new_files,
            new_files,
            ignored_duplicate_files,
            source_file_states,
        )

    known_content_digests = _collect_known_content_digests(
        old_file_states=old_file_states,
        source_file_states=source_file_states,
    )
    candidate_new_file_paths = [
        file_path for file_path in source_files if file_path not in old_file_states
    ]
    new_files, ignored_duplicate_files = _dedupe_new_files_by_content(
        candidate_file_paths=candidate_new_file_paths,
        source_file_states=source_file_states,
        known_content_digests=known_content_digests,
    )

    modified_existing_files = [
        file_path
        for file_path in source_files
        if file_path in old_file_states
        and _source_file_contents_changed(old_file_states[file_path], source_file_states[file_path])
    ]
    changed_or_new_files = [
        file_path for file_path in source_files if file_path in new_files or file_path in modified_existing_files
    ]
    if not changed_or_new_files:
        return metadata_dict, old_schema, [], [], ignored_duplicate_files, source_file_states

    impacted_year_months = _extract_impacted_year_months(
        file_paths=modified_existing_files,
        ext_type=ext_type,
        time_column=time_column,
    )
    files_for_impacted_partitions = {
        file_path
        for file_path in source_files
        if extract_source_file_year_months(
            file_path,
            ext_type=ext_type,
            time_column=time_column,
        )
        & impacted_year_months
    }
    files_to_read_now = [
        file_path
        for file_path in source_files
        if file_path in new_files or file_path in files_for_impacted_partitions
    ]
    return (
        metadata_dict,
        old_schema,
        changed_or_new_files,
        files_to_read_now,
        ignored_duplicate_files,
        source_file_states,
    )


def read_data_jsons(
    file_paths: list[str],
    schema: SchemaDict | None = None,
) -> pl.LazyFrame | None:
    if not file_paths:
        return None

    dataframes = [
        pl.read_json(path, schema=schema) if schema else pl.read_json(path) for path in file_paths
    ]
    return pl.concat(dataframes).lazy()


def read_all_files(
    data_folder: str,
    metadata_folder_path: str,
    metadata_file_name: str,
    insur_type: str = "premium",
    dataset_type: str = "transaction",
    ext_type: str = ".json",
    time_column: str = "created_at",
) -> tuple[
    pl.LazyFrame | None,
    MetadataDict | None,
    SchemaDict | None,
    list[str],
    list[str],
    list[str],
    SourceFileStates,
]:
    (
        metadata_dict,
        old_schema,
        changed_or_new_files,
        files_list_read,
        ignored_duplicate_files,
        source_file_states,
    ) = files_to_read(
        data_folder=data_folder,
        metadata_folder_path=metadata_folder_path,
        metadata_file_name=metadata_file_name,
        insur_type=insur_type,
        dataset_type=dataset_type,
        ext_type=ext_type,
        time_column=time_column,
    )

    lazyframe = read_data_jsons(files_list_read, schema=old_schema)
    return (
        lazyframe,
        metadata_dict,
        old_schema,
        changed_or_new_files,
        files_list_read,
        ignored_duplicate_files,
        source_file_states,
    )


def build_bronze_metadata_payload(
    existing_metadata: MetadataDict | None,
    df_schema: Mapping[str, object],
    source_file_states: SourceFileStates,
    year_month_read: dict[int, dict[int, list[int]]],
    missing_days: dict[int, dict[int, list[int]]],
) -> MetadataDict:
    existing_metadata = existing_metadata or {}
    merged_file_states = _merge_source_file_states(existing_metadata, source_file_states)

    return {
        "old_schema": serialize_schema(df_schema),
        "raw_files_list": sorted(merged_file_states),
        "raw_file_states": merged_file_states,
        "yet_to_read_bronze_year_month": year_month_read,
        "missing_days": missing_days,
    }


def write_raw_data_bronze_out(
    df: pl.LazyFrame,
    metadata_dict: MetadataDict | None,
    source_file_states: SourceFileStates,
    changed_or_new_files: list[str],
    list_data_read: list[str],
    data_folder_out: str,
    metadata_path: str,
    metadata_file_name: str,
    ext_type: str = ".json",
    time_column: str = "created_at",
    new_time_column: str = "created_at_timestamp",
) -> MetadataDict | None:
    if df is None:
        return None

    df_enriched = enrich_timestamp(df, time_column, new_time_column)
    collected_df = df_enriched.collect()

    output_path = Path(data_folder_out)
    output_path.mkdir(parents=True, exist_ok=True)

    modified_existing_file_paths = [
        file_path
        for file_path in changed_or_new_files
        if file_path in _read_source_file_states(metadata_dict or {})
    ]
    partitions_to_rebuild = _extract_impacted_year_months(
        file_paths=modified_existing_file_paths,
        ext_type=ext_type,
        time_column=time_column,
    )
    rebuild_filter = _build_year_month_filter(partitions_to_rebuild)

    if rebuild_filter is None:
        append_df = collected_df
    else:
        replace_df = collected_df.filter(rebuild_filter)
        append_df = collected_df.filter(~rebuild_filter)
        _write_bronze_dataset(
            dataframe=replace_df,
            output_path=output_path,
            existing_data_behavior="delete_matching",
        )

    _write_bronze_dataset(
        dataframe=append_df,
        output_path=output_path,
        existing_data_behavior="overwrite_or_ignore",
    )

    touched_year_months = _extract_impacted_year_months(
        file_paths=changed_or_new_files,
        ext_type=ext_type,
        time_column=time_column,
    )
    year_month_read = _read_written_partition_days(
        output_path=output_path,
        touched_year_months=touched_year_months,
    )
    missing_days = find_missing_days(year_month_read)

    metadata_payload = build_bronze_metadata_payload(
        existing_metadata=metadata_dict,
        df_schema=collected_df.schema,
        source_file_states=source_file_states,
        year_month_read=year_month_read,
        missing_days=missing_days,
    )

    write_metadata_file(
        metadata_folder_path=metadata_path,
        metadata_file_name=metadata_file_name,
        metadata=metadata_payload,
    )
    return metadata_payload


def acknowledge_duplicate_source_files(
    existing_metadata: MetadataDict | None,
    source_file_states: SourceFileStates,
    metadata_path: str,
    metadata_file_name: str,
) -> MetadataDict | None:
    if existing_metadata is None:
        return None

    metadata_payload = dict(existing_metadata)
    merged_file_states = _merge_source_file_states(existing_metadata, source_file_states)
    metadata_payload["raw_files_list"] = sorted(merged_file_states)
    metadata_payload["raw_file_states"] = merged_file_states

    write_metadata_file(
        metadata_folder_path=metadata_path,
        metadata_file_name=metadata_file_name,
        metadata=metadata_payload,
    )
    return metadata_payload


def _read_source_file_states(metadata_dict: MetadataDict) -> SourceFileStates:
    raw_file_states = metadata_dict.get("raw_file_states")
    if not isinstance(raw_file_states, dict):
        return {}

    return {
        file_path: state
        for file_path, state in raw_file_states.items()
        if isinstance(file_path, str) and isinstance(state, dict)
    }


def _merge_source_file_states(
    existing_metadata: MetadataDict,
    source_file_states: SourceFileStates,
) -> SourceFileStates:
    merged_states = _read_source_file_states(existing_metadata)
    merged_states.update(source_file_states)
    return dict(sorted(merged_states.items()))


def _collect_known_content_digests(
    old_file_states: SourceFileStates,
    source_file_states: SourceFileStates,
) -> set[str]:
    known_content_digests: set[str] = set()

    for file_path, old_state in old_file_states.items():
        digest = _read_source_file_digest(old_state)
        if digest is None and file_path in source_file_states:
            digest = _read_source_file_digest(source_file_states[file_path])

        if digest is not None:
            known_content_digests.add(digest)

    return known_content_digests


def _dedupe_new_files_by_content(
    candidate_file_paths: list[str],
    source_file_states: SourceFileStates,
    known_content_digests: set[str],
) -> tuple[list[str], list[str]]:
    unique_file_paths: list[str] = []
    ignored_duplicate_files: list[str] = []
    seen_content_digests = set(known_content_digests)

    for file_path in candidate_file_paths:
        current_digest = _read_source_file_digest(source_file_states[file_path])
        if current_digest is not None and current_digest in seen_content_digests:
            ignored_duplicate_files.append(file_path)
            continue

        unique_file_paths.append(file_path)
        if current_digest is not None:
            seen_content_digests.add(current_digest)

    return unique_file_paths, ignored_duplicate_files


def _read_source_file_digest(source_file_state: SourceFileState) -> str | None:
    content_digest = source_file_state.get("content_digest")
    return content_digest if isinstance(content_digest, str) and content_digest else None


def _source_file_contents_changed(
    old_state: SourceFileState,
    new_state: SourceFileState,
) -> bool:
    old_digest = _read_source_file_digest(old_state)
    new_digest = _read_source_file_digest(new_state)

    if old_digest is not None and new_digest is not None:
        return old_digest != new_digest

    return (
        old_state.get("size_bytes") != new_state.get("size_bytes")
        or old_state.get("modified_time_ns") != new_state.get("modified_time_ns")
    )


def _extract_impacted_year_months(
    file_paths: list[str],
    ext_type: str,
    time_column: str,
) -> set[tuple[int, int]]:
    return {
        year_month
        for file_path in file_paths
        for year_month in extract_source_file_year_months(
            file_path,
            ext_type=ext_type,
            time_column=time_column,
        )
    }


def _build_year_month_filter(
    year_months: set[tuple[int, int]],
) -> pl.Expr | None:
    partition_filter: pl.Expr | None = None

    for year, month in sorted(year_months):
        current_filter = (pl.col("year") == year) & (pl.col("month") == month)
        partition_filter = (
            current_filter if partition_filter is None else partition_filter | current_filter
        )

    return partition_filter


def _write_bronze_dataset(
    dataframe: pl.DataFrame,
    output_path: Path,
    existing_data_behavior: str,
) -> None:
    if dataframe.is_empty():
        return

    ds.write_dataset(
        data=dataframe.to_arrow(),
        base_dir=str(output_path),
        format="parquet",
        partitioning=BRONZE_PARTITION_SCHEMA,
        basename_template=f"part-{uuid4().hex}-{{i}}.parquet",
        existing_data_behavior=existing_data_behavior,
    )


def _read_written_partition_days(
    output_path: Path,
    touched_year_months: set[tuple[int, int]],
) -> dict[int, dict[int, list[int]]]:
    if not touched_year_months:
        return {}

    parquet_paths = [
        str(output_path / f"year={year}" / f"month={month}" / "*.parquet")
        for year, month in sorted(touched_year_months)
    ]
    return get_days_month_df(pl.scan_parquet(parquet_paths, hive_partitioning=True))
