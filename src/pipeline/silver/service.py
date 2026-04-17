from __future__ import annotations

from pathlib import Path
from typing import Sequence

import polars as pl

from pipeline.config import DEFAULT_TARGET_TABLE
from pipeline.ports.database import DatabaseWriter, WriteRequest
from pipeline.types import DatabaseWriteMode, MetadataDict
from pipeline.utils.metadata import write_metadata_file
from pipeline.utils.partitions import enrich_time_features, extract_year_month_globs


def get_pending_bronze_partitions(
    metadata: MetadataDict | None,
) -> dict[str, dict[str, list[int]]]:
    if not metadata:
        return {}

    pending_partitions = metadata.get("yet_to_read_bronze_year_month")
    return pending_partitions if pending_partitions else {}


def has_pending_bronze_partitions(metadata: MetadataDict | None) -> bool:
    return bool(get_pending_bronze_partitions(metadata))


def resolve_silver_metadata_source(
    current_bronze_metadata: MetadataDict | None,
    existing_bronze_metadata: MetadataDict | None,
) -> MetadataDict | None:
    if has_pending_bronze_partitions(current_bronze_metadata):
        return current_bronze_metadata

    if has_pending_bronze_partitions(existing_bronze_metadata):
        return existing_bronze_metadata

    return None


def build_silver_lazyframe_from_bronze(
    bronze_metadata: MetadataDict,
    bronze_output_path: str,
    time_column: str = "created_at_timestamp",
    write_mode: DatabaseWriteMode = "replace",
) -> pl.LazyFrame | None:
    parquet_paths = resolve_bronze_parquet_paths(
        bronze_metadata=bronze_metadata,
        bronze_output_path=bronze_output_path,
        write_mode=write_mode,
    )
    if not parquet_paths:
        return None

    silver_df = pl.scan_parquet(parquet_paths)
    return enrich_time_features(silver_df, time_column=time_column)


def resolve_bronze_parquet_paths(
    bronze_metadata: MetadataDict | None,
    bronze_output_path: str,
    write_mode: DatabaseWriteMode = "replace",
) -> list[str]:
    pending_partitions = get_pending_bronze_partitions(bronze_metadata)
    if not pending_partitions:
        return []

    if write_mode == "replace":
        return sorted(str(path) for path in Path(bronze_output_path).rglob("*.parquet"))

    return extract_year_month_globs(
        data=pending_partitions,
        output_path=bronze_output_path,
    )


def build_silver_metadata_payload(
    status: str,
    source_bronze_metadata: MetadataDict | None = None,
    rows_written: int = 0,
    table_name: str = DEFAULT_TARGET_TABLE,
    reason: str | None = None,
    write_mode: DatabaseWriteMode = "replace",
) -> MetadataDict:
    source_bronze_metadata = source_bronze_metadata or {}
    pending_partitions = get_pending_bronze_partitions(source_bronze_metadata)

    payload: MetadataDict = {
        "silver_status": status,
        "table_name": table_name,
        "rows_written": rows_written,
        "write_mode": write_mode,
        "source_bronze_year_month": pending_partitions,
        "silver_was_skipped": status == "skipped",
    }

    if reason is not None:
        payload["skip_reason"] = reason

    return payload


def write_silver_metadata(
    silver_metadata_path: str,
    silver_metadata_file_name: str,
    metadata: MetadataDict,
) -> MetadataDict:
    write_metadata_file(
        metadata_folder_path=silver_metadata_path,
        metadata_file_name=silver_metadata_file_name,
        metadata=metadata,
    )
    return metadata


def write_silver_data_out(
    df: pl.LazyFrame,
    bronze_metadata_path: str,
    bronze_metadata_dict: MetadataDict,
    bronze_metadata_file_name: str,
    silver_metadata_path: str,
    silver_metadata_file_name: str,
    database_writer: DatabaseWriter,
    table_name: str = DEFAULT_TARGET_TABLE,
    batch_size: int = 100_000,
    write_mode: DatabaseWriteMode = "replace",
    merge_keys: Sequence[str] | None = None,
) -> MetadataDict:
    rows_written = database_writer.write_lazyframe(
        lf=df,
        request=WriteRequest(
            target_name=table_name,
            batch_size=batch_size,
            mode=write_mode,
            merge_keys=merge_keys,
        ),
    )

    silver_metadata = build_silver_metadata_payload(
        status="completed",
        source_bronze_metadata=bronze_metadata_dict,
        rows_written=rows_written,
        table_name=table_name,
        write_mode=write_mode,
    )
    write_silver_metadata(
        silver_metadata_path=silver_metadata_path,
        silver_metadata_file_name=silver_metadata_file_name,
        metadata=silver_metadata,
    )

    updated_bronze_metadata = dict(bronze_metadata_dict)
    updated_bronze_metadata["yet_to_read_bronze_year_month"] = {}

    write_metadata_file(
        metadata_folder_path=bronze_metadata_path,
        metadata_file_name=bronze_metadata_file_name,
        metadata=updated_bronze_metadata,
    )
    return silver_metadata
