from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds

from pipeline.utils.files import list_source_files
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
) -> tuple[dict[str, Any] | None, SchemaDict | None, list[str]]:
    source_files = list_source_files(
        directory_path=data_folder,
        insur_type=insur_type,
        dataset_type=dataset_type,
        ext_type=ext_type,
    )

    metadata_dict = try_read_metadata_file(metadata_folder_path, metadata_file_name)
    if metadata_dict is None:
        return None, None, source_files

    old_schema = deserialize_schema(metadata_dict.get("old_schema"))
    old_files_read = metadata_dict.get("raw_files_list", [])

    if not source_files:
        return metadata_dict, old_schema, []

    new_files_to_read = sorted(set(source_files) - set(old_files_read))
    return metadata_dict, old_schema, new_files_to_read


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
) -> tuple[pl.LazyFrame | None, dict[str, Any] | None, SchemaDict | None, list[str]]:
    metadata_dict, old_schema, files_list_read = files_to_read(
        data_folder=data_folder,
        metadata_folder_path=metadata_folder_path,
        metadata_file_name=metadata_file_name,
        insur_type=insur_type,
        dataset_type=dataset_type,
        ext_type=ext_type,
    )

    lazyframe = read_data_jsons(files_list_read, schema=old_schema)
    return lazyframe, metadata_dict, old_schema, files_list_read


def build_bronze_metadata_payload(
    existing_metadata: dict[str, Any] | None,
    df_schema: dict[str, Any],
    list_data_read: list[str],
    year_month_read: dict[int, dict[int, list[int]]],
    missing_days: dict[int, dict[int, list[int]]],
) -> dict[str, Any]:
    existing_metadata = existing_metadata or {}
    existing_files = existing_metadata.get("raw_files_list", [])

    return {
        "old_schema": serialize_schema(df_schema),
        "raw_files_list": sorted(set(existing_files + list_data_read)),
        "yet_to_read_bronze_year_month": year_month_read,
        "missing_days": missing_days,
    }


def write_raw_data_bronze_out(
    df: pl.LazyFrame,
    metadata_dict: dict[str, Any] | None,
    list_data_read: list[str],
    data_folder_out: str,
    metadata_path: str,
    metadata_file_name: str,
    time_column: str = "created_at",
    new_time_column: str = "created_at_timestamp",
) -> dict[str, Any] | None:
    if df is None:
        return None

    df_enriched = enrich_timestamp(df, time_column, new_time_column)
    collected_df = df_enriched.collect()

    output_path = Path(data_folder_out)
    output_path.mkdir(parents=True, exist_ok=True)

    ds.write_dataset(
        data=collected_df.to_arrow(),
        base_dir=str(output_path),
        format="parquet",
        partitioning=BRONZE_PARTITION_SCHEMA,
        basename_template=f"part-{uuid4().hex}-{{i}}.parquet",
        existing_data_behavior="overwrite_or_ignore",
    )

    year_month_read = get_days_month_df(df_enriched)
    missing_days = find_missing_days(year_month_read)

    metadata_payload = build_bronze_metadata_payload(
        existing_metadata=metadata_dict,
        df_schema=collected_df.schema,
        list_data_read=list_data_read,
        year_month_read=year_month_read,
        missing_days=missing_days,
    )

    write_metadata_file(
        metadata_folder_path=metadata_path,
        metadata_file_name=metadata_file_name,
        metadata=metadata_payload,
    )
    return metadata_payload
