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
    """Extract pending bronze partitions from a metadata payload.

    Args:
        metadata: Bronze metadata payload or None.

    Returns:
        Pending year-month-day structure, or an empty mapping when none exists.
    """
    if not metadata:
        return {}

    pending_partitions = metadata.get("yet_to_read_bronze_year_month")
    return pending_partitions if pending_partitions else {}


def has_pending_bronze_partitions(metadata: MetadataDict | None) -> bool:
    """Report whether bronze metadata still contains pending partitions.

    Args:
        metadata: Bronze metadata payload or None.

    Returns:
        True when pending bronze partitions are present, otherwise False.
    """
    return bool(get_pending_bronze_partitions(metadata))


def resolve_silver_metadata_source(
    current_bronze_metadata: MetadataDict | None,
    existing_bronze_metadata: MetadataDict | None,
) -> MetadataDict | None:
    """Choose the bronze metadata payload that should drive the silver step.

    Args:
        current_bronze_metadata: Metadata produced in the current run.
        existing_bronze_metadata: Previously persisted bronze metadata.

    Returns:
        The metadata payload that still contains pending bronze partitions, or
        None when no silver work remains.
    """
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
    """Build the silver lazyframe from the selected bronze partitions.

    Args:
        bronze_metadata: Bronze metadata that defines pending partitions.
        bronze_output_path: Root bronze parquet directory.
        time_column: Timestamp column used for additional time features.
        write_mode: Database write mode that determines partition selection.

    Returns:
        LazyFrame enriched with time features, or None when no parquet paths are
        resolved.
    """
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
    """Resolve bronze parquet globs to read for the silver step.

    Args:
        bronze_metadata: Bronze metadata payload.
        bronze_output_path: Root bronze parquet directory.
        write_mode: Database write mode that determines whether all bronze data
            or only pending partitions should be scanned.

    Returns:
        List of parquet paths or globs for the silver read.
    """
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
    """Build the persisted silver metadata payload.

    Args:
        status: Silver run status.
        source_bronze_metadata: Bronze metadata that fed the silver step.
        rows_written: Number of rows written to the trusted table.
        table_name: Trusted table name.
        reason: Optional skip reason.
        write_mode: Database write mode used for the trusted table write.

    Returns:
        Silver metadata payload for persistence and reporting.
    """
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
    """Persist silver metadata and return the payload.

    Args:
        silver_metadata_path: Directory where silver metadata is stored.
        silver_metadata_file_name: Metadata filename.
        metadata: Payload to persist.

    Returns:
        The same metadata payload after it is written.
    """
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
) -> tuple[MetadataDict, MetadataDict]:
    """Write silver data to the trusted table and update metadata state.

    Args:
        df: Silver lazyframe to write.
        bronze_metadata_path: Bronze metadata directory.
        bronze_metadata_dict: Bronze metadata that fed the write.
        bronze_metadata_file_name: Bronze metadata filename.
        silver_metadata_path: Silver metadata directory.
        silver_metadata_file_name: Silver metadata filename.
        database_writer: Database writer implementation.
        table_name: Trusted table name.
        batch_size: Write batch size.
        write_mode: Database write mode.
        merge_keys: Optional merge keys for upsert mode.

    Returns:
        Tuple of persisted silver metadata and updated bronze metadata.
    """
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
    return silver_metadata, updated_bronze_metadata
