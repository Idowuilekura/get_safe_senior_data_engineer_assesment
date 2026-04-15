from __future__ import annotations

import logging
from typing import Any

from pipeline.bronze.service import read_all_files, write_raw_data_bronze_out
from pipeline.config import PipelineConfig
from pipeline.ports.database import DatabaseWriter
from pipeline.silver.service import (
    build_silver_lazyframe_from_bronze,
    build_silver_metadata_payload,
    resolve_silver_metadata_source,
    write_silver_data_out,
    write_silver_metadata,
)

logger = logging.getLogger(__name__)


def run_pipeline(
    config: PipelineConfig,
    database_writer: DatabaseWriter,
) -> dict[str, Any]:
    bronze_df, existing_bronze_metadata, _, files_list_read = read_all_files(
        data_folder=config.data_folder_path,
        metadata_folder_path=config.bronze_output_path,
        metadata_file_name=config.bronze_metadata_file_name,
        insur_type=config.insur_type,
        dataset_type=config.dataset_type,
        ext_type=config.ext_type,
    )

    bronze_metadata_result: dict[str, Any] | None = None

    if bronze_df is not None:
        logger.info("New raw files detected. Running bronze write for %s file(s).", len(files_list_read))
        bronze_metadata_result = write_raw_data_bronze_out(
            df=bronze_df,
            metadata_dict=existing_bronze_metadata,
            list_data_read=files_list_read,
            data_folder_out=config.bronze_output_path,
            metadata_path=config.bronze_output_path,
            metadata_file_name=config.bronze_metadata_file_name,
            time_column=config.bronze_time_column,
            new_time_column=config.bronze_timestamp_column,
        )
    else:
        logger.info("No new raw files detected. Bronze write will be skipped.")

    silver_metadata_source = resolve_silver_metadata_source(
        current_bronze_metadata=bronze_metadata_result,
        existing_bronze_metadata=existing_bronze_metadata,
    )

    if silver_metadata_source is None:
        logger.info("No pending bronze partitions found. Silver write will be skipped.")
        silver_metadata = build_silver_metadata_payload(
            status="skipped",
            rows_written=0,
            table_name=config.table_name,
            reason="no_pending_bronze_partitions",
            write_mode=config.database_write_mode,
        )
        write_silver_metadata(
            silver_metadata_path=config.silver_metadata_path,
            silver_metadata_file_name=config.silver_metadata_file_name,
            metadata=silver_metadata,
        )
        return {
            "bronze_metadata": bronze_metadata_result,
            "silver_metadata": silver_metadata,
        }

    silver_df = build_silver_lazyframe_from_bronze(
        bronze_metadata=silver_metadata_source,
        bronze_output_path=config.bronze_output_path,
        time_column=config.silver_time_column,
        write_mode=config.database_write_mode,
    )

    if silver_df is None:
        logger.info("No bronze parquet partitions resolved. Silver write will be skipped.")
        silver_metadata = build_silver_metadata_payload(
            status="skipped",
            source_bronze_metadata=silver_metadata_source,
            rows_written=0,
            table_name=config.table_name,
            reason="no_bronze_parquet_globs_resolved",
            write_mode=config.database_write_mode,
        )
        write_silver_metadata(
            silver_metadata_path=config.silver_metadata_path,
            silver_metadata_file_name=config.silver_metadata_file_name,
            metadata=silver_metadata,
        )
        return {
            "bronze_metadata": bronze_metadata_result,
            "silver_metadata": silver_metadata,
        }

    silver_metadata = write_silver_data_out(
        df=silver_df,
        bronze_metadata_path=config.bronze_output_path,
        bronze_metadata_dict=silver_metadata_source,
        bronze_metadata_file_name=config.bronze_metadata_file_name,
        silver_metadata_path=config.silver_metadata_path,
        silver_metadata_file_name=config.silver_metadata_file_name,
        database_writer=database_writer,
        table_name=config.table_name,
        batch_size=config.batch_size,
        write_mode=config.database_write_mode,
        merge_keys=config.merge_keys,
    )

    return {
        "bronze_metadata": bronze_metadata_result,
        "silver_metadata": silver_metadata,
    }
