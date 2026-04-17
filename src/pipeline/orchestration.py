from __future__ import annotations

import logging

from pipeline.bronze.service import (
    acknowledge_duplicate_source_files,
    read_all_files,
    write_raw_data_bronze_out,
)
from pipeline.config import PipelineConfig
from pipeline.ports.database import DatabaseWriter
from pipeline.silver.service import (
    build_silver_lazyframe_from_bronze,
    build_silver_metadata_payload,
    resolve_silver_metadata_source,
    write_silver_data_out,
    write_silver_metadata,
)
from pipeline.types import MetadataDict

logger = logging.getLogger(__name__)


def run_pipeline(
    config: PipelineConfig,
    database_writer: DatabaseWriter,
) -> dict[str, MetadataDict | None]:
    (
        bronze_df,
        existing_bronze_metadata,
        _,
        changed_or_new_files,
        files_list_read,
        ignored_duplicate_files,
        source_file_states,
    ) = read_all_files(
        data_folder=config.data_folder_path,
        metadata_folder_path=config.bronze_output_path,
        metadata_file_name=config.bronze_metadata_file_name,
        insur_type=config.insur_type,
        dataset_type=config.dataset_type,
        ext_type=config.ext_type,
        time_column=config.bronze_time_column,
    )

    bronze_metadata_result: MetadataDict | None = None

    if bronze_df is not None:
        logger.info(
            "Detected %s new/changed raw file(s). Running bronze write for %s impacted file(s).",
            len(changed_or_new_files),
            len(files_list_read),
        )
        bronze_metadata_result = write_raw_data_bronze_out(
            df=bronze_df,
            metadata_dict=existing_bronze_metadata,
            source_file_states=source_file_states,
            changed_or_new_files=changed_or_new_files,
            list_data_read=files_list_read,
            data_folder_out=config.bronze_output_path,
            metadata_path=config.bronze_output_path,
            metadata_file_name=config.bronze_metadata_file_name,
            ext_type=config.ext_type,
            time_column=config.bronze_time_column,
            new_time_column=config.bronze_timestamp_column,
        )
        if ignored_duplicate_files:
            logger.info(
                "Ignored %s duplicate raw file(s) with already ingested contents.",
                len(ignored_duplicate_files),
            )
    else:
        if ignored_duplicate_files:
            logger.info(
                "Ignored %s duplicate raw file(s) with already ingested contents. "
                "Bronze write will be skipped.",
                len(ignored_duplicate_files),
            )
            existing_bronze_metadata = acknowledge_duplicate_source_files(
                existing_metadata=existing_bronze_metadata,
                source_file_states=source_file_states,
                metadata_path=config.bronze_output_path,
                metadata_file_name=config.bronze_metadata_file_name,
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
