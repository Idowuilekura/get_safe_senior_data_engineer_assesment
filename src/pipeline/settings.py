from __future__ import annotations

import os
from collections.abc import Mapping

from pipeline.config import (
    DEFAULT_BRONZE_OUTPUT_PATH,
    DEFAULT_DATA_FOLDER_PATH,
    DEFAULT_METADATA_FILE_NAME,
    DEFAULT_SILVER_METADATA_PATH,
    DEFAULT_TARGET_TABLE,
    DatabaseWriteEngine,
    PipelineConfig,
)

DATABASE_CONNECTION_URI_ENV_VARS = (
    "DATABASE_CONNECTION_URI",
    "DATABASE_URL",
)


def load_pipeline_config_from_env(environment: Mapping[str, str] | None = None) -> PipelineConfig:
    env = os.environ if environment is None else environment

    return PipelineConfig(
        data_folder_path=env.get("PIPELINE_DATA_FOLDER_PATH", DEFAULT_DATA_FOLDER_PATH),
        bronze_output_path=env.get("PIPELINE_BRONZE_OUTPUT_PATH", DEFAULT_BRONZE_OUTPUT_PATH),
        bronze_metadata_file_name=env.get(
            "PIPELINE_BRONZE_METADATA_FILE_NAME", DEFAULT_METADATA_FILE_NAME
        ),
        silver_metadata_path=env.get("PIPELINE_SILVER_METADATA_PATH", DEFAULT_SILVER_METADATA_PATH),
        silver_metadata_file_name=env.get(
            "PIPELINE_SILVER_METADATA_FILE_NAME", DEFAULT_METADATA_FILE_NAME
        ),
        database_connection_uri=resolve_database_connection_uri_from_env(env),
        table_name=env.get("PIPELINE_TABLE_NAME", DEFAULT_TARGET_TABLE),
        database_backend=env.get("PIPELINE_DATABASE_BACKEND", "auto"),
        database_write_engine=_read_write_engine(env),
        database_write_mode=env.get("PIPELINE_DATABASE_WRITE_MODE", "replace"),
        merge_keys=_read_csv_env(env, "PIPELINE_MERGE_KEYS"),
        insur_type=env.get("PIPELINE_INSUR_TYPE", "premium"),
        dataset_type=env.get("PIPELINE_DATASET_TYPE", "transaction"),
        ext_type=env.get("PIPELINE_EXT_TYPE", ".json"),
        bronze_time_column=env.get("PIPELINE_BRONZE_TIME_COLUMN", "created_at"),
        bronze_timestamp_column=env.get("PIPELINE_BRONZE_TIMESTAMP_COLUMN", "created_at_timestamp"),
        silver_time_column=env.get("PIPELINE_SILVER_TIME_COLUMN", "created_at_timestamp"),
        batch_size=_read_int_env(env, "PIPELINE_BATCH_SIZE", default=100_000),
    )


def resolve_database_connection_uri_from_env(environment: Mapping[str, str] | None = None) -> str:
    env = os.environ if environment is None else environment

    for env_var in DATABASE_CONNECTION_URI_ENV_VARS:
        connection_uri = env.get(env_var)
        if connection_uri:
            return connection_uri

    supported_env_vars = " or ".join(DATABASE_CONNECTION_URI_ENV_VARS)
    raise ValueError(f"Database connection URI is missing. Set {supported_env_vars}.")


def _read_csv_env(
    environment: Mapping[str, str],
    env_var: str,
) -> tuple[str, ...] | None:
    raw_value = environment.get(env_var)
    if not raw_value:
        return None

    values = tuple(value.strip() for value in raw_value.split(",") if value.strip())
    return values or None


def _read_int_env(
    environment: Mapping[str, str],
    env_var: str,
    default: int,
) -> int:
    raw_value = environment.get(env_var)
    if raw_value is None:
        return default

    try:
        return int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{env_var} must be an integer.") from exc


def _read_write_engine(environment: Mapping[str, str]) -> DatabaseWriteEngine:
    raw_value = environment.get("PIPELINE_DATABASE_WRITE_ENGINE", "sqlalchemy")
    if raw_value not in {"sqlalchemy", "adbc"}:
        raise ValueError("PIPELINE_DATABASE_WRITE_ENGINE must be 'sqlalchemy' or 'adbc'.")

    return "adbc" if raw_value == "adbc" else "sqlalchemy"
