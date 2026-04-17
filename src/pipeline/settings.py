from __future__ import annotations

import os
from collections.abc import Mapping
from urllib.parse import quote

from pipeline.config import (
    DEFAULT_BRONZE_OUTPUT_PATH,
    DEFAULT_DATA_FOLDER_PATH,
    DEFAULT_METADATA_FILE_NAME,
    DEFAULT_SILVER_METADATA_PATH,
    DEFAULT_TARGET_TABLE,
    DatabaseWriteEngine,
    PipelineConfig,
)
from pipeline.types import DatabaseWriteMode

DATABASE_CONNECTION_URI_ENV_VARS = (
    "DATABASE_CONNECTION_URI",
    "DATABASE_URL",
)
DATABASE_TYPE_ENV_VARS = (
    "PIPELINE_DATABASE_TYPE",
    "DATABASE_TYPE",
)
POSTGRES_HOST_ENV_VARS = (
    "PIPELINE_DATABASE_HOST",
    "DATABASE_HOST",
    "PGHOST",
)
POSTGRES_PORT_ENV_VARS = (
    "PIPELINE_DATABASE_PORT",
    "DATABASE_PORT",
    "PGPORT",
)
POSTGRES_NAME_ENV_VARS = (
    "PIPELINE_DATABASE_NAME",
    "DATABASE_NAME",
    "PGDATABASE",
)
POSTGRES_USER_ENV_VARS = (
    "PIPELINE_DATABASE_USER",
    "DATABASE_USER",
    "PGUSER",
)
POSTGRES_PASSWORD_ENV_VARS = (
    "PIPELINE_DATABASE_PASSWORD",
    "DATABASE_PASSWORD",
    "PGPASSWORD",
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
        database_write_mode=_read_write_mode(env),
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

    database_type = _read_first_env(env, DATABASE_TYPE_ENV_VARS)
    if database_type:
        return _build_connection_uri_from_components(
            environment=env,
            database_type=database_type,
        )

    supported_env_vars = " or ".join(DATABASE_CONNECTION_URI_ENV_VARS)
    supported_component_env_vars = ", ".join(
        (
            *DATABASE_TYPE_ENV_VARS,
            *POSTGRES_HOST_ENV_VARS[:2],
            *POSTGRES_PORT_ENV_VARS[:2],
            *POSTGRES_NAME_ENV_VARS[:2],
            *POSTGRES_USER_ENV_VARS[:2],
            *POSTGRES_PASSWORD_ENV_VARS[:2],
        )
    )
    raise ValueError(
        "Database connection settings are missing. "
        f"Set {supported_env_vars}, or provide component variables such as "
        f"{supported_component_env_vars}."
    )


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


def _read_first_env(environment: Mapping[str, str], env_vars: tuple[str, ...]) -> str | None:
    for env_var in env_vars:
        value = environment.get(env_var)
        if value:
            return value

    return None


def _build_connection_uri_from_components(
    environment: Mapping[str, str],
    database_type: str,
) -> str:
    normalized_database_type = database_type.strip().lower()
    if normalized_database_type in {"postgres", "postgresql"}:
        return _build_postgres_connection_uri(environment)

    raise ValueError(
        "Component-based database settings currently support only postgres/postgresql. "
        "Set DATABASE_CONNECTION_URI or DATABASE_URL for other database types."
    )


def _build_postgres_connection_uri(environment: Mapping[str, str]) -> str:
    host = _require_first_env(environment, POSTGRES_HOST_ENV_VARS)
    database_name = _require_first_env(environment, POSTGRES_NAME_ENV_VARS)
    username = _require_first_env(environment, POSTGRES_USER_ENV_VARS)
    password = _require_first_env(environment, POSTGRES_PASSWORD_ENV_VARS)
    raw_port = _read_first_env(environment, POSTGRES_PORT_ENV_VARS) or "5432"

    try:
        port = int(raw_port)
    except ValueError as exc:
        supported_port_env_vars = " or ".join(POSTGRES_PORT_ENV_VARS)
        raise ValueError(f"{supported_port_env_vars} must be an integer.") from exc

    escaped_username = quote(username, safe="")
    escaped_password = quote(password, safe="")
    escaped_database_name = quote(database_name, safe="")

    return (
        f"postgresql+psycopg://{escaped_username}:{escaped_password}"
        f"@{host}:{port}/{escaped_database_name}"
    )


def _require_first_env(environment: Mapping[str, str], env_vars: tuple[str, ...]) -> str:
    value = _read_first_env(environment, env_vars)
    if value:
        return value

    supported_env_vars = " or ".join(env_vars)
    raise ValueError(
        f"Missing database setting. Set one of {supported_env_vars}."
    )


def _read_write_engine(environment: Mapping[str, str]) -> DatabaseWriteEngine:
    raw_value = environment.get("PIPELINE_DATABASE_WRITE_ENGINE", "sqlalchemy")
    if raw_value not in {"sqlalchemy", "adbc"}:
        raise ValueError("PIPELINE_DATABASE_WRITE_ENGINE must be 'sqlalchemy' or 'adbc'.")

    return "adbc" if raw_value == "adbc" else "sqlalchemy"


def _read_write_mode(environment: Mapping[str, str]) -> DatabaseWriteMode:
    raw_value = environment.get("PIPELINE_DATABASE_WRITE_MODE", "replace")
    if raw_value not in {"replace", "append", "upsert"}:
        raise ValueError("PIPELINE_DATABASE_WRITE_MODE must be 'replace', 'append', or 'upsert'.")

    if raw_value == "append":
        return "append"

    if raw_value == "upsert":
        return "upsert"

    return "replace"
