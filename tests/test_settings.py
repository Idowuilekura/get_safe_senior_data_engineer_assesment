import pytest

from pipeline.settings import (
    load_pipeline_config_from_env,
    resolve_database_connection_uri_from_env,
)


def test_load_pipeline_config_from_env_uses_database_connection_uri() -> None:
    config = load_pipeline_config_from_env(
        {
            "DATABASE_CONNECTION_URI": "postgresql+psycopg://postgres:postgres@localhost:5432/mydb",
        }
    )

    assert (
        config.database_connection_uri
        == "postgresql+psycopg://postgres:postgres@localhost:5432/mydb"
    )


def test_load_pipeline_config_from_env_uses_output_defaults_for_pipeline_artifacts() -> None:
    config = load_pipeline_config_from_env(
        {
            "DATABASE_CONNECTION_URI": "sqlite:///tmp/pipeline.db",
        }
    )

    assert config.data_folder_path == "data"
    assert config.bronze_output_path == "output/bronze"
    assert config.silver_metadata_path == "output/silver"


def test_load_pipeline_config_from_env_parses_merge_keys() -> None:
    config = load_pipeline_config_from_env(
        {
            "DATABASE_CONNECTION_URI": "sqlite:///tmp/pipeline.db",
            "PIPELINE_MERGE_KEYS": "policy_id, transaction_id",
        }
    )

    assert config.merge_keys == ("policy_id", "transaction_id")


def test_resolve_database_connection_uri_from_env_uses_database_url_alias() -> None:
    connection_uri = resolve_database_connection_uri_from_env(
        {
            "DATABASE_URL": "sqlite:///tmp/pipeline.db",
        }
    )

    assert connection_uri == "sqlite:///tmp/pipeline.db"


def test_resolve_database_connection_uri_from_env_builds_postgres_uri_from_components() -> None:
    connection_uri = resolve_database_connection_uri_from_env(
        {
            "DATABASE_TYPE": "postgres",
            "DATABASE_HOST": "db",
            "DATABASE_PORT": "5432",
            "DATABASE_NAME": "premium",
            "DATABASE_USER": "etl_user",
            "DATABASE_PASSWORD": "s3cr3t",
        }
    )

    assert connection_uri == "postgresql+psycopg://etl_user:s3cr3t@db:5432/premium"


def test_resolve_database_connection_uri_from_env_supports_pg_env_aliases() -> None:
    connection_uri = resolve_database_connection_uri_from_env(
        {
            "PIPELINE_DATABASE_TYPE": "postgresql",
            "PGHOST": "postgres",
            "PGDATABASE": "premium",
            "PGUSER": "etl_user",
            "PGPASSWORD": "s3cr3t",
        }
    )

    assert connection_uri == "postgresql+psycopg://etl_user:s3cr3t@postgres:5432/premium"


def test_resolve_database_connection_uri_from_env_prefers_explicit_uri_over_components() -> None:
    connection_uri = resolve_database_connection_uri_from_env(
        {
            "DATABASE_CONNECTION_URI": "sqlite:///tmp/pipeline.db",
            "DATABASE_TYPE": "postgres",
            "DATABASE_HOST": "db",
            "DATABASE_NAME": "premium",
            "DATABASE_USER": "etl_user",
            "DATABASE_PASSWORD": "s3cr3t",
        }
    )

    assert connection_uri == "sqlite:///tmp/pipeline.db"


def test_load_pipeline_config_from_env_requires_database_connection_settings() -> None:
    with pytest.raises(ValueError, match="Database connection settings are missing"):
        load_pipeline_config_from_env({})


def test_resolve_database_connection_uri_from_env_requires_supported_component_database_type() -> None:
    with pytest.raises(ValueError, match="support only postgres/postgresql"):
        resolve_database_connection_uri_from_env(
            {
                "DATABASE_TYPE": "mysql",
            }
        )


def test_resolve_database_connection_uri_from_env_requires_host_for_component_settings() -> None:
    with pytest.raises(ValueError, match="PIPELINE_DATABASE_HOST or DATABASE_HOST or PGHOST"):
        resolve_database_connection_uri_from_env(
            {
                "DATABASE_TYPE": "postgres",
                "DATABASE_NAME": "premium",
                "DATABASE_USER": "etl_user",
                "DATABASE_PASSWORD": "s3cr3t",
            }
        )


def test_load_pipeline_config_from_env_requires_integer_batch_size() -> None:
    with pytest.raises(ValueError, match="PIPELINE_BATCH_SIZE must be an integer"):
        load_pipeline_config_from_env(
            {
                "DATABASE_CONNECTION_URI": "sqlite:///tmp/pipeline.db",
                "PIPELINE_BATCH_SIZE": "large",
            }
        )


def test_load_pipeline_config_from_env_requires_supported_write_mode() -> None:
    with pytest.raises(
        ValueError,
        match="PIPELINE_DATABASE_WRITE_MODE must be 'replace', 'append', or 'upsert'",
    ):
        load_pipeline_config_from_env(
            {
                "DATABASE_CONNECTION_URI": "sqlite:///tmp/pipeline.db",
                "PIPELINE_DATABASE_WRITE_MODE": "merge",
            }
        )
