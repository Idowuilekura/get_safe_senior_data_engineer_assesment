import pytest

from pipeline.settings import load_pipeline_config_from_env
from pipeline.settings import resolve_database_connection_uri_from_env


def test_load_pipeline_config_from_env_uses_database_connection_uri() -> None:
    config = load_pipeline_config_from_env(
        {
            "DATABASE_CONNECTION_URI": "postgresql+psycopg://postgres:postgres@localhost:5432/mydb",
        }
    )

    assert config.database_connection_uri == "postgresql+psycopg://postgres:postgres@localhost:5432/mydb"


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


def test_load_pipeline_config_from_env_requires_database_connection_uri() -> None:
    with pytest.raises(ValueError, match="DATABASE_CONNECTION_URI or DATABASE_URL"):
        load_pipeline_config_from_env({})


def test_load_pipeline_config_from_env_requires_integer_batch_size() -> None:
    with pytest.raises(ValueError, match="PIPELINE_BATCH_SIZE must be an integer"):
        load_pipeline_config_from_env(
            {
                "DATABASE_CONNECTION_URI": "sqlite:///tmp/pipeline.db",
                "PIPELINE_BATCH_SIZE": "large",
            }
        )
