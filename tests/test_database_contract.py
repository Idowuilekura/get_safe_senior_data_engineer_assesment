import pytest

from pipeline.config import PipelineConfig
from pipeline.adapters.factory import DatabaseWriterFactory
from pipeline.adapters.postgres import PostgresPolarsWriter
from pipeline.adapters.sql import SqlAlchemyPolarsWriter
from pipeline.ports.database import WriteRequest


def test_write_request_defaults() -> None:
    request = WriteRequest(target_name="premium_transaction")

    assert request.mode == "replace"
    assert request.batch_size == 100_000
    assert request.merge_keys is None


def test_database_writer_factory_builds_postgres_writer() -> None:
    config = PipelineConfig(
        data_folder_path="data",
        bronze_output_path="bronze",
        bronze_metadata_file_name="metadata.json",
        silver_metadata_path="silver",
        silver_metadata_file_name="metadata.json",
        database_connection_uri="postgresql://postgres:postgres@localhost:5432/mydb",
    )

    writer = DatabaseWriterFactory.create(config)

    assert isinstance(writer, PostgresPolarsWriter)


def test_database_writer_factory_builds_generic_sql_writer_for_sqlite() -> None:
    config = PipelineConfig(
        data_folder_path="data",
        bronze_output_path="bronze",
        bronze_metadata_file_name="metadata.json",
        silver_metadata_path="silver",
        silver_metadata_file_name="metadata.json",
        database_connection_uri="sqlite:///tmp/pipeline.db",
    )

    writer = DatabaseWriterFactory.create(config)

    assert isinstance(writer, SqlAlchemyPolarsWriter)
    assert not isinstance(writer, PostgresPolarsWriter)


def test_postgres_writer_normalizes_plain_postgres_scheme() -> None:
    assert (
        PostgresPolarsWriter._normalize_connection_uri(
            "postgresql://postgres:postgres@localhost:5432/mydb"
        )
        == "postgresql+psycopg://postgres:postgres@localhost:5432/mydb"
    )


def test_database_writer_factory_rejects_upsert_for_generic_sql_backend() -> None:
    config = PipelineConfig(
        data_folder_path="data",
        bronze_output_path="bronze",
        bronze_metadata_file_name="metadata.json",
        silver_metadata_path="silver",
        silver_metadata_file_name="metadata.json",
        database_connection_uri="sqlite:///tmp/pipeline.db",
        database_write_mode="upsert",
    )

    with pytest.raises(ValueError, match="requires the Postgres adapter"):
        DatabaseWriterFactory.create(config)
