from pathlib import Path

import polars as pl
import pytest

from pipeline.adapters.factory import DatabaseWriterFactory
from pipeline.adapters.postgres import PostgresPolarsWriter
from pipeline.adapters.sql import SqlAlchemyPolarsWriter
from pipeline.config import PipelineConfig
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


def test_sqlalchemy_writer_can_write_with_default_engine(tmp_path: Path) -> None:
    database_path = tmp_path / "pipeline.db"
    writer = SqlAlchemyPolarsWriter(
        connection_uri=f"sqlite:///{database_path}",
    )

    rows_written = writer.write_lazyframe(
        lf=pl.DataFrame(
            {
                "policy_id": ["p1", "p2"],
                "amount": [10.5, 20.0],
            }
        ).lazy(),
        request=WriteRequest(target_name="premium_transaction"),
    )

    assert rows_written == 2


def test_sqlalchemy_writer_falls_back_to_batch_height_when_driver_reports_unknown_rowcount(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "pipeline.db"
    writer = SqlAlchemyPolarsWriter(
        connection_uri=f"sqlite:///{database_path}",
    )

    def fake_write_database(self, **kwargs):  # type: ignore[no-untyped-def]
        return -1

    monkeypatch.setattr(pl.DataFrame, "write_database", fake_write_database)

    rows_written = writer.write_lazyframe(
        lf=pl.DataFrame(
            {
                "policy_id": ["p1", "p2"],
                "amount": [10.5, 20.0],
            }
        ).lazy(),
        request=WriteRequest(target_name="premium_transaction"),
    )

    assert rows_written == 2
