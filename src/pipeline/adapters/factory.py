from __future__ import annotations

from pipeline.adapters.postgres import PostgresPolarsWriter
from pipeline.adapters.sql import SqlAlchemyPolarsWriter
from pipeline.config import PipelineConfig
from pipeline.ports.database import DatabaseWriter


class DatabaseWriterFactory:
    """Creates database adapters from application config."""

    @staticmethod
    def create(config: PipelineConfig) -> DatabaseWriter:
        backend = DatabaseWriterFactory._resolve_backend(config)

        if backend == "postgres":
            return PostgresPolarsWriter(
                connection_uri=config.database_connection_uri,
                write_engine=config.database_write_engine,
            )

        if config.database_write_mode == "upsert":
            raise ValueError(
                "database_write_mode='upsert' requires the Postgres adapter. "
                "Use write mode 'replace' for generic SQL backends."
            )

        return SqlAlchemyPolarsWriter(
            connection_uri=config.database_connection_uri,
            write_engine=config.database_write_engine,
        )

    @staticmethod
    def _resolve_backend(config: PipelineConfig) -> str:
        if config.database_backend != "auto":
            normalized_backend = config.database_backend.lower()
            return (
                "postgres"
                if normalized_backend in {"postgres", "postgresql"}
                else normalized_backend
            )

        dialect = config.database_connection_uri.split("://", 1)[0].split("+", 1)[0].lower()
        return "postgres" if dialect in {"postgres", "postgresql"} else "sqlalchemy"
