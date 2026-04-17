from __future__ import annotations

import logging
import uuid
from contextlib import contextmanager
from typing import TYPE_CHECKING, Iterator, Sequence

import polars as pl
from sqlalchemy import inspect, text

from pipeline.adapters.sql import SqlAlchemyPolarsWriter
from pipeline.config import DatabaseWriteEngine
from pipeline.ports.database import WriteRequest

if TYPE_CHECKING:
    from polars._typing import DbWriteMode

logger = logging.getLogger(__name__)


class PostgresPolarsWriter(SqlAlchemyPolarsWriter):
    """Postgres adapter for replace and upsert writes using Polars.

    The application layer stays database agnostic. This adapter owns:
    - connection management
    - batch loading
    - replace semantics
    - idempotent upsert semantics
    """

    def __init__(
        self,
        connection_uri: str,
        write_engine: DatabaseWriteEngine = "sqlalchemy",
        sql_echo: bool = False,
    ) -> None:
        super().__init__(
            connection_uri=connection_uri,
            write_engine=write_engine,
            sql_echo=sql_echo,
        )

    def write_lazyframe(
        self,
        lf: pl.LazyFrame,
        request: WriteRequest,
    ) -> int:
        dataframe = lf.collect(engine="streaming")
        if dataframe.is_empty():
            logger.info(
                "No rows available for target '%s'. Skipping database write.", request.target_name
            )
            return 0

        if request.mode == "replace":
            return self._replace_dataframe(dataframe, request.target_name, request.batch_size)

        if request.mode == "upsert":
            if not request.merge_keys:
                raise ValueError("merge_keys are required when mode='upsert'.")
            return self._upsert_dataframe(
                dataframe=dataframe,
                target_name=request.target_name,
                merge_keys=request.merge_keys,
                batch_size=request.batch_size,
            )

        raise ValueError(f"Unsupported write mode: {request.mode}")

    def _upsert_dataframe(
        self,
        dataframe: pl.DataFrame,
        target_name: str,
        merge_keys: Sequence[str],
        batch_size: int,
    ) -> int:
        if not self._table_exists(target_name):
            logger.info(
                "Target '%s' does not exist. Falling back to replace write before future upserts.",
                target_name,
            )
            return self._replace_dataframe(
                dataframe=dataframe,
                target_name=target_name,
                batch_size=batch_size,
            )

        staging_table = self._build_staging_table_name(target_name)

        try:
            total_rows_written = 0
            for batch_index, batch_df in enumerate(dataframe.iter_slices(n_rows=batch_size)):
                staging_mode: DbWriteMode = "replace" if batch_index == 0 else "append"
                total_rows_written += self._write_batch(
                    df=batch_df,
                    table_name=staging_table,
                    mode=staging_mode,
                )

            self._merge_staging_into_target(
                staging_table=staging_table,
                target_name=target_name,
                merge_keys=merge_keys,
                columns=dataframe.columns,
            )

            logger.info(
                (
                    "Completed upsert write for target '%s' using staging table '%s'. "
                    "Rows processed=%s"
                ),
                target_name,
                staging_table,
                total_rows_written,
            )
            return total_rows_written
        finally:
            self._drop_table_if_exists(staging_table)

    def _merge_staging_into_target(
        self,
        staging_table: str,
        target_name: str,
        merge_keys: Sequence[str],
        columns: Sequence[str],
    ) -> None:
        if not set(merge_keys).issubset(columns):
            raise ValueError("All merge_keys must exist in the source columns.")

        update_columns = [column for column in columns if column not in merge_keys]
        insert_columns_sql = ", ".join(self._quote_identifier(column) for column in columns)
        select_columns_sql = ", ".join(f"s.{self._quote_identifier(column)}" for column in columns)
        conflict_columns_sql = ", ".join(self._quote_identifier(column) for column in merge_keys)

        if update_columns:
            update_assignments_sql = ", ".join(
                f"{self._quote_identifier(column)} = EXCLUDED.{self._quote_identifier(column)}"
                for column in update_columns
            )
            conflict_action_sql = f"DO UPDATE SET {update_assignments_sql}"
        else:
            conflict_action_sql = "DO NOTHING"

        statement = f"""
            INSERT INTO {self._quote_identifier(target_name)} ({insert_columns_sql})
            SELECT {select_columns_sql}
            FROM {self._quote_identifier(staging_table)} AS s
            ON CONFLICT ({conflict_columns_sql})
            {conflict_action_sql}
        """

        with self._begin_transaction() as connection:
            connection.execute(text(statement))

    def _drop_table_if_exists(self, table_name: str) -> None:
        statement = f"DROP TABLE IF EXISTS {self._quote_identifier(table_name)}"
        with self._begin_transaction() as connection:
            connection.execute(text(statement))

    def _build_staging_table_name(self, target_name: str) -> str:
        schema_name, object_name = self._split_table_name(target_name)
        staging_table_name = f"{object_name}__staging__{uuid.uuid4().hex[:8]}"
        if schema_name is None:
            return staging_table_name

        return f"{schema_name}.{staging_table_name}"

    def _table_exists(self, table_name: str) -> bool:
        schema_name, object_name = self._split_table_name(table_name)
        inspector = inspect(self._sqlalchemy_engine)
        return inspector.has_table(object_name, schema=schema_name)

    @contextmanager
    def _begin_transaction(self) -> Iterator:
        with self._sqlalchemy_engine.begin() as connection:
            yield connection

    @classmethod
    def _quote_identifier(cls, identifier: str) -> str:
        parts = identifier.split(".")
        return ".".join(cls._quote_identifier_part(part) for part in parts)

    @staticmethod
    def _quote_identifier_part(identifier_part: str) -> str:
        escaped_identifier = identifier_part.replace('"', '""')
        return f'"{escaped_identifier}"'

    @staticmethod
    def _split_table_name(table_name: str) -> tuple[str | None, str]:
        if "." not in table_name:
            return None, table_name

        schema_name, object_name = table_name.split(".", 1)
        return schema_name, object_name

    @staticmethod
    def _normalize_connection_uri(connection_uri: str) -> str:
        if connection_uri.startswith("postgresql://"):
            return connection_uri.replace("postgresql://", "postgresql+psycopg://", 1)

        if connection_uri.startswith("postgres://"):
            return connection_uri.replace("postgres://", "postgresql+psycopg://", 1)

        return connection_uri
