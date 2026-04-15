from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Literal

import polars as pl
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from pipeline.config import DatabaseWriteEngine
from pipeline.ports.database import DatabaseWriter, WriteRequest

if TYPE_CHECKING:
    from polars._typing import DbWriteMode

logger = logging.getLogger(__name__)


class SqlAlchemyPolarsWriter(DatabaseWriter):
    """Generic SQL adapter for replace/append writes via SQLAlchemy."""

    def __init__(
        self,
        connection_uri: str,
        write_engine: DatabaseWriteEngine = "sqlalchemy",
        sql_echo: bool = False,
    ) -> None:
        self._connection_uri = self._normalize_connection_uri(connection_uri)
        self._write_engine: DatabaseWriteEngine = write_engine
        self._sqlalchemy_engine: Engine = create_engine(
            self._connection_uri,
            future=True,
            echo=sql_echo,
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
            return self._write_dataframe(
                dataframe=dataframe,
                target_name=request.target_name,
                batch_size=request.batch_size,
                initial_mode="replace",
            )

        if request.mode == "append":
            return self._write_dataframe(
                dataframe=dataframe,
                target_name=request.target_name,
                batch_size=request.batch_size,
                initial_mode="append",
            )

        raise ValueError(
            f"Unsupported write mode '{request.mode}' for generic SQL adapter. "
            "Use a backend-specific adapter for dialect-specific merge semantics."
        )

    def _write_dataframe(
        self,
        dataframe: pl.DataFrame,
        target_name: str,
        batch_size: int,
        initial_mode: Literal["replace", "append"],
    ) -> int:
        total_rows_written = 0

        for batch_index, batch_df in enumerate(dataframe.iter_slices(n_rows=batch_size)):
            write_mode: DbWriteMode = initial_mode if batch_index == 0 else "append"
            total_rows_written += self._write_batch(
                df=batch_df,
                table_name=target_name,
                mode=write_mode,
            )

        logger.info(
            "Completed %s write for target '%s'. Total rows written=%s",
            initial_mode,
            target_name,
            total_rows_written,
        )
        return total_rows_written

    def _replace_dataframe(
        self,
        dataframe: pl.DataFrame,
        target_name: str,
        batch_size: int,
    ) -> int:
        return self._write_dataframe(
            dataframe=dataframe,
            target_name=target_name,
            batch_size=batch_size,
            initial_mode="replace",
        )

    def _write_batch(
        self,
        df: pl.DataFrame,
        table_name: str,
        mode: DbWriteMode,
    ) -> int:
        rows_written = df.write_database(
            table_name=table_name,
            connection=self._connection_uri,
            if_table_exists=mode,
            engine=self._write_engine,
        )
        return rows_written if rows_written is not None else df.height

    @staticmethod
    def _normalize_connection_uri(connection_uri: str) -> str:
        return connection_uri
