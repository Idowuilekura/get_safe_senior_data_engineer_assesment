from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Sequence

import polars as pl

from pipeline.types import DatabaseWriteMode


@dataclass(frozen=True)
class WriteRequest:
    """Write contract used by the application layer."""

    target_name: str
    batch_size: int = 100_000
    mode: DatabaseWriteMode = "replace"
    merge_keys: Sequence[str] | None = None


class DatabaseWriter(ABC):
    """Port for persisting tabular data into a database target."""

    @abstractmethod
    def write_lazyframe(
        self,
        lf: pl.LazyFrame,
        request: WriteRequest,
    ) -> int:
        """Persist a lazyframe and return the number of rows processed."""
        raise NotImplementedError
