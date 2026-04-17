from __future__ import annotations

from typing import Any, Literal, TypeAlias, TypeVar

import polars as pl

DatabaseWriteMode: TypeAlias = Literal["replace", "append", "upsert"]
MetadataDict: TypeAlias = dict[str, Any]
SourceFileState: TypeAlias = dict[str, int | str]
SourceFileStates: TypeAlias = dict[str, SourceFileState]

FrameT = TypeVar("FrameT", pl.DataFrame, pl.LazyFrame)
