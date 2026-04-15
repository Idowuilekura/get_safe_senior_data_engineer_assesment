from __future__ import annotations

from typing import TypeVar

import polars as pl

FrameT = TypeVar("FrameT", pl.DataFrame, pl.LazyFrame)
