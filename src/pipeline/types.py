from __future__ import annotations

from typing import TypeAlias

import polars as pl

FrameT: TypeAlias = pl.DataFrame | pl.LazyFrame
