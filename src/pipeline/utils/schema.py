from __future__ import annotations

from typing import Any

import polars as pl

_POLARS_DTYPE_MAP: dict[str, pl.DataType] = {
    "Int8": pl.Int8,
    "Int16": pl.Int16,
    "Int32": pl.Int32,
    "Int64": pl.Int64,
    "UInt8": pl.UInt8,
    "UInt16": pl.UInt16,
    "UInt32": pl.UInt32,
    "UInt64": pl.UInt64,
    "Float32": pl.Float32,
    "Float64": pl.Float64,
    "Boolean": pl.Boolean,
    "String": pl.String,
    "Utf8": pl.String,
    "Date": pl.Date,
    "Datetime": pl.Datetime,
}


def serialize_schema(schema: dict[str, Any] | None) -> dict[str, str] | None:
    if not schema:
        return None

    return {column: str(dtype) for column, dtype in schema.items()}


def deserialize_schema(schema_dict: dict[str, str] | None) -> dict[str, pl.DataType] | None:
    if not schema_dict:
        return None

    schema: dict[str, pl.DataType] = {}

    for column, dtype_name in schema_dict.items():
        normalized_dtype = dtype_name.split("(")[0]
        schema[column] = _POLARS_DTYPE_MAP.get(normalized_dtype, pl.String)

    return schema
