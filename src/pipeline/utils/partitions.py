from __future__ import annotations

import calendar
from pathlib import Path

import polars as pl

from pipeline.types import FrameT


def enrich_timestamp(df: pl.LazyFrame, time_column: str, new_time_column: str) -> pl.LazyFrame:
    parsed_timestamp = pl.col(time_column).str.to_datetime("%m/%d/%Y %H:%M:%S")

    return df.with_columns(
        [
            parsed_timestamp.alias(new_time_column),
            parsed_timestamp.dt.year().alias("year"),
            parsed_timestamp.dt.month().alias("month"),
            parsed_timestamp.dt.day().alias("day"),
        ]
    )


def get_days_month_df(df: pl.LazyFrame) -> dict[int, dict[int, list[int]]]:
    years_months_days = (
        df.select(["year", "month", "day"])
        .unique()
        .sort(["year", "month", "day"])
    )

    result: dict[int, dict[int, list[int]]] = {}

    for row in years_months_days.collect().to_dicts():
        year = row["year"]
        month = row["month"]
        day = row["day"]
        result.setdefault(year, {}).setdefault(month, []).append(day)

    return result


def find_missing_days(data: dict[int, dict[int, list[int]]]) -> dict[int, dict[int, list[int]]]:
    missing: dict[int, dict[int, list[int]]] = {}

    for year, months in data.items():
        missing[year] = {}

        for month, days in months.items():
            total_days = calendar.monthrange(year, month)[1]
            expected_days = set(range(1, total_days + 1))
            actual_days = set(days)
            missing[year][month] = sorted(expected_days - actual_days)

    return missing


def extract_year_month_globs(
    data: dict[str, dict[str, list[int]]] | dict[int, dict[int, list[int]]],
    output_path: str,
    file_pattern: str = "*.parquet",
) -> list[str]:
    base_path = Path(output_path)

    return [
        str(base_path / f"year={year}" / f"month={month}" / file_pattern)
        for year, months in data.items()
        for month in months
    ]


def enrich_time_features(df: FrameT, time_column: str) -> FrameT:
    timestamp_column = pl.col(time_column)

    return df.with_columns(
        [
            timestamp_column.dt.strftime("%A").alias("day_name"),
            timestamp_column.dt.week().alias("week_of_year"),
            (
                (timestamp_column.dt.month() == 12)
                | (
                    (timestamp_column.dt.month() == 1)
                    & (timestamp_column.dt.day() <= 7)
                )
            ).alias("is_festive_season"),
        ]
    )
