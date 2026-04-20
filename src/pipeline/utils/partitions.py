from __future__ import annotations

import calendar
from datetime import date
from pathlib import Path

import polars as pl

from pipeline.types import FrameT


def _current_date() -> date:
    return date.today()


def enrich_timestamp(df: pl.LazyFrame, time_column: str, new_time_column: str) -> pl.LazyFrame:
    """Parse a source timestamp column and derive partition fields.

    Args:
        df: Source lazyframe.
        time_column: String timestamp column to parse.
        new_time_column: Name of the parsed timestamp column to create.

    Returns:
        LazyFrame with parsed timestamp plus year, month, and day columns.
    """
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
    """Build a year-month-day index from a bronze dataset.

    Args:
        df: LazyFrame containing year, month, and day columns.

    Returns:
        Nested mapping of years to months to sorted day lists.
    """
    years_months_days = df.select(["year", "month", "day"]).unique().sort(["year", "month", "day"])

    result: dict[int, dict[int, list[int]]] = {}

    for row in years_months_days.collect().to_dicts():
        year = row["year"]
        month = row["month"]
        day = row["day"]
        result.setdefault(year, {}).setdefault(month, []).append(day)

    return result


def find_missing_days(data: dict[int, dict[int, list[int]]]) -> dict[int, dict[int, list[int]]]:
    """Compute missing calendar days within observed months.

    Args:
        data: Nested mapping of years to months to observed day lists.

    Returns:
        Nested mapping of years to months to missing day lists.
    """
    missing: dict[int, dict[int, list[int]]] = {}
    current_date = _current_date()

    for year, months in data.items():
        missing[year] = {}

        for month, days in months.items():
            total_days = calendar.monthrange(year, month)[1]
            expected_day_limit = total_days
            if year == current_date.year and month == current_date.month:
                expected_day_limit = min(current_date.day, total_days)

            expected_days = set(range(1, expected_day_limit + 1))
            actual_days = set(days)
            missing[year][month] = sorted(expected_days - actual_days)

    return missing


def extract_year_month_globs(
    data: dict[str, dict[str, list[int]]] | dict[int, dict[int, list[int]]],
    output_path: str,
    file_pattern: str = "*.parquet",
) -> list[str]:
    """Build parquet glob paths from a year-month mapping.

    Args:
        data: Nested mapping of years to months to any value payload.
        output_path: Bronze output root path.
        file_pattern: File glob pattern to append for each partition.

    Returns:
        List of parquet glob paths.
    """
    base_path = Path(output_path)

    return [
        str(base_path / f"year={year}" / f"month={month}" / file_pattern)
        for year, months in data.items()
        for month in months
    ]


def enrich_time_features(df: FrameT, time_column: str) -> FrameT:
    """Add reusable calendar features to a timestamped frame.

    Args:
        df: Polars frame or lazyframe containing the timestamp column.
        time_column: Timestamp column used to derive features.

    Returns:
        Frame with additional calendar feature columns.
    """
    timestamp_column = pl.col(time_column)

    return df.with_columns(
        [
            timestamp_column.dt.strftime("%A").alias("day_name"),
            timestamp_column.dt.week().alias("week_of_year"),
            (
                (timestamp_column.dt.month() == 12)
                | ((timestamp_column.dt.month() == 1) & (timestamp_column.dt.day() <= 7))
            ).alias("is_festive_season"),
        ]
    )
