from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from pipeline.utils.metadata import try_read_metadata_file

DEFAULT_BRONZE_METADATA_FILE_NAME = "metadata.json"
DEFAULT_SILVER_METADATA_FILE_NAME = "metadata.json"


@dataclass(frozen=True, order=True)
class YearMonth:
    """Simple comparable month identifier."""

    year: int
    month: int

    def to_key(self) -> str:
        """Render the month as a YYYY-MM string."""
        return f"{self.year:04d}-{self.month:02d}"


def parse_year_month(value: str) -> YearMonth:
    """Parse a YYYY-MM string into a YearMonth value.

    Args:
        value: Month string in YYYY-MM format.

    Returns:
        Parsed year-month value.

    Raises:
        ValueError: If the input is not a valid YYYY-MM string.
    """
    try:
        year_text, month_text = value.split("-", 1)
        year = int(year_text)
        month = int(month_text)
    except ValueError as exc:
        raise ValueError(f"Invalid year-month value: {value!r}. Use YYYY-MM format.") from exc

    if month < 1 or month > 12:
        raise ValueError(f"Invalid month in year-month value: {value!r}. Use YYYY-MM format.")

    return YearMonth(year=year, month=month)


def iter_year_months(start: YearMonth, end: YearMonth) -> list[YearMonth]:
    """Expand an inclusive month range.

    Args:
        start: Inclusive range start.
        end: Inclusive range end.

    Returns:
        Ordered list of months from start through end.

    Raises:
        ValueError: If start is after end.
    """
    start_date = date(start.year, start.month, 1)
    end_date = date(end.year, end.month, 1)
    if start_date > end_date:
        raise ValueError(
            f"Backfill range start {start.to_key()} must be before or equal to {end.to_key()}."
        )

    values: list[YearMonth] = []
    year = start.year
    month = start.month

    while True:
        values.append(YearMonth(year=year, month=month))
        if year == end.year and month == end.month:
            return values

        month += 1
        if month == 13:
            year += 1
            month = 1


def discover_bronze_year_months(bronze_output_path: str | Path) -> list[YearMonth]:
    """Discover available bronze partitions from the filesystem.

    Args:
        bronze_output_path: Bronze output root containing year/month partitions.

    Returns:
        Sorted distinct list of available bronze months.
    """
    base_path = Path(bronze_output_path)
    if not base_path.exists():
        return []

    months: list[YearMonth] = []
    for year_dir in sorted(base_path.glob("year=*")):
        if not year_dir.is_dir():
            continue

        try:
            year = int(year_dir.name.split("=", 1)[1])
        except (IndexError, ValueError):
            continue

        for month_dir in sorted(year_dir.glob("month=*")):
            if not month_dir.is_dir():
                continue

            try:
                month = int(month_dir.name.split("=", 1)[1])
            except (IndexError, ValueError):
                continue

            if 1 <= month <= 12:
                months.append(YearMonth(year=year, month=month))

    return sorted(set(months))


def _metadata_partitions_to_year_months(data: object) -> list[YearMonth]:
    if not isinstance(data, dict):
        return []

    months: list[YearMonth] = []
    for year_key, year_value in data.items():
        try:
            year = int(year_key)
        except (TypeError, ValueError):
            continue

        if not isinstance(year_value, dict):
            continue

        for month_key in year_value:
            try:
                month = int(month_key)
            except (TypeError, ValueError):
                continue

            if 1 <= month <= 12:
                months.append(YearMonth(year=year, month=month))

    return sorted(set(months))


def load_latest_silver_loaded_months(
    silver_metadata_path: str | Path,
    metadata_file_name: str = DEFAULT_SILVER_METADATA_FILE_NAME,
) -> list[YearMonth]:
    """Read the latest silver-loaded month coverage from metadata.

    Args:
        silver_metadata_path: Directory containing the silver metadata file.
        metadata_file_name: Metadata filename to read.

    Returns:
        Sorted distinct list of months recorded in the latest silver metadata.
    """
    metadata = try_read_metadata_file(str(silver_metadata_path), metadata_file_name)
    if metadata is None:
        return []

    return _metadata_partitions_to_year_months(metadata.get("source_bronze_year_month"))


def build_backfill_plan(
    *,
    start_month: str,
    end_month: str,
    bronze_output_path: str | Path,
    silver_metadata_path: str | Path,
    silver_metadata_file_name: str = DEFAULT_SILVER_METADATA_FILE_NAME,
) -> dict[str, Any]:
    """Build a month-level backfill planning payload.

    Args:
        start_month: Inclusive backfill start month in YYYY-MM format.
        end_month: Inclusive backfill end month in YYYY-MM format.
        bronze_output_path: Bronze output root to inspect.
        silver_metadata_path: Silver metadata directory to inspect.
        silver_metadata_file_name: Silver metadata filename to read.

    Returns:
        JSON-serializable backfill planning details including requested months,
        bronze availability, and latest silver coverage.
    """
    requested_months = iter_year_months(parse_year_month(start_month), parse_year_month(end_month))
    available_in_bronze = discover_bronze_year_months(bronze_output_path)
    latest_silver_loaded = load_latest_silver_loaded_months(
        silver_metadata_path=silver_metadata_path,
        metadata_file_name=silver_metadata_file_name,
    )

    available_set = set(available_in_bronze)
    requested_set = set(requested_months)

    selected_for_backfill = sorted(requested_set & available_set)
    not_available_in_bronze = sorted(requested_set - available_set)

    return {
        "command": "plan-backfill",
        "requested_months": [value.to_key() for value in requested_months],
        "available_in_bronze": [value.to_key() for value in available_in_bronze],
        "selected_for_backfill": [value.to_key() for value in selected_for_backfill],
        "not_available_in_bronze": [value.to_key() for value in not_available_in_bronze],
        "latest_silver_loaded_months": [value.to_key() for value in latest_silver_loaded],
    }
