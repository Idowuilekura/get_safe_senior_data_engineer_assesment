from datetime import date
from pathlib import Path

from pipeline.silver.service import resolve_bronze_parquet_paths, resolve_silver_metadata_source
from pipeline.utils import partitions
from pipeline.utils.partitions import extract_year_month_globs, find_missing_days


def test_extract_year_month_globs() -> None:
    globs = extract_year_month_globs(
        data={"2024": {"6": [1, 2], "7": [1]}},
        output_path="/tmp/bronze",
    )
    assert globs == [
        "/tmp/bronze/year=2024/month=6/*.parquet",
        "/tmp/bronze/year=2024/month=7/*.parquet",
    ]


def test_resolve_silver_metadata_source_prefers_current_bronze() -> None:
    current = {"yet_to_read_bronze_year_month": {"2024": {"6": [1]}}}
    existing = {"yet_to_read_bronze_year_month": {"2024": {"5": [1]}}}

    result = resolve_silver_metadata_source(current, existing)

    assert result == current


def test_resolve_bronze_parquet_paths_scans_full_bronze_dataset_for_replace(tmp_path: Path) -> None:
    older_partition = tmp_path / "year=2024" / "month=5" / "part-0.parquet"
    newer_partition = tmp_path / "year=2024" / "month=6" / "part-0.parquet"
    older_partition.parent.mkdir(parents=True, exist_ok=True)
    newer_partition.parent.mkdir(parents=True, exist_ok=True)
    older_partition.touch()
    newer_partition.touch()

    paths = resolve_bronze_parquet_paths(
        bronze_metadata={"yet_to_read_bronze_year_month": {"2024": {"6": [1]}}},
        bronze_output_path=str(tmp_path),
        write_mode="replace",
    )

    assert paths == sorted([str(older_partition), str(newer_partition)])


def test_find_missing_days_ignores_future_days_in_current_month(
    monkeypatch,
) -> None:
    monkeypatch.setattr(partitions, "_current_date", lambda: date(2026, 4, 16))

    missing_days = find_missing_days(
        {
            2026: {
                4: [1, 2, 3, 5, 16],
            }
        }
    )

    assert missing_days == {
        2026: {
            4: [4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        }
    }
