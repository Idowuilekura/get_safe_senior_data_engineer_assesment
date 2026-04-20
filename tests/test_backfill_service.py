from pathlib import Path

import pytest

from pipeline.backfill.service import build_backfill_plan, iter_year_months, parse_year_month


def test_parse_year_month_rejects_invalid_month() -> None:
    with pytest.raises(ValueError):
        parse_year_month("2024-13")


def test_iter_year_months_covers_inclusive_range() -> None:
    values = iter_year_months(parse_year_month("2024-11"), parse_year_month("2025-02"))

    assert [value.to_key() for value in values] == ["2024-11", "2024-12", "2025-01", "2025-02"]


def test_build_backfill_plan_uses_bronze_partitions_and_latest_silver_metadata(
    tmp_path: Path,
) -> None:
    bronze_dir = tmp_path / "bronze"
    (bronze_dir / "year=2024" / "month=6").mkdir(parents=True)
    (bronze_dir / "year=2024" / "month=7").mkdir(parents=True)
    (bronze_dir / "year=2024" / "month=9").mkdir(parents=True)

    silver_dir = tmp_path / "silver"
    silver_dir.mkdir()
    (silver_dir / "metadata.json").write_text(
        (
            '{'
            '"source_bronze_year_month": {'
            '"2024": {"6": [1, 2], "7": [1, 2], "8": [1] }'
            "}"
            "}"
        ),
        encoding="utf-8",
    )

    plan = build_backfill_plan(
        start_month="2024-06",
        end_month="2024-09",
        bronze_output_path=bronze_dir,
        silver_metadata_path=silver_dir,
    )

    assert plan["requested_months"] == ["2024-06", "2024-07", "2024-08", "2024-09"]
    assert plan["available_in_bronze"] == ["2024-06", "2024-07", "2024-09"]
    assert plan["selected_for_backfill"] == ["2024-06", "2024-07", "2024-09"]
    assert plan["not_available_in_bronze"] == ["2024-08"]
    assert plan["latest_silver_loaded_months"] == ["2024-06", "2024-07", "2024-08"]
