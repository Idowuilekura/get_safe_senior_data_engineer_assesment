from __future__ import annotations

import json
from pathlib import Path

from pipeline import container_cli


def test_build_full_etl_summary_marks_completed_rows_as_new_data() -> None:
    summary = container_cli.build_full_etl_summary(
        {
            "bronze_metadata": {"raw_files_list": ["a.json"]},
            "silver_metadata": {
                "silver_status": "completed",
                "rows_written": 7,
                "table_name": "premium_transaction",
            },
        }
    )

    assert summary["command"] == "full-etl-pipeline"
    assert summary["has_new_data"] is True
    assert summary["rows_written"] == 7
    assert summary["silver_status"] == "completed"


def test_build_full_etl_summary_treats_unknown_negative_rowcount_as_new_data() -> None:
    summary = container_cli.build_full_etl_summary(
        {
            "bronze_metadata": {"raw_files_list": ["a.json"]},
            "silver_metadata": {
                "silver_status": "completed",
                "rows_written": -1,
                "silver_was_skipped": False,
                "table_name": "premium_transaction",
            },
        }
    )

    assert summary["has_new_data"] is True
    assert summary["rows_written"] == -1


def test_full_etl_command_prints_false_and_writes_summary(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(container_cli, "configure_logging", lambda: None)
    monkeypatch.setattr(container_cli, "load_pipeline_config_from_env", lambda: object())
    monkeypatch.setattr(container_cli.DatabaseWriterFactory, "create", lambda _: object())
    monkeypatch.setattr(
        container_cli,
        "run_pipeline",
        lambda **_: {
            "bronze_metadata": None,
            "silver_metadata": {
                "silver_status": "skipped",
                "rows_written": 0,
                "table_name": "premium_transaction",
                "silver_was_skipped": True,
            },
        },
    )

    result_path = tmp_path / "full_etl_result.json"
    exit_code = container_cli.main(["full-etl-pipeline", "--result-path", str(result_path)])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.out.strip() == "false"

    persisted_summary = json.loads(result_path.read_text(encoding="utf-8"))
    assert persisted_summary["has_new_data"] is False
    assert persisted_summary["silver_status"] == "skipped"


def test_gold_export_command_prints_csv_path(tmp_path: Path, monkeypatch, capsys) -> None:
    expected_path = tmp_path / "output" / "gold" / "fct_monthly_partner_premium.csv"

    monkeypatch.setattr(container_cli, "configure_logging", lambda: None)
    monkeypatch.setattr(
        container_cli,
        "export_monthly_partner_premium_csv",
        lambda **_: expected_path,
    )

    exit_code = container_cli.main(["gold-export"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.out.strip() == str(expected_path)
