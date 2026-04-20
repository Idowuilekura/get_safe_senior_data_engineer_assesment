from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from export.monthly_partner_premium import (
    DEFAULT_EXPORT_OUTPUT_DIR,
    export_monthly_partner_premium_csv,
    resolve_gold_relation_name,
)
from pipeline.adapters.factory import DatabaseWriterFactory
from pipeline.backfill.service import build_backfill_plan
from pipeline.email_report import send_status_email
from pipeline.logging_config import configure_logging
from pipeline.orchestration import run_pipeline
from pipeline.settings import load_pipeline_config_from_env
from pipeline.types import MetadataDict

DEFAULT_PIPELINE_RUN_RESULT_ENV_VAR = "PIPELINE_RUN_RESULT_PATH"
DEFAULT_BRONZE_OUTPUT_PATH = "output/bronze"
DEFAULT_SILVER_METADATA_PATH = "output/silver"


def build_full_etl_summary(
    result: Mapping[str, MetadataDict | None],
) -> dict[str, Any]:
    silver_metadata = result.get("silver_metadata") or {}
    rows_written = silver_metadata.get("rows_written")
    silver_was_skipped = silver_metadata.get("silver_was_skipped")

    has_new_data = (
        silver_metadata.get("silver_status") == "completed"
        and silver_was_skipped is not True
        and isinstance(rows_written, int)
        and rows_written != 0
    )

    return {
        "command": "full-etl-pipeline",
        "has_new_data": has_new_data,
        "rows_written": rows_written if isinstance(rows_written, int) else 0,
        "silver_status": silver_metadata.get("silver_status"),
        "table_name": silver_metadata.get("table_name"),
        "result": dict(result),
    }


def write_full_etl_summary(summary: Mapping[str, Any], destination: str | Path) -> Path:
    output_path = Path(destination)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return output_path


def run_full_etl_pipeline(result_path: str | None = None) -> bool:
    configure_logging()

    config = load_pipeline_config_from_env()
    database_writer = DatabaseWriterFactory.create(config)
    result = run_pipeline(config=config, database_writer=database_writer)

    summary = build_full_etl_summary(result)
    resolved_result_path = result_path or os.environ.get(DEFAULT_PIPELINE_RUN_RESULT_ENV_VAR)
    if resolved_result_path:
        write_full_etl_summary(summary=summary, destination=resolved_result_path)

    print("true" if summary["has_new_data"] else "false")
    return bool(summary["has_new_data"])


def run_gold_export() -> Path:
    configure_logging()

    csv_path = export_monthly_partner_premium_csv(
        output_dir=os.environ.get("PIPELINE_EXPORT_OUTPUT_DIR", DEFAULT_EXPORT_OUTPUT_DIR),
        relation_name=resolve_gold_relation_name(),
    )
    print(csv_path)
    return csv_path


def run_status_email() -> bool:
    configure_logging()
    return send_status_email()


def run_plan_backfill(
    *,
    start_month: str,
    end_month: str,
    bronze_output_path: str = DEFAULT_BRONZE_OUTPUT_PATH,
    silver_metadata_path: str = DEFAULT_SILVER_METADATA_PATH,
) -> dict[str, Any]:
    configure_logging()

    plan = build_backfill_plan(
        start_month=start_month,
        end_month=end_month,
        bronze_output_path=bronze_output_path,
        silver_metadata_path=silver_metadata_path,
    )
    print(json.dumps(plan, indent=2, sort_keys=True))
    return plan


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="premium-container",
        description="Run the premium pipeline container entrypoints.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    full_etl_parser = subparsers.add_parser(
        "full-etl-pipeline",
        help="Run the ETL pipeline and print whether new data was written.",
    )
    full_etl_parser.add_argument(
        "--result-path",
        help=(
            "Optional JSON summary output path. Defaults to the "
            f"{DEFAULT_PIPELINE_RUN_RESULT_ENV_VAR} environment variable when set."
        ),
    )

    subparsers.add_parser(
        "gold-export",
        help="Export the monthly partner premium gold relation to CSV.",
    )
    subparsers.add_parser(
        "send-run-email",
        help="Send a status email for the pipeline run when email is configured.",
    )
    plan_backfill_parser = subparsers.add_parser(
        "plan-backfill",
        help="Plan a month-level backfill from available bronze partitions.",
    )
    plan_backfill_parser.add_argument(
        "--from",
        dest="start_month",
        required=True,
        help="Inclusive backfill start month in YYYY-MM format.",
    )
    plan_backfill_parser.add_argument(
        "--to",
        dest="end_month",
        required=True,
        help="Inclusive backfill end month in YYYY-MM format.",
    )
    plan_backfill_parser.add_argument(
        "--bronze-output-path",
        default=DEFAULT_BRONZE_OUTPUT_PATH,
        help=f"Bronze output path to inspect. Defaults to {DEFAULT_BRONZE_OUTPUT_PATH!r}.",
    )
    plan_backfill_parser.add_argument(
        "--silver-metadata-path",
        default=DEFAULT_SILVER_METADATA_PATH,
        help=f"Silver metadata path to inspect. Defaults to {DEFAULT_SILVER_METADATA_PATH!r}.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "full-etl-pipeline":
        run_full_etl_pipeline(result_path=args.result_path)
        return 0

    if args.command == "gold-export":
        run_gold_export()
        return 0

    if args.command == "send-run-email":
        run_status_email()
        return 0

    if args.command == "plan-backfill":
        run_plan_backfill(
            start_month=args.start_month,
            end_month=args.end_month,
            bronze_output_path=args.bronze_output_path,
            silver_metadata_path=args.silver_metadata_path,
        )
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
