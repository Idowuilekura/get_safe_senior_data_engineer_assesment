from __future__ import annotations

import json
import os
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from pathlib import Path
from typing import Any

DEFAULT_PIPELINE_RUN_RESULT_PATH = "/app/output/full_etl_result.json"


def _read_bool_env(env_var: str, default: bool = False) -> bool:
    raw_value = os.environ.get(env_var)
    if raw_value is None:
        return default

    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def _read_int_env(env_var: str, default: int) -> int:
    raw_value = os.environ.get(env_var)
    if raw_value is None:
        return default

    return int(raw_value)


def _read_csv_env(env_var: str) -> tuple[str, ...]:
    raw_value = os.environ.get(env_var, "")
    return tuple(value.strip() for value in raw_value.split(",") if value.strip())


@dataclass(frozen=True)
class EmailSettings:
    enabled: bool
    host: str | None
    port: int
    username: str | None
    password: str | None
    sender: str | None
    recipients: tuple[str, ...]
    use_tls: bool

    @classmethod
    def from_env(cls) -> "EmailSettings":
        return cls(
            enabled=_read_bool_env("PIPELINE_SEND_EMAIL"),
            host=os.environ.get("PIPELINE_EMAIL_HOST"),
            port=_read_int_env("PIPELINE_EMAIL_PORT", default=587),
            username=os.environ.get("PIPELINE_EMAIL_USERNAME"),
            password=os.environ.get("PIPELINE_EMAIL_PASSWORD"),
            sender=os.environ.get("PIPELINE_EMAIL_FROM"),
            recipients=_read_csv_env("PIPELINE_EMAIL_TO"),
            use_tls=_read_bool_env("PIPELINE_EMAIL_USE_TLS", default=True),
        )

    def should_send(self) -> bool:
        return self.enabled and all(
            (
                self.host,
                self.username,
                self.password,
                self.sender,
                self.recipients,
            )
        )


def load_pipeline_summary(result_path: str | Path) -> dict[str, Any] | None:
    """Load a persisted pipeline summary if it exists.

    Args:
        result_path: Path to the pipeline summary JSON file.

    Returns:
        Parsed summary payload, or None when the file does not exist.
    """
    path = Path(result_path)
    if not path.exists():
        return None

    return json.loads(path.read_text(encoding="utf-8"))


def build_status_email(
    *,
    dag_id: str,
    run_id: str,
    pipeline_state: str,
    dbt_state: str,
    export_state: str,
    summary: dict[str, Any] | None,
    export_path: str | None,
) -> tuple[str, str]:
    """Build the email subject and body for a pipeline run.

    Args:
        dag_id: Airflow DAG identifier.
        run_id: Airflow run identifier.
        pipeline_state: Task state for the ETL step.
        dbt_state: Task state for the dbt step.
        export_state: Task state for the export step.
        summary: Optional persisted pipeline summary payload.
        export_path: Optional host-visible export path.

    Returns:
        Tuple of email subject and plain-text body.
    """
    has_new_data = bool(summary.get("has_new_data")) if summary else False
    rows_written = summary.get("rows_written") if summary else None
    silver_status = summary.get("silver_status") if summary else None
    table_name = summary.get("table_name") if summary else None

    normalized_states = {pipeline_state.lower(), dbt_state.lower(), export_state.lower()}
    if "failed" in normalized_states or "upstream_failed" in normalized_states:
        overall_status = "failed"
    elif has_new_data:
        overall_status = "succeeded with new data"
    else:
        overall_status = "succeeded with no new data"

    subject = f"Premium pipeline {overall_status}"

    lines = [
        f"DAG: {dag_id}",
        f"Run ID: {run_id}",
        "",
        "Status:",
        f"Overall status: {overall_status}",
        f"New data written: {'yes' if has_new_data else 'no'}",
        "",
        "Task results:",
        f"Pipeline: {pipeline_state or 'unknown'}",
        f"dbt: {dbt_state or 'unknown'}",
        f"CSV export: {export_state or 'unknown'}",
    ]

    metric_lines: list[str] = []
    if rows_written is not None:
        metric_lines.append(f"Rows written: {rows_written}")
    if silver_status:
        metric_lines.append(f"Silver status: {silver_status}")
    if table_name:
        metric_lines.append(f"Target table: {table_name}")
    if metric_lines:
        lines.extend(("", "Key metrics:", *metric_lines))

    if export_path:
        lines.extend(("", "Artifacts:", f"Export path: {export_path}"))
    if summary:
        concise_summary = {
            "command": summary.get("command"),
            "has_new_data": summary.get("has_new_data"),
            "rows_written": summary.get("rows_written"),
            "silver_status": summary.get("silver_status"),
            "table_name": summary.get("table_name"),
        }
        lines.extend(
            (
                "",
                "Summary JSON:",
                json.dumps(concise_summary, indent=2, sort_keys=True),
                "",
                "Detailed run result: attached as JSON.",
            )
        )

    return subject, "\n".join(lines)


def send_status_email() -> bool:
    """Send a pipeline status email when email delivery is configured.

    Returns:
        True if an email was sent successfully, otherwise False.

    Raises:
        ValueError: If required SMTP fields are unexpectedly missing after
            validation.
    """
    settings = EmailSettings.from_env()
    if not settings.enabled:
        print("Skipping email: PIPELINE_SEND_EMAIL is false.")
        return False

    if not settings.should_send():
        print("Skipping email: missing email configuration.")
        return False

    result_path = os.environ.get("PIPELINE_RUN_RESULT_PATH", DEFAULT_PIPELINE_RUN_RESULT_PATH)
    summary = load_pipeline_summary(result_path)

    subject, body = build_status_email(
        dag_id=os.environ.get("PIPELINE_EMAIL_DAG_ID", "premium_pipeline"),
        run_id=os.environ.get("PIPELINE_EMAIL_RUN_ID", "unknown"),
        pipeline_state=os.environ.get("PIPELINE_EMAIL_PIPELINE_STATE", "unknown"),
        dbt_state=os.environ.get("PIPELINE_EMAIL_DBT_STATE", "unknown"),
        export_state=os.environ.get("PIPELINE_EMAIL_EXPORT_STATE", "unknown"),
        summary=summary,
        export_path=os.environ.get("PIPELINE_EMAIL_EXPORT_PATH"),
    )

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = settings.sender
    message["To"] = ", ".join(settings.recipients)
    message.set_content(body)
    if summary:
        message.add_attachment(
            json.dumps(summary.get("result", {}), indent=2, sort_keys=True).encode("utf-8"),
            maintype="application",
            subtype="json",
            filename="premium_pipeline_run_result.json",
        )

    host = settings.host
    username = settings.username
    password = settings.password

    if host is None or username is None or password is None:
        raise ValueError(
            "Email settings were validated as sendable but required fields are missing."
        )

    with smtplib.SMTP(host, settings.port) as smtp:
        if settings.use_tls:
            smtp.starttls()
        smtp.login(username, password)
        smtp.send_message(message)

    print(f"Sent status email to {', '.join(settings.recipients)}")
    return True
