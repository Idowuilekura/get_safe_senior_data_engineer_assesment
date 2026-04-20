from pipeline.email_report import build_status_email


def test_build_status_email_orders_sections_cleanly() -> None:
    _, body = build_status_email(
        dag_id="premium_pipeline",
        run_id="manual__1",
        pipeline_state="success",
        dbt_state="success",
        export_state="success",
        summary={
            "command": "full-etl-pipeline",
            "has_new_data": True,
            "rows_written": 2660,
            "silver_status": "completed",
            "table_name": "premium_transaction",
        },
        export_path="/app/output/gold/monthly_partner_premium_summary.csv",
    )

    assert body.index("Status:") < body.index("Task results:")
    assert body.index("Task results:") < body.index("Key metrics:")
    assert body.index("Key metrics:") < body.index("Artifacts:")
    assert body.index("Artifacts:") < body.index("Summary JSON:")
    assert "Pipeline: success" in body
    assert "dbt: success" in body
    assert "CSV export: success" in body
