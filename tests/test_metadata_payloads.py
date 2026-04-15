from pipeline.bronze.service import build_bronze_metadata_payload
from pipeline.silver.service import build_silver_metadata_payload


def test_build_bronze_metadata_payload_preserves_old_files() -> None:
    payload = build_bronze_metadata_payload(
        existing_metadata={"raw_files_list": ["old.json"]},
        df_schema={"id": "Int64"},
        list_data_read=["new.json"],
        year_month_read={2024: {6: [1, 2]}},
        missing_days={2024: {6: [3]}},
    )

    assert payload["raw_files_list"] == ["new.json", "old.json"]
    assert payload["yet_to_read_bronze_year_month"] == {2024: {6: [1, 2]}}


def test_build_silver_metadata_payload_for_skip() -> None:
    payload = build_silver_metadata_payload(
        status="skipped",
        rows_written=0,
        reason="no_pending_bronze_partitions",
    )

    assert payload["silver_was_skipped"] is True
    assert payload["skip_reason"] == "no_pending_bronze_partitions"
