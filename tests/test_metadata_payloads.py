from pipeline.bronze.service import build_bronze_metadata_payload
from pipeline.silver.service import build_silver_metadata_payload


def test_build_bronze_metadata_payload_preserves_old_files() -> None:
    payload = build_bronze_metadata_payload(
        existing_metadata={
            "raw_files_list": ["old.json"],
            "raw_file_states": {
                "old.json": {
                    "size_bytes": 10,
                    "modified_time_ns": 1,
                    "content_digest": "old-digest",
                }
            },
        },
        df_schema={"id": "Int64"},
        source_file_states={
            "new.json": {
                "size_bytes": 11,
                "modified_time_ns": 2,
                "content_digest": "new-digest",
            }
        },
        year_month_read={2024: {6: [1, 2]}},
        missing_days={2024: {6: [3]}},
    )

    assert payload["raw_files_list"] == ["new.json", "old.json"]
    assert payload["raw_file_states"]["new.json"]["size_bytes"] == 11
    assert payload["yet_to_read_bronze_year_month"] == {2024: {6: [1, 2]}}


def test_build_silver_metadata_payload_for_skip() -> None:
    payload = build_silver_metadata_payload(
        status="skipped",
        rows_written=0,
        reason="no_pending_bronze_partitions",
    )

    assert payload["silver_was_skipped"] is True
    assert payload["skip_reason"] == "no_pending_bronze_partitions"


def test_build_silver_metadata_payload_uses_provenance_friendly_bronze_key() -> None:
    payload = build_silver_metadata_payload(
        status="completed",
        rows_written=10,
        source_bronze_metadata={
            "yet_to_read_bronze_year_month": {2024: {6: [1, 2]}},
        },
    )

    assert payload["source_bronze_year_month"] == {2024: {6: [1, 2]}}
    assert "source_pending_bronze_year_month" not in payload
