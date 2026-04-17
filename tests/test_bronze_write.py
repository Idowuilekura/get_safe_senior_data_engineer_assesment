import json
import os
from pathlib import Path

import polars as pl

from pipeline.bronze.service import (
    acknowledge_duplicate_source_files,
    read_all_files,
    write_raw_data_bronze_out,
)


def test_bronze_refresh_reloads_existing_month_when_new_file_arrives(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    bronze_dir = tmp_path / "bronze"
    source_dir.mkdir()

    first_source_file = source_dir / "premium_transaction_2024_01_01.json"
    first_source_file.write_text(
        json.dumps([{"id": 1, "created_at": "01/01/2024 00:00:00"}]),
        encoding="utf-8",
    )

    bronze_df, metadata_dict, _, _, files_list_read, _, source_file_states = read_all_files(
        data_folder=str(source_dir),
        metadata_folder_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )
    assert bronze_df is not None
    first_metadata = write_raw_data_bronze_out(
        df=bronze_df,
        metadata_dict=metadata_dict,
        source_file_states=source_file_states,
        changed_or_new_files=files_list_read,
        list_data_read=files_list_read,
        data_folder_out=str(bronze_dir),
        metadata_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )

    second_source_file = source_dir / "premium_transaction_2024_01_02.json"
    second_source_file.write_text(
        json.dumps([{"id": 2, "created_at": "01/02/2024 00:00:00"}]),
        encoding="utf-8",
    )

    (
        bronze_df,
        metadata_dict,
        _,
        changed_or_new_files,
        files_list_read,
        ignored_duplicate_files,
        source_file_states,
    ) = read_all_files(
        data_folder=str(source_dir),
        metadata_folder_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )

    assert changed_or_new_files == [str(second_source_file)]
    assert files_list_read == [str(second_source_file)]
    assert ignored_duplicate_files == []
    assert bronze_df is not None

    write_raw_data_bronze_out(
        df=bronze_df,
        metadata_dict=first_metadata,
        source_file_states=source_file_states,
        changed_or_new_files=changed_or_new_files,
        list_data_read=files_list_read,
        data_folder_out=str(bronze_dir),
        metadata_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )

    bronze_table = (
        pl.scan_parquet(str(bronze_dir / "year=2024" / "month=1" / "*.parquet"))
        .sort("id")
        .collect()
    )

    assert bronze_table["id"].to_list() == [1, 2]


def test_bronze_refresh_reprocesses_corrected_file_at_same_path(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    bronze_dir = tmp_path / "bronze"
    source_dir.mkdir()

    source_file = source_dir / "premium_transaction_2024_01_01.json"
    source_file.write_text(
        json.dumps([{"id": 1, "created_at": "01/01/2024 00:00:00"}]),
        encoding="utf-8",
    )

    bronze_df, metadata_dict, _, _, files_list_read, _, source_file_states = read_all_files(
        data_folder=str(source_dir),
        metadata_folder_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )
    assert bronze_df is not None
    write_raw_data_bronze_out(
        df=bronze_df,
        metadata_dict=metadata_dict,
        source_file_states=source_file_states,
        changed_or_new_files=files_list_read,
        list_data_read=files_list_read,
        data_folder_out=str(bronze_dir),
        metadata_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )

    original_mtime_ns = source_file.stat().st_mtime_ns
    source_file.write_text(
        json.dumps([{"id": 99, "created_at": "01/01/2024 00:00:00"}]),
        encoding="utf-8",
    )
    os.utime(source_file, ns=(original_mtime_ns, original_mtime_ns + 1_000_000_000))

    (
        bronze_df,
        metadata_dict,
        _,
        changed_or_new_files,
        files_list_read,
        _,
        source_file_states,
    ) = read_all_files(
        data_folder=str(source_dir),
        metadata_folder_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )

    assert changed_or_new_files == [str(source_file)]
    assert files_list_read == [str(source_file)]
    assert bronze_df is not None

    updated_metadata = write_raw_data_bronze_out(
        df=bronze_df,
        metadata_dict=metadata_dict,
        source_file_states=source_file_states,
        changed_or_new_files=changed_or_new_files,
        list_data_read=files_list_read,
        data_folder_out=str(bronze_dir),
        metadata_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )

    bronze_table = (
        pl.scan_parquet(str(bronze_dir / "year=2024" / "month=1" / "*.parquet")).collect()
    )
    assert updated_metadata is not None

    assert bronze_table["id"].to_list() == [99]
    assert (
        updated_metadata["raw_file_states"][str(source_file)]["modified_time_ns"]
        > original_mtime_ns
    )


def test_bronze_refresh_accepts_compact_date_anywhere_in_filename(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    bronze_dir = tmp_path / "bronze"
    source_dir.mkdir()

    source_file = source_dir / "premium_transactions_data_20250306.json"
    source_file.write_text(
        json.dumps([{"id": 1, "created_at": "03/06/2025 00:00:00"}]),
        encoding="utf-8",
    )

    bronze_df, metadata_dict, _, _, files_list_read, _, source_file_states = read_all_files(
        data_folder=str(source_dir),
        metadata_folder_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )
    assert bronze_df is not None
    assert files_list_read == [str(source_file)]

    write_raw_data_bronze_out(
        df=bronze_df,
        metadata_dict=metadata_dict,
        source_file_states=source_file_states,
        changed_or_new_files=files_list_read,
        list_data_read=files_list_read,
        data_folder_out=str(bronze_dir),
        metadata_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )

    bronze_df, metadata_dict, _, _, files_list_read, _, _ = read_all_files(
        data_folder=str(source_dir),
        metadata_folder_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )

    assert bronze_df is None
    assert metadata_dict is not None
    assert files_list_read == []


def test_bronze_refresh_uses_payload_dates_when_filename_date_mismatches(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    bronze_dir = tmp_path / "bronze"
    source_dir.mkdir()

    source_file = source_dir / "premium_transactions_data_20250306.json"
    source_file.write_text(
        json.dumps(
            [
                {"id": 1, "created_at": "06/30/2024 23:59:59"},
                {"id": 2, "created_at": "07/01/2024 00:00:00"},
            ]
        ),
        encoding="utf-8",
    )

    bronze_df, metadata_dict, _, _, files_list_read, _, source_file_states = read_all_files(
        data_folder=str(source_dir),
        metadata_folder_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )

    assert bronze_df is not None
    assert metadata_dict is None
    assert files_list_read == [str(source_file)]

    bronze_metadata = write_raw_data_bronze_out(
        df=bronze_df,
        metadata_dict=metadata_dict,
        source_file_states=source_file_states,
        changed_or_new_files=files_list_read,
        list_data_read=files_list_read,
        data_folder_out=str(bronze_dir),
        metadata_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )

    assert bronze_metadata is not None
    assert bronze_metadata["yet_to_read_bronze_year_month"] == {2024: {6: [30], 7: [1]}}
    assert (bronze_dir / "year=2024" / "month=6").exists()
    assert (bronze_dir / "year=2024" / "month=7").exists()
    assert not (bronze_dir / "year=2025").exists()


def test_bronze_refresh_can_fall_back_to_created_at_when_filename_has_no_date(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    bronze_dir = tmp_path / "bronze"
    source_dir.mkdir()

    source_file = source_dir / "premium_transactions_partner_feed.json"
    source_file.write_text(
        json.dumps([{"id": 1, "created_at": "03/06/2025 00:00:00"}]),
        encoding="utf-8",
    )

    bronze_df, metadata_dict, _, _, files_list_read, _, source_file_states = read_all_files(
        data_folder=str(source_dir),
        metadata_folder_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )
    assert bronze_df is not None

    write_raw_data_bronze_out(
        df=bronze_df,
        metadata_dict=metadata_dict,
        source_file_states=source_file_states,
        changed_or_new_files=files_list_read,
        list_data_read=files_list_read,
        data_folder_out=str(bronze_dir),
        metadata_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )

    original_mtime_ns = source_file.stat().st_mtime_ns
    source_file.write_text(
        json.dumps([{"id": 2, "created_at": "03/06/2025 12:00:00"}]),
        encoding="utf-8",
    )
    os.utime(source_file, ns=(original_mtime_ns, original_mtime_ns + 1_000_000_000))

    (
        bronze_df,
        metadata_dict,
        _,
        changed_or_new_files,
        files_list_read,
        _,
        _,
    ) = read_all_files(
        data_folder=str(source_dir),
        metadata_folder_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )

    assert bronze_df is not None
    assert metadata_dict is not None
    assert changed_or_new_files == [str(source_file)]
    assert files_list_read == [str(source_file)]


def test_bronze_refresh_reprocesses_all_impacted_months_for_undated_batch_files(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "source"
    bronze_dir = tmp_path / "bronze"
    source_dir.mkdir()

    spanning_source_file = source_dir / "premium_transactions_partner_feed.json"
    spanning_source_file.write_text(
        json.dumps(
            [
                {"id": 1, "created_at": "01/31/2025 23:00:00"},
                {"id": 2, "created_at": "02/01/2025 00:10:00"},
            ]
        ),
        encoding="utf-8",
    )

    february_source_file = source_dir / "premium_transaction_2025_02_02.json"
    february_source_file.write_text(
        json.dumps([{"id": 3, "created_at": "02/02/2025 00:00:00"}]),
        encoding="utf-8",
    )

    bronze_df, metadata_dict, _, _, files_list_read, _, source_file_states = read_all_files(
        data_folder=str(source_dir),
        metadata_folder_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )
    assert bronze_df is not None

    write_raw_data_bronze_out(
        df=bronze_df,
        metadata_dict=metadata_dict,
        source_file_states=source_file_states,
        changed_or_new_files=files_list_read,
        list_data_read=files_list_read,
        data_folder_out=str(bronze_dir),
        metadata_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )

    original_mtime_ns = spanning_source_file.stat().st_mtime_ns
    spanning_source_file.write_text(
        json.dumps(
            [
                {"id": 10, "created_at": "01/31/2025 23:00:00"},
                {"id": 20, "created_at": "02/01/2025 00:10:00"},
            ]
        ),
        encoding="utf-8",
    )
    os.utime(spanning_source_file, ns=(original_mtime_ns, original_mtime_ns + 1_000_000_000))

    (
        bronze_df,
        metadata_dict,
        _,
        changed_or_new_files,
        files_list_read,
        _,
        _,
    ) = read_all_files(
        data_folder=str(source_dir),
        metadata_folder_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )

    assert bronze_df is not None
    assert metadata_dict is not None
    assert changed_or_new_files == [str(spanning_source_file)]
    assert set(files_list_read) == {
        str(spanning_source_file),
        str(february_source_file),
    }


def test_bronze_refresh_ignores_duplicate_copy_with_new_filename(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    bronze_dir = tmp_path / "bronze"
    source_dir.mkdir()

    source_file = source_dir / "premium_transaction_2024_01_01.json"
    source_file.write_text(
        json.dumps([{"id": 1, "created_at": "01/01/2024 00:00:00"}]),
        encoding="utf-8",
    )

    bronze_df, metadata_dict, _, _, files_list_read, _, source_file_states = read_all_files(
        data_folder=str(source_dir),
        metadata_folder_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )
    assert bronze_df is not None

    first_metadata = write_raw_data_bronze_out(
        df=bronze_df,
        metadata_dict=metadata_dict,
        source_file_states=source_file_states,
        changed_or_new_files=files_list_read,
        list_data_read=files_list_read,
        data_folder_out=str(bronze_dir),
        metadata_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )

    duplicate_copy = source_dir / "premium_transaction_2024_01_01_copy.json"
    duplicate_copy.write_text(source_file.read_text(encoding="utf-8"), encoding="utf-8")

    (
        bronze_df,
        metadata_dict,
        _,
        changed_or_new_files,
        files_list_read,
        ignored_duplicate_files,
        source_file_states,
    ) = read_all_files(
        data_folder=str(source_dir),
        metadata_folder_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )

    assert bronze_df is None
    assert metadata_dict is not None
    assert changed_or_new_files == []
    assert files_list_read == []
    assert ignored_duplicate_files == [str(duplicate_copy)]

    acknowledge_duplicate_source_files(
        existing_metadata=first_metadata,
        source_file_states=source_file_states,
        metadata_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )

    (
        bronze_df,
        _,
        _,
        changed_or_new_files,
        files_list_read,
        ignored_duplicate_files,
        _,
    ) = read_all_files(
        data_folder=str(source_dir),
        metadata_folder_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )

    assert bronze_df is None
    assert changed_or_new_files == []
    assert files_list_read == []
    assert ignored_duplicate_files == []


def test_bronze_refresh_ignores_duplicate_copy_when_both_arrive_together(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    bronze_dir = tmp_path / "bronze"
    source_dir.mkdir()

    source_file = source_dir / "premium_transactions_data_20250306.json"
    payload = [{"id": 1, "created_at": "03/06/2025 00:00:00"}]
    source_file.write_text(json.dumps(payload), encoding="utf-8")

    duplicate_copy = source_dir / "premium_transactions_data_20250306_copy.json"
    duplicate_copy.write_text(json.dumps(payload), encoding="utf-8")

    (
        bronze_df,
        metadata_dict,
        _,
        changed_or_new_files,
        files_list_read,
        ignored_duplicate_files,
        source_file_states,
    ) = read_all_files(
        data_folder=str(source_dir),
        metadata_folder_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )

    assert bronze_df is not None
    assert metadata_dict is None
    assert changed_or_new_files == [str(source_file)]
    assert files_list_read == [str(source_file)]
    assert ignored_duplicate_files == [str(duplicate_copy)]

    bronze_metadata = write_raw_data_bronze_out(
        df=bronze_df,
        metadata_dict=metadata_dict,
        source_file_states=source_file_states,
        changed_or_new_files=changed_or_new_files,
        list_data_read=files_list_read,
        data_folder_out=str(bronze_dir),
        metadata_path=str(bronze_dir),
        metadata_file_name="metadata.json",
    )

    assert bronze_metadata is not None
    bronze_table = (
        pl.scan_parquet(str(bronze_dir / "year=2025" / "month=3" / "*.parquet"))
        .sort("id")
        .collect()
    )
    assert bronze_table["id"].to_list() == [1]
    assert bronze_metadata["raw_files_list"] == sorted(
        [
            str(duplicate_copy),
            str(source_file),
        ]
    )
