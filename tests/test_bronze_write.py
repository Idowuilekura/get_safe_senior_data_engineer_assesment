import polars as pl

from pipeline.bronze.service import write_raw_data_bronze_out


def test_write_raw_data_bronze_out_appends_new_files_within_existing_month(tmp_path) -> None:
    first_metadata = write_raw_data_bronze_out(
        df=pl.DataFrame(
            {
                "id": [1],
                "created_at": ["01/01/2024 00:00:00"],
            }
        ).lazy(),
        metadata_dict=None,
        list_data_read=["premium_transaction_2024_01_01.json"],
        data_folder_out=str(tmp_path),
        metadata_path=str(tmp_path),
        metadata_file_name="metadata.json",
    )

    write_raw_data_bronze_out(
        df=pl.DataFrame(
            {
                "id": [2],
                "created_at": ["01/02/2024 00:00:00"],
            }
        ).lazy(),
        metadata_dict=first_metadata,
        list_data_read=["premium_transaction_2024_01_02.json"],
        data_folder_out=str(tmp_path),
        metadata_path=str(tmp_path),
        metadata_file_name="metadata.json",
    )

    bronze_df = (
        pl.scan_parquet(str(tmp_path / "year=2024" / "month=1" / "*.parquet"))
        .sort("id")
        .collect()
    )

    assert bronze_df["id"].to_list() == [1, 2]
