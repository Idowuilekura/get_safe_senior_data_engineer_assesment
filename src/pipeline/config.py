from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Sequence

DatabaseWriteEngine = Literal["sqlalchemy", "adbc"]


DEFAULT_TARGET_TABLE = "premium_transaction"
DEFAULT_DATA_FOLDER_PATH = "data"
DEFAULT_BRONZE_OUTPUT_PATH = "data/raw_parquet"
DEFAULT_METADATA_FILE_NAME = "metadata.json"
DEFAULT_SILVER_METADATA_PATH = "data/silver"


@dataclass(frozen=True)
class PipelineConfig:
    data_folder_path: str
    bronze_output_path: str
    bronze_metadata_file_name: str
    silver_metadata_path: str
    silver_metadata_file_name: str
    database_connection_uri: str
    table_name: str = DEFAULT_TARGET_TABLE
    database_backend: str = "auto"
    database_write_engine: DatabaseWriteEngine = "sqlalchemy"
    database_write_mode: str = "replace"
    merge_keys: Sequence[str] | None = None
    insur_type: str = "premium"
    dataset_type: str = "transaction"
    ext_type: str = ".json"
    bronze_time_column: str = "created_at"
    bronze_timestamp_column: str = "created_at_timestamp"
    silver_time_column: str = "created_at_timestamp"
    batch_size: int = 100_000
