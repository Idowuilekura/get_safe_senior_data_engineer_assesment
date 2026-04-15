import json
from pathlib import Path

import polars as pl
import pytest

from pipeline.ports.database import DatabaseWriter, WriteRequest
from pipeline.silver.service import write_silver_data_out


class StubDatabaseWriter(DatabaseWriter):
    def write_lazyframe(
        self,
        lf: pl.LazyFrame,
        request: WriteRequest,
    ) -> int:
        return 1


def test_write_silver_data_out_keeps_pending_bronze_metadata_if_silver_metadata_write_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bronze_metadata = {"yet_to_read_bronze_year_month": {"2024": {"6": [1]}}}
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    bronze_dir.mkdir()
    silver_dir.mkdir()
    bronze_metadata_file = bronze_dir / "metadata.json"
    bronze_metadata_file.write_text(json.dumps(bronze_metadata), encoding="utf-8")

    def fail_write_silver_metadata(*args: object, **kwargs: object) -> dict[str, object]:
        raise RuntimeError("unable to persist silver metadata")

    monkeypatch.setattr(
        "pipeline.silver.service.write_silver_metadata",
        fail_write_silver_metadata,
    )

    with pytest.raises(RuntimeError, match="unable to persist silver metadata"):
        write_silver_data_out(
            df=pl.DataFrame({"id": [1]}).lazy(),
            bronze_metadata_path=str(bronze_dir),
            bronze_metadata_dict=bronze_metadata,
            bronze_metadata_file_name="metadata.json",
            silver_metadata_path=str(silver_dir),
            silver_metadata_file_name="metadata.json",
            database_writer=StubDatabaseWriter(),
        )

    assert json.loads(bronze_metadata_file.read_text(encoding="utf-8")) == bronze_metadata
