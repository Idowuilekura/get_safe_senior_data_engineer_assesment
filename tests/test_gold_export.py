from __future__ import annotations

from pathlib import Path

import pytest

from export_dlt import db_file_export


def test_build_monthly_partner_premium_query_uses_expected_columns() -> None:
    query = db_file_export.build_monthly_partner_premium_query(
        "analytics.fct_monthly_partner_premium"
    )

    assert "partner" in query
    assert "month" in query
    assert "total_premium" in query
    assert "from analytics.fct_monthly_partner_premium" in query


def test_build_monthly_partner_premium_query_rejects_invalid_relation_name() -> None:
    with pytest.raises(ValueError, match="relation_name"):
        db_file_export.build_monthly_partner_premium_query(
            "fct_monthly_partner_premium; drop table"
        )


def test_export_monthly_partner_premium_csv_writes_output_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_query: dict[str, str] = {}
    captured_connection: dict[str, object] = {}

    class DummyEngine:
        def __init__(self) -> None:
            self.disposed = False

        def dispose(self) -> None:
            self.disposed = True

    class DummyFrame:
        def write_csv(self, path: str | Path) -> None:
            Path(path).write_text("partner,month,total_premium\nA,2024-01-01,100.0\n")

    engine = DummyEngine()

    def fake_create_engine(uri: str) -> DummyEngine:
        assert uri == "postgresql+psycopg://postgres:postgres@localhost:5432/mydb"
        return engine

    def fake_relation_exists(_: DummyEngine, relation_name: str) -> bool:
        return relation_name == db_file_export.DEFAULT_GOLD_RELATION

    def fake_read_database(query: str, connection: object) -> DummyFrame:
        captured_query["query"] = query
        captured_connection["connection"] = connection
        return DummyFrame()

    monkeypatch.setattr(db_file_export, "create_engine", fake_create_engine)
    monkeypatch.setattr(db_file_export, "relation_exists", fake_relation_exists)
    monkeypatch.setattr(db_file_export.pl, "read_database", fake_read_database)

    csv_path = db_file_export.export_monthly_partner_premium_csv(
        output_dir=tmp_path / "output" / "gold",
        connection_uri="postgresql+psycopg://postgres:postgres@localhost:5432/mydb",
    )

    assert csv_path == tmp_path / "output" / "gold" / "fct_monthly_partner_premium.csv"
    assert csv_path.read_text() == "partner,month,total_premium\nA,2024-01-01,100.0\n"
    assert "from fct_monthly_partner_premium" in captured_query["query"]
    assert captured_connection["connection"] is engine
    assert engine.disposed is True


def test_export_monthly_partner_premium_csv_raises_when_gold_relation_is_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummyEngine:
        def dispose(self) -> None:
            return None

    def fake_create_engine(_: str) -> DummyEngine:
        return DummyEngine()

    monkeypatch.setattr(db_file_export, "create_engine", fake_create_engine)
    monkeypatch.setattr(db_file_export, "relation_exists", lambda *_: False)

    with pytest.raises(ValueError, match="gold aggregate relation"):
        db_file_export.export_monthly_partner_premium_csv(
            output_dir=tmp_path / "output" / "gold",
            connection_uri="postgresql+psycopg://postgres:postgres@localhost:5432/mydb",
        )
