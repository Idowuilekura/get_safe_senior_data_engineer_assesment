from __future__ import annotations

from pathlib import Path

import pytest

from export import monthly_partner_premium as export_module


def test_build_monthly_partner_premium_query_uses_expected_columns() -> None:
    query = export_module.build_monthly_partner_premium_query(
        "analytics.monthly_partner_premiums"
    )

    assert "partner" in query
    assert "month" in query
    assert "total_premium" in query
    assert "from analytics.monthly_partner_premiums" in query


def test_build_monthly_partner_premium_query_rejects_invalid_relation_name() -> None:
    with pytest.raises(ValueError, match="relation_name"):
        export_module.build_monthly_partner_premium_query(
            "monthly_partner_premiums; drop table"
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
        return relation_name == export_module.DEFAULT_GOLD_RELATION

    def fake_read_database(query: str, connection: object) -> DummyFrame:
        captured_query["query"] = query
        captured_connection["connection"] = connection
        return DummyFrame()

    monkeypatch.setattr(export_module, "create_engine", fake_create_engine)
    monkeypatch.setattr(export_module, "relation_exists", fake_relation_exists)
    monkeypatch.setattr(export_module.pl, "read_database", fake_read_database)

    csv_path = export_module.export_monthly_partner_premium_csv(
        output_dir=tmp_path / "output" / "gold",
        connection_uri="postgresql+psycopg://postgres:postgres@localhost:5432/mydb",
    )

    assert csv_path == tmp_path / "output" / "gold" / "monthly_partner_premium_summary.csv"
    assert csv_path.read_text() == "partner,month,total_premium\nA,2024-01-01,100.0\n"
    assert "from monthly_partner_premiums" in captured_query["query"]
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

    monkeypatch.setattr(export_module, "create_engine", fake_create_engine)
    monkeypatch.setattr(export_module, "relation_exists", lambda *_: False)

    with pytest.raises(ValueError, match="gold aggregate relation"):
        export_module.export_monthly_partner_premium_csv(
            output_dir=tmp_path / "output" / "gold",
            connection_uri="postgresql+psycopg://postgres:postgres@localhost:5432/mydb",
        )


def test_resolve_gold_relation_name_prefers_explicit_relation() -> None:
    relation_name = export_module.resolve_gold_relation_name(
        {
            "PIPELINE_GOLD_MONTHLY_PARTNER_PREMIUM_RELATION": "reporting.custom_relation",
            "DBT_MARTS_SCHEMA": "analytics",
        }
    )

    assert relation_name == "reporting.custom_relation"


def test_resolve_gold_relation_name_uses_dbt_schema_when_relation_is_not_overridden() -> None:
    relation_name = export_module.resolve_gold_relation_name(
        {
            "DBT_MARTS_SCHEMA": "analytics",
        }
    )

    assert relation_name == "analytics.monthly_partner_premiums"
