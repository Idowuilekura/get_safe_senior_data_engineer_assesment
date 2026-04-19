from __future__ import annotations

import os
import re
from pathlib import Path
from collections.abc import Mapping

import polars as pl
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine

from pipeline.settings import resolve_database_connection_uri_from_env

DEFAULT_EXPORT_OUTPUT_DIR = "output/gold"
DEFAULT_EXPORT_FILE_NAME = "monthly_partner_premium_summary.csv"
DEFAULT_GOLD_RELATION = "monthly_partner_premiums"
DEFAULT_GOLD_SCHEMA_ENV_VARS = (
    "PIPELINE_GOLD_MONTHLY_PARTNER_PREMIUM_SCHEMA",
    "DBT_MARTS_SCHEMA",
)
RELATION_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


def validate_relation_name(relation_name: str) -> str:
    if not RELATION_NAME_PATTERN.fullmatch(relation_name):
        raise ValueError(
            "relation_name must be a table name or schema-qualified table name containing only "
            "letters, numbers, and underscores."
        )

    return relation_name


def build_monthly_partner_premium_query(
    relation_name: str = DEFAULT_GOLD_RELATION,
) -> str:
    validated_relation_name = validate_relation_name(relation_name)
    return f"""
        select
            partner,
            month,
            total_premium
        from {validated_relation_name}
        order by month desc, partner asc
    """.strip()


def split_relation_name(relation_name: str) -> tuple[str | None, str]:
    validated_relation_name = validate_relation_name(relation_name)
    if "." not in validated_relation_name:
        return None, validated_relation_name

    schema_name, table_name = validated_relation_name.split(".", 1)
    return schema_name, table_name


def resolve_gold_relation_name(environment: Mapping[str, str] | None = None) -> str:
    env = os.environ if environment is None else environment

    explicit_relation = env.get("PIPELINE_GOLD_MONTHLY_PARTNER_PREMIUM_RELATION")
    if explicit_relation:
        return explicit_relation

    for env_var in DEFAULT_GOLD_SCHEMA_ENV_VARS:
        schema_name = env.get(env_var)
        if schema_name:
            return f"{schema_name}.{DEFAULT_GOLD_RELATION}"

    return f"analytics.{DEFAULT_GOLD_RELATION}"


def relation_exists(engine: Engine, relation_name: str) -> bool:
    schema_name, table_name = split_relation_name(relation_name)
    return inspect(engine).has_table(table_name, schema=schema_name)


def ensure_relation_exists(engine: Engine, relation_name: str) -> None:
    if relation_exists(engine, relation_name):
        return

    raise ValueError(
        "Unable to export monthly partner premium because the gold aggregate relation "
        f"'{relation_name}' does not exist. Materialize the gold model before running the export."
    )


def export_monthly_partner_premium_csv(
    output_dir: str | Path = DEFAULT_EXPORT_OUTPUT_DIR,
    connection_uri: str | None = None,
    relation_name: str = DEFAULT_GOLD_RELATION,
) -> Path:
    resolved_connection_uri = connection_uri or resolve_database_connection_uri_from_env()

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    csv_path = output_path / DEFAULT_EXPORT_FILE_NAME
    engine = create_engine(resolved_connection_uri)
    try:
        ensure_relation_exists(engine=engine, relation_name=relation_name)
        query = build_monthly_partner_premium_query(relation_name)
        dataframe = pl.read_database(query=query, connection=engine)
    finally:
        engine.dispose()

    dataframe.write_csv(csv_path)
    return csv_path


def main() -> None:
    csv_path = export_monthly_partner_premium_csv(
        output_dir=os.environ.get("PIPELINE_EXPORT_OUTPUT_DIR", DEFAULT_EXPORT_OUTPUT_DIR),
        relation_name=resolve_gold_relation_name(),
    )
    print(f"Exported monthly partner premium CSV to {csv_path}")
