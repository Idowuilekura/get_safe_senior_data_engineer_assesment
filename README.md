# get_safe_senior_data_engineer_assesment

Repository to store my solution to the Get Safe Insurance Tech senior data engineer assessment.

## Premium pipeline

A small bronze and silver pipeline split into clear modules.

What it keeps from the notebook:
- bronze reads only new raw files using bronze metadata
- if bronze parquet already exists but silver failed later, the next run can use bronze metadata already on disk
- silver runs from current bronze metadata first, then falls back to existing bronze metadata
- if neither source has pending bronze partitions, silver is skipped
- silver metadata is written even when silver is skipped, so downstream work can also skip deterministically

## Layout

- `pipeline/config.py`: application config model
- `pipeline/settings.py`: environment/bootstrap loading
- `pipeline/utils`: file, schema, metadata, and partition helpers
- `pipeline/bronze`: bronze read and write logic
- `pipeline/silver`: silver build and load logic
- `pipeline/ports`: small interfaces for outbound dependencies
- `pipeline/adapters`: generic SQL adapter plus backend-specific specializations
- `pipeline/orchestration.py`: pipeline entry point

## Example

```python
from pipeline.adapters.factory import DatabaseWriterFactory
from pipeline.orchestration import run_pipeline
from pipeline.settings import load_pipeline_config_from_env

config = load_pipeline_config_from_env()

result = run_pipeline(
    config=config,
    database_writer=DatabaseWriterFactory.create(config),
)
print(result)
```

## Database writes

The generic SQL adapter supports:
- `replace`: replace on first batch, append remaining batches

The Postgres adapter additionally supports:
- `upsert`: stage batches into a temporary table, then merge into the target with `ON CONFLICT`

The pipeline stays database-agnostic at the application layer. The factory chooses the adapter from `database_connection_uri`, so switching to another SQLAlchemy-supported database is a config change. `upsert` remains Postgres-specific for now because merge semantics are dialect-specific.

## Local setup with uv

```bash
export DATABASE_CONNECTION_URI="postgresql+psycopg://postgres:postgres@localhost:5432/mydb"
uv sync
uv run pytest
uv run premium-pipeline
```

For a different backend, swap only the connection URI, for example:

```bash
export DATABASE_CONNECTION_URI="sqlite:///data/pipeline.db"
```

Optional runtime settings can also be provided via environment variables:

```bash
export PIPELINE_TABLE_NAME="premium_transaction"
export PIPELINE_DATABASE_WRITE_MODE="replace"
export PIPELINE_BATCH_SIZE="100000"
export PIPELINE_MERGE_KEYS="policy_id,transaction_id"
```
