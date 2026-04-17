# get_safe_senior_data_engineer_assesment

Repository to store my solution to the Get Safe Insurance Tech senior data engineer assessment.

## Premium pipeline

A small bronze and silver pipeline split into clear modules.

What it keeps from the notebook:
- bronze detects only new or changed raw files using bronze metadata, then refreshes the impacted bronze partitions
- bronze uses payload `created_at` values as the source of truth for partition dates; filename dates are only a fallback
- if bronze parquet already exists but silver failed later, the next run can use bronze metadata already on disk
- silver runs from current bronze metadata first, then falls back to existing bronze metadata
- if neither source has pending bronze partitions, silver is skipped
- silver metadata is written even when silver is skipped, so downstream work can also skip deterministically

Default filesystem contract:
- raw input files live under `data/`
- derived bronze parquet plus bronze metadata live under `output/bronze/`
- silver metadata lives under `output/silver/`
- gold export files live under `output/gold/`

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
export DATABASE_TYPE="postgres"
export DATABASE_HOST="localhost"
export DATABASE_PORT="5432"
export DATABASE_NAME="mydb"
export DATABASE_USER="postgres"
export DATABASE_PASSWORD="postgres"
uv sync
uv run pytest
uv run premium-pipeline
```

If you prefer, you can still provide the full URI directly:

```bash
export DATABASE_CONNECTION_URI="postgresql+psycopg://postgres:postgres@localhost:5432/mydb"
```

For a different backend, provide the full connection URI, for example:

```bash
export DATABASE_CONNECTION_URI="sqlite:///data/pipeline.db"
```

Optional runtime settings can also be provided via environment variables:

```bash
export PIPELINE_DATA_FOLDER_PATH="data"
export PIPELINE_BRONZE_OUTPUT_PATH="output/bronze"
export PIPELINE_SILVER_METADATA_PATH="output/silver"
export PIPELINE_TABLE_NAME="premium_transaction"
export PIPELINE_DATABASE_WRITE_MODE="replace"
export PIPELINE_BATCH_SIZE="100000"
export PIPELINE_MERGE_KEYS="policy_id,transaction_id"
```

## Gold export

To export the gold monthly partner premium aggregate to `output/gold/fct_monthly_partner_premium.csv`:

```bash
export DATABASE_TYPE="postgres"
export DATABASE_HOST="localhost"
export DATABASE_PORT="5432"
export DATABASE_NAME="mydb"
export DATABASE_USER="postgres"
export DATABASE_PASSWORD="postgres"
uv run premium-export-monthly-partner-premium
```

The export job first tries to read the materialized gold relation. If that table is not present yet, it falls back to computing the same monthly aggregation directly from `silver_transaction`, then writes the CSV to `output/gold/`.
The export job reads the materialized gold relation directly. If that table is not present yet, the job fails fast with a clear upstream dependency error so the missing gold build is handled explicitly rather than silently changing the export logic.

You can override the gold relation and destination folder with:

```bash
export PIPELINE_GOLD_MONTHLY_PARTNER_PREMIUM_RELATION="analytics.fct_monthly_partner_premium"
export PIPELINE_EXPORT_OUTPUT_DIR="output/gold"
```

## Docker

The container definition lives in [infra/docker/Dockerfile](/Users/id/Desktop/get_safe/premium_pipeline_project_updated/infra/docker/Dockerfile). The repo-level [.dockerignore](/Users/id/Desktop/get_safe/premium_pipeline_project_updated/.dockerignore) stays at the repository root on purpose because Docker applies ignore rules from the build context root, not from the Dockerfile directory.

The Python runtime is packaged as a single image with one container entrypoint and two commands:

- `full-etl-pipeline`: runs the bronze/silver load and prints `true` when fresh rows were written to the target table, otherwise `false`
- `dlt-export`: exports the gold monthly partner premium relation and prints the generated CSV path

Build the image:

```bash
docker build -f infra/docker/Dockerfile -t premium-pipeline .
```

Run the ETL container and pass runtime configuration through environment variables:

```bash
docker run --rm \
  --env DATABASE_TYPE="postgres" \
  --env DATABASE_HOST="host.docker.internal" \
  --env DATABASE_PORT="5432" \
  --env DATABASE_NAME="mydb" \
  --env DATABASE_USER="postgres" \
  --env DATABASE_PASSWORD="postgres" \
  --env PIPELINE_RUN_RESULT_PATH="/app/output/full_etl_result.json" \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/output:/app/output" \
  premium-pipeline full-etl-pipeline
```

The ETL command exits successfully when no new data is available and writes only `true` or `false` to stdout so downstream orchestration can gate dbt and the export step without parsing logs. If `PIPELINE_RUN_RESULT_PATH` is set, the container also persists a JSON summary for the run.

If your database is running on the host machine, avoid `localhost` from inside Docker because that points back to the container itself. Use `host.docker.internal` on Docker Desktop, or the database service name when running with Docker Compose.

Run the export from the same image:

```bash
docker run --rm \
  --env DATABASE_TYPE="postgres" \
  --env DATABASE_HOST="host.docker.internal" \
  --env DATABASE_PORT="5432" \
  --env DATABASE_NAME="mydb" \
  --env DATABASE_USER="postgres" \
  --env DATABASE_PASSWORD="postgres" \
  -v "$(pwd)/output:/app/output" \
  premium-pipeline dlt-export
```
