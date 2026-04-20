# Premium Reconciliation Pipeline

Production-style solution for the Getsafe Senior Data Engineer case study.

This repository implements a local batch pipeline that ingests raw JSON premium transactions, persists a rerunnable bronze layer in Parquet, loads a trusted transaction table into a SQL database, models accepted and rejected rows in dbt, and exports a monthly partner premium report to CSV.

## Output

The final report has the following shape:

```text
partner,month,total_premium
liadigital,2024-07-01,104.32
...
```

Business rules applied to the exported report:

- `partner` is the insurance partner charged on the transaction.
- `month` is the first day of the calendar month.
- `total_premium` is the sum of accepted premium amounts after FX normalization, rounded to 2 decimal places.
- Only rows with `status = 'processed'` and a positive `amount` contribute to the final aggregate.

## Business Rules

- `created_at` is the authoritative business timestamp for reporting month and partition impact. Filename dates are fallback metadata only.
- The finance-facing export is `analytics.monthly_partner_premiums`, which normalizes GBP amounts to EUR using a single documented 2024 ECB annual-average reference rate.
- A companion mart, `analytics.monthly_partner_premiums_by_currency`, preserves monthly totals in source currency for auditability.
- `processed` rows with `amount <= 0` are treated as reconciliation exceptions and routed to the rejected path rather than counted as earned premium.
- Low positive premiums remain valid. The case study does not impose a minimum-positive-amount rejection threshold.

## Architecture

```mermaid
flowchart LR
    A["Raw JSON files"] --> B["Bronze parquet + metadata"]
    B --> C["Trusted transaction table"]
    C --> D["dbt models"]
    D --> E["analytics.monthly_partner_premiums"]
    E --> F["CSV export"]
```

Pipeline stages:

1. Bronze ingestion discovers matching JSON files, deduplicates already-seen content, parses event timestamps, and writes Parquet partitions plus metadata.
2. Silver loading reads pending bronze data, enriches time features, and writes the trusted transaction table.
3. dbt models classify accepted and rejected rows, then build dimensional and reporting models.
4. Export reads the EUR-normalized `analytics.monthly_partner_premiums` mart and writes `monthly_partner_premium_summary.csv`.

## Operational Guarantees

- Payload event time is authoritative. File discovery and partition impact detection prefer `created_at`; filename date parsing is only a fallback.
- Bronze ingestion is metadata-driven. Seen file content is tracked with content digests, file size, and modified time.
- Exact duplicate contents are not re-ingested, even if they arrive under a new filename.
- If a previously ingested file changes at the same path, impacted year/month Parquet partitions are rebuilt.
- Silver metadata is only cleared after a successful database write.
- dbt keeps accepted and rejected records explicit through separate models.
- The main exported mart normalizes GBP amounts to EUR using a single fixed 2024 ECB annual-average rate.
- A companion mart preserves monthly totals split by source currency for auditability.

Operational scope:

- The ETL defaults to `replace` mode for database writes.
- PostgreSQL `upsert` behaviour is available, but only when `PIPELINE_DATABASE_WRITE_MODE=upsert` and `PIPELINE_MERGE_KEYS` are configured.
- Concurrent writers are not coordinated by the codebase. Run this pipeline sequentially.

## Event Time Policy

The sample files in this repository have names such as `premium_transactions_data_20250306_copy.json`, but the payload timestamps are in 2024. The implementation intentionally treats payload `created_at` values as the source of truth and only falls back to filename parsing when payload dates are unavailable.

That behaviour is implemented in the ingestion utilities and covered by tests, so monthly grouping follows transaction event time rather than filename conventions.

## Repository Layout

```text
premium_pipeline_project_updated/
├── analytics_premium/              # dbt project
│   ├── models/
│   └── infra/docker/
├── airflow/                        # Airflow DAG, local Compose stack, shared volumes
│   ├── dags/
│   ├── data/
│   ├── output/
│   └── utils/
├── data/                           # Local CLI input files
├── output/                         # Local CLI outputs and metadata
├── infra/docker/                   # ETL container image
├── src/
│   ├── export/
│   └── pipeline/
├── tests/
├── pyproject.toml
└── README.md
```

Key files:

- `src/pipeline/bronze/service.py`: source file discovery, deduplication, metadata, Parquet writes
- `src/pipeline/silver/service.py`: bronze-to-database loading and silver metadata handling
- `src/pipeline/adapters/postgres.py`: PostgreSQL `ON CONFLICT` upsert support
- `src/pipeline/orchestration.py`: end-to-end ETL orchestration
- `src/export/monthly_partner_premium.py`: CSV export
- `airflow/dags/dags_air.py`: Airflow DAG used for the containerized flow
- `analytics_premium/models/`: dbt staging, intermediate, and mart models

## Data Model

Relevant source fields:

- `transaction_id`
- `created_at`
- `amount`
- `currency`
- `charged_partner`
- `status`

dbt layers:

| Layer | Relation(s) | Purpose |
| --- | --- | --- |
| Staging | `stg_premium__transactions` | Source-aligned cleanup and surrogate key generation |
| Intermediate | `premium_transaction_quality`, `premium_transactions`, `premium_rejected_transactions` | Quality flags and accepted/rejected split |
| Marts | `dim_partner`, `dim_date`, `fct_transaction`, `monthly_partner_premiums`, `monthly_partner_premiums_by_currency` | Reporting models, EUR-normalized export mart, and by-currency audit mart |

Quality policy:

- null `transaction_id`
- duplicate `transaction_id`
- duplicate surrogate key
- missing partner
- missing timestamp
- non-positive amounts (`<= 0`)

## Running The Pipeline

### Option 1: Airflow Stack

This is the closest thing to the end-to-end intended demo flow.

1. Review `airflow/.env` before running the stack.
2. Put input files in `airflow/data/`.
3. Start Airflow:

```bash
docker compose -f airflow/docker-compose.yaml up airflow-init
docker compose -f airflow/docker-compose.yaml up -d
```

4. Open Airflow at `http://localhost:8081`.
5. Trigger the `premium_pipeline` DAG.
6. Read the final CSV from `airflow/output/gold/monthly_partner_premium_summary.csv`.

Notes:

- The checked-in Airflow config currently points shared mounts at the `airflow/` subdirectory, so Airflow uses `airflow/data/` and `airflow/output/` by default.
- If you want Airflow to use the repository root `data/` and `output/` folders instead, update `AIRFLOW_HOST_ROOT_DIR` before starting Compose.
- The DAG runs three containerized steps: ETL, `dbt build`, and CSV export. It can also send an optional status email.

### Option 2: Local CLI + dbt Container

Use this path if you want to run the ETL directly from the repo instead of through Airflow.

1. Install project dependencies:

```bash
uv sync --frozen --all-groups
```

2. Make sure PostgreSQL is running and configure the ETL:

```bash
export DATABASE_CONNECTION_URI="postgresql+psycopg://postgres:postgres@localhost:5432/mydb"
export PIPELINE_DATABASE_WRITE_MODE="upsert"
export PIPELINE_MERGE_KEYS="transaction_id"
```

3. Put input files in `data/` and run the ETL:

```bash
uv run premium-pipeline
```

4. Materialize the dbt models:

```bash
docker compose -f analytics_premium/infra/docker/docker-compose.yml run --rm dbt build
```

5. Export the gold report:

```bash
uv run premium-export-monthly-partner-premium
```

The local CLI writes to:

- bronze metadata and Parquet under `output/bronze/`
- silver metadata under `output/silver/`
- final CSV under `output/gold/monthly_partner_premium_summary.csv`

## CLI Commands

The package exposes three entrypoints:

| Command | Purpose |
| --- | --- |
| `premium-pipeline` | Run the ETL pipeline |
| `premium-export-monthly-partner-premium` | Export the gold relation to CSV |
| `premium-container` | Container-oriented entrypoint used by Airflow |

`premium-container` subcommands:

- `full-etl-pipeline`
- `gold-export`
- `send-run-email`

## Configuration

### ETL Environment Variables

| Variable | Default | Notes |
| --- | --- | --- |
| `DATABASE_CONNECTION_URI` / `DATABASE_URL` | none | Full database URI |
| `DATABASE_TYPE`, `DATABASE_HOST`, `DATABASE_PORT`, `DATABASE_NAME`, `DATABASE_USER`, `DATABASE_PASSWORD` | none | Alternative component-based Postgres config |
| `PIPELINE_DATA_FOLDER_PATH` | `data` | Local CLI input directory |
| `PIPELINE_BRONZE_OUTPUT_PATH` | `output/bronze` | Bronze Parquet + metadata path |
| `PIPELINE_SILVER_METADATA_PATH` | `output/silver` | Silver metadata path |
| `PIPELINE_TABLE_NAME` | `premium_transaction` | Trusted transaction table name |
| `PIPELINE_DATABASE_WRITE_MODE` | `replace` | `replace`, `append`, or `upsert` |
| `PIPELINE_MERGE_KEYS` | unset | Required for Postgres `upsert` |
| `PIPELINE_BATCH_SIZE` | `100000` | Database write batch size |
| `PIPELINE_GOLD_MONTHLY_PARTNER_PREMIUM_RELATION` | `analytics.monthly_partner_premiums` | Export source relation override |

### dbt Environment Variables

| Variable | Default | Notes |
| --- | --- | --- |
| `DBT_SOURCE_SCHEMA` | `public` | Schema for the trusted transaction table |
| `DBT_SOURCE_IDENTIFIER` | `premium_transaction` | Physical source table name |
| `DBT_STAGING_SCHEMA` | `staging` | dbt staging schema |
| `DBT_INTERMEDIATE_SCHEMA` | `intermediate` | dbt intermediate schema |
| `DBT_MARTS_SCHEMA` | `analytics` | dbt marts schema and default export schema |

## Testing And Quality Checks

The repository CI runs:

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy src tests
uv run pytest
uv build
```

## Explicit Assumptions

FX assumption used in this case study:

- GBP amounts are normalized with the 2024 ECB annual average series `EXR.A.GBP.EUR.SP00.A` = `0.8466166015625` GBP per EUR.
- Equivalent practical conversion in the mart: `1 GBP ≈ 1.1811722073 EUR`.
- Because all business timestamps in the provided sample fall in 2024, the repository uses one documented 2024 reference rate instead of introducing a separate historical FX ingestion workflow.

Amount-quality assumption used in this case study:

- Non-positive `processed` amounts are modeled as reconciliation exceptions rather than earned premium.
- This is a deliberate reporting assumption for the assessment, not a claim that insurance premiums can never be zero in all commercial contexts.
- Public Getsafe pricing shows that positive low-value premiums are plausible, so the pipeline does not reject low positive amounts by threshold alone.
- The sample data supports this cutoff: there are no zero-value rows, while the only non-positive rows are three negative `processed` transactions.

## Known Limitations

- No explicit concurrency control exists for bronze metadata or database writes.
- No dedicated backfill CLI is implemented.
- Late-arriving records are handled on the next run; there is no separate late-data workflow.
- The export step assumes the dbt gold relation already exists.
- Bronze timestamp enrichment expects the sample-style `created_at` format used by this dataset.

## Development Notes

- The ETL can write to generic SQLAlchemy-supported databases in `replace` or `append` mode.
- `upsert` mode is Postgres-only.
- The release workflow builds the Python package and publishes three container images: Airflow, ETL, and dbt.

## Summary

This codebase is strongest as a small, auditable, single-node batch pipeline. It prioritizes rerunnable ingestion, explicit data quality handling, and a clean separation between operational ETL and analytical modeling without pretending to be a distributed platform.
