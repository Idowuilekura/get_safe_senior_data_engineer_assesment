# Premium Reconciliation Pipeline

Local batch pipeline for auditable monthly premium reconciliation.

The implementation is intentionally narrow in scope: local, sequential, and optimized for reruns, traceability, and clear failure boundaries rather than distributed scale. It ingests raw JSON premium transactions, persists a rerunnable bronze layer in Parquet, loads a trusted transaction table into PostgreSQL, models accepted and rejected records in dbt, and exports a monthly partner premium report to CSV.

## Design Intent

This system is designed for a single-node monthly reconciliation workflow where the main priorities are:

- deterministic reruns
- explicit accepted and rejected record handling
- auditable transformation boundaries
- low operational complexity for local execution

It is not optimized for:

- streaming latency
- concurrent writers
- distributed compute
- enterprise-grade orchestration controls

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

## Design Trade-Offs

- Polars is used for ingestion because the dataset fits comfortably in memory and the local development model benefits more from low setup overhead than from distributed execution.
- Bronze data is persisted in Parquet rather than read repeatedly from raw JSON so reruns benefit from columnar reads, stable schema handling, and inexpensive partition rebuilds.
- PostgreSQL is used as the trusted relational layer because it provides a simple audited handoff into dbt without introducing warehouse-specific complexity into the ingestion code.
- Accepted and rejected paths are modeled explicitly in dbt rather than filtered silently in ETL so reporting remains auditable and data-quality decisions stay inspectable in SQL.
- FX conversion uses one documented 2024 reference rate rather than a daily historical lookup because all sample business timestamps are in 2024 and the additional ingestion path would add more operational surface area than analytical value for this exercise.

## Architecture

![Premium pipeline architecture](https://github.com/user-attachments/assets/27c7eadc-7829-4848-be9d-61082eda0dd4)

Pipeline stages:

1. Bronze ingestion discovers matching JSON files, deduplicates already-seen content, parses event timestamps, and writes Parquet partitions plus metadata.
2. Silver loading reads pending bronze data, enriches time features, and writes the trusted transaction table.
3. dbt models classify accepted and rejected rows, then build dimensional and reporting models.
4. Export reads the EUR-normalized `analytics.monthly_partner_premiums` mart and writes `monthly_partner_premium_summary.csv`.

## System Guarantees

- Payload event time is authoritative. File discovery and partition impact detection prefer `created_at`; filename date parsing is only a fallback.
- Bronze ingestion is metadata-driven. Seen file content is tracked with content digests, file size, and modified time.
- Exact duplicate contents are not re-ingested, even if they arrive under a new filename.
- If a previously ingested file changes at the same path, impacted year/month Parquet partitions are rebuilt.
- Silver metadata is only cleared after a successful database write.
- dbt keeps accepted and rejected records explicit through separate models.
- The main exported mart normalizes GBP amounts to EUR using a single fixed 2024 ECB annual-average rate.
- A companion mart preserves monthly totals split by source currency for auditability.

Execution scope:

- The ETL defaults to `replace` mode for database writes. In this implementation, that is intentional: when new bronze data exists, the trusted transaction table is rebuilt from the current bronze state before dbt runs. At this scale, deterministic rebuilds are simpler to reason about than incremental silver maintenance and reduce partial-staleness risk in the trusted base layer.
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

This is the primary end-to-end execution path for the repository.

Airflow task graph:

![Airflow DAG graph view](https://github.com/user-attachments/assets/afa001c2-734d-4639-88fd-09d443d04e8e)

1. Review `airflow/.env` before running the stack.
2. Put input files in `airflow/data/`.
3. Start Airflow:

```bash
docker compose -f airflow/docker-compose.yaml up airflow-init
docker compose -f airflow/docker-compose.yaml up -d
```

4. Open Airflow at `http://localhost:8080`.
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
- `plan-backfill`

`plan-backfill` is a read-only month-level planner. It scans available bronze partitions on disk, compares them to a requested month range, and reports:

- requested months
- months available in bronze
- months selected for backfill
- months not available in bronze
- months loaded by the latest successful silver run

Example:

```bash
uv run premium-container plan-backfill --from 2024-01 --to 2025-12
```

The planner intentionally uses bronze partitions as the source of truth for availability and does not infer month availability from source filenames.

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

## Troubleshooting

| Symptom | Likely Cause | What To Do |
| --- | --- | --- |
| `airflow-apiserver` is unhealthy | The running container is still using an outdated healthcheck or the stack was not recreated after Compose changes | Restart the affected Airflow services or recreate the stack with `docker compose -f airflow/docker-compose.yaml down` followed by `docker compose -f airflow/docker-compose.yaml up -d` |
| dbt fails with `Credentials in profile ... invalid` | The dbt container is running with stale or incomplete connection environment variables | Restart `airflow-scheduler`, `airflow-dag-processor`, `airflow-worker`, and `airflow-apiserver` so the latest DAG configuration is loaded |
| The DAG runs but no rows are newly ingested | Bronze ingestion deduplicates source files by content digest, so identical payloads are treated as already processed | Add a genuinely new file or intentionally correct an existing file and rerun the pipeline |
| Status email is not sent | SMTP settings are incomplete or the mail provider rejected authentication | Verify the `PIPELINE_EMAIL_*` settings in `airflow/.env`; for Gmail, use an app password rather than a primary account password |
| Airflow or Postgres does not start on the expected host port | Another local service is already bound to `8080` or `5432` | Stop the conflicting service or change the host-side port mapping in `airflow/docker-compose.yaml` |

## Operational Expectations

- Intended for scheduled batch execution with operator review of Airflow task state and email notifications.
- The repository includes basic run-status email alerts for success and failure outcomes.
- It does not implement formal SLA/SLO tracking, paging escalation, or on-call support.
- Recovery is based on deterministic reruns after the underlying issue is corrected.

## Business Assumptions

FX assumption:

- GBP amounts are normalized with the 2024 ECB annual average series `EXR.A.GBP.EUR.SP00.A` = `0.8466166015625` GBP per EUR.
- Equivalent practical conversion in the mart: `1 GBP ≈ 1.1811722073 EUR`.
- Because all business timestamps in the provided sample fall in 2024, the repository uses one documented 2024 reference rate instead of introducing a separate historical FX ingestion workflow.

Amount-quality assumption:

- Non-positive `processed` amounts are modeled as reconciliation exceptions rather than earned premium.
- This is a deliberate reporting assumption for this repository, not a claim that insurance premiums can never be zero in all commercial contexts.
- Public Getsafe pricing shows that positive low-value premiums are plausible, so the pipeline does not reject low positive amounts by threshold alone.
- The sample data supports this cutoff: there are no zero-value rows, while the only non-positive rows are three negative `processed` transactions.

## Operational Limitations

- No explicit concurrency control exists for bronze metadata or database writes.
- Backfill planning is implemented at month grain through bronze partition inspection, but there is not yet a mutating backfill execution command.
- Late-arriving records are handled on the next run; there is no separate late-data workflow.
- The export step assumes the dbt gold relation already exists.
- Bronze timestamp enrichment expects the sample-style `created_at` format used by this dataset.

## If This Moved To Production

The first changes I would make are:

- move bronze and silver run metadata from filesystem state into a transactional metadata store
- replace the fixed FX assumption with provider-tracked historical reference data joined by business date
- add orchestration safeguards around concurrency, alerting, and failure recovery rather than relying on sequential local execution discipline

## Development Notes

- The ETL can write to generic SQLAlchemy-supported databases in `replace` or `append` mode.
- `upsert` mode is Postgres-only.
- The release workflow builds the Python package and publishes three container images: Airflow, ETL, and dbt.

## Summary

This codebase is intentionally designed as a small, auditable, single-node batch system. The strongest parts of the design are rerunnable ingestion, explicit accepted and rejected data paths, and clear separation between operational ETL and analytical modeling. It does not claim distributed-scale guarantees, cross-run concurrency control, or a fully generalized reconciliation platform.
