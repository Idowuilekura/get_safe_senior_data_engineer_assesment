# Getsafe Data Engineer Case Study

Production-oriented solution for the Getsafe Senior Data Engineer assessment.

This repository implements a batch data pipeline for insurance premium transactions. It ingests raw JSON files, builds bronze and silver data products, models a gold analytics layer with dbt, and exports the finance-facing monthly premium reconciliation report required by the case study.

## Table of Contents

- [Overview](#overview)
- [Business Context](#business-context)
- [Problem Statement](#problem-statement)
- [Input Data](#input-data)
- [File Naming and Timestamp Rules](#file-naming-and-timestamp-rules)
- [Expected Output](#expected-output)
- [Architecture](#architecture)
- [Technology Choices](#technology-choices)
- [Repository Structure](#repository-structure)
- [Pipeline Stages](#pipeline-stages)
- [Local Setup](#local-setup)
- [How to Run](#how-to-run)
- [Docker Usage](#docker-usage)
- [dbt Analytics Layer](#dbt-analytics-layer)
- [Quality and CI](#quality-and-ci)
- [Assumptions](#assumptions)
- [Future Improvements](#future-improvements)

## Overview

Getsafe is an insurance company with a digital operating model. In that environment, finance and accounting teams need trustworthy, reproducible reporting on premium transactions charged on behalf of insurance partners.

This solution focuses on a practical insurance data workflow:

- ingest raw premium transaction files
- preserve lineage and replayability
- standardize and validate records
- model accepted and rejected transaction paths
- publish a monthly partner premium aggregate for finance reconciliation

The final report is exported as:

```text
output/gold/fct_monthly_partner_premium.csv
```

## Business Context

The business use case is monthly closing and reconciliation.

Finance wants to verify invoice-based premium totals against raw transactional records. In insurance, this requires more than a simple aggregation job: the pipeline should be auditable, handle duplicate or malformed records safely, and make it clear which records were accepted for reporting versus which require operational follow-up.

That is why this repository uses a layered design:

- Bronze preserves raw transaction history with technical metadata
- Silver produces a trusted transaction layer for downstream use
- Gold exposes business-ready reporting models, including monthly premium totals by partner

## Problem Statement

The input dataset contains premium transactions charged on behalf of insurance partners. The required batch job must calculate total successfully charged premium per month per partner.

Required output shape:

```text
partner, month, total_premium
liadigital, 2022-06-01, 104.32
...
```

Where:

- `partner` is the charged insurance partner
- `month` is the first day of the calendar month in `YYYY-MM-DD` format
- `total_premium` is the total successful premium charged for that partner in that month

## Input Data

The main source file is `premium_transactions_data_20250306.json`.

Relevant fields:

- `transaction_id`: unique transaction identifier
- `created_at`: timestamp of the premium charge
- `amount`: charged premium amount
- `currency`: transaction currency
- `charged_partner`: insurance partner for whom the premium was charged
- `status`: transaction outcome or processing status

This repository also contains additional sample JSON files in `data/` to exercise incremental load behavior, duplicate handling, and rerun safety.

## File Naming and Timestamp Rules

File naming is intentionally treated as a weak operational hint, not as a reliable business contract.

### Raw file naming

By default, the Python ingestion layer looks for files that:

- contain `premium` in the filename
- contain `transaction` in the filename
- end in `.json`

Those defaults come from the runtime configuration:

- `PIPELINE_INSUR_TYPE=premium`
- `PIPELINE_DATASET_TYPE=transaction`
- `PIPELINE_EXT_TYPE=.json`

Example matching filenames:

- `premium_transactions_data_20250306.json`
- `premium_transaction_2025_03_06.json`
- `partner_premium_transaction_dump_20250306.json`

The pipeline can attempt to extract a file date from either of these filename patterns:

- `YYYYMMDD`
- `YYYY_MM_DD`

However, filename dates are not treated as the authoritative event date. During the exercise, it became clear that filename usage was not fully consistent, which makes it risky to treat the filename itself as a trusted business field. In practice, filenames may be inconsistent, copied, renamed, or otherwise unsuitable as a stable semantic contract.

For that reason:

- `created_at` inside the JSON payload is the primary source of truth for event time and partitioning
- filename date parsing is only a fallback when payload timestamps are unavailable
- file discovery uses broad substring matching for ingestion, but reporting logic should not depend on filename structure beyond that
- filename dates are treated as an operational hint only, for example when a producer appears to intend a date in the file name but the naming itself is not reliable enough to stand alone

Operational assumption:

- when a filename contains a parseable date, it is treated as a best-effort hint rather than guaranteed truth
- that hint is used only when the payload does not provide usable timestamps
- this assumption exists because the filename appears intended to communicate timing, even though the observed naming pattern is not consistent enough to rely on by itself

### Dataset and model naming

The solution follows a fairly standard analytics naming style:

- raw operational dataset loaded by the ETL: `premium_transaction`
- exported gold reporting relation: `fct_monthly_partner_premium`
- exported CSV file: `output/gold/fct_monthly_partner_premium.csv`

Within the dbt layer:

- `bronze_` models preserve source-facing shape
- `silver_` models represent trusted and quality-classified transaction data
- `fct_` models represent reporting facts
- `dim_` models represent reporting dimensions

This makes it easier to understand which tables are operational landing artifacts versus finance-ready insurance reporting outputs.

## Expected Output

The required deliverables from the case study are covered here as follows:

- production-ready batch job: implemented in `src/pipeline/`
- CSV written to `output/`: exported by `src/export_dlt/db_file_export.py`
- containerized runtime: implemented with Docker
- meaningful tests: implemented in `tests/`
- notes on choices and assumptions: documented in this README

## Architecture

The implementation is split into two main layers:

1. Python ETL pipeline
   Handles raw file ingestion, bronze parquet generation, metadata tracking, silver writes, and operational orchestration.
2. dbt analytics project
   Builds bronze, silver, and gold analytics models on top of the loaded transactional data, including the final monthly partner premium aggregate.

High-level flow:

```text
Raw JSON files
    -> Python bronze ingestion
    -> bronze parquet + metadata
    -> Python silver load to database
    -> dbt bronze/silver/gold models
    -> fct_monthly_partner_premium
    -> CSV export to output/gold/
```

This design separates operational ingestion concerns from analytics modeling concerns, which is a common industry pattern in modern insurance and fintech data platforms.

## Technology Choices

### Polars instead of Spark

The assessment allows any data processing framework. I chose `Polars` because:

- the dataset is small enough to process efficiently on a single machine
- local development is faster and simpler
- infrastructure and operational costs stay low
- the implementation remains expressive and testable

Spark would be a stronger fit only once data volume, orchestration complexity, or concurrency meaningfully outgrows the current workload.

### SQLAlchemy-based write layer

The ETL application layer stays database-agnostic, while still supporting Postgres-specific capabilities where needed.

- generic SQL writes through SQLAlchemy-backed adapters
- Postgres-specific upsert behavior for stronger merge semantics

### dbt for the analytics layer

dbt is used for the reporting layer because it provides:

- clear model lineage
- reusable SQL transformations
- auditable accepted/rejected modeling patterns
- a natural path to production analytics workflows

## Repository Structure

```text
premium_pipeline_project_updated/
├── analytics_premium/
│   ├── infra/
│   │   └── docker/
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   └── README.md
├── data/
│   └── premium_transactions_data_*.json
├── infra/
│   └── docker/
│       └── Dockerfile
├── output/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── src/
│   ├── export_dlt/
│   │   └── db_file_export.py
│   └── pipeline/
│       ├── adapters/
│       ├── bronze/
│       ├── ports/
│       ├── silver/
│       ├── utils/
│       ├── config.py
│       ├── container_cli.py
│       ├── main.py
│       ├── orchestration.py
│       └── settings.py
├── tests/
├── .github/workflows/
├── pyproject.toml
├── uv.lock
└── README.md
```

Key components:

- `src/pipeline/bronze/service.py`: file discovery, deduplication, partition-aware bronze writes
- `src/pipeline/silver/service.py`: silver transformation and load logic
- `src/pipeline/adapters/`: generic SQL and Postgres-specific write implementations
- `src/pipeline/orchestration.py`: end-to-end ETL orchestration
- `src/export_dlt/db_file_export.py`: export of monthly premium report to CSV
- `analytics_premium/models/bronze/bronze_transaction.sql`: source-faithful analytics bronze model
- `analytics_premium/models/silver/`: accepted, rejected, and quality-classified transaction models
- `analytics_premium/models/gold/`: dimensional reporting models, including `fct_monthly_partner_premium`
- `analytics_premium/infra/docker/`: dbt container build and runtime assets

## Pipeline Stages

### Bronze

The bronze stage reads one or more raw JSON files from `data/` and writes parquet to `output/bronze/`.

Current behavior:

- processes multiple landed files, not just one file
- tracks source file metadata for incremental reruns
- uses payload `created_at` as the primary partitioning truth
- falls back to filename date parsing when needed
- refreshes only impacted partitions
- keeps metadata needed for deterministic reprocessing

### Silver

The silver stage builds the trusted transactional layer and writes it into the configured database.

Current behavior:

- standardizes the transaction data into a reusable operational dataset
- supports database-agnostic writes
- supports Postgres upserts when requested
- writes metadata even when a stage is skipped, so downstream tasks can behave deterministically

### Gold

The gold stage is modeled in dbt under `analytics_premium/`.

Current behavior:

- models accepted and rejected transaction paths explicitly
- builds dimensional and fact-style reporting models
- publishes `fct_monthly_partner_premium` for finance reporting
- supports CSV export for the case-study deliverable

Current dbt model layout:

- Bronze:
  `bronze_transaction`
- Silver:
  `silver_transaction_quality`
  `silver_transaction`
  `silver_transaction_rejected`
- Gold:
  `dim_partner`
  `dim_date`
  `fct_transaction`
  `fct_monthly_partner_premium`

Design choices in the dbt layer:

- Bronze remains source-faithful rather than applying business-quality filtering too early
- Silver owns the accepted versus rejected split
- rejected records are kept in `silver_transaction_rejected` to preserve an auditable path without overbuilding a separate quarantine layer for the take-home
- Gold is dimensional and reporting-friendly, with a transaction fact plus the monthly finance reconciliation aggregate

### Metadata and Traceability

The solution includes metadata to improve auditability and rerun safety.

Examples:

- bronze metadata tracks processed source files and covered partitions
- silver metadata captures run status and rows written
- container execution can persist a JSON run summary through `PIPELINE_RUN_RESULT_PATH`

## Local Setup

### Prerequisites

- Python 3.11+
- `uv`
- Docker
- a target database such as Postgres or SQLite

### Install dependencies

```bash
uv sync --all-groups
```

### Configure database access

Example component-style configuration:

```bash
export DATABASE_TYPE="postgres"
export DATABASE_HOST="localhost"
export DATABASE_PORT="5432"
export DATABASE_NAME="mydb"
export DATABASE_USER="postgres"
export DATABASE_PASSWORD="postgres"
```

Or a full URI:

```bash
export DATABASE_CONNECTION_URI="postgresql+psycopg://postgres:postgres@localhost:5432/mydb"
```

Optional runtime settings:

```bash
export PIPELINE_DATA_FOLDER_PATH="data"
export PIPELINE_BRONZE_OUTPUT_PATH="output/bronze"
export PIPELINE_SILVER_METADATA_PATH="output/silver"
export PIPELINE_TABLE_NAME="premium_transaction"
export PIPELINE_DATABASE_WRITE_MODE="replace"
export PIPELINE_BATCH_SIZE="100000"
export PIPELINE_MERGE_KEYS="policy_id,transaction_id"
```

## How to Run

Run the ETL pipeline:

```bash
uv run premium-pipeline
```

Run the export:

```bash
uv run premium-export-monthly-partner-premium
```

Expected CSV output:

```text
output/gold/fct_monthly_partner_premium.csv
```

## Docker Usage

The Python runtime image is defined in `infra/docker/Dockerfile`.

Build:

```bash
docker build -f infra/docker/Dockerfile -t premium-pipeline .
```

Run full ETL:

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

Run export:

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

## dbt Analytics Layer

The dbt project in `analytics_premium/` builds the reporting layer on top of the loaded transaction data.

Modeling approach:

- `bronze_transaction` keeps the raw source shape while adding deterministic lineage keys
- `silver_transaction_quality` classifies quality issues including:
  - null `transaction_id`
  - duplicate `transaction_id`
  - duplicate `sur_key`
  - missing `charged_partner`
  - missing `created_at_timestamp`
- `silver_transaction` keeps only accepted rows
- `silver_transaction_rejected` keeps rejected rows plus rejection metadata
- `fct_transaction` preserves transaction grain for downstream reuse
- `fct_monthly_partner_premium` produces the finance-facing monthly premium rollup by partner from trusted silver data, filtered to `status = 'processed'`

This structure was chosen to keep the project compact but still senior in shape:

- source-faithful Bronze
- explicit quality handling in Silver
- auditable rejected path
- dimensional, reporting-ready Gold

Testing in the dbt layer includes:

- null and uniqueness checks
- relationship tests
- accepted-versus-rejected reconciliation checks
- no-overlap checks on accepted and rejected `sur_key`
- uniqueness of `(partner, month)` in `fct_monthly_partner_premium`

All dbt Docker assets live under:

```text
analytics_premium/infra/docker/
```

That directory contains:

- `Dockerfile`
- `docker-entrypoint.sh`
- `docker-compose.yml`
- `render_profiles.py`
- `build-image.sh`
- `.env.example`

Container design decisions:

- non-root runtime user
- minimal runtime image
- adapter-aware builds via `DBT_ADAPTER_PACKAGE`, `DBT_ADAPTER_VERSION`, and `DBT_TYPE`
- runtime dbt profile rendered from environment variables
- source database, schema, and table can be swapped without changing versioned YAML
- Compose is image-first and pull-oriented
- default container command is `dbt run`
- `DBT_DEFAULT_SELECT` can narrow the default run target

Run with Docker Compose:

```bash
cd analytics_premium
cp infra/docker/.env.example infra/docker/.env
docker compose -f infra/docker/docker-compose.yml run --rm dbt
```

Default target:

```bash
dbt run --select +fct_monthly_partner_premium
```

This modeling layer makes data quality handling explicit by separating accepted and rejected transactions, which is especially useful in insurance finance workflows where reconciliations must remain explainable.

Latest local dbt validation during development:

```bash
uv run dbt build --full-refresh
```

The reported result from the latest local validation was `43/43` green.

## Quality and CI

Local checks mirrored in CI:

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy src tests
uv run pytest
uv build
```

GitHub Actions runs the same quality gates from `.github/workflows/ci.yml`.

## Assumptions

- More JSON files may arrive over time, not just one file.
- Files must be discoverable by the configured ingestion pattern. With the current defaults, that means the filename must contain `premium`, contain `transaction`, and end in `.json`.
- Event-time handling relies primarily on parseable `created_at` values in the JSON payload. A parseable filename date is only a fallback when payload timestamps are unavailable.
- A date embedded in the filename is treated as a fallback operational hint, not as authoritative business data. This is a deliberate assumption because the naming pattern observed in the exercise was not consistent enough to be trusted on its own.
- Source files are assumed to be stable after landing, though the current implementation still detects changed files and refreshes affected bronze partitions when necessary.
- Some records may be incomplete or invalid from a business-quality perspective. The analytics layer already separates accepted and rejected transaction paths, but this is not yet a full quarantine-and-alerting workflow.
- Schema drift is not a primary target for this version. The pipeline persists prior schema metadata and reuses it on incremental reads, but it does not yet implement a full schema-evolution and notification workflow.
- Missing-day tracking is intentionally capped for the current calendar month. The pipeline reports gaps only up to today's day-of-month and does not mark future days in the current month as missing.
- Polars is preferred over Spark for the current scale to keep execution simpler and cheaper without sacrificing correctness.
- The business reporting grain is monthly total premium per insurance partner, using the first day of the month as the month key.

## Future Improvements

- add explicit quarantine storage and alerting for invalid insurance transaction rows
- add fuller end-to-end integration tests across ETL, dbt, and CSV export
- add scheduler/orchestrator support such as Airflow when operational cadence grows
- extend cloud deployment guidance for object storage, managed compute, and secrets handling
- harden schema evolution handling and downstream notification flows
