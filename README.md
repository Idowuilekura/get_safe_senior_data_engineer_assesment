# Getsafe Data Engineer Case Study

Production-oriented solution for the Getsafe Senior Data Engineer assessment.

This repository implements a batch data pipeline for insurance premium transactions. It ingests raw JSON files, builds bronze and silver data products, models a gold analytics layer with dbt, and exports the finance-facing monthly premium reconciliation report required by the case study.

## Table of Contents

- [Overview](#overview)
- [Design Principles and Tradeoffs](#design-principles-and-tradeoffs)
- [Business Context](#business-context)
- [Problem Statement](#problem-statement)
- [Input Data](#input-data)
- [File Naming and Timestamp Rules](#file-naming-and-timestamp-rules)
- [Expected Output](#expected-output)
- [Architecture](#architecture)
- [System Guarantees](#system-guarantees)
- [Technology Choices](#technology-choices)
- [Repository Structure](#repository-structure)
- [Pipeline Stages](#pipeline-stages)
- [Failure Modes and Recovery](#failure-modes-and-recovery)
- [Observability](#observability)
- [Local Setup](#local-setup)
- [How to Run](#how-to-run)
- [Container Images](#container-images)
- [dbt Analytics Layer](#dbt-analytics-layer)
- [Quality and CI](#quality-and-ci)
- [Assumptions](#assumptions)
- [Future Improvements](#future-improvements)

## Overview

Getsafe is an insurance company with a digital operating model. In that environment, finance and accounting teams need trustworthy, reproducible reporting on premium transactions charged on behalf of insurance partners.

This solution implements a focused insurance data workflow:

- ingest raw premium transaction files
- preserve lineage and replayability
- standardize and validate records
- model accepted and rejected transaction paths
- publish a monthly partner premium aggregate for finance reconciliation

The final report is exported as:

```text
output/gold/fct_monthly_partner_premium.csv
```

## Design Principles and Tradeoffs

This solution is designed around a small set of principles:

- idempotent reruns over one-time success
- event-time correctness over filename conventions
- separation of ingestion and reporting concerns
- auditability over silent data loss
- simplicity at the current scale over premature distributed complexity

These principles show up directly in the implementation: bronze metadata drives rerun behavior, `created_at` is the primary time source, ETL and dbt responsibilities are separated, and accepted and rejected records are modeled explicitly.

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

Filenames are treated as non-authoritative. Event time is derived from the payload through `created_at`. Filename parsing is used only as a fallback when payload timestamps are unavailable.

The ingestion layer discovers files using the configured filename pattern:

- contains `premium`
- contains `transaction`
- ends in `.json`

If a fallback date is needed, the supported filename patterns are:

- `YYYYMMDD`
- `YYYY_MM_DD`

This avoids coupling reporting logic to inconsistent naming conventions while still allowing pragmatic recovery when source files do not contain usable timestamps.

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
   Handles raw file ingestion, bronze parquet generation, metadata tracking, and silver writes.
2. dbt analytics project
   Builds bronze, silver, and gold models on top of the transactional dataset, including the final monthly partner premium aggregate.

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

This design separates ingestion and storage concerns from analytics modeling concerns. The ETL layer owns file handling and durable writes; the dbt layer owns reporting semantics.

## System Guarantees

Within the scope of the current design, the system guarantees:

- reruns do not re-ingest duplicate source contents into bronze
- corrected source files trigger reprocessing of impacted bronze partitions
- payload event time takes precedence over filename-derived dates
- accepted and rejected transaction paths remain explicit in the analytics layer
- when the configured target is PostgreSQL, keyed upserts provide idempotent local merge behavior as long as stable merge keys are supplied

These guarantees are local-batch guarantees. They do not imply distributed exactly-once semantics or support for concurrent writers.

## Technology Choices

### Polars instead of Spark

The assessment allows any data processing framework. I chose `Polars` because:

- the dataset is small enough to process efficiently on a single machine
- local development is faster and simpler
- infrastructure and operational costs stay low
- the implementation remains expressive and testable

Spark would be a stronger fit only once data volume, orchestration complexity, or concurrency meaningfully outgrows the current workload.

### SQLAlchemy-based write layer

The ETL code writes through a common SQLAlchemy-based database interface. That keeps pipeline orchestration and transformation logic independent of the target engine, while allowing adapter-specific behavior where required.

Generic SQL adapters support standard replace and append writes across engines.

PostgreSQL is the concrete database used in the local implementation because it is open source, straightforward to run in Docker, integrates cleanly with Airflow, dbt, and SQLAlchemy, and supports `ON CONFLICT` upserts for rerunnable transactional loads.

Other databases were not chosen for the assessment because the workload does not require warehouse-scale compute or vendor-specific features, and adding a second operational dependency would increase setup complexity without materially improving the current design.

The PostgreSQL adapter extends the generic write layer with keyed upsert behavior, so corrected silver records can be merged deterministically instead of only appended or fully replaced.

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
- persists metadata needed for deterministic reprocessing

### Silver

The silver stage builds the trusted transactional layer and writes it into the configured database.

Current behavior:

- standardizes the transaction data into a reusable operational dataset
- writes through the database adapter layer
- can use PostgreSQL keyed upserts for rerunnable merges
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

Design choices:

- Bronze remains source-faithful rather than applying business-quality filtering too early
- Silver owns the accepted versus rejected split
- rejected records are kept in `silver_transaction_rejected` to preserve an auditable path without introducing a separate quarantine subsystem for this scope
- Gold is reporting-oriented, with a transaction fact plus the monthly finance reconciliation aggregate

### Metadata and Traceability

The solution includes metadata to improve auditability and rerun safety.

Examples:

- bronze metadata tracks processed source files and covered partitions
- silver metadata captures run status and rows written
- container execution can persist a JSON run summary through `PIPELINE_RUN_RESULT_PATH`

## Failure Modes and Recovery

The system is designed to fail in a recoverable way:

- no new input files: bronze and silver work is skipped without mutating downstream state
- invalid records: records are classified into rejected models rather than dropped silently
- corrected source files: impacted bronze partitions are rebuilt on the next run
- downstream failures: upstream bronze and silver artifacts remain reusable for reruns
- schema drift beyond the persisted schema assumptions: the run may fail and requires operator intervention

Recovery is operationally simple: fix the underlying issue, then rerun the pipeline. The metadata model is designed to make that rerun deterministic.

## Observability

The current implementation emphasizes traceability rather than a full production observability stack.

- bronze metadata tracks processed files, schema snapshots, and partition state
- silver metadata records run status and rows written
- container execution can persist a JSON run summary
- Airflow task logs provide orchestration visibility for the containerized path
- dbt artifacts and logs provide model-level execution detail

A production extension would add freshness metrics, anomaly detection, explicit alerting, and ownership for operational failures.

## Local Setup

### Prerequisites

- Docker
- Docker Compose

The supported local setup is containerized. Airflow, Postgres, and Redis run in the local Compose stack, while the ETL and dbt steps run as containerized tasks from the Airflow DAG.

The Compose stack is already configured to use `airflow_stuff/` as the mounted workspace root through `AIRFLOW_HOST_ROOT_DIR`, so the default local path does not require additional host-side configuration.

## How to Run

Airflow is the supported orchestration path for this project.

Clone the repository and move into the project directory:

```bash
git clone https://github.com/Idowuilekura/get_safe_senior_data_engineer_assesment.git
cd get_safe_senior_data_engineer_assesment
```

Start the local stack:

```bash
docker compose -f airflow_stuff/docker-compose.yaml up airflow-init
docker compose -f airflow_stuff/docker-compose.yaml up -d
```

Then:

1. Open Airflow at `http://localhost:8080`
2. Enable the DAG `premium_pipeline`
3. Trigger a run

Expected CSV output:

```text
airflow_stuff/output/gold/fct_monthly_partner_premium.csv
```

Input files for the containerized path should be placed in:

```text
airflow_stuff/data/
```

## Container Images

The workflow uses three images:

- `idowuilekura/premium-pipeline-airflow:3.2.0` for the Airflow services
- `idowuilekura/premium-pipeline:latest` for the ETL and export tasks
- `idowuilekura/analytics-premium-dbt:latest` for the dbt task

The custom Airflow image is defined in `airflow_stuff/Dockerfile` and built on top of `apache/airflow:3.2.0`.

It packages the dependencies required for Airflow to launch container-based tasks from the DAG:

- `apache-airflow-providers-docker`
- the Python `docker` client package

The ETL and dbt task images are referenced directly in `airflow_stuff/dags/dags_air.py` and are pulled by Docker when those tasks execute.

If the local Docker cache is stale, you can refresh the Airflow service image with:

```bash
docker compose -f airflow_stuff/docker-compose.yaml pull
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

This structure keeps the project compact while preserving clear model responsibilities:

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

This modeling layer keeps data quality handling explicit by separating accepted and rejected transactions, which is useful in finance workflows where reconciliations need an audit trail.

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
- extend cloud deployment guidance for object storage, managed compute, and secrets handling
- harden schema evolution handling and downstream notification flows
