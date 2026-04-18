# Getsafe Data Engineer Case Study

Production-oriented solution for the Getsafe Senior Data Engineer assessment.

This repository implements a batch data pipeline for insurance premium transactions. It ingests raw JSON files, persists an operational bronze layer, writes a trusted transactional base table to PostgreSQL, builds analytical bronze, silver, and gold models with dbt, and exports the finance-facing monthly premium reconciliation report required by the case study.

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
- [Quality](#quality)
- [Delivery and Release Flow](#delivery-and-release-flow)
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

At the container interface, the ETL image exposes two operational commands:

- `full-etl-pipeline` for the end-to-end ingestion and silver load path
- `gold-export` for exporting the finance-facing gold aggregate to CSV

The final report is exported as:

```text
output/gold/fct_monthly_partner_premium.csv
```

In the local Airflow Compose setup, that same file is visible on the host at:

```text
airflow_stuff/output/gold/fct_monthly_partner_premium.csv
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

The business use case is monthly reconciliation of premium transactions processed on behalf of insurance partners. The platform handles payment execution, but the premium belongs to the underlying partner, so reporting must be auditable and traceable back to raw transactions.

That is why the pipeline uses a layered design: bronze preserves landed history and metadata, the Python ETL writes a trusted transactional base table to PostgreSQL, and dbt builds the analytical models that produce the final partner-level monthly report.

## Problem Statement

The input dataset contains premium transactions charged on behalf of insurance partners. The required batch job must calculate total successfully charged premium per month per partner.

Only successfully processed transactions should contribute to the reported totals, so failed or incomplete payments do not distort finance-facing outputs.

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

### Key Concepts

- `premium`: the amount paid by a customer for an insurance policy
- `partner`: the insurance provider that ultimately receives the premium
- `successful transaction`: a payment completed without failure or reversal
- `monthly aggregation`: grouping transactions by calendar month for reporting

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

Filenames are non-authoritative. Event time comes from `created_at`, and filename parsing is used only as a fallback.

The ingestion layer discovers files that:

- contain `premium`
- contain `transaction`
- end in `.json`

If a fallback date is needed, the supported filename patterns are `YYYYMMDD` and `YYYY_MM_DD`.

### Dataset and model naming

The naming convention is straightforward:

- raw operational dataset loaded by the ETL: `premium_transaction`
- exported gold reporting relation: `fct_monthly_partner_premium`
- exported CSV file: `output/gold/fct_monthly_partner_premium.csv`

Within the dbt layer:

- `bronze_` models preserve source-facing shape
- `silver_` models represent trusted and quality-classified transaction data
- `fct_` models represent reporting facts
- `dim_` models represent reporting dimensions

## Expected Output

The main deliverable is a CSV report with the shape:

```text
partner, month, total_premium
```

Export locations:

- runtime path inside the task container: `output/gold/fct_monthly_partner_premium.csv`
- host-visible path in the local Airflow setup: `airflow_stuff/output/gold/fct_monthly_partner_premium.csv`

The repository also includes the supporting ETL code, dbt models, Docker assets, tests, and delivery workflow needed to run and assess the solution end to end.

## Architecture

The repository has one execution flow and two modeling layers:

- Python ETL handles file discovery, operational bronze persistence, metadata tracking, and writes the trusted base table to PostgreSQL
- dbt handles the analytical bronze, silver, and gold models built on top of that base table

Terminology used in this repository:

- `landing`: raw JSON files in `data/`
- `operational bronze`: persisted parquet plus metadata written by the Python ETL
- `trusted base table`: the reusable transactional dataset written by the Python ETL to PostgreSQL
- `analytics bronze`, `analytics silver`, `analytics gold`: the dbt model layers used for reporting
- `gold export`: the final monthly CSV

End-to-end flow:

```text
Raw JSON files
    -> operational bronze parquet + metadata
    -> trusted base table in PostgreSQL
    -> dbt bronze/silver/gold models
    -> fct_monthly_partner_premium
    -> CSV export to output/gold/
```

The important boundary is that the Python layer owns ingestion and durable writes, while the dbt layer owns reporting semantics.

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

### Parquet in Bronze instead of staying on raw JSON

The bronze layer converts landed JSON into parquet rather than repeatedly reading JSON for downstream processing.

That choice is deliberate:

- parquet is materially faster for repeated analytical reads than raw JSON
- parquet preserves an explicit schema, which reduces ambiguity during downstream transformation
- typed columnar storage makes selective scans and projection cheaper
- metadata-driven partition refreshes are easier to reason about when the persisted bronze layer has a stable physical format
- it separates raw landing concerns from reusable analytical consumption concerns

In this repository, raw JSON is the landing and interchange format, while Parquet is the canonical persisted representation used by the bronze layer. That keeps the landed source intact while giving downstream processing a typed, columnar working format that is faster, more predictable, and easier to maintain.

### Database adapter layer

The ETL writes through a database adapter layer so pipeline logic stays mostly database-agnostic while adapters can still handle engine-specific behavior.

In the current implementation, the adapters are SQLAlchemy-backed. For this assessment, PostgreSQL is the concrete target because it is easy to run locally with Docker and supports `ON CONFLICT` upserts for rerunnable transactional loads. The PostgreSQL adapter extends the generic write layer with keyed upsert behavior, so corrected records can be merged deterministically instead of only appended or fully replaced.

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

Raw JSON files are persisted as operational bronze parquet plus metadata for rerunnable processing.

### Trusted Base Table

The Python ETL standardizes transactions and writes the reusable base table to PostgreSQL.

### Analytical Models

dbt builds the analytical bronze, silver, and gold models, culminating in `fct_monthly_partner_premium` and the final CSV export.

### Metadata and Traceability

The solution includes metadata to improve auditability and rerun safety.

Examples:

- bronze metadata tracks processed source files and covered partitions
- base-table load metadata captures run status and rows written
- container execution can persist a JSON run summary through `PIPELINE_RUN_RESULT_PATH`

## Failure Modes and Recovery

The system is designed to fail in a recoverable way:

- no new input files: bronze persistence and base-table loading are skipped without mutating downstream state
- invalid records: records are classified into rejected models rather than dropped silently
- corrected source files: impacted bronze partitions are rebuilt on the next run
- downstream failures: upstream bronze artifacts and the loaded base table remain reusable for reruns
- schema drift beyond the persisted schema assumptions: the run may fail and requires operator intervention

Recovery is operationally simple: fix the underlying issue, then rerun the pipeline. The metadata model is designed to make that rerun deterministic.

## Observability

The current implementation emphasizes traceability rather than a full production observability stack.

- bronze metadata tracks processed files, schema snapshots, and partition state
- base-table load metadata records run status and rows written
- container execution can persist a JSON run summary
- Airflow task logs provide orchestration visibility for the containerized path
- dbt artifacts and logs provide model-level execution detail

A production extension would add freshness metrics, anomaly detection, explicit alerting, and ownership for operational failures.

## Local Setup

### Prerequisites

- Docker
- Docker Compose

The supported local setup is fully containerized. You do not need to install Airflow, PostgreSQL, Redis, or dbt on the host machine.

When you start the local Compose stack, it brings up the core services used by the pipeline:

- Airflow for orchestration
- PostgreSQL for transactional storage
- Redis for Airflow task coordination

The ETL, dbt, and CSV export steps are then run by the Airflow DAG as containers. The stack is already configured to use `airflow_stuff/` as the mounted workspace root through `AIRFLOW_HOST_ROOT_DIR`, so the default project layout works without additional host-side setup.

## How to Run

Airflow is the supported local execution path.

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

This starts the local Airflow, PostgreSQL, and Redis services.

Then:

1. Place input files in `airflow_stuff/data/`
2. Open Airflow at `http://localhost:8080`
3. Enable the DAG `premium_pipeline`
4. Trigger a run

Expected CSV output:

```text
airflow_stuff/output/gold/fct_monthly_partner_premium.csv
```

## Container Images

The local runtime uses three images:

- `idowuilekura/premium-pipeline-airflow:3.2.0` runs the Airflow services
- `idowuilekura/premium-pipeline:latest` runs the ETL and CSV export tasks
- `idowuilekura/analytics-premium-dbt:latest` runs the dbt transformations

When the local stack is up, Airflow orchestrates the task images in sequence:

- `premium-container full-etl-pipeline` runs the pipeline
- the dbt image builds the reporting models
- `premium-container gold-export` exports `fct_monthly_partner_premium` to CSV

The task images are referenced from `airflow_stuff/dags/dags_air.py` and are pulled when the DAG executes.

If the local Docker cache is stale, you can refresh the Airflow service image with:

```bash
docker compose -f airflow_stuff/docker-compose.yaml pull
```

## dbt Analytics Layer

The dbt project in `analytics_premium/` is responsible for the analytical layer after the trusted base table has been loaded into PostgreSQL.

Core models:

- `bronze_transaction` is the source-faithful analytical starting point
- `silver_transaction_quality` classifies quality issues including:
  - null `transaction_id`
  - duplicate `transaction_id`
  - duplicate `sur_key`
  - missing `charged_partner`
  - missing `created_at_timestamp`
- `silver_transaction` keeps only accepted rows
- `silver_transaction_rejected` keeps rejected rows plus rejection metadata
- `fct_transaction` preserves transaction grain for downstream reuse
- `fct_monthly_partner_premium` produces the finance-facing monthly premium rollup by partner from trusted analytical data, filtered to `status = 'processed'`

Testing in the dbt layer includes:

- null and uniqueness checks
- relationship tests
- accepted-versus-rejected reconciliation checks
- no-overlap checks on accepted and rejected `sur_key`
- uniqueness of `(partner, month)` in `fct_monthly_partner_premium`

## Quality

The repository includes formatting, linting, type-checking, tests, and package-build checks, all enforced in GitHub Actions under `.github/workflows/`.

## Delivery and Release Flow

Delivery follows a protected-branch workflow: changes land on a feature branch, go through a pull request, and merge into `master`.

`master` is the release source of truth, and `latest` image tags are CI-owned outputs from merged code. The release workflow in `.github/workflows/release.yml` publishes Docker images from `master`, including `sha-<commit>` tags and `latest`.

## Assumptions

- Input files are JSON files whose names contain `premium` and `transaction`.
- `created_at` is the primary event-time field; filename dates are fallback only.
- Filename dates are operational hints, not authoritative business timestamps.
- Source files are expected to remain stable after landing.
- The business reporting grain is monthly total premium per partner, keyed to the first day of the month.
- Full schema-evolution handling is out of scope for this version.

## Future Improvements

- add explicit quarantine storage and alerting for invalid insurance transaction rows
- add fuller end-to-end integration tests across ETL, dbt, and CSV export
- extend cloud deployment guidance for object storage, managed compute, and secrets handling
- harden schema evolution handling and downstream notification flows
