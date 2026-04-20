# Getsafe Data Engineer Case Study

Production-oriented solution for the Getsafe Senior Data Engineer assessment.

Batch pipeline for auditable monthly premium reconciliation reporting.

## Executive Summary

This repository solves a monthly premium reconciliation problem for insurance partners. It ingests raw JSON transaction files, preserves rerunnable operational history, writes a trusted transactional base table to PostgreSQL, builds warehouse models with dbt, and produces a finance-facing monthly partner premium report with traceability back to raw inputs.

## Design Highlights

This solution is designed around a small set of principles:

- idempotent reruns over one-time success
- event-time correctness over filename conventions
- separation of ingestion and reporting concerns
- auditability over silent data loss
- simplicity at the current scale over premature distributed complexity

In practice, that means bronze metadata drives rerun behavior, `created_at` is the primary time source, ETL and dbt responsibilities stay separated, and accepted and rejected records remain explicit.

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

## Architecture

The repository has one execution flow and two modeling layers:

- Python ETL handles file discovery, operational bronze persistence, metadata tracking, and writes the trusted base table to PostgreSQL
- dbt handles the warehouse modeling layer built on top of that base table through `staging`, `intermediate`, and `marts`

Terminology used in this repository:

- `landing`: raw JSON files in `data/`
- `operational bronze`: persisted parquet plus metadata written by the Python ETL
- `trusted base table`: the reusable transactional dataset written by the Python ETL to PostgreSQL
- `staging`: dbt source cleanup and source-aligned normalization
- `intermediate`: dbt quality classification and accepted/rejected transaction logic
- `marts`: dbt dimensional and aggregate reporting outputs
- `gold export`: the final monthly CSV

### Architectural Diagram


<img width="1892" height="1064" alt="image" src="https://github.com/user-attachments/assets/05ea6fbd-1b2d-4635-aa90-f078ea3995aa" />


End-to-end flow:

```text
Raw JSON files
    -> operational bronze parquet + metadata
    -> trusted base table in PostgreSQL
    -> dbt staging/intermediate/marts models
    -> analytics.monthly_partner_premiums
    -> CSV export to output/gold/
```

The important boundary is that the Python layer owns ingestion and durable writes, while the dbt layer owns reporting semantics.

## Detailed Sections

### Input Data

The main source file is `premium_transactions_data_20250306.json`.

Relevant fields:

- `transaction_id`: unique transaction identifier
- `created_at`: timestamp of the premium charge
- `amount`: charged premium amount
- `currency`: transaction currency
- `charged_partner`: insurance partner for whom the premium was charged
- `status`: transaction outcome or processing status

This repository also contains additional sample JSON files in `data/` to exercise incremental load behavior, duplicate handling, and rerun safety.

### File Naming and Timestamp Rules

Filenames are non-authoritative. Event time comes from `created_at`, and filename parsing is used only as a fallback.

The ingestion layer discovers files that:

- contain `premium`
- contain `transaction`
- end in `.json`

If a fallback date is needed, the supported filename patterns are `YYYYMMDD` and `YYYY_MM_DD`.

### Dataset and Model Naming

The naming convention is straightforward:

- raw operational dataset loaded by the ETL: `premium_transaction`
- warehouse reporting relation built by dbt: `monthly_partner_premiums`
- exported CSV file: `output/gold/monthly_partner_premium_summary.csv`

Within the dbt layer:

- `stg_` models standardize source-facing shape for downstream reuse
- intermediate models represent trusted and quality-classified transaction data
- `fct_` models represent reporting facts
- `dim_` models represent reporting dimensions

### Expected Output

The main deliverable is a CSV report with the shape:

```text
partner, month, total_premium
```

Export locations:

- runtime path inside the task container: `output/gold/monthly_partner_premium_summary.csv`
- host-visible path in the local Airflow setup: `airflow/output/gold/monthly_partner_premium_summary.csv`

The repository also includes the supporting ETL code, dbt models, Docker assets, tests, and delivery workflow needed to run and assess the solution end to end.

### System Guarantees

Within the scope of the current design, the system guarantees:

- reruns do not re-ingest duplicate source contents into bronze
- corrected source files trigger reprocessing of impacted bronze partitions
- payload event time takes precedence over filename-derived dates
- accepted and rejected transaction paths remain explicit in the analytics layer
- when the configured target is PostgreSQL, keyed upserts provide idempotent local merge behavior as long as stable merge keys are supplied

These guarantees are local-batch guarantees. They do not imply distributed exactly-once semantics or support for concurrent writers.

### Technology Choices

#### Polars instead of Spark

The assessment allows any data processing framework. I chose `Polars` because:

- the dataset is small enough to process efficiently on a single machine
- local development is faster and simpler
- infrastructure and operational costs stay low
- the implementation remains expressive and testable

Spark would be a stronger fit only once data volume, orchestration complexity, or concurrency meaningfully outgrows the current workload.

#### Parquet in Bronze instead of staying on raw JSON

The bronze layer converts landed JSON into parquet rather than repeatedly reading JSON for downstream processing.

That choice is deliberate:

- parquet is materially faster for repeated analytical reads than raw JSON
- parquet preserves an explicit schema, which reduces ambiguity during downstream transformation
- typed columnar storage makes selective scans and projection cheaper
- metadata-driven partition refreshes are easier to reason about when the persisted bronze layer has a stable physical format
- it separates raw landing concerns from reusable analytical consumption concerns

In this repository, raw JSON is the landing and interchange format, while Parquet is the canonical persisted representation used by the bronze layer. That keeps the landed source intact while giving downstream processing a typed, columnar working format that is faster, more predictable, and easier to maintain.

#### Database adapter layer

The ETL writes through a database adapter layer so pipeline logic stays mostly database-agnostic while adapters can still handle engine-specific behavior.

In the current implementation, the adapters are SQLAlchemy-backed. For this assessment, PostgreSQL is the concrete target because it is easy to run locally with Docker and supports `ON CONFLICT` upserts for rerunnable transactional loads. The PostgreSQL adapter extends the generic write layer with keyed upsert behavior, so corrected records can be merged deterministically instead of only appended or fully replaced.

#### dbt for the analytics layer

dbt is used for the reporting layer because it provides:

- clear model lineage
- reusable SQL transformations
- auditable accepted/rejected modeling patterns
- a natural path to production analytics workflows

### Repository Structure

```text
premium_pipeline_project_updated/
├── analytics_premium/
│   ├── infra/
│   │   └── docker/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
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
│   ├── export/
│   │   └── monthly_partner_premium.py
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
- `src/export/monthly_partner_premium.py`: export of monthly premium report to CSV
- `analytics_premium/models/staging/premium/stg_premium__transactions.sql`: source-aligned warehouse staging model
- `analytics_premium/models/intermediate/premium/`: accepted, rejected, and quality-classified transaction models
- `analytics_premium/models/marts/premium/`: dimensional reporting models, including `monthly_partner_premiums`
- `analytics_premium/infra/docker/`: dbt container build and runtime assets

### Pipeline Stages

#### Bronze

Raw JSON files are persisted as operational bronze parquet plus metadata for rerunnable processing.

#### Trusted Base Table

The Python ETL standardizes transactions and writes the reusable base table to PostgreSQL.

#### Analytical Models

dbt builds the warehouse `staging`, `intermediate`, and `marts` models on top of that trusted base table.

#### Metadata and Traceability

The solution includes metadata to improve auditability and rerun safety.

Examples:

- bronze metadata tracks processed source files and covered partitions
- base-table load metadata captures run status and rows written
- container execution can persist a JSON run summary through `PIPELINE_RUN_RESULT_PATH`

### Failure Modes and Recovery

The system is designed to fail in a recoverable way:

- no new input files: bronze persistence and base-table loading are skipped without mutating downstream state
- invalid records: records are classified into rejected models rather than dropped silently
- corrected source files: impacted bronze partitions are rebuilt on the next run
- downstream failures: upstream bronze artifacts and the loaded base table remain reusable for reruns
- schema drift beyond the persisted schema assumptions: the run may fail and requires operator intervention

Recovery is operationally simple: fix the underlying issue, then rerun the pipeline. The metadata model is designed to make that rerun deterministic.

### Observability

The current implementation emphasizes traceability rather than a full production observability stack.

- bronze metadata tracks processed files, schema snapshots, and partition state
- base-table load metadata records run status and rows written
- container execution can persist a JSON run summary
- Airflow task logs provide orchestration visibility for the containerized path
- dbt artifacts and logs provide model-level execution detail

A production extension would add freshness metrics, anomaly detection, explicit alerting, and ownership for operational failures.

### Local Setup

#### Prerequisites

- Docker
- Docker Compose

The supported local setup is fully containerized. You do not need to install Airflow, PostgreSQL, Redis, or dbt on the host machine.

When you start the local Compose stack, it brings up the core services used by the pipeline:

- Airflow for orchestration
- PostgreSQL for transactional storage
- Redis for Airflow task coordination

The ETL, dbt, and CSV export steps are then run by the Airflow DAG as containers. The stack is already configured to use `airflow/` as the mounted workspace root through `AIRFLOW_HOST_ROOT_DIR`, so the default project layout works without additional host-side setup.

### How to Run

Airflow is the supported local execution path.

Clone the repository and move into the project directory:

```bash
git clone https://github.com/Idowuilekura/get_safe_senior_data_engineer_assesment.git
cd get_safe_senior_data_engineer_assesment
```

Start the local stack:

```bash
docker compose -f airflow/docker-compose.yaml up airflow-init
docker compose -f airflow/docker-compose.yaml up -d
```

This starts the local Airflow, PostgreSQL, and Redis services.

Then:

1. Place input files in `airflow/data/`
2. Open Airflow at `http://localhost:8080`
3. Enable the DAG `premium_pipeline`
4. Trigger a run

Expected CSV output:

```text
airflow/output/gold/monthly_partner_premium_summary.csv
```

### Container Images

The local runtime uses three images:

- `idowuilekura/premium-pipeline-airflow:3.2.0` runs the Airflow services
- `idowuilekura/premium-pipeline:latest` runs the ETL and CSV export tasks
- `idowuilekura/analytics-premium-dbt:latest` runs the dbt transformations

When the local stack is up, Airflow orchestrates the task images that implement the architecture described above:

- `premium-container full-etl-pipeline` runs the Python ETL
- the dbt image builds the reporting models
- `premium-container gold-export` exports `analytics.monthly_partner_premiums` to CSV

The ETL image exposes two operational commands:

- `full-etl-pipeline` for the end-to-end ingestion and trusted-base-table load path
- `gold-export` for exporting the monthly partner premium summary to CSV

The task images are referenced from `airflow/dags/dags_air.py` and are pulled when the DAG executes.

If the local Docker cache is stale, you can refresh the Airflow service image with:

```bash
docker compose -f airflow/docker-compose.yaml pull
```

### dbt Analytics Layer

The dbt project in `analytics_premium/` is responsible for the warehouse reporting layer after the trusted base table has been loaded into PostgreSQL.

Core models:

- `stg_premium__transactions` is the source-aligned warehouse starting point
- `premium_transaction_quality` classifies quality issues including:
  - null `transaction_id`
  - duplicate `transaction_id`
  - duplicate `sur_key`
  - missing `charged_partner`
  - missing `created_at_timestamp`
- `premium_transactions` keeps only accepted rows
- `premium_rejected_transactions` keeps rejected rows plus rejection metadata
- `fct_transaction` preserves transaction grain for downstream reuse
- `monthly_partner_premiums` produces the business-facing monthly premium rollup by partner from trusted analytical data, filtered to `status = 'processed'`

Testing in the dbt layer includes:

- null and uniqueness checks
- relationship tests
- accepted-versus-rejected reconciliation checks
- no-overlap checks on accepted and rejected `sur_key`
- uniqueness of `(partner, month)` in `monthly_partner_premiums`

### Quality

The repository includes formatting, linting, type-checking, tests, and package-build checks, all enforced in GitHub Actions under `.github/workflows/`.

### Delivery and Release Flow

Delivery follows a protected-branch workflow: changes land on a feature branch, go through a pull request, and merge into `master`.

`master` is the release source of truth, and `latest` image tags are CI-owned outputs from merged code. The release workflow in `.github/workflows/release.yml` publishes Docker images from `master`, including `sha-<commit>` tags and `latest`.

### Assumptions

- Input files are JSON files whose names contain `premium` and `transaction`.
- `created_at` is the primary event-time field; filename dates are fallback only.
- Filename dates are operational hints, not authoritative business timestamps.
- Source files are expected to remain stable after landing.
- The business reporting grain is monthly total premium per partner, keyed to the first day of the month.
- Full schema-evolution handling is out of scope for this version.

### Future Improvements

- add explicit quarantine storage and alerting for invalid insurance transaction rows
- add fuller end-to-end integration tests across ETL, dbt, and CSV export
- extend cloud deployment guidance for object storage, managed compute, and secrets handling
- harden schema evolution handling and downstream notification flows
