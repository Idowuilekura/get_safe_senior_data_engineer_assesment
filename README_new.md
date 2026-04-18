# Getsafe Data Engineer Case Study

Production-oriented solution for the Getsafe Senior Data Engineer assessment.

This repository implements a batch data pipeline for insurance premium transactions. It ingests raw JSON files, builds bronze and silver data products, models a gold analytics layer with dbt, and exports the finance-facing monthly premium reconciliation report required by the case study.

---

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

---

## Overview

Getsafe is an insurance company with a digital operating model. Finance teams require trustworthy, reproducible reporting on premium transactions charged on behalf of insurance partners.

This solution implements a focused insurance data workflow:

- ingest raw premium transaction files  
- preserve lineage and replayability  
- standardize and validate records  
- model accepted and rejected transaction paths  
- publish a monthly partner premium aggregate for reconciliation  

Final output:

output/gold/fct_monthly_partner_premium.csv

---

## Design Principles and Tradeoffs

This solution is guided by a small set of principles that prioritize correctness, auditability, and operational reliability over unnecessary complexity.

### Idempotent reruns over one-time success

Batch pipelines are retried, backfilled, and rerun. The design ensures reruns do not create duplicate results. Bronze tracks file and partition metadata, and downstream writes are structured to remain safe under repeated execution.

### Event-time correctness over filename conventions

Event timestamps (created_at) are the source of truth. Filenames are treated as operational hints and only used as fallback. This avoids coupling reporting logic to inconsistent naming conventions.

### Separation of ingestion and reporting concerns

The ETL layer owns ingestion (file handling, normalization, partitioning).  
The dbt layer owns reporting (business logic, aggregation, modeling).

This separation reduces coupling and allows each layer to evolve independently.

### Auditability over silent data loss

Invalid records are not dropped silently. Accepted and rejected paths are modeled explicitly, ensuring reconciliation remains explainable and traceable.

### Simplicity at the current scale

The system uses Polars for deterministic single-node execution. This keeps development simple and testable while allowing future evolution to distributed systems if needed.

---

## Business Context

The business use case is monthly financial reconciliation.

Finance teams need to validate invoice totals against transactional data. This requires:

- auditability  
- duplicate handling  
- clear separation between valid and invalid records  

The layered architecture supports this:

- Bronze: raw, lineage-preserving  
- Silver: trusted, quality-classified  
- Gold: reporting-ready  

---

## Problem Statement

Compute total successfully charged premium per month per partner.

Required output:

partner, month, total_premium

---

## Input Data

Primary file:

premium_transactions_data_20250306.json

Key fields:

- transaction_id
- created_at
- amount
- currency
- charged_partner
- status

Additional files in data/ simulate incremental loads and duplicates.

---

## File Naming and Timestamp Rules

Filenames are treated as non-authoritative.

Event time is derived from the payload (created_at). Filename parsing is used only as a fallback when timestamps are unavailable.

This avoids reliance on inconsistent naming conventions.

---

## Expected Output

- ETL pipeline (src/pipeline/)
- CSV export (output/gold/)
- containerized execution
- tests (tests/)
- documentation (this README)

---

## Architecture

The system is split into two layers:

1. Python ETL pipeline  
2. dbt analytics layer  

Flow:

Raw JSON → Bronze → Silver → dbt → Gold → CSV

Responsibilities:

- ETL: ingestion, normalization, partitioning, storage  
- dbt: business modeling, aggregation, reporting  

This separation ensures ingestion logic remains independent from reporting semantics and reduces cross-layer coupling.

---

## System Guarantees

Within the scope of this design, the pipeline provides:

- partition-level idempotent reruns  
- deterministic outputs for identical inputs  
- event-time driven partitioning and aggregation  
- explicit separation of accepted and rejected records  

These guarantees support reliable financial reconciliation workflows.

---

## Technology Choices

### Polars

Chosen for efficiency and simplicity at current scale.

The design does not assume Polars as a permanent constraint. If data volume or concurrency increases, ingestion can move to distributed compute without affecting downstream contracts.

### SQLAlchemy

Provides a consistent database abstraction layer with support for Postgres-specific upsert behavior.

### dbt

Used for:

- lineage and model structure  
- reusable transformations  
- explicit data quality modeling  

---

## Repository Structure

(unchanged)

---

## Pipeline Stages

### Bronze

- raw ingestion from JSON  
- metadata tracking  
- partition-aware writes  

### Silver

- standardized dataset  
- upsert support  
- deterministic reruns  

### Gold

- dbt modeling layer  
- accepted vs rejected logic  
- final reporting output  

---

## Failure Modes and Recovery

The system is designed to handle common failure scenarios without compromising data integrity:

- partial ingestion → partition-level reprocessing  
- invalid records → routed to rejected models  
- downstream failures → upstream data remains reusable  
- reruns → deterministic, no duplication  

The design favors recoverability and transparency over tightly coupled execution.

---

## Observability

The current implementation focuses on traceability rather than full observability infrastructure.

- metadata outputs support rerun tracking and lineage  
- Airflow logs provide execution visibility  
- dbt artifacts expose model-level execution details  

A production system would extend this with metrics, alerting, and freshness monitoring.

---

## Local Setup

Docker-based setup using Airflow orchestration.

---

## How to Run

Clone and start the system:

git clone https://github.com/Idowuilekura/get_safe_senior_data_engineer_assesment.git
cd get_safe_senior_data_engineer_assesment
docker compose -f airflow_stuff/docker-compose.yaml up airflow-init
docker compose -f airflow_stuff/docker-compose.yaml up -d

Then:

1. Open Airflow at http://localhost:8080  
2. Enable DAG premium_pipeline  
3. Trigger a run  

---

## Container Images

- Airflow image  
- ETL pipeline image  
- dbt image  

Used to isolate execution environments and ensure reproducibility.

---

## dbt Analytics Layer

The dbt project builds the reporting layer.

Key models:

- bronze_transaction  
- silver_transaction  
- silver_transaction_rejected  
- fct_monthly_partner_premium  

Testing includes:

- uniqueness checks  
- relationship checks  
- accepted vs rejected reconciliation  

---

## Quality and CI

uv run ruff check .
uv run mypy src tests
uv run pytest

CI mirrors local checks via GitHub Actions.

---

## Assumptions

- multiple files may arrive over time  
- filenames are not reliable business contracts  
- payload timestamps are primary  
- some records may be invalid  
- schema drift is not fully handled  
- workload fits single-node execution  

---

## Future Improvements

- alerting and monitoring  
- schema evolution handling  
- cloud storage integration  
- full end-to-end testing  
