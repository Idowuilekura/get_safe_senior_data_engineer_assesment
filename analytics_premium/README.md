# analytics_premium

This dbt project models premium transaction data through a layered analytics workflow:

- Bronze for raw ingestion and standardization
- Silver for cleaned, idempotent transaction records
- Gold for dimensional models that support downstream reporting and analysis

## Design Decisions

The project follows a simple Bronze, Silver, Gold layering strategy. Bronze is a source-faithful ingestion layer that preserves the raw transaction rows while adding a deterministic technical key for lineage. Silver is the trusted transactional layer where records are classified into accepted and rejected paths, making data quality handling explicit and auditable without over-fragmenting the model graph. Gold presents the dimensional layer, separating facts from dimensions so reporting models remain clear and reusable.

The fact table is designed at a transaction grain, with one row per transaction. It does not aggregate data yet, which keeps the model flexible for downstream consumers that may need different rollup levels. In this design, `amount` is treated as a measure rather than part of the business key.

The surrogate key strategy is deterministic so the pipeline can be rerun safely without creating duplicate business events. The transaction keying approach is anchored on `transaction_id`; in the current implementation, the upstream transaction surrogate key is generated deterministically from `transaction_id` and `charged_partner`, which keeps reruns predictable while matching the available source shape.

The dimensional layer keeps descriptive context outside the fact table. `dim_partner` captures partner attributes for analysis by partner, while `dim_date` provides reusable calendar fields that can support filtering, grouping, and time-based reporting across multiple use cases. This separation keeps the fact model focused on the transaction event itself.

The incremental merge strategy in the accepted transactional layer supports updates such as status changes while preserving idempotency during reruns. This design favors reliability and reuse over premature complexity, avoids early aggregation, and leaves room for future marts or reporting models to build on a stable foundation.

Duplicate handling is explicit rather than implicit. Bronze remains reconcilable to the source, while silver classifies records into accepted and rejected paths. Rejected rows capture duplicate business keys, duplicate surrogate keys, and records missing required dimensional attributes in a single audit model, which keeps the pipeline lean while preserving a clear operational trail for follow-up.

## Docker

The dbt project can run in a container with a secure default posture:

- the image runs as a non-root user
- the runtime image contains only the dbt environment and project files
- database credentials are provided through environment variables rather than committed config
- the source database, schema, and physical source table can be swapped without editing versioned YAML
- the Docker profile is isolated from local dbt usage, so local development can keep using `~/.dbt/profiles.yml`
- the container is adapter-aware: you choose the dbt adapter package at build time, and the runtime profile is rendered from environment variables

Build the image:

```bash
docker build \
  --build-arg DBT_ADAPTER_PACKAGE=dbt-postgres \
  --build-arg DBT_ADAPTER_VERSION=1.10.0 \
  --build-arg DBT_TYPE=postgres \
  -t analytics-premium-dbt .
```

For another warehouse, rebuild with the matching adapter package and type. Example:

```bash
docker build \
  --build-arg DBT_ADAPTER_PACKAGE=dbt-snowflake \
  --build-arg DBT_ADAPTER_VERSION=1.11.0 \
  --build-arg DBT_TYPE=snowflake \
  -t analytics-premium-dbt .
```

Run the default materialization flow:

```bash
docker run --rm --env-file .env analytics-premium-dbt
```

By default, the container runs:

```bash
dbt run --select +fct_monthly_partner_premium
```

You can override the command to run other dbt tasks:

```bash
docker run --rm --env-file .env analytics-premium-dbt build --full-refresh
docker run --rm --env-file .env analytics-premium-dbt test
```

For local development with Docker Compose:

```bash
cp .env.example .env
docker compose run --rm dbt
```

Important environment variables:

- `DBT_TYPE` declares the active dbt adapter type for the rendered runtime profile
- `DBT_TARGET_OUTPUT_JSON` can provide a full adapter-specific dbt output object for any warehouse
- `DBT_OUTPUT_EXTRA_JSON` can add adapter-specific fields on top of the common env-based profile
- `DBT_HOST`, `DBT_PORT`, `DBT_USER`, `DBT_PASSWORD`, `DBT_DATABASE`, `DBT_SCHEMA` control the common SQL-style target connection fields
- `DBT_SOURCE_DATABASE`, `DBT_SOURCE_SCHEMA`, `DBT_SOURCE_IDENTIFIER` control which raw source table the project reads from
- `DBT_DEFAULT_SELECT` controls the default `dbt run --select ...` target when no explicit command is passed

Example fully generic runtime profile input for any adapter:

```bash
export DBT_TYPE=snowflake
export DBT_TARGET_OUTPUT_JSON='{"type":"snowflake","account":"acme","user":"svc_dbt","password":"secret","database":"RAW","schema":"PUBLIC","warehouse":"TRANSFORM","role":"ANALYST","threads":4}'
docker run --rm --env-file .env analytics-premium-dbt build
```
