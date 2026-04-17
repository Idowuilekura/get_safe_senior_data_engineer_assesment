ARG PYTHON_VERSION=3.11
ARG UV_VERSION=0.7.21
ARG DBT_CORE_VERSION=1.11.8
ARG DBT_ADAPTER_PACKAGE=dbt-postgres
ARG DBT_ADAPTER_VERSION=1.10.0
ARG DBT_TYPE=postgres

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM python:${PYTHON_VERSION}-slim AS builder
ARG DBT_TYPE
COPY --from=uv /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    DBT_TYPE=${DBT_TYPE} \
    PATH="/opt/dbt-venv/bin:$PATH"

WORKDIR /app

RUN python -m venv /opt/dbt-venv

RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install \
    --python /opt/dbt-venv/bin/python \
    "dbt-core==${DBT_CORE_VERSION}" \
    "${DBT_ADAPTER_PACKAGE}==${DBT_ADAPTER_VERSION}"

COPY dbt_project.yml packages.yml /app/
COPY docker /app/docker
COPY models /app/models
COPY macros /app/macros
COPY analyses /app/analyses
COPY seeds /app/seeds
COPY snapshots /app/snapshots
COPY tests /app/tests

RUN mkdir -p /app/.runtime \
    && python /app/docker/render_profiles.py /app/.runtime/profiles.yml \
    && dbt deps --project-dir /app --profiles-dir /app/.runtime

FROM python:${PYTHON_VERSION}-slim AS runtime
ARG DBT_TYPE

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DBT_PROFILES_DIR=/app/.runtime \
    DBT_TYPE=${DBT_TYPE} \
    PATH="/opt/dbt-venv/bin:$PATH"

RUN groupadd --system app \
    && useradd --system --gid app --create-home --home-dir /home/app app

WORKDIR /app

COPY --from=builder /opt/dbt-venv /opt/dbt-venv
COPY --from=builder /app /app
COPY --chown=app:app docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN chmod 0555 /usr/local/bin/docker-entrypoint.sh \
    && mkdir -p /app/target /app/logs /app/.runtime \
    && chown -R app:app /app /opt/dbt-venv

USER app

ENTRYPOINT ["docker-entrypoint.sh"]
CMD []
