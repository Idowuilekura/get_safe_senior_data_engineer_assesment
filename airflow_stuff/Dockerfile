FROM ghcr.io/astral-sh/uv:0.7.21 AS uv

FROM apache/airflow:3.2.0

USER root

COPY --from=uv /uv /uvx /usr/local/bin/

ENV UV_SYSTEM_PYTHON=1

RUN uv pip install --system --no-cache \
    "apache-airflow-providers-docker" \
    "docker>=7,<8"

USER airflow
