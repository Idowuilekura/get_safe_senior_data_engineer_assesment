#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)

IMAGE_NAME="${1:-idowuilekura/analytics-premium-dbt:latest}"

docker build \
  --build-arg DBT_ADAPTER_PACKAGE="${DBT_ADAPTER_PACKAGE:-dbt-postgres}" \
  --build-arg DBT_ADAPTER_VERSION="${DBT_ADAPTER_VERSION:-1.10.0}" \
  --build-arg DBT_TYPE="${DBT_TYPE:-postgres}" \
  -f "$SCRIPT_DIR/Dockerfile" \
  -t "$IMAGE_NAME" \
  "$PROJECT_ROOT"
