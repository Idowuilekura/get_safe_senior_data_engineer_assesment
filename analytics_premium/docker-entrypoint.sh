#!/bin/sh
set -eu

cd /app

mkdir -p /app/.runtime
python /app/docker/render_profiles.py /app/.runtime/profiles.yml

if [ "${1:-}" = "dbt" ]; then
    shift
fi

if [ "$#" -eq 0 ]; then
    set -- run --select "${DBT_DEFAULT_SELECT:-+fct_monthly_partner_premium}"
fi

exec dbt --project-dir /app --profiles-dir /app/.runtime "$@"
