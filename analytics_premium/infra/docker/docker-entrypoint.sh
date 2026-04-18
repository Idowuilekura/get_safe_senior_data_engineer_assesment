#!/bin/sh
set -eu

cd /app

mkdir -p /app/.runtime
python /app/infra/docker/render_profiles.py /app/.runtime/profiles.yml

if [ "${1:-}" = "dbt" ]; then
    shift
fi

if [ "$#" -eq 0 ]; then
    set -- run
    if [ -n "${DBT_DEFAULT_SELECT:-}" ]; then
        set -- "$@" --select "${DBT_DEFAULT_SELECT}"
    fi
fi

exec dbt "$@" --project-dir /app --profiles-dir /app/.runtime
