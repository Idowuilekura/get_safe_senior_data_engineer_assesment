# ruff: noqa: E402

import sys
from pathlib import Path

from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sdk import DAG
from pendulum import datetime

AIRFLOW_SUPPORT_ROOT = Path(__file__).resolve().parents[1]
if str(AIRFLOW_SUPPORT_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_SUPPORT_ROOT))

from utils.premium_pipeline_utils import (
    build_docker_task,
    collect_pipeline_result,
    db_environment,
    dbt_environment,
    ensure_output_dir,
    mounts,
    should_continue_downstream_processing,
)

with DAG(
    dag_id="premium_pipeline",
    start_date=datetime(2025, 10, 2),
    schedule=None,
    catchup=False,
) as dag:
    prepare_output = PythonOperator(
        task_id="prepare_output",
        python_callable=ensure_output_dir,
    )

    run_pipeline = build_docker_task(
        task_id="run_premium_pipeline",
        image="idowuilekura/premium-pipeline:latest",
        command="full-etl-pipeline",
        environment=db_environment,
        mounts=mounts,
    )

    collect_pipeline_result_task = PythonOperator(
        task_id="collect_pipeline_result",
        python_callable=collect_pipeline_result,
    )

    continue_downstream_processing = ShortCircuitOperator(
        task_id="continue_downstream_processing",
        python_callable=should_continue_downstream_processing,
    )

    run_dbt = build_docker_task(
        task_id="run_dbt_export",
        image="idowuilekura/analytics-premium-dbt:latest",
        environment=dbt_environment,
    )

    run_csv_export = build_docker_task(
        task_id="run_csv_export",
        image="idowuilekura/premium-pipeline:latest",
        command="gold-export",
        environment=db_environment,
        mounts=mounts,
    )

    prepare_output >> run_pipeline >> collect_pipeline_result_task >> continue_downstream_processing
    continue_downstream_processing >> run_dbt >> run_csv_export
