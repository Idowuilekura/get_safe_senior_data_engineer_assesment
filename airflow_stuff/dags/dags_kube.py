import json
import os

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import DAG
from kubernetes.client import models as k8s
from pendulum import datetime

AIRFLOW_NAMESPACE = os.environ.get("AIRFLOW_K8S_NAMESPACE", "airflow")
WORKSPACE_PVC_NAME = os.environ.get(
    "PREMIUM_PIPELINE_WORKSPACE_PVC",
    "premium-pipeline-workspace",
)
PIPELINE_REPO_URL = os.environ.get(
    "PREMIUM_PIPELINE_GIT_REPO",
    "https://github.com/Idowuilekura/get_safe_senior_data_engineer_assesment.git",
)
PIPELINE_REPO_BRANCH = os.environ.get("PREMIUM_PIPELINE_GIT_BRANCH", "master")
PIPELINE_REPO_DATA_SUBPATH = os.environ.get(
    "PREMIUM_PIPELINE_GIT_DATA_SUBPATH",
    "airflow_stuff/data",
)
PIPELINE_RESULT_PATH = "/app/output/full_etl_result.json"
DB_HOST = os.environ.get("PREMIUM_PIPELINE_DB_HOST", "airflow-postgresql")
DB_PORT = os.environ.get("PREMIUM_PIPELINE_DB_PORT", "5432")
DB_NAME = os.environ.get("PREMIUM_PIPELINE_DB_NAME", "postgres")
DB_USER = os.environ.get("PREMIUM_PIPELINE_DB_USER", "postgres")
DB_PASSWORD_SECRET = os.environ.get(
    "PREMIUM_PIPELINE_DB_PASSWORD_SECRET",
    "airflow-postgresql",
)
DB_PASSWORD_SECRET_KEY = os.environ.get(
    "PREMIUM_PIPELINE_DB_PASSWORD_SECRET_KEY",
    "postgres-password",
)


def _env_vars(extra_values=None):
    extra_values = extra_values or {}
    env_vars = [
        k8s.V1EnvVar(name="DATABASE_TYPE", value="postgres"),
        k8s.V1EnvVar(name="DATABASE_HOST", value=DB_HOST),
        k8s.V1EnvVar(name="DATABASE_PORT", value=DB_PORT),
        k8s.V1EnvVar(name="DATABASE_NAME", value=DB_NAME),
        k8s.V1EnvVar(name="DATABASE_USER", value=DB_USER),
        k8s.V1EnvVar(
            name="DATABASE_PASSWORD",
            value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(
                    name=DB_PASSWORD_SECRET,
                    key=DB_PASSWORD_SECRET_KEY,
                )
            ),
        ),
        k8s.V1EnvVar(name="PIPELINE_RUN_RESULT_PATH", value=PIPELINE_RESULT_PATH),
    ]
    env_vars.extend(k8s.V1EnvVar(name=name, value=value) for name, value in extra_values.items())
    return env_vars


def _base_volumes():
    return [
        k8s.V1Volume(
            name="workspace",
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                claim_name=WORKSPACE_PVC_NAME
            ),
        ),
        k8s.V1Volume(name="repo", empty_dir=k8s.V1EmptyDirVolumeSource()),
    ]


def _output_mount():
    return k8s.V1VolumeMount(name="workspace", mount_path="/app/output", sub_path="output")


def _repo_data_mount():
    return k8s.V1VolumeMount(
        name="repo",
        mount_path="/app/data",
        sub_path=PIPELINE_REPO_DATA_SUBPATH,
        read_only=True,
    )


def _clone_repo_init_container():
    clone_script = f"""
set -eu
rm -rf /repo-src/*
git clone --depth 1 --branch "{PIPELINE_REPO_BRANCH}" "{PIPELINE_REPO_URL}" /repo-src
test -d "/repo-src/{PIPELINE_REPO_DATA_SUBPATH}"
"""
    return k8s.V1Container(
        name="clone-pipeline-repo",
        image="alpine/git:2.47.2",
        command=["/bin/sh", "-ec", clone_script],
        volume_mounts=[k8s.V1VolumeMount(name="repo", mount_path="/repo-src")],
    )


def _pipeline_result_has_new_data(ti):
    result = ti.xcom_pull(task_ids="run_premium_pipeline") or {}
    if isinstance(result, str):
        try:
            result = json.loads(result)
        except json.JSONDecodeError:
            return False
    return bool(result.get("has_new_data"))


def _kubernetes_task(
    *,
    task_id,
    image,
    env_vars,
    volume_mounts,
    init_containers=None,
    script=None,
    cmds=None,
    arguments=None,
):
    if script is not None:
        cmds = ["/bin/sh", "-ec"]
        arguments = [script]

    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id,
        namespace=AIRFLOW_NAMESPACE,
        image=image,
        cmds=cmds,
        arguments=arguments,
        env_vars=env_vars,
        volumes=_base_volumes(),
        volume_mounts=volume_mounts,
        init_containers=init_containers or [],
        get_logs=True,
        log_events_on_failure=True,
        startup_timeout_seconds=180,
        do_xcom_push=task_id == "run_premium_pipeline",
        on_finish_action="delete_pod",
        labels={"dag_id": "premium_pipeline_kubernetes"},
    )


with DAG(
    dag_id="premium_pipeline_kubernetes",
    start_date=datetime(2025, 10, 2),
    schedule=None,
    catchup=False,
    doc_md="""
    Kubernetes-native version of the premium pipeline.

    Requirements:
    - A PVC named by `PREMIUM_PIPELINE_WORKSPACE_PVC` must exist in the Airflow namespace.
    - The PVC must allow writing an `output/` directory used by the pipeline containers.
    - The Git repo in `PREMIUM_PIPELINE_GIT_REPO` must be reachable from the cluster.
    """,
) as dag:
    prepare_workspace = KubernetesPodOperator(
        task_id="prepare_workspace",
        name="prepare-workspace",
        namespace=AIRFLOW_NAMESPACE,
        image="busybox:1.36",
        cmds=["/bin/sh", "-ec"],
        arguments=["mkdir -p /workspace/output"],
        volumes=_base_volumes(),
        volume_mounts=[k8s.V1VolumeMount(name="workspace", mount_path="/workspace")],
        get_logs=True,
        log_events_on_failure=True,
        startup_timeout_seconds=120,
        on_finish_action="delete_pod",
        labels={"dag_id": "premium_pipeline_kubernetes"},
    )

    run_pipeline = _kubernetes_task(
        task_id="run_premium_pipeline",
        image="idowuilekura/premium-pipeline:latest",
        script="""
set -eu
mkdir -p /app/output /airflow/xcom
full-etl-pipeline
python - <<'PY'
import json
import os
from pathlib import Path

result_path = Path(os.environ["PIPELINE_RUN_RESULT_PATH"])
payload = {"has_new_data": False, "result_path": str(result_path)}
if result_path.exists():
    payload = json.loads(result_path.read_text())
Path("/airflow/xcom/return.json").write_text(json.dumps(payload))
PY
""",
        env_vars=_env_vars(),
        volume_mounts=[_output_mount(), _repo_data_mount()],
        init_containers=[_clone_repo_init_container()],
    )

    continue_downstream_processing = ShortCircuitOperator(
        task_id="continue_downstream_processing",
        python_callable=_pipeline_result_has_new_data,
    )

    run_dbt_export = _kubernetes_task(
        task_id="run_dbt_export",
        image="idowuilekura/analytics-premium-dbt:latest",
        env_vars=_env_vars({"DBT_SOURCE_IDENTIFIER": "premium_transaction"}),
        volume_mounts=[_output_mount()],
    )

    run_csv_export = _kubernetes_task(
        task_id="run_csv_export",
        image="idowuilekura/premium-pipeline:latest",
        script="gold-export",
        env_vars=_env_vars(),
        volume_mounts=[_output_mount()],
    )

    prepare_workspace >> run_pipeline >> continue_downstream_processing
    continue_downstream_processing >> run_dbt_export >> run_csv_export
