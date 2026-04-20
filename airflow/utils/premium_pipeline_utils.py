import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.task.trigger_rule import TriggerRule
from docker.types import Mount

logger = logging.getLogger(__name__)


def resolve_host_project_dir(base_dir=None) -> Path:
    if base_dir is not None:
        return Path(base_dir).expanduser().resolve()

    configured_dir = os.environ.get("AIRFLOW_HOST_ROOT_DIR")
    if configured_dir:
        return Path(configured_dir).expanduser().resolve()

    configured_dir = os.environ.get("AIRFLOW_PROJ_DIR")
    if configured_dir:
        return Path(configured_dir).expanduser().resolve()

    return Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class DatabaseConfig:
    database_type: str = field(default_factory=lambda: os.environ.get("DATABASE_TYPE", "postgres"))
    database_host: str = field(
        default_factory=lambda: os.environ.get("DATABASE_HOST", "app-postgres")
    )
    database_port: str = field(default_factory=lambda: os.environ.get("DATABASE_PORT", "5432"))
    database_name: str = field(default_factory=lambda: os.environ.get("DATABASE_NAME", "mydb"))
    database_user: str = field(default_factory=lambda: os.environ.get("DATABASE_USER", "postgres"))
    database_password: str = field(
        default_factory=lambda: os.environ.get("DATABASE_PASSWORD", "postgres")
    )
    pipeline_run_result_path: str = field(
        default_factory=lambda: os.environ.get(
            "PIPELINE_RUN_RESULT_PATH",
            "/app/output/full_etl_result.json",
        )
    )


@dataclass(frozen=True)
class DockerMount:
    host_base_dir: Optional[Union[Path, str]] = None
    local_base_dir: Path = Path("/opt/airflow")
    data_target: str = "/app/data"
    output_target: str = "/app/output"

    def __post_init__(self):
        object.__setattr__(self, "host_base_dir", resolve_host_project_dir(self.host_base_dir))
        object.__setattr__(self, "local_base_dir", Path(self.local_base_dir))

    @property
    def data_source(self) -> str:
        return str(self.host_base_dir / "data")

    @property
    def output_source(self) -> str:
        return str(self.host_base_dir / "output")

    @property
    def local_data_dir(self) -> Path:
        return self.local_base_dir / "data"

    @property
    def local_output_dir(self) -> Path:
        return self.local_base_dir / "output"


db_config = DatabaseConfig()
host_project_dir = resolve_host_project_dir()
docker_mount = DockerMount(host_base_dir=host_project_dir)
staging_schema = os.environ.get("DBT_STAGING_SCHEMA", "staging")
intermediate_schema = os.environ.get("DBT_INTERMEDIATE_SCHEMA", "intermediate")
marts_schema = os.environ.get("DBT_MARTS_SCHEMA", "analytics")
host_export_path = str(
    Path(docker_mount.output_source) / "gold" / "monthly_partner_premium_summary.csv"
)

db_environment = {
    "DATABASE_TYPE": db_config.database_type,
    "DATABASE_HOST": db_config.database_host,
    "DATABASE_PORT": db_config.database_port,
    "DATABASE_NAME": db_config.database_name,
    "DATABASE_USER": db_config.database_user,
    "DATABASE_PASSWORD": db_config.database_password,
    "PIPELINE_RUN_RESULT_PATH": db_config.pipeline_run_result_path,
    "PIPELINE_GOLD_MONTHLY_PARTNER_PREMIUM_SCHEMA": marts_schema,
}

email_environment = {
    **db_environment,
    "PIPELINE_SEND_EMAIL": os.environ.get("PIPELINE_SEND_EMAIL", "false"),
    "PIPELINE_EMAIL_HOST": os.environ.get("PIPELINE_EMAIL_HOST", ""),
    "PIPELINE_EMAIL_PORT": os.environ.get("PIPELINE_EMAIL_PORT", "587"),
    "PIPELINE_EMAIL_USERNAME": os.environ.get("PIPELINE_EMAIL_USERNAME", ""),
    "PIPELINE_EMAIL_PASSWORD": os.environ.get("PIPELINE_EMAIL_PASSWORD", ""),
    "PIPELINE_EMAIL_FROM": os.environ.get("PIPELINE_EMAIL_FROM", ""),
    "PIPELINE_EMAIL_TO": os.environ.get("PIPELINE_EMAIL_TO", ""),
    "PIPELINE_EMAIL_USE_TLS": os.environ.get("PIPELINE_EMAIL_USE_TLS", "true"),
    "PIPELINE_EMAIL_DAG_ID": "{{ dag.dag_id }}",
    "PIPELINE_EMAIL_RUN_ID": "{{ run_id }}",
    "PIPELINE_EMAIL_PIPELINE_STATE": (
        "{{ dag_run.get_task_instance('run_premium_pipeline').state if dag_run else 'unknown' }}"
    ),
    "PIPELINE_EMAIL_DBT_STATE": (
        "{{ dag_run.get_task_instance('run_dbt_export').state if dag_run else 'unknown' }}"
    ),
    "PIPELINE_EMAIL_EXPORT_STATE": (
        "{{ dag_run.get_task_instance('run_csv_export').state if dag_run else 'unknown' }}"
    ),
    "PIPELINE_EMAIL_EXPORT_PATH": (
        "{{ '" + host_export_path + "' "
        "if dag_run and dag_run.get_task_instance('run_csv_export').state == 'success' "
        "else '' }}"
    ),
}

dbt_environment = {
    **db_environment,
    "DBT_TYPE": "postgres",
    "DBT_HOST": db_config.database_host,
    "DBT_PORT": db_config.database_port,
    "DBT_USER": db_config.database_user,
    "DBT_PASSWORD": db_config.database_password,
    "DBT_DATABASE": db_config.database_name,
    "DBT_SCHEMA": os.environ.get("DBT_SCHEMA", "public"),
    "DBT_TARGET": os.environ.get("DBT_TARGET", "default"),
    "DBT_STAGING_SCHEMA": staging_schema,
    "DBT_INTERMEDIATE_SCHEMA": intermediate_schema,
    "DBT_MARTS_SCHEMA": marts_schema,
    "DBT_SOURCE_SCHEMA": os.environ.get("DBT_SOURCE_SCHEMA", "public"),
    "DBT_SOURCE_IDENTIFIER": "premium_transaction",
}

mounts = [
    Mount(
        source=docker_mount.data_source,
        target=docker_mount.data_target,
        type="bind",
    ),
    Mount(
        source=docker_mount.output_source,
        target=docker_mount.output_target,
        type="bind",
    ),
]

OUTPUT_DIR = docker_mount.local_output_dir
RESULT_FILE = OUTPUT_DIR / "full_etl_result.json"


def ensure_output_dir():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def collect_pipeline_result(ti):
    docker_output = ti.xcom_pull(task_ids="run_premium_pipeline")

    result = None
    if RESULT_FILE.exists():
        try:
            with RESULT_FILE.open("r", encoding="utf-8") as result_file:
                result = json.load(result_file)
        except (OSError, json.JSONDecodeError):
            logger.exception("Failed to load pipeline result from %s", RESULT_FILE)

    logger.info("Docker XCom output: %s", docker_output)
    logger.info("File result: %s", result)

    ti.xcom_push(key="docker_output", value=docker_output)
    return {
        "docker_output": docker_output,
        "file_result": result,
    }


def should_continue_downstream_processing(ti):
    docker_output = ti.xcom_pull(
        task_ids="collect_pipeline_result",
        key="docker_output",
    )
    return str(docker_output).strip().lower() == "true"


def build_docker_task(
    *,
    task_id,
    image,
    environment,
    command=None,
    mounts=None,
    trigger_rule=TriggerRule.ALL_SUCCESS,
):
    return DockerOperator(
        task_id=task_id,
        image=image,
        command=command,
        docker_url="unix://var/run/docker.sock",
        network_mode="airflow-shared",
        mount_tmp_dir=False,
        auto_remove="success",
        do_xcom_push=True,
        xcom_all=False,
        environment=environment,
        mounts=mounts,
        trigger_rule=trigger_rule,
    )
