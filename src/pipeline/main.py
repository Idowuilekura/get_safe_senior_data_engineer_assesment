from __future__ import annotations

from pipeline.adapters.factory import DatabaseWriterFactory
from pipeline.logging_config import configure_logging
from pipeline.orchestration import run_pipeline
from pipeline.settings import load_pipeline_config_from_env


def main() -> None:
    configure_logging()

    config = load_pipeline_config_from_env()

    database_writer = DatabaseWriterFactory.create(config)
    result = run_pipeline(config=config, database_writer=database_writer)
    print(result)


if __name__ == "__main__":
    main()
