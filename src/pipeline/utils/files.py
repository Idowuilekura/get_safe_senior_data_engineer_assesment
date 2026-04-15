from __future__ import annotations

import datetime as dt
import os
from pathlib import Path


def get_file_info(file_path: str) -> dict[str, dict[str, str]]:
    path = Path(file_path)

    if not path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    stat_result = path.stat()
    modified_datetime = dt.datetime.fromtimestamp(stat_result.st_mtime).isoformat()

    return {
        str(path): {
            "modified": modified_datetime,
            "size_bytes": str(stat_result.st_size),
        }
    }


def is_matching_file(
    filename: str,
    insur_type: str,
    dataset_type: str,
    ext_type: str,
) -> bool:
    return insur_type in filename and dataset_type in filename and filename.endswith(ext_type)


def sort_files_by_date(files: list[str], ext_type: str) -> list[str]:
    return sorted(
        files,
        key=lambda file_path: Path(file_path).name.split("_")[-1].replace(ext_type, ""),
    )


def list_source_files(
    directory_path: str,
    insur_type: str,
    dataset_type: str,
    ext_type: str,
) -> list[str]:
    directory = Path(directory_path)

    if not directory.exists():
        raise FileNotFoundError(f"Directory not found: {directory_path}")

    matched_files = [
        str(directory / file_name)
        for file_name in os.listdir(directory)
        if is_matching_file(file_name, insur_type, dataset_type, ext_type)
    ]

    return sort_files_by_date(matched_files, ext_type=ext_type) if matched_files else []
