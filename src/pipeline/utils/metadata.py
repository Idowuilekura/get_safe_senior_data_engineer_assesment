from __future__ import annotations

import json
from pathlib import Path

from pipeline.types import MetadataDict


def metadata_exists(metadata_folder_path: str, metadata_file_name: str) -> bool:
    metadata_path = Path(metadata_folder_path) / metadata_file_name
    return metadata_path.exists()


def read_metadata_file(metadata_folder_path: str, metadata_file_name: str) -> MetadataDict:
    full_path = Path(metadata_folder_path) / metadata_file_name

    with full_path.open("r", encoding="utf-8") as file:
        return json.load(file)


def try_read_metadata_file(
    metadata_folder_path: str,
    metadata_file_name: str,
) -> MetadataDict | None:
    if not metadata_exists(metadata_folder_path, metadata_file_name):
        return None

    return read_metadata_file(metadata_folder_path, metadata_file_name)


def write_metadata_file(
    metadata_folder_path: str,
    metadata_file_name: str,
    metadata: MetadataDict,
) -> Path:
    folder = Path(metadata_folder_path)
    folder.mkdir(parents=True, exist_ok=True)

    full_path = folder / metadata_file_name
    with full_path.open("w", encoding="utf-8") as file:
        json.dump(metadata, file, indent=2)

    return full_path
