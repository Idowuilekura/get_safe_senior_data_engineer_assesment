from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime
from pathlib import Path

from pipeline.types import SourceFileState, SourceFileStates

SOURCE_FILE_DATE_PATTERNS = (
    re.compile(r"(?<!\d)(\d{4})_(\d{2})_(\d{2})(?!\d)"),
    re.compile(r"(?<!\d)(\d{4})(\d{2})(\d{2})(?!\d)"),
)
SUPPORTED_CREATED_AT_FORMATS = (
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
)


def is_matching_file(
    filename: str,
    insur_type: str,
    dataset_type: str,
    ext_type: str,
) -> bool:
    """Check whether a file matches the expected pipeline naming pattern.

    Args:
        filename: File name to evaluate.
        insur_type: Insurance type token expected in the file name.
        dataset_type: Dataset type token expected in the file name.
        ext_type: Expected file extension.

    Returns:
        True if the file matches the expected pattern, otherwise False.
    """
    return insur_type in filename and dataset_type in filename and filename.endswith(ext_type)


def sort_files_by_date(
    files: list[str],
    ext_type: str,
    time_column: str = "created_at",
) -> list[str]:
    """Sort source files by the earliest date inferred from content or filename.

    Args:
        files: Source file paths to sort.
        ext_type: File extension used for filename parsing fallback.
        time_column: Payload timestamp column to inspect first.

    Returns:
        Deterministically sorted file path list.
    """
    return sorted(
        files,
        key=lambda file_path: (
            extract_source_file_date(
                file_path=file_path,
                ext_type=ext_type,
                time_column=time_column,
            ),
            file_path,
        ),
    )


def list_source_files(
    directory_path: str,
    insur_type: str,
    dataset_type: str,
    ext_type: str,
    time_column: str = "created_at",
) -> list[str]:
    """List matching source files from a landing directory.

    Args:
        directory_path: Source directory to scan.
        insur_type: Insurance type token expected in file names.
        dataset_type: Dataset type token expected in file names.
        ext_type: Expected file extension.
        time_column: Payload timestamp column used for sorting.

    Returns:
        Sorted list of matching source file paths.

    Raises:
        FileNotFoundError: If the source directory does not exist.
    """
    directory = Path(directory_path)

    if not directory.exists():
        raise FileNotFoundError(f"Directory not found: {directory_path}")

    matched_files = [
        str(directory / file_name)
        for file_name in os.listdir(directory)
        if is_matching_file(file_name, insur_type, dataset_type, ext_type)
    ]

    return (
        sort_files_by_date(matched_files, ext_type=ext_type, time_column=time_column)
        if matched_files
        else []
    )


def describe_source_file(file_path: str) -> SourceFileState:
    """Collect size, mtime, and digest state for a source file.

    Args:
        file_path: Source file path.

    Returns:
        Metadata snapshot for the file.
    """
    file_stat = Path(file_path).stat()
    return {
        "size_bytes": file_stat.st_size,
        "modified_time_ns": file_stat.st_mtime_ns,
        "content_digest": calculate_source_file_digest(file_path),
    }


def describe_source_files(file_paths: list[str]) -> SourceFileStates:
    """Collect metadata state for multiple source files.

    Args:
        file_paths: Source file paths to describe.

    Returns:
        Mapping of file paths to file state snapshots.
    """
    return {file_path: describe_source_file(file_path) for file_path in file_paths}


def calculate_source_file_digest(file_path: str) -> str:
    """Calculate a stable content digest for a source file.

    Args:
        file_path: Source file path.

    Returns:
        SHA-256 digest hex string.
    """
    digest = hashlib.sha256()

    with Path(file_path).open("rb") as source_file:
        for chunk in iter(lambda: source_file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def extract_source_file_date(
    file_path: str,
    ext_type: str,
    time_column: str = "created_at",
) -> tuple[int, int, int]:
    """Extract the earliest business date for a source file.

    Args:
        file_path: Source file path.
        ext_type: File extension used for filename parsing fallback.
        time_column: Payload timestamp column to inspect first.

    Returns:
        Earliest available year, month, and day tuple.

    Raises:
        ValueError: If neither payload timestamps nor the filename provide a
            parseable date.
    """
    payload_datetimes = _try_extract_source_file_payload_datetimes(
        file_path=file_path,
        time_column=time_column,
    )
    if payload_datetimes:
        earliest_datetime = min(payload_datetimes)
        return earliest_datetime.year, earliest_datetime.month, earliest_datetime.day

    filename_date = _extract_source_file_date_from_filename(file_path=file_path, ext_type=ext_type)
    if filename_date is not None:
        return filename_date

    raise ValueError(
        f"Source file '{Path(file_path).name}' does not contain a parseable date in its name "
        f"or any parseable '{time_column}' values in the JSON payload."
    )


def extract_source_file_year_month(
    file_path: str,
    ext_type: str,
    time_column: str = "created_at",
) -> tuple[int, int]:
    """Extract the primary year and month for a source file.

    Args:
        file_path: Source file path.
        ext_type: File extension used for filename parsing fallback.
        time_column: Payload timestamp column to inspect first.

    Returns:
        Year and month tuple derived from the earliest available business date.
    """
    year, month, _ = extract_source_file_date(
        file_path=file_path,
        ext_type=ext_type,
        time_column=time_column,
    )
    return year, month


def extract_source_file_year_months(
    file_path: str,
    ext_type: str,
    time_column: str = "created_at",
) -> set[tuple[int, int]]:
    """Extract all year-month pairs represented by a source file.

    Args:
        file_path: Source file path.
        ext_type: File extension used for filename parsing fallback.
        time_column: Payload timestamp column to inspect first.

    Returns:
        Set of year-month tuples represented in the file.

    Raises:
        ValueError: If neither payload timestamps nor the filename provide a
            parseable date.
    """
    payload_datetimes = _try_extract_source_file_payload_datetimes(
        file_path=file_path,
        time_column=time_column,
    )
    if payload_datetimes:
        return {(value.year, value.month) for value in payload_datetimes}

    filename_date = _extract_source_file_date_from_filename(file_path=file_path, ext_type=ext_type)
    if filename_date is not None:
        year, month, _ = filename_date
        return {(year, month)}

    raise ValueError(
        f"Source file '{Path(file_path).name}' does not contain a parseable date in its name "
        f"or any parseable '{time_column}' values in the JSON payload."
    )


def _extract_source_file_date_from_filename(
    file_path: str,
    ext_type: str,
) -> tuple[int, int, int] | None:
    file_name = Path(file_path).name
    if ext_type and file_name.endswith(ext_type):
        file_name = file_name[: -len(ext_type)]

    for pattern in SOURCE_FILE_DATE_PATTERNS:
        match = pattern.search(file_name)
        if match is not None:
            year, month, day = match.groups()
            return int(year), int(month), int(day)

    return None


def _extract_source_file_payload_datetimes(
    file_path: str,
    time_column: str,
) -> list[datetime]:
    with Path(file_path).open("r", encoding="utf-8") as source_file:
        payload = json.load(source_file)

    records = payload if isinstance(payload, list) else [payload]
    payload_datetimes: list[datetime] = []

    for record in records:
        if not isinstance(record, dict):
            continue

        created_at_value = record.get(time_column)
        if not isinstance(created_at_value, str) or not created_at_value.strip():
            continue

        payload_datetimes.append(_parse_created_at(created_at_value.strip()))

    if payload_datetimes:
        return payload_datetimes

    raise ValueError(
        f"Source file '{Path(file_path).name}' does not contain a parseable date in its name "
        f"or any parseable '{time_column}' values in the JSON payload."
    )


def _try_extract_source_file_payload_datetimes(
    file_path: str,
    time_column: str,
) -> list[datetime]:
    try:
        return _extract_source_file_payload_datetimes(
            file_path=file_path,
            time_column=time_column,
        )
    except ValueError:
        return []


def _parse_created_at(created_at_value: str) -> datetime:
    for supported_format in SUPPORTED_CREATED_AT_FORMATS:
        try:
            return datetime.strptime(created_at_value, supported_format)
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(created_at_value)
    except ValueError as exc:
        raise ValueError(f"Unsupported created_at value '{created_at_value}'.") from exc
