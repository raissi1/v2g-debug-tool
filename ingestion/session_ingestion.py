"""Session ingestion orchestration (folder/zip -> detected files)."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import BinaryIO

from core.models import DetectedFiles
from utils.file_detector import detect_session_files
from utils.zip_loader import extract_zip_to_temp


def ingest_session_folder(folder: Path) -> DetectedFiles:
    return detect_session_files(folder)


def ingest_session_zip(upload_name: str, payload: bytes) -> tuple[DetectedFiles, tempfile.TemporaryDirectory[str]]:
    temp_dir = tempfile.TemporaryDirectory(prefix="v2g_session_")
    zip_path = Path(temp_dir.name) / upload_name
    zip_path.write_bytes(payload)
    extracted = extract_zip_to_temp(zip_path, Path(temp_dir.name))
    return detect_session_files(extracted), temp_dir
