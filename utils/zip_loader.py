"""Utilities to safely extract ZIP archives for session analysis."""

from __future__ import annotations

import zipfile
from pathlib import Path


def _safe_extract_path(base_dir: Path, member_name: str) -> Path:
    """Compute a safe extraction target path and prevent path traversal."""
    target = (base_dir / member_name).resolve()
    if not str(target).startswith(str(base_dir.resolve())):
        raise ValueError(f"Unsafe ZIP member path detected: {member_name}")
    return target


def extract_zip_to_temp(zip_path: Path, destination_root: Path) -> Path:
    """Extract a ZIP archive into a dedicated folder inside destination_root.

    Args:
        zip_path: Path to the input ZIP archive.
        destination_root: Existing directory that will receive extracted files.

    Returns:
        Path to the extraction directory.
    """
    if not zip_path.exists() or zip_path.suffix.lower() != ".zip":
        raise ValueError(f"Invalid ZIP path: {zip_path}")

    destination_root.mkdir(parents=True, exist_ok=True)
    extract_dir = destination_root / f"extracted_{zip_path.stem}"
    extract_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as archive:
        for member in archive.infolist():
            safe_target = _safe_extract_path(extract_dir, member.filename)
            if member.is_dir():
                safe_target.mkdir(parents=True, exist_ok=True)
            else:
                safe_target.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(member, "r") as src, safe_target.open("wb") as dst:
                    dst.write(src.read())

    return extract_dir
