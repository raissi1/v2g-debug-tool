"""File detection layer for generic V2G session inputs."""

from __future__ import annotations

from pathlib import Path

from core.models import DetectedFiles

LOG_EXTENSIONS = {".log", ".txt", ".jsonl"}
PCAP_EXTENSIONS = {".pcap", ".pcapng"}
MEASURE_EXTENSIONS = {".csv", ".tsv", ".json"}

MEASURE_HINTS = {"measure", "meter", "telemetry", "signal", "timeseries"}


def _is_measure_file(path: Path) -> bool:
    lower_name = path.name.lower()
    return any(hint in lower_name for hint in MEASURE_HINTS)


def detect_session_files(root: Path) -> DetectedFiles:
    """Detect session artifacts (logs, pcaps and measure files) recursively."""
    root = root.expanduser().resolve()
    if not root.exists() or not root.is_dir():
        raise ValueError(f"Invalid session folder: {root}")

    detected = DetectedFiles(root=root)

    for path in root.rglob("*"):
        if not path.is_file():
            continue

        suffix = path.suffix.lower()
        if suffix in PCAP_EXTENSIONS:
            detected.pcaps.append(path)
        elif suffix in LOG_EXTENSIONS:
            detected.logs.append(path)
        elif suffix in MEASURE_EXTENSIONS and _is_measure_file(path):
            detected.measures.append(path)
        else:
            detected.others.append(path)

    detected.logs.sort()
    detected.pcaps.sort()
    detected.measures.sort()
    detected.others.sort()
    return detected
