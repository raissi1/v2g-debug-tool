"""File detection layer for generic V2G session inputs."""

from __future__ import annotations

from pathlib import Path

from core.models import DetectedFiles

LOG_EXTENSIONS = {".log", ".txt", ".jsonl"}
PCAP_EXTENSIONS = {".pcap", ".pcapng"}
MEASURE_EXTENSIONS = {".csv", ".tsv", ".json"}

MEASURE_HINTS = {"measure", "meter", "telemetry", "signal", "timeseries"}

ENERGY_MANAGER_HINTS = {"energymanager", "energy_manager"}
CHARGER_APP_HINTS = {"chargerapp", "charger_app"}
METER_DISPATCHER_HINTS = {"iotc-meter-dispatcher", "meter_dispatcher", "dispatcher"}


def _matches_any_hint(path: Path, hints: set[str]) -> bool:
    lower_name = path.name.lower()
    return any(hint in lower_name for hint in hints)


def _is_measure_file(path: Path) -> bool:
    return _matches_any_hint(path, MEASURE_HINTS)


def detect_session_files(root: Path) -> DetectedFiles:
    """Detect session artifacts recursively.

    Expected highlights:
      - EnergyManager logs
      - ChargerApp logs
      - iotc-meter-dispatcher logs
      - PCAP
      - measurement files
    """
    root = root.expanduser().resolve()
    if not root.exists() or not root.is_dir():
        raise ValueError(f"Invalid session folder: {root}")

    detected = DetectedFiles(root=root)

    for path in root.rglob("*"):
        if not path.is_file():
            continue

        suffix = path.suffix.lower()

        if _matches_any_hint(path, ENERGY_MANAGER_HINTS):
            detected.energy_manager.append(path)
        elif _matches_any_hint(path, CHARGER_APP_HINTS):
            detected.charger_app.append(path)
        elif _matches_any_hint(path, METER_DISPATCHER_HINTS):
            detected.iotc_meter_dispatcher.append(path)

        if suffix in PCAP_EXTENSIONS:
            detected.pcaps.append(path)
        elif suffix in LOG_EXTENSIONS:
            detected.logs.append(path)
        elif suffix in MEASURE_EXTENSIONS and _is_measure_file(path):
            detected.measures.append(path)
        else:
            detected.others.append(path)

    for attr in (
        "energy_manager",
        "charger_app",
        "iotc_meter_dispatcher",
        "pcaps",
        "measures",
        "logs",
        "others",
    ):
        getattr(detected, attr).sort()

    return detected
