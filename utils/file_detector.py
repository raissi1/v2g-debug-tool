"""File detection layer for generic V2G session inputs."""

from __future__ import annotations

from pathlib import Path

from core.models import DetectedFiles

LOG_EXTENSIONS = {".log", ".txt", ".jsonl"}
PCAP_EXTENSIONS = {".pcap", ".pcapng", ".cap"}
MEASURE_EXTENSIONS = {".csv", ".tsv", ".json"}

MEASURE_HINTS = {"measure", "meter", "telemetry", "signal", "timeseries"}

ENERGY_MANAGER_HINTS = {"energymanager", "energy_manager"}
CHARGER_APP_HINTS = {"chargerapp", "charger_app"}
METER_DISPATCHER_HINTS = {"iotc-meter-dispatcher", "meter_dispatcher", "dispatcher"}

EXTENSIONLESS_LOG_DIR_HINTS = {
    "log",
    "energymanager",
    "chargerapp",
    "iotc-meter-dispatcher",
    "netlogger",
}
EXTENSIONLESS_LOG_NAME_HINTS = {"log", "trace", "journal", "event"}


def _matches_any_hint(path: Path, hints: set[str]) -> bool:
    lower_name = path.name.lower()
    return any(hint in lower_name for hint in hints)


def _path_parts_lower(path: Path) -> set[str]:
    return {part.lower() for part in path.parts}


def _is_measure_file(path: Path) -> bool:
    return _matches_any_hint(path, MEASURE_HINTS)


def _is_pcap_candidate(path: Path) -> bool:
    lower_name = path.name.lower()
    suffix = path.suffix.lower()
    parts = _path_parts_lower(path)

    if suffix in PCAP_EXTENSIONS:
        return True

    # Field case reported by user: PCAP artifacts located under `netlogger`
    # can be extensionless or have non-standard names.
    if "netlogger" in parts and (suffix == "" or "pcap" in lower_name):
        return True

    return False


def _is_extensionless_log(path: Path) -> bool:
    if path.suffix:
        return False

    lower_name = path.name.lower()
    parts = _path_parts_lower(path.parent)
    in_log_folder = any(hint in parts for hint in EXTENSIONLESS_LOG_DIR_HINTS)
    log_like_name = any(hint in lower_name for hint in EXTENSIONLESS_LOG_NAME_HINTS)
    return in_log_folder or log_like_name


def detect_session_files(root: Path) -> DetectedFiles:
    """Detect session artifacts recursively.

    Expected highlights:
      - EnergyManager logs
      - ChargerApp logs
      - iotc-meter-dispatcher logs
      - PCAP (including netlogger folder artifacts)
      - measurement files
      - extensionless log files
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

        if _is_pcap_candidate(path):
            detected.pcaps.append(path)
        elif suffix in LOG_EXTENSIONS or _is_extensionless_log(path):
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
