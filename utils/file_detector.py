"""File detection layer for generic V2G session inputs."""

from __future__ import annotations

from pathlib import Path

from core.models import DetectedFiles

LOG_EXTENSIONS = {".log", ".txt", ".jsonl"}
LOG_COMPRESSION_SUFFIXES = {".log.gz", ".txt.gz", ".jsonl.gz"}
PCAP_EXTENSIONS = {".pcap", ".pcapng", ".cap"}
PCAP_COMPRESSION_SUFFIXES = {".pcap.gz", ".pcapng.gz", ".cap.gz"}
MEASURE_EXTENSIONS = {".csv", ".tsv", ".json"}
CONFIG_EXTENSIONS = {".properties", ".conf", ".ini", ".yaml", ".yml"}

MEASURE_HINTS = {"measure", "meter", "telemetry", "signal", "timeseries"}
ENERGY_MANAGER_HINTS = {"energymanager", "energy_manager"}
CHARGER_APP_HINTS = {"chargerapp", "charger_app"}
METER_DISPATCHER_HINTS = {"iotc-meter-dispatcher", "meter_dispatcher", "dispatcher"}

EXTENSIONLESS_LOG_DIR_HINTS = {
    "log",
    "energymanager",
    "chargerapp",
    "iotc-meter-dispatcher",
}
EXTENSIONLESS_LOG_NAME_HINTS = {"log", "trace", "journal", "event"}


def _matches_any_hint(path: Path, hints: set[str]) -> bool:
    lower_name = path.name.lower()
    return any(hint in lower_name for hint in hints)


def _path_parts_lower(path: Path) -> set[str]:
    return {part.lower() for part in path.parts}


def _is_measure_file(path: Path) -> bool:
    return _matches_any_hint(path, MEASURE_HINTS)


def _is_config_file(path: Path) -> bool:
    lower_name = path.name.lower()
    return path.suffix.lower() in CONFIG_EXTENSIONS or lower_name.endswith(".properties")


def _is_pcap_candidate(path: Path) -> bool:
    lower_name = path.name.lower()
    suffix = path.suffix.lower()
    parts = _path_parts_lower(path)

    if suffix in PCAP_EXTENSIONS or any(lower_name.endswith(suf) for suf in PCAP_COMPRESSION_SUFFIXES):
        return True

    # In many datasets, netlogger contains capture artifacts, sometimes extensionless.
    if "netlogger" in parts and (suffix == "" or "pcap" in lower_name):
        return True

    return False


def _is_log_candidate(path: Path) -> bool:
    lower_name = path.name.lower()
    suffix = path.suffix.lower()

    if suffix in LOG_EXTENSIONS or any(lower_name.endswith(suf) for suf in LOG_COMPRESSION_SUFFIXES):
        return True

    if suffix:
        return False

    parts = _path_parts_lower(path.parent)
    in_log_folder = any(hint in parts for hint in EXTENSIONLESS_LOG_DIR_HINTS)
    log_like_name = any(hint in lower_name for hint in EXTENSIONLESS_LOG_NAME_HINTS)
    return in_log_folder or log_like_name


def detect_session_files(root: Path) -> DetectedFiles:
    """Detect session artifacts recursively for generic V2G debugging."""
    root = root.expanduser().resolve()
    if not root.exists() or not root.is_dir():
        raise ValueError(f"Invalid session folder: {root}")

    detected = DetectedFiles(root=root)

    for path in root.rglob("*"):
        if not path.is_file():
            continue

        if _matches_any_hint(path, ENERGY_MANAGER_HINTS):
            detected.energy_manager.append(path)
        elif _matches_any_hint(path, CHARGER_APP_HINTS):
            detected.charger_app.append(path)
        elif _matches_any_hint(path, METER_DISPATCHER_HINTS):
            detected.iotc_meter_dispatcher.append(path)

        suffix = path.suffix.lower()
        if _is_config_file(path):
            detected.configs.append(path)
        elif _is_pcap_candidate(path):
            detected.pcaps.append(path)
        elif suffix in MEASURE_EXTENSIONS and _is_measure_file(path):
            detected.measures.append(path)
        elif _is_log_candidate(path):
            detected.logs.append(path)
        else:
            detected.others.append(path)

    for attr in (
        "energy_manager",
        "charger_app",
        "iotc_meter_dispatcher",
        "pcaps",
        "measures",
        "logs",
        "configs",
        "others",
    ):
        getattr(detected, attr).sort()

    return detected
