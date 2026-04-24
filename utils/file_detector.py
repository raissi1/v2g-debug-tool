"""Strict session file detection scoped to /var/aux package contents."""

from __future__ import annotations

import re
from pathlib import Path

from core.models import DetectedFiles

ALLOWED_AUX_DIRS = {"ChargerApp", "EnergyManager", "iotc-meter-dispatcher", "netlogger"}
CONFIG_EXTENSIONS = {".properties", ".conf", ".ini", ".yaml", ".yml", ".json", ".xml", ".cfg"}

LOG_PATTERN = re.compile(r".+\.log(?:\..+)?\.gz$", re.IGNORECASE)
NETLOGGER_LOG_PATTERN = re.compile(r"^netlogger\.log(?:\..+)?\.gz$", re.IGNORECASE)


def _find_aux_root(root: Path) -> Path | None:
    root = root.resolve()
    if root.name == "aux" and root.parent.name == "var":
        return root
    candidate = root / "aux"
    if root.name == "var" and candidate.is_dir():
        return candidate
    for aux_candidate in root.rglob("aux"):
        if aux_candidate.is_dir() and aux_candidate.parent.name == "var":
            return aux_candidate
    return None


def _is_config_file(path: Path) -> bool:
    suffix = path.suffix.lower()
    return suffix in CONFIG_EXTENSIONS or path.name.lower().endswith(".properties")


def _is_log_file(name: str) -> bool:
    lower = name.lower()
    return lower.endswith(".log") or LOG_PATTERN.fullmatch(lower) is not None


def _is_netlogger_pcap(name: str) -> bool:
    lower = name.lower()
    return lower.endswith(".pcap") or lower.endswith(".pcap.gz")


def _is_netlogger_log(name: str) -> bool:
    lower = name.lower()
    return lower == "netlogger.log" or NETLOGGER_LOG_PATTERN.fullmatch(lower) is not None


def _is_dewesoft_csv(path: Path) -> bool:
    if path.suffix.lower() != ".csv":
        return False
    lower = str(path).lower()
    return any(token in lower for token in ("dewesoft", "dewe", "measure", "measurement", "daq"))


def detect_session_files(root: Path) -> DetectedFiles:
    """Detect relevant files for generic V2G debug workflow."""
    root = root.expanduser().resolve()
    if not root.exists() or not root.is_dir():
        raise ValueError(f"Invalid session folder: {root}")

    detected = DetectedFiles(root=root)
    aux_root = _find_aux_root(root)

    if aux_root is None:
        for path in root.rglob("*"):
            if path.is_file():
                if _is_dewesoft_csv(path):
                    detected.dewesoft_csv.append(path)
                else:
                    detected.ignored_files.append(path)
        detected.dewesoft_csv.sort()
        detected.ignored_files.sort()
        return detected

    detected.aux_root = aux_root

    for path in root.rglob("*"):
        if not path.is_file():
            continue

        # Dewesoft CSV can be outside /var/aux.
        if _is_dewesoft_csv(path):
            detected.dewesoft_csv.append(path)
            continue

        try:
            path.relative_to(aux_root)
        except ValueError:
            detected.ignored_files.append(path)

    for path in aux_root.rglob("*"):
        if not path.is_file():
            continue

        relative_parts = path.relative_to(aux_root).parts
        if not relative_parts:
            detected.ignored_files.append(path)
            continue

        top_level = relative_parts[0]
        filename = path.name

        if top_level not in ALLOWED_AUX_DIRS:
            detected.ignored_files.append(path)
            continue

        if _is_config_file(path):
            detected.ignored_files.append(path)
            continue

        if top_level == "ChargerApp":
            if _is_log_file(filename):
                detected.charger_app.append(path)
            else:
                detected.ignored_files.append(path)
        elif top_level == "EnergyManager":
            if _is_log_file(filename):
                detected.energy_manager.append(path)
            else:
                detected.ignored_files.append(path)
        elif top_level == "iotc-meter-dispatcher":
            if _is_log_file(filename):
                detected.iotc_meter_dispatcher.append(path)
            else:
                detected.ignored_files.append(path)
        elif top_level == "netlogger":
            if _is_netlogger_pcap(filename):
                detected.netlogger_pcaps.append(path)
            elif _is_netlogger_log(filename):
                detected.netlogger_logs.append(path)
            else:
                detected.ignored_files.append(path)

    for attr in (
        "charger_app",
        "energy_manager",
        "iotc_meter_dispatcher",
        "netlogger_pcaps",
        "netlogger_logs",
        "dewesoft_csv",
        "ignored_files",
    ):
        getattr(detected, attr).sort()

    return detected
