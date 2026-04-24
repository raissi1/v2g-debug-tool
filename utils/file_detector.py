"""Session file detection for IoT.ON /var/aux and full test folders."""

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


def _is_pcap_file(name: str) -> bool:
    lower = name.lower()
    return (
        lower.endswith(".pcap")
        or lower.endswith(".pcapng")
        or lower.endswith(".pcap.gz")
        or lower.endswith(".pcapng.gz")
    )


def _is_dewesoft_csv(path: Path) -> bool:
    if path.suffix.lower() != ".csv":
        return False
    lower = str(path).lower()
    return any(token in lower for token in ("dewesoft", "dewe", "acquisition", "measure", "measurement", "daq"))


def _is_dewesoft_binary(path: Path) -> bool:
    return path.suffix.lower() in {".d7d", ".dxd"}


def _detect_full_test_structure(path: Path) -> str | None:
    lower_parts = {part.lower() for part in path.parts}
    if "acquisitions" in lower_parts:
        return "acquisitions"
    if "log" in lower_parts:
        return "log"
    if "pcap" in lower_parts or "pcaps" in lower_parts:
        return "pcap"
    return None


def detect_session_files(root: Path) -> DetectedFiles:
    """Detect relevant files for generic V2G debug workflow."""
    root = root.expanduser().resolve()
    if not root.exists() or not root.is_dir():
        raise ValueError(f"Invalid session folder: {root}")

    detected = DetectedFiles(root=root)
    aux_root = _find_aux_root(root)
    detected.aux_root = aux_root

    for path in root.rglob("*"):
        if not path.is_file():
            continue

        # Dewesoft acquisitions are accepted globally.
        if _is_dewesoft_binary(path):
            detected.dewesoft_raw.append(path)
            continue
        if _is_dewesoft_csv(path):
            detected.dewesoft_csv.append(path)
            continue

        # Mode 1: strict /var/aux package parsing
        if aux_root is not None:
            try:
                rel = path.relative_to(aux_root)
                if not rel.parts:
                    detected.ignored_files.append(path)
                    continue

                top = rel.parts[0]
                if top not in ALLOWED_AUX_DIRS:
                    detected.ignored_files.append(path)
                    continue
                if _is_config_file(path):
                    detected.ignored_files.append(path)
                    continue

                if top == "ChargerApp":
                    if _is_log_file(path.name):
                        detected.charger_app.append(path)
                    else:
                        detected.ignored_files.append(path)
                elif top == "EnergyManager":
                    if _is_log_file(path.name):
                        detected.energy_manager.append(path)
                    else:
                        detected.ignored_files.append(path)
                elif top == "iotc-meter-dispatcher":
                    if _is_log_file(path.name):
                        detected.iotc_meter_dispatcher.append(path)
                    else:
                        detected.ignored_files.append(path)
                elif top == "netlogger":
                    if _is_pcap_file(path.name):
                        detected.netlogger_pcaps.append(path)
                    elif path.name.lower() == "netlogger.log" or NETLOGGER_LOG_PATTERN.fullmatch(path.name.lower()):
                        detected.netlogger_logs.append(path)
                    else:
                        detected.ignored_files.append(path)
                continue
            except ValueError:
                # not in /var/aux, try full-folder mode below
                pass

        # Mode 2: full test folder (Acquisitions/Log/pcap)
        folder_type = _detect_full_test_structure(path)
        if folder_type == "log":
            if _is_config_file(path):
                detected.ignored_files.append(path)
            elif _is_log_file(path.name):
                low = path.name.lower()
                if "charg" in low:
                    detected.charger_app.append(path)
                elif "energy" in low:
                    detected.energy_manager.append(path)
                elif "meter" in low or "dispatch" in low:
                    detected.iotc_meter_dispatcher.append(path)
                else:
                    detected.generic_logs.append(path)
            else:
                detected.ignored_files.append(path)
        elif folder_type == "pcap":
            if _is_pcap_file(path.name):
                detected.generic_pcaps.append(path)
            else:
                detected.ignored_files.append(path)
        elif folder_type == "acquisitions":
            # non-csv/non-d7d/non-dxd in Acquisitions are ignored for now.
            detected.ignored_files.append(path)
        else:
            detected.ignored_files.append(path)

    for attr in (
        "charger_app",
        "energy_manager",
        "iotc_meter_dispatcher",
        "netlogger_pcaps",
        "netlogger_logs",
        "generic_logs",
        "generic_pcaps",
        "dewesoft_csv",
        "dewesoft_raw",
        "ignored_files",
    ):
        getattr(detected, attr).sort()

    return detected
