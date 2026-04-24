"""EnergyManager log parser for generic V2G debugging."""

from __future__ import annotations

import gzip
import re
from datetime import datetime
from pathlib import Path
from typing import Iterable

from core.models import Event

ISO_TS_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?")
NUMBER = r"([-+]?\d+(?:[\.,]\d+)?)"

EVENT_PATTERNS: list[tuple[str, tuple[str, ...]]] = [
    ("error", (" error ", "exception", "failed", "fatal", "traceback")),
    ("warning", (" warning ", "warn", "degraded", "fallback")),
    ("timeout", ("timeout", "timed out", "watchdog", "no response")),
    ("gridcodes", ("gridcode", "grid code", "lvrt", "hvrt", "frt")),
    ("power_limit", ("power limit", "curtail", "derating", "max power", "limit=")),
    ("protocol_event", ("iso15118", "din70121", "ocpp", "slac", "session setup", "authorization")),
    ("session_event", ("session start", "session stop", "charging started", "charging stopped", "plug in", "unplug")),
]

PHYSICAL_PATTERNS = {
    "Ptarget": [re.compile(rf"(?:ptarget|p_target|active\s*power\s*setpoint|setpoint\s*p|p\s*set)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "Qtarget": [re.compile(rf"(?:qtarget|q_target|reactive\s*power\s*setpoint|setpoint\s*q|q\s*set)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "P": [re.compile(rf"(?:p\s*meas(?:ured)?|measured\s*power|active\s*power)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "Q": [re.compile(rf"(?:q\s*meas(?:ured)?|reactive\s*power)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "U": [re.compile(rf"(?:voltage|tension|\bu\b)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "AvailableDischargePower": [re.compile(rf"(?:availabledischargepower|available\s*discharge\s*power)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "Pcalc": [re.compile(rf"(?:recalculated\s*setpoint\s*p|calculated\s*p|pcalc)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "Qcalc": [re.compile(rf"(?:recalculated\s*setpoint\s*q|calculated\s*q|qcalc)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "Smax": [re.compile(rf"(?:smax|max\s*apparent\s*power)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "derating": [re.compile(rf"(?:derating|derate|limit\s*factor)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
}
STATE_PATTERNS = [
    ("start", re.compile(r"session\s*start|start\s*session|charging\s*started", re.IGNORECASE)),
    ("stop", re.compile(r"session\s*stop|stop\s*session|charging\s*stopped", re.IGNORECASE)),
    ("charging", re.compile(r"\bcharging\b", re.IGNORECASE)),
]


def _open_text(path: Path):
    if path.name.lower().endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="ignore")
    return path.open("r", encoding="utf-8", errors="ignore")


def _to_float(raw: str) -> float | None:
    try:
        return float(raw.replace(",", "."))
    except ValueError:
        return None


def _extract_physical_signals(line: str) -> dict[str, float | str]:
    signals: dict[str, float | str] = {}
    for key, patterns in PHYSICAL_PATTERNS.items():
        for pattern in patterns:
            m = pattern.search(line)
            if m:
                value = _to_float(m.group(1))
                if value is not None:
                    signals[key] = value
                    break

    for state_name, pattern in STATE_PATTERNS:
        if pattern.search(line):
            signals["state"] = state_name
            break

    return signals


def _physical_event_type(signals: dict[str, float | str]) -> str | None:
    if "state" in signals:
        return "state_change"
    if "Ptarget" in signals or "Qtarget" in signals:
        return "setpoint"
    if any(k in signals for k in ("Smax", "derating", "Pcalc", "Qcalc")):
        return "power_limit"
    if any(k in signals for k in ("P", "Q", "U", "AvailableDischargePower")):
        return "physical_measurement"
    return None


def _parse_timestamp(line: str) -> datetime | None:
    match = ISO_TS_PATTERN.search(line)
    if not match:
        return None
    raw = match.group(0)
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _classify_event(line: str) -> str:
    normalized = f" {line.lower()} "
    for event_type, keywords in EVENT_PATTERNS:
        if any(keyword in normalized for keyword in keywords):
            return event_type
    return "log_line"


def parse_energy_manager(path: Path) -> Iterable[Event]:
    with _open_text(path) as stream:
        for idx, line in enumerate(stream, 1):
            text = line.strip()
            if not text:
                continue

            signals = _extract_physical_signals(text)
            base_event_type = _classify_event(text)
            event_type = _physical_event_type(signals) or base_event_type

            payload = {
                "line": idx,
                "path": str(path),
                "parser": "energy_manager",
                "source_group": "energy_manager",
                "future_diagnostic_side": "to_be_inferred",
                "base_event_type": base_event_type,
            }
            payload.update(signals)

            yield Event(
                timestamp=_parse_timestamp(text),
                source=path.name,
                event_type=event_type,
                message=text,
                payload=payload,
            )
