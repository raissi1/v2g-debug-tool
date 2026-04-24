"""iotc-meter-dispatcher parser for generic V2G debugging."""

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
    ("error", (" error ", "exception", "failed", "decode error", "invalid")),
    ("warning", (" warning ", "warn", "drift", "outlier")),
    ("timeout", ("timeout", "timed out", "stale", "no sample")),
    ("power_limit", ("power limit", "clamp", "limited", "cap", "max power")),
    ("gridcodes", ("gridcode", "grid code", "frequency event", "voltage event")),
    ("protocol_event", ("modbus", "mqtt", "publish", "packet", "frame")),
    ("session_event", ("session start", "session stop", "sampling started", "sampling stopped")),
]

PHYSICAL_PATTERNS = {
    "Ptarget": [re.compile(rf"(?:ptarget|p_target|requested\s*power|target\s*power)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "Qtarget": [re.compile(rf"(?:qtarget|q_target|requested\s*reactive\s*power|target\s*reactive)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "P": [re.compile(rf"(?:p\s*meas(?:ured)?|measured\s*power|active\s*power)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "Q": [re.compile(rf"(?:q\s*meas(?:ured)?|reactive\s*power)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "U": [re.compile(rf"(?:voltage|tension|\bu\b)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "frequency": [re.compile(rf"(?:freq(?:uency)?|\bhz\b)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "AvailableDischargePower": [re.compile(rf"(?:availabledischargepower|available\s*discharge\s*power)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
}
STATE_PATTERNS = [
    ("start", re.compile(r"session\s*start|start\s*session|sampling\s*started", re.IGNORECASE)),
    ("stop", re.compile(r"session\s*stop|stop\s*session|sampling\s*stopped", re.IGNORECASE)),
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


    # Slice parser: e.g. "Slice ... P=12.3 Q=-1.2 U=229.8 F=50.0"
    slice_match = re.search(r"slice[^\n]*", line, re.IGNORECASE)
    if slice_match:
        slice_text = slice_match.group(0)
        for key, pattern in {
            "P": re.compile(rf"\bP\s*[=:]\s*{NUMBER}", re.IGNORECASE),
            "Q": re.compile(rf"\bQ\s*[=:]\s*{NUMBER}", re.IGNORECASE),
            "U": re.compile(rf"\bU\s*[=:]\s*{NUMBER}", re.IGNORECASE),
            "frequency": re.compile(rf"(?:\bF\b|freq(?:uency)?)\s*[=:]\s*{NUMBER}", re.IGNORECASE),
        }.items():
            m = pattern.search(slice_text)
            if m:
                value = _to_float(m.group(1))
                if value is not None:
                    signals[key] = value

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


def parse_meter_dispatcher(path: Path) -> Iterable[Event]:
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
                "parser": "meter_dispatcher",
                "source_group": "meter_dispatcher",
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
