"""ChargerApp log parser for generic V2G debugging."""

from __future__ import annotations

import gzip
import re
from datetime import datetime
from pathlib import Path
from typing import Iterable

from core.models import Event

ISO_TS_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?")

EVENT_PATTERNS: list[tuple[str, tuple[str, ...]]] = [
    ("error", (" error ", "exception", "failed", "fatal", "cannot")),
    ("warning", (" warning ", "warn", "retry", "unstable")),
    ("timeout", ("timeout", "timed out", "no response", "waiting too long")),
    ("session_event", ("session start", "session stop", "charging started", "charging stopped", "ev connected", "ev disconnected")),
    ("protocol_event", ("iso15118", "ocpp", "din70121", "slac", "sdp", "handshake")),
    ("gridcodes", ("gridcode", "grid code", "lvrt", "hvrt", "frt")),
    ("setpoint_change", ("setpoint", "set-point", "target current", "target power", "p_set", "q_set")),
    ("power_limit", ("power limit", "max current", "max power", "curtail", "limited")),
]


def _open_text(path: Path):
    if path.name.lower().endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="ignore")
    return path.open("r", encoding="utf-8", errors="ignore")


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


def parse_charger_app(path: Path) -> Iterable[Event]:
    with _open_text(path) as stream:
        for idx, line in enumerate(stream, 1):
            text = line.strip()
            if not text:
                continue
            yield Event(
                timestamp=_parse_timestamp(text),
                source=path.name,
                event_type=_classify_event(text),
                message=text,
                payload={
                    "line": idx,
                    "path": str(path),
                    "parser": "charger_app",
                    "source_group": "charger_app",
                    "future_diagnostic_side": "to_be_inferred",
                },
            )
