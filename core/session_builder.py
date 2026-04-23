"""Build a unified timeline from detected V2G session artifacts."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

from core.models import DetectedFiles, Event

ISO_TS_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?")


def _parse_timestamp(raw: str) -> datetime | None:
    raw = raw.strip()
    if not raw:
        return None

    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _extract_ts_from_text(line: str) -> datetime | None:
    match = ISO_TS_PATTERN.search(line)
    if not match:
        return None
    return _parse_timestamp(match.group(0))


def _events_from_log(path: Path) -> Iterable[Event]:
    with path.open("r", encoding="utf-8", errors="ignore") as stream:
        for idx, line in enumerate(stream, 1):
            stripped = line.strip()
            if not stripped:
                continue
            yield Event(
                timestamp=_extract_ts_from_text(stripped),
                source=path.name,
                event_type="log_line",
                message=stripped,
                payload={"line": idx, "path": str(path)},
            )


def _events_from_measure(path: Path) -> Iterable[Event]:
    suffix = path.suffix.lower()

    if suffix in {".csv", ".tsv"}:
        sep = "\t" if suffix == ".tsv" else ","
        frame = pd.read_csv(path, sep=sep)
        if frame.empty:
            return

        ts_column = next((c for c in frame.columns if "time" in c.lower() or "date" in c.lower()), None)
        for idx, row in frame.iterrows():
            ts = _parse_timestamp(str(row[ts_column])) if ts_column else None
            payload = row.to_dict()
            yield Event(
                timestamp=ts,
                source=path.name,
                event_type="measure_row",
                message=f"Measurement row #{idx}",
                payload=payload,
            )

    elif suffix == ".json":
        with path.open("r", encoding="utf-8", errors="ignore") as stream:
            content = json.load(stream)

        records = content if isinstance(content, list) else [content]
        for idx, rec in enumerate(records):
            if not isinstance(rec, dict):
                rec = {"value": rec}
            ts_key = next((k for k in rec.keys() if "time" in k.lower() or "date" in k.lower()), None)
            ts = _parse_timestamp(str(rec[ts_key])) if ts_key else None
            yield Event(
                timestamp=ts,
                source=path.name,
                event_type="measure_record",
                message=f"Measurement record #{idx}",
                payload=rec,
            )


def _events_from_pcap(path: Path) -> Iterable[Event]:
    # Generic fallback: we only register file metadata, binary packets are not parsed here.
    stat = path.stat()
    yield Event(
        timestamp=datetime.fromtimestamp(stat.st_mtime),
        source=path.name,
        event_type="pcap_file",
        message="PCAP detected (payload not parsed in this generic module)",
        payload={"size_bytes": stat.st_size, "path": str(path)},
    )


def build_session_timeline(detected: DetectedFiles) -> pd.DataFrame:
    """Create a timeline DataFrame ordered by timestamp when available."""
    events: list[Event] = []

    for log in detected.logs:
        events.extend(_events_from_log(log))
    for measure in detected.measures:
        events.extend(_events_from_measure(measure))
    for pcap in detected.pcaps:
        events.extend(_events_from_pcap(pcap))

    frame = pd.DataFrame(
        [
            {
                "timestamp": e.timestamp.isoformat() if e.timestamp else None,
                "source": e.source,
                "event_type": e.event_type,
                "message": e.message,
                "payload": e.payload,
            }
            for e in events
        ]
    )

    if frame.empty:
        return frame

    frame["_sort_ts"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame = frame.sort_values(by=["_sort_ts", "source", "event_type"], na_position="last").drop(columns=["_sort_ts"])
    frame = frame.reset_index(drop=True)
    return frame
