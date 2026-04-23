"""Build a unified timeline from detected V2G session artifacts."""

from __future__ import annotations

import gzip
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

from core.models import DetectedFiles, Event
from parsers.charger_app import parse_charger_app
from parsers.energy_manager import parse_energy_manager
from parsers.meter_dispatcher import parse_meter_dispatcher

ISO_TS_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?")

GENERIC_PATTERNS: list[tuple[str, tuple[str, ...]]] = [
    ("error", (" error ", "exception", "failed", "fatal")),
    ("warning", (" warning ", "warn", "retry", "degraded")),
    ("timeout", ("timeout", "timed out", "no response")),
    ("gridcodes", ("gridcode", "grid code", "lvrt", "hvrt", "frt")),
    ("setpoint_change", ("setpoint", "set-point", "set point", "p_set", "q_set", "target power")),
    ("power_limit", ("power limit", "curtail", "derating", "limited", "max power")),
    ("protocol_event", ("iso15118", "ocpp", "din70121", "slac", "handshake", "session setup")),
]

SESSION_MARKERS = ("session", "charging", "plug", "unplug", "authorize", "start", "stop", "transaction")


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


def _classify_generic_line(line: str) -> str:
    normalized = f" {line.lower()} "
    for event_type, keywords in GENERIC_PATTERNS:
        if any(keyword in normalized for keyword in keywords):
            return event_type
    return "log_line"


def _open_text(path: Path):
    if path.name.lower().endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="ignore")
    return path.open("r", encoding="utf-8", errors="ignore")


def _events_from_log(path: Path) -> Iterable[Event]:
    with _open_text(path) as stream:
        for idx, line in enumerate(stream, 1):
            stripped = line.strip()
            if not stripped:
                continue
            yield Event(
                timestamp=_extract_ts_from_text(stripped),
                source=path.name,
                event_type=_classify_generic_line(stripped),
                message=stripped,
                payload={"line": idx, "path": str(path), "parser": "generic"},
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
                event_type="measure_row",
                message=f"Measurement record #{idx}",
                payload=rec,
            )


def _events_from_pcap(path: Path) -> Iterable[Event]:
    stat = path.stat()
    yield Event(
        timestamp=datetime.fromtimestamp(stat.st_mtime),
        source=path.name,
        event_type="protocol_event",
        message="PCAP detected (binary payload not parsed in generic mode)",
        payload={"size_bytes": stat.st_size, "path": str(path)},
    )


def _iter_events_for_log(path: Path) -> Iterable[Event]:
    lower_path = str(path).lower()
    if "energymanager" in lower_path or "energy_manager" in lower_path:
        return parse_energy_manager(path)
    if "chargerapp" in lower_path or "charger_app" in lower_path:
        return parse_charger_app(path)
    if "iotc-meter-dispatcher" in lower_path or "meter_dispatcher" in lower_path:
        return parse_meter_dispatcher(path)
    return _events_from_log(path)


def _select_useful_session_window(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "timestamp" not in frame.columns:
        return frame

    work = frame.copy()
    work["_ts"] = pd.to_datetime(work["timestamp"], utc=True, errors="coerce")
    valid = work.dropna(subset=["_ts"]).sort_values("_ts")
    if valid.empty:
        return frame

    gap = valid["_ts"].diff().gt(pd.Timedelta(minutes=30)).fillna(False)
    valid["_cluster"] = gap.cumsum()

    cluster_scores: list[tuple[int, int, pd.Timestamp]] = []
    for cluster_id, chunk in valid.groupby("_cluster"):
        useful = chunk["event_type"].isin({"error", "warning", "gridcodes", "setpoint_change", "power_limit", "timeout", "protocol_event"}).sum()
        marker_bonus = chunk["message"].str.lower().str.contains("|".join(SESSION_MARKERS), regex=True).sum()
        score = len(chunk) + 3 * useful + 2 * marker_bonus
        cluster_scores.append((int(cluster_id), int(score), chunk["_ts"].max()))

    best_cluster = sorted(cluster_scores, key=lambda x: (x[1], x[2]), reverse=True)[0][0]
    focused = valid[valid["_cluster"] == best_cluster].drop(columns=["_cluster"])

    # Keep only focused cluster (useful session) + non-timestamp events from same sources.
    source_set = set(focused["source"].tolist())
    no_ts_same_sources = work[work["_ts"].isna() & work["source"].isin(source_set)]

    result = pd.concat([focused, no_ts_same_sources], ignore_index=True)
    result = result.sort_values(by=["_ts", "source", "event_type"], na_position="last").drop(columns=["_ts"])
    return result.reset_index(drop=True)


def build_session_timeline(detected: DetectedFiles) -> pd.DataFrame:
    """Create a focused timeline DataFrame for generic V2G session debugging."""
    events: list[Event] = []

    for log in detected.all_text_logs():
        # Config files are intentionally excluded from event parsing.
        if log.suffix.lower() == ".properties" or log.name.lower().endswith(".properties"):
            continue
        events.extend(_iter_events_for_log(log))

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

    return _select_useful_session_window(frame)
