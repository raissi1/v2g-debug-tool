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
from parsers.dewesoft import parse_dewesoft_file

ISO_TS_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?")
NUMBER = r"([-+]?\d+(?:[\.,]\d+)?)"

GENERIC_PATTERNS: list[tuple[str, tuple[str, ...]]] = [
    ("error", (" error ", "exception", "failed", "fatal")),
    ("warning", (" warning ", "warn", "retry", "degraded")),
    ("timeout", ("timeout", "timed out", "no response")),
    ("gridcodes", ("gridcode", "grid code", "lvrt", "hvrt", "frt")),
    ("power_limit", ("power limit", "curtail", "derating", "limited", "max power")),
    ("protocol_event", ("iso15118", "ocpp", "din70121", "slac", "handshake", "session setup")),
    ("session_event", ("session start", "session stop", "charging started", "charging stopped", "plug", "unplug")),
]

GENERIC_PHYSICAL_PATTERNS = {
    "Ptarget": [re.compile(rf"(?:ptarget|p_target|setpoint\s*p|target\s*power)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "Qtarget": [re.compile(rf"(?:qtarget|q_target|setpoint\s*q|target\s*reactive)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "P": [re.compile(rf"(?:p\s*meas(?:ured)?|measured\s*power|active\s*power)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "Q": [re.compile(rf"(?:q\s*meas(?:ured)?|reactive\s*power)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "U": [re.compile(rf"(?:voltage|tension|\bu\b)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "frequency": [re.compile(rf"(?:freq(?:uency)?|\bhz\b)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "AvailableDischargePower": [re.compile(rf"(?:availabledischargepower|available\s*discharge\s*power)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "Pcalc": [re.compile(rf"(?:recalculated\s*setpoint\s*p|calculated\s*p|pcalc)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "Qcalc": [re.compile(rf"(?:recalculated\s*setpoint\s*q|calculated\s*q|qcalc)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "Smax": [re.compile(rf"(?:smax|max\s*apparent\s*power)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "derating": [re.compile(rf"(?:derating|derate|limit\s*factor)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
}
GENERIC_STATE_PATTERNS = [
    ("start", re.compile(r"session\s*start|start\s*session|charging\s*started", re.IGNORECASE)),
    ("stop", re.compile(r"session\s*stop|stop\s*session|charging\s*stopped", re.IGNORECASE)),
    ("charging", re.compile(r"\bcharging\b", re.IGNORECASE)),
]

SESSION_MARKERS = (
    "session",
    "charging",
    "plug",
    "unplug",
    "authorize",
    "transaction",
    "handshake",
    "energy transfer",
)

FOCUS_EVENT_TYPES = {
    "error",
    "warning",
    "gridcodes",
    "setpoint",
    "power_limit",
    "timeout",
    "protocol_event",
    "session_event",
    "physical_measurement",
    "state_change",
}


def _to_float(raw: str) -> float | None:
    try:
        return float(raw.replace(",", "."))
    except ValueError:
        return None


def _extract_generic_signals(line: str) -> dict[str, float | str]:
    signals: dict[str, float | str] = {}
    for key, patterns in GENERIC_PHYSICAL_PATTERNS.items():
        for pattern in patterns:
            m = pattern.search(line)
            if m:
                value = _to_float(m.group(1))
                if value is not None:
                    signals[key] = value
                    break

    for state_name, pattern in GENERIC_STATE_PATTERNS:
        if pattern.search(line):
            signals["state"] = state_name
            break

    return signals


def _physical_event_type(signals: dict[str, float | str]) -> str | None:
    if "state" in signals:
        return "state_change"
    if "Ptarget" in signals or "Qtarget" in signals:
        return "setpoint"
    if any(k in signals for k in ("Pcalc", "Qcalc", "Smax", "derating")):
        return "power_limit"
    if any(k in signals for k in ("P", "Q", "U", "AvailableDischargePower")):
        return "physical_measurement"
    return None


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


def _events_from_log(path: Path, source_group: str = "generic") -> Iterable[Event]:
    with _open_text(path) as stream:
        for idx, line in enumerate(stream, 1):
            stripped = line.strip()
            if not stripped:
                continue

            signals = _extract_generic_signals(stripped)
            base_event_type = _classify_generic_line(stripped)
            event_type = _physical_event_type(signals) or base_event_type

            payload = {
                "line": idx,
                "path": str(path),
                "parser": source_group if source_group != "generic" else "generic",
                "source_group": source_group,
                "future_diagnostic_side": "to_be_inferred",
                "base_event_type": base_event_type,
            }
            payload.update(signals)

            yield Event(
                timestamp=_extract_ts_from_text(stripped),
                source=path.name,
                event_type=event_type,
                message=stripped,
                payload=payload,
            )


def _events_from_measure(path: Path) -> Iterable[Event]:
    events, _warning = parse_dewesoft_file(path)
    for event in events:
        yield event

    # Fallback for non-dewesoft files kept for extensibility.
    suffix = path.suffix.lower()
    if suffix in {".tsv", ".json"}:
        try:
            if suffix == ".tsv":
                frame = pd.read_csv(path, sep="\t")
                ts_column = next((c for c in frame.columns if "time" in c.lower() or "date" in c.lower()), None)
                for idx, row in frame.iterrows():
                    ts = _parse_timestamp(str(row[ts_column])) if ts_column else None
                    payload = row.to_dict()
                    payload.update({"source_group": "measure", "future_diagnostic_side": "to_be_inferred"})
                    yield Event(timestamp=ts, source=path.name, event_type="physical_measurement", message=f"Measurement row #{idx}", payload=payload)
            elif suffix == ".json":
                with path.open("r", encoding="utf-8", errors="ignore") as stream:
                    content = json.load(stream)
                records = content if isinstance(content, list) else [content]
                for idx, rec in enumerate(records):
                    if not isinstance(rec, dict):
                        rec = {"value": rec}
                    ts_key = next((k for k in rec.keys() if "time" in k.lower() or "date" in k.lower()), None)
                    ts = _parse_timestamp(str(rec[ts_key])) if ts_key else None
                    rec.update({"source_group": "measure", "future_diagnostic_side": "to_be_inferred"})
                    yield Event(timestamp=ts, source=path.name, event_type="physical_measurement", message=f"Measurement record #{idx}", payload=rec)
        except Exception:
            return


def _events_from_pcap(path: Path) -> Iterable[Event]:
    stat = path.stat()
    yield Event(
        timestamp=datetime.fromtimestamp(stat.st_mtime),
        source=path.name,
        event_type="protocol_event",
        message="PCAP detected (binary payload not parsed in generic mode)",
        payload={
            "size_bytes": stat.st_size,
            "path": str(path),
            "parser": "netlogger",
            "source_group": "netlogger",
            "future_diagnostic_side": "to_be_inferred",
        },
    )


def _iter_events_for_log(path: Path) -> Iterable[Event]:
    lower_path = str(path).lower()
    if "energymanager" in lower_path or "energy_manager" in lower_path:
        return parse_energy_manager(path)
    if "chargerapp" in lower_path or "charger_app" in lower_path:
        return parse_charger_app(path)
    if "iotc-meter-dispatcher" in lower_path or "meter_dispatcher" in lower_path:
        return parse_meter_dispatcher(path)
    if "netlogger" in lower_path:
        return _events_from_log(path, source_group="netlogger")
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
    marker_regex = "|".join(SESSION_MARKERS)
    for cluster_id, chunk in valid.groupby("_cluster"):
        useful = chunk["event_type"].isin(FOCUS_EVENT_TYPES).sum()
        marker_bonus = chunk["message"].str.lower().str.contains(marker_regex, regex=True).sum()
        score = len(chunk) + 4 * useful + 2 * marker_bonus
        cluster_scores.append((int(cluster_id), int(score), chunk["_ts"].max()))

    best_cluster = sorted(cluster_scores, key=lambda x: (x[1], x[2]), reverse=True)[0][0]
    focused = valid[valid["_cluster"] == best_cluster].drop(columns=["_cluster"])

    source_set = set(focused["source"].tolist())
    no_ts_same_sources = work[work["_ts"].isna() & work["source"].isin(source_set)]

    result = pd.concat([focused, no_ts_same_sources], ignore_index=True)
    result = result.sort_values(by=["_ts", "source", "event_type"], na_position="last").drop(columns=["_ts"])
    return result.reset_index(drop=True)


def _extract_payload_value(payload: object, key: str) -> float | str | None:
    if isinstance(payload, dict):
        return payload.get(key)
    return None


def _add_physical_columns(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame["Ptarget"] = frame["payload"].apply(lambda p: _extract_payload_value(p, "Ptarget"))
    frame["Qtarget"] = frame["payload"].apply(lambda p: _extract_payload_value(p, "Qtarget"))
    frame["P"] = frame["payload"].apply(lambda p: _extract_payload_value(p, "P"))
    frame["Q"] = frame["payload"].apply(lambda p: _extract_payload_value(p, "Q"))
    frame["U"] = frame["payload"].apply(lambda p: _extract_payload_value(p, "U"))
    frame["frequency"] = frame["payload"].apply(lambda p: _extract_payload_value(p, "frequency"))
    frame["AvailableDischargePower"] = frame["payload"].apply(lambda p: _extract_payload_value(p, "AvailableDischargePower"))
    frame["Pcalc"] = frame["payload"].apply(lambda p: _extract_payload_value(p, "Pcalc"))
    frame["Qcalc"] = frame["payload"].apply(lambda p: _extract_payload_value(p, "Qcalc"))
    frame["Smax"] = frame["payload"].apply(lambda p: _extract_payload_value(p, "Smax"))
    frame["derating"] = frame["payload"].apply(lambda p: _extract_payload_value(p, "derating"))
    frame["state"] = frame["payload"].apply(lambda p: _extract_payload_value(p, "state"))

    for col in ("Ptarget", "Qtarget", "P", "Q", "U", "frequency", "AvailableDischargePower", "Pcalc", "Qcalc", "Smax", "derating"):
        frame[col] = pd.to_numeric(frame[col], errors="coerce")

    # Reconstruct behavior timeline by timestamp proximity: propagate nearby values.
    frame["_ts"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame = frame.sort_values(by=["_ts", "source", "event_type"], na_position="last")
    for col in ("Ptarget", "Qtarget", "P", "Q", "U", "frequency", "AvailableDischargePower", "Pcalc", "Qcalc", "Smax", "derating", "state"):
        frame[col] = frame[col].ffill()

    # Build a coarse merged index for close timestamps (1-second bins).
    frame["merged_ts"] = frame["_ts"].dt.floor("s")
    frame = frame.drop(columns=["_ts"])
    return frame




def _extract_value_snapshot(payload: dict) -> dict:
    keys = ["Ptarget", "Qtarget", "P", "Q", "U", "frequency", "Pcalc", "Qcalc", "Smax", "derating", "state", "AvailableDischargePower"]
    return {k: payload.get(k) for k in keys if k in payload and payload.get(k) is not None}


def _short_interpretation(event_type: str) -> str:
    mapping = {
        "setpoint": "Consigne détectée",
        "physical_measurement": "Mesure physique détectée",
        "state_change": "Changement d'état de session",
        "error": "Erreur applicative",
        "warning": "Avertissement applicatif",
        "timeout": "Timeout de communication",
        "protocol_event": "Événement protocolaire",
        "gridcodes": "Événement GridCode",
        "power_limit": "Limitation interne",
        "session_event": "Événement de session",
    }
    return mapping.get(event_type, "Événement générique")
def build_session_timeline(detected: DetectedFiles) -> pd.DataFrame:
    """Create a focused timeline DataFrame for generic V2G session debugging."""
    events: list[Event] = []

    for log in detected.all_text_logs():
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
                "extracted_value": _extract_value_snapshot(e.payload),
                "raw_message": e.message,
                "interpretation": _short_interpretation(e.event_type),
            }
            for e in events
        ]
    )

    if frame.empty:
        return frame

    frame["_sort_ts"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame = frame.sort_values(by=["_sort_ts", "source", "event_type"], na_position="last").drop(columns=["_sort_ts"])
    frame = frame.reset_index(drop=True)

    focused = _select_useful_session_window(frame)
    return _add_physical_columns(focused)
