"""Dewesoft CSV parser (generic, heuristic column mapping)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from core.models import Event


def _find_column(columns: list[str], candidates: tuple[str, ...]) -> str | None:
    lowered = {c.lower(): c for c in columns}
    for key, original in lowered.items():
        if any(candidate in key for candidate in candidates):
            return original
    return None


def parse_dewesoft_csv(path: Path) -> tuple[list[Event], pd.DataFrame]:
    frame = pd.read_csv(path)
    if frame.empty:
        return [], pd.DataFrame()

    cols = list(frame.columns)
    ts_col = _find_column(cols, ("time", "timestamp", "date"))
    p_col = _find_column(cols, ("p", "active power", "power"))
    q_col = _find_column(cols, ("q", "reactive"))
    u_col = _find_column(cols, ("u", "voltage", "tension"))
    f_col = _find_column(cols, ("freq", "frequency", "hz"))

    events: list[Event] = []
    for idx, row in frame.iterrows():
        payload = {
            "path": str(path),
            "line": int(idx),
            "parser": "dewesoft_csv",
            "source_group": "measure",
            "future_diagnostic_side": "to_be_inferred",
        }

        ts = None
        if ts_col is not None:
            ts_value = pd.to_datetime(row.get(ts_col), utc=True, errors="coerce")
            if not pd.isna(ts_value):
                ts = ts_value.to_pydatetime()

        if p_col is not None:
            payload["P"] = pd.to_numeric(row.get(p_col), errors="coerce")
        if q_col is not None:
            payload["Q"] = pd.to_numeric(row.get(q_col), errors="coerce")
        if u_col is not None:
            payload["U"] = pd.to_numeric(row.get(u_col), errors="coerce")
        if f_col is not None:
            payload["frequency"] = pd.to_numeric(row.get(f_col), errors="coerce")

        events.append(
            Event(
                timestamp=ts,
                source=path.name,
                event_type="physical_measurement",
                message=f"Dewesoft sample #{idx}",
                payload=payload,
            )
        )

    normalized = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(frame[ts_col], utc=True, errors="coerce") if ts_col else pd.NaT,
            "P": pd.to_numeric(frame[p_col], errors="coerce") if p_col else pd.NA,
            "Q": pd.to_numeric(frame[q_col], errors="coerce") if q_col else pd.NA,
            "U": pd.to_numeric(frame[u_col], errors="coerce") if u_col else pd.NA,
            "frequency": pd.to_numeric(frame[f_col], errors="coerce") if f_col else pd.NA,
            "source": path.name,
        }
    )
    return events, normalized
