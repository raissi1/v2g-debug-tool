"""Dewesoft CSV parser (generic, heuristic column mapping)."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from core.models import Event


def _find_column(columns: list[str], candidates: tuple[str, ...]) -> str | None:
    lowered = {c.lower().strip(): c for c in columns}
    for key, original in lowered.items():
        if any(re.search(candidate, key) for candidate in candidates):
            return original
    return None


def parse_dewesoft_csv(path: Path) -> tuple[list[Event], pd.DataFrame]:
    frame = pd.read_csv(path)
    if frame.empty:
        return [], pd.DataFrame()

    cols = list(frame.columns)
    ts_col = _find_column(cols, (r"\btime\b", r"timestamp", r"\bdate\b", r"temps"))
    p_col = _find_column(cols, (r"power[_\s]*active", r"\bp[_\s]*w\b", r"\bpower\b", r"puissance[_\s]*active"))
    q_col = _find_column(cols, (r"power[_\s]*reactive", r"\bq[_\s]*var\b", r"reactive", r"puissance[_\s]*reactive"))
    u_col = _find_column(cols, (r"voltage", r"tension", r"\bu[_\s]*v\b", r"volt"))
    f_col = _find_column(cols, (r"freq", r"frequency", r"\bhz\b"))
    i_col = _find_column(cols, (r"current", r"courant", r"\bi[_\s]*a\b"))

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
            payload["P_W"] = payload["P"]
            payload["P_dewesoft_W"] = payload["P"]
        if q_col is not None:
            payload["Q"] = pd.to_numeric(row.get(q_col), errors="coerce")
            payload["Q_var"] = payload["Q"]
            payload["Q_dewesoft_var"] = payload["Q"]
        if u_col is not None:
            payload["U"] = pd.to_numeric(row.get(u_col), errors="coerce")
            payload["U_V"] = payload["U"]
            payload["U_dewesoft_V"] = payload["U"]
        if f_col is not None:
            payload["frequency"] = pd.to_numeric(row.get(f_col), errors="coerce")
            payload["frequency_Hz"] = payload["frequency"]
            payload["frequency_dewesoft_Hz"] = payload["frequency"]
        if i_col is not None:
            payload["I_A"] = pd.to_numeric(row.get(i_col), errors="coerce")
            payload["I_dewesoft_A"] = payload["I_A"]

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
            "P_W": pd.to_numeric(frame[p_col], errors="coerce") if p_col else pd.NA,
            "P_dewesoft_W": pd.to_numeric(frame[p_col], errors="coerce") if p_col else pd.NA,
            "Q": pd.to_numeric(frame[q_col], errors="coerce") if q_col else pd.NA,
            "Q_var": pd.to_numeric(frame[q_col], errors="coerce") if q_col else pd.NA,
            "Q_dewesoft_var": pd.to_numeric(frame[q_col], errors="coerce") if q_col else pd.NA,
            "U": pd.to_numeric(frame[u_col], errors="coerce") if u_col else pd.NA,
            "U_V": pd.to_numeric(frame[u_col], errors="coerce") if u_col else pd.NA,
            "U_dewesoft_V": pd.to_numeric(frame[u_col], errors="coerce") if u_col else pd.NA,
            "frequency": pd.to_numeric(frame[f_col], errors="coerce") if f_col else pd.NA,
            "frequency_Hz": pd.to_numeric(frame[f_col], errors="coerce") if f_col else pd.NA,
            "frequency_dewesoft_Hz": pd.to_numeric(frame[f_col], errors="coerce") if f_col else pd.NA,
            "I_A": pd.to_numeric(frame[i_col], errors="coerce") if i_col else pd.NA,
            "I_dewesoft_A": pd.to_numeric(frame[i_col], errors="coerce") if i_col else pd.NA,
            "source": path.name,
        }
    )
    return events, normalized
