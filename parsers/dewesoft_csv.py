"""Dewesoft CSV parser (generic, heuristic column mapping)."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from core.models import Event


def _load_csv(path: Path) -> pd.DataFrame:
    attempts = [
        {"sep": ";", "decimal": ",", "quotechar": '"'},
        {"sep": ",", "decimal": ".", "quotechar": '"'},
        {"sep": None, "engine": "python"},
    ]
    for options in attempts:
        try:
            frame = pd.read_csv(path, **options)
            if not frame.empty and len(frame.columns) > 1:
                return frame
        except Exception:
            continue
    return pd.read_csv(path)


def _find_column(columns: list[str], candidates: tuple[str, ...]) -> str | None:
    lowered = {str(column).lower().strip(): column for column in columns}
    for key, original in lowered.items():
        if any(re.search(candidate, key) for candidate in candidates):
            return original
    return None


def _num(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.replace(",", ".")
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return None
    return float(numeric)


def _build_timestamp_series(frame: pd.DataFrame, ts_col: str | None, path: Path) -> pd.Series:
    if ts_col is None:
        return pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]")

    raw = frame[ts_col]
    numeric = pd.to_numeric(raw.astype(str).str.replace(",", ".", regex=False), errors="coerce")
    if numeric.notna().sum() >= max(1, len(frame) // 2):
        base = pd.Timestamp(path.stat().st_mtime, unit="s", tz="UTC")
        if base.year < 2000:
            base = pd.Timestamp.utcnow().tz_localize("UTC").floor("s")
        first = numeric.dropna().iloc[0]
        return base + pd.to_timedelta(numeric - first, unit="s")

    parsed = pd.to_datetime(raw, utc=True, errors="coerce")
    if parsed.notna().any():
        return parsed

    return pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]")


def parse_dewesoft_csv(path: Path) -> tuple[list[Event], pd.DataFrame]:
    frame = _load_csv(path)
    if frame.empty:
        return [], pd.DataFrame()

    cols = list(frame.columns)
    ts_col = _find_column(cols, (r"\btime\b", r"timestamp", r"\bdate\b", r"temps"))
    p_col = _find_column(cols, (r"power[_\s]*active", r"\bp[_\s]*w\b", r"\bp[_a-z0-9@/\s\[\]-]*\[w\]", r"^\s*p[_@]", r"puissance[_\s]*active"))
    q_col = _find_column(cols, (r"power[_\s]*reactive", r"\bq[_\s]*var\b", r"\bq[_a-z0-9@/\s\[\]-]*\[var\]", r"^\s*q[_@]", r"reactive", r"puissance[_\s]*reactive"))
    u_col = _find_column(cols, (r"voltage", r"tension", r"\bu[_\s]*v\b", r"\bu[123]?[_a-z0-9@/\s\[\]-]*\[v\]", r"^u[123]?_"))
    f_col = _find_column(cols, (r"freq", r"frequency", r"\bf[_a-z0-9@/\s\[\]-]*\[hz\]", r"^f[_@]"))
    i_col = _find_column(cols, (r"current", r"courant", r"\bi[_\s]*a\b", r"\bi[123]?[_a-z0-9@/\s\[\]-]*\[a\]", r"^i[123]?_"))

    timestamp_series = _build_timestamp_series(frame, ts_col, path)

    events: list[Event] = []
    for idx, row in frame.iterrows():
        payload = {
            "path": str(path),
            "line": int(idx) if not isinstance(idx, tuple) else int(idx[0]),
            "parser": "dewesoft_csv",
            "source_group": "measure",
            "future_diagnostic_side": "to_be_inferred",
        }

        ts_value = timestamp_series.loc[idx] if idx in timestamp_series.index else pd.NaT
        ts = ts_value.round("ms").to_pydatetime() if not pd.isna(ts_value) else None

        if ts_col is not None:
            payload["relative_time_raw"] = row.get(ts_col)
        if p_col is not None:
            payload["P"] = _num(row.get(p_col))
            payload["P_W"] = payload["P"]
            payload["P_dewesoft_W"] = payload["P"]
        if q_col is not None:
            payload["Q"] = _num(row.get(q_col))
            payload["Q_var"] = payload["Q"]
            payload["Q_dewesoft_var"] = payload["Q"]
        if u_col is not None:
            payload["U"] = _num(row.get(u_col))
            payload["U_V"] = payload["U"]
            payload["U_dewesoft_V"] = payload["U"]
        if f_col is not None:
            payload["frequency"] = _num(row.get(f_col))
            payload["frequency_Hz"] = payload["frequency"]
            payload["frequency_dewesoft_Hz"] = payload["frequency"]
        if i_col is not None:
            payload["I_A"] = _num(row.get(i_col))
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
            "timestamp": timestamp_series,
            "P": frame[p_col].map(_num) if p_col else pd.NA,
            "P_W": frame[p_col].map(_num) if p_col else pd.NA,
            "P_dewesoft_W": frame[p_col].map(_num) if p_col else pd.NA,
            "Q": frame[q_col].map(_num) if q_col else pd.NA,
            "Q_var": frame[q_col].map(_num) if q_col else pd.NA,
            "Q_dewesoft_var": frame[q_col].map(_num) if q_col else pd.NA,
            "U": frame[u_col].map(_num) if u_col else pd.NA,
            "U_V": frame[u_col].map(_num) if u_col else pd.NA,
            "U_dewesoft_V": frame[u_col].map(_num) if u_col else pd.NA,
            "frequency": frame[f_col].map(_num) if f_col else pd.NA,
            "frequency_Hz": frame[f_col].map(_num) if f_col else pd.NA,
            "frequency_dewesoft_Hz": frame[f_col].map(_num) if f_col else pd.NA,
            "I_A": frame[i_col].map(_num) if i_col else pd.NA,
            "I_dewesoft_A": frame[i_col].map(_num) if i_col else pd.NA,
            "source": path.name,
        }
    )
    return events, normalized
