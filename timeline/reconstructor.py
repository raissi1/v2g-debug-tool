"""Timeline reconstruction helpers for visual/debug views."""

from __future__ import annotations

import pandas as pd


def build_timeseries_view(timeline: pd.DataFrame) -> pd.DataFrame:
    if timeline.empty:
        return timeline

    view = timeline.copy()
    view["timestamp"] = pd.to_datetime(view["timestamp"], utc=True, errors="coerce")
    view = view.dropna(subset=["timestamp"]).sort_values("timestamp")

    keep = [c for c in ["timestamp", "Ptarget", "Qtarget", "P", "Q", "U", "frequency", "state", "source", "event_type"] if c in view.columns]
    return view[keep].copy()
