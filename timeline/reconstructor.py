"""Timeline reconstruction helpers for visual/debug views."""

from __future__ import annotations

import pandas as pd


def build_timeseries_view(timeline: pd.DataFrame) -> pd.DataFrame:
    if timeline.empty:
        return timeline

    view = timeline.copy()
    view["timestamp"] = pd.to_datetime(view["timestamp"], utc=True, errors="coerce")
    view = view.dropna(subset=["timestamp"]).sort_values("timestamp")

    keep = [
        c
        for c in [
            "timestamp",
            "Ptarget",
            "Qtarget",
            "P",
            "Q",
            "S",
            "S_VA",
            "U",
            "U_V",
            "U_avg",
            "U_avg_V",
            "U_phase_A",
            "U_phase_A_V",
            "U_phase_B",
            "U_phase_B_V",
            "U_phase_C",
            "U_phase_C_V",
            "I_A",
            "I_phase_A",
            "I_phase_A_A",
            "I_phase_B",
            "I_phase_B_A",
            "I_phase_C",
            "I_phase_C_A",
            "frequency",
            "frequency_Hz",
            "state",
            "source",
            "event_type",
        ]
        if c in view.columns
    ]
    return view[keep].copy()
