"""Timeline reconstruction helpers for visual/debug views."""

from __future__ import annotations

import pandas as pd


def build_timeseries_view(timeline: pd.DataFrame) -> pd.DataFrame:
    if timeline.empty:
        return timeline

    view = timeline.copy()
    view["timestamp"] = pd.to_datetime(view["timestamp"], utc=True, errors="coerce")
    view = view.dropna(subset=["timestamp"]).sort_values("timestamp")

    if "payload" in view.columns:
        view["source_group"] = view["payload"].apply(lambda p: p.get("source_group") if isinstance(p, dict) else None)
        view["P_meter"] = view.apply(lambda r: r.get("P") if str(r.get("source_group", "")).lower().find("meter_dispatcher") >= 0 else pd.NA, axis=1)
        view["Q_meter"] = view.apply(lambda r: r.get("Q") if str(r.get("source_group", "")).lower().find("meter_dispatcher") >= 0 else pd.NA, axis=1)
        view["U_meter"] = view.apply(lambda r: r.get("U") if str(r.get("source_group", "")).lower().find("meter_dispatcher") >= 0 else pd.NA, axis=1)
        view["frequency_meter"] = view.apply(lambda r: r.get("frequency") if str(r.get("source_group", "")).lower().find("meter_dispatcher") >= 0 else pd.NA, axis=1)
        view["P_dewesoft"] = view.apply(lambda r: r.get("P") if str(r.get("source_group", "")).lower().find("measure") >= 0 else pd.NA, axis=1)
        view["Q_dewesoft"] = view.apply(lambda r: r.get("Q") if str(r.get("source_group", "")).lower().find("measure") >= 0 else pd.NA, axis=1)
        view["U_dewesoft"] = view.apply(lambda r: r.get("U") if str(r.get("source_group", "")).lower().find("measure") >= 0 else pd.NA, axis=1)
        view["frequency_dewesoft"] = view.apply(lambda r: r.get("frequency") if str(r.get("source_group", "")).lower().find("measure") >= 0 else pd.NA, axis=1)

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
            "P_meter",
            "Q_meter",
            "U_meter",
            "frequency_meter",
            "P_dewesoft",
            "Q_dewesoft",
            "U_dewesoft",
            "frequency_dewesoft",
        ]
        if c in view.columns
    ]
    return view[keep].copy()
