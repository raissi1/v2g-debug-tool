"""Detect the first meaningful divergence in a V2G session."""

from __future__ import annotations

import pandas as pd


def _empty_divergence() -> dict:
    return {
        "timestamp": None,
        "source": None,
        "event_type": None,
        "category": "indetermine",
        "severity": "low",
        "reason": "Aucun point de divergence net n'a ete determine.",
        "evidence": {},
    }


def detect_first_divergence(session_df: pd.DataFrame, cross_analysis: dict | None = None) -> dict:
    if session_df.empty:
        return _empty_divergence()

    work = session_df.copy()
    work["timestamp"] = pd.to_datetime(work["timestamp"], utc=True, errors="coerce")
    work = work.dropna(subset=["timestamp"]).sort_values("timestamp")
    if work.empty:
        return _empty_divergence()

    cross_rows = pd.DataFrame((cross_analysis or {}).get("rows", []))
    if not cross_rows.empty:
        cross_rows["timestamp"] = pd.to_datetime(cross_rows["timestamp"], utc=True, errors="coerce")
        cross_rows = cross_rows.dropna(subset=["timestamp"]).sort_values("timestamp")
        if "Ptarget" in cross_rows.columns:
            measured = pd.Series(pd.NA, index=cross_rows.index, dtype="object")
            if "P_dewesoft" in cross_rows.columns:
                measured = cross_rows["P_dewesoft"]
            if "P_meter" in cross_rows.columns:
                measured = measured.where(pd.notna(measured), cross_rows["P_meter"])
            if measured.notna().any():
                ptarget = pd.to_numeric(cross_rows["Ptarget"], errors="coerce")
                measured = pd.to_numeric(measured, errors="coerce")
                mismatch = (ptarget - measured).abs() > 0.3 * ptarget.abs().clip(lower=1.0)
                mismatch_rows = cross_rows[mismatch.fillna(False)]
                if not mismatch_rows.empty:
                    row = mismatch_rows.iloc[0]
                    return {
                        "timestamp": row["timestamp"].isoformat(),
                        "source": row.get("source"),
                        "event_type": row.get("event_type"),
                        "category": "consigne_non_suivie",
                        "severity": "high",
                        "reason": "Premier ecart net entre la consigne demandee et la puissance mesuree.",
                        "evidence": {
                            "Ptarget": row.get("Ptarget"),
                            "P_meter": row.get("P_meter"),
                            "P_dewesoft": row.get("P_dewesoft"),
                        },
                    }

    borne_rows = work[
        (work["event_type"].isin(["power_limit", "gridcodes", "error"]))
        | work["message"].astype(str).str.contains("recalculated|published|maxpower|derating|limit applied|crash|fatal", case=False, na=False)
    ]
    if not borne_rows.empty:
        row = borne_rows.iloc[0]
        return {
            "timestamp": row["timestamp"].isoformat(),
            "source": row.get("source"),
            "event_type": row.get("event_type"),
            "category": "borne",
            "severity": "high" if str(row.get("event_type")) in {"error", "power_limit"} else "medium",
            "reason": "Premier evenement borne significatif detecte dans la timeline.",
            "evidence": {"message": row.get("message")},
        }

    comm_rows = work[
        (work["event_type"].isin(["timeout", "protocol_event", "warning"]))
        & work["message"].astype(str).str.contains("timeout|handshake|protocol|no response|pcap", case=False, na=False)
    ]
    if not comm_rows.empty:
        row = comm_rows.iloc[0]
        return {
            "timestamp": row["timestamp"].isoformat(),
            "source": row.get("source"),
            "event_type": row.get("event_type"),
            "category": "communication",
            "severity": "medium",
            "reason": "Premier evenement protocolaire suspect detecte.",
            "evidence": {"message": row.get("message")},
        }

    generic_rows = work[work["event_type"].isin(["warning", "error", "timeout", "gridcodes", "power_limit"])]
    if not generic_rows.empty:
        row = generic_rows.iloc[0]
        return {
            "timestamp": row["timestamp"].isoformat(),
            "source": row.get("source"),
            "event_type": row.get("event_type"),
            "category": "generic_issue",
            "severity": "medium",
            "reason": "Premier evenement anormal remonte par les regles generiques.",
            "evidence": {"message": row.get("message")},
        }

    return _empty_divergence()
