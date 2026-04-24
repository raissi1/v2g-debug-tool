"""Cross-source comparison and weighted evidence scoring for diagnostics."""

from __future__ import annotations

import pandas as pd

NOISE_PATTERNS = (
    "keep alive",
    "connectedmeters request",
    "port added",
    "queue created",
    "subscriber queue created",
    "initialization",
    "meter_subscribe_request",
)


def _is_noise_message(message: str) -> bool:
    msg = message.lower()
    return any(token in msg for token in NOISE_PATTERNS)


def compare_sources(session_df: pd.DataFrame) -> dict:
    if session_df.empty:
        return {"rows": [], "insights": ["Données insuffisantes: timeline vide."], "scores": {}, "evidence_table": []}

    work = session_df.copy()
    work["timestamp"] = pd.to_datetime(work["timestamp"], utc=True, errors="coerce")
    work = work.dropna(subset=["timestamp"]).sort_values("timestamp")
    work["message"] = work.get("message", "").astype(str)
    work = work[~work["message"].str.lower().apply(_is_noise_message)]

    def _src_group(payload: object) -> str:
        if isinstance(payload, dict):
            return str(payload.get("source_group", ""))
        return ""

    work["source_group"] = work.get("payload", pd.Series([None] * len(work))).apply(_src_group)
    for col in ["Ptarget", "Qtarget", "P", "Q", "U", "frequency"]:
        if col not in work.columns:
            work[col] = pd.NA
        work[col] = pd.to_numeric(work[col], errors="coerce")

    meter = work[work["source_group"].str.contains("meter_dispatcher", case=False, na=False)][["timestamp", "P", "Q", "U", "frequency"]].rename(
        columns={"P": "P_meter", "Q": "Q_meter", "U": "U_meter", "frequency": "frequency_meter"}
    )
    dew = work[work["source_group"].str.contains("measure", case=False, na=False)][["timestamp", "P", "Q", "U", "frequency"]].rename(
        columns={"P": "P_dewesoft", "Q": "Q_dewesoft", "U": "U_dewesoft", "frequency": "frequency_dewesoft"}
    )
    target = work[work[["Ptarget", "Qtarget"]].notna().any(axis=1)][["timestamp", "Ptarget", "Qtarget"]]

    base = work[["timestamp", "source", "event_type", "message"]].drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
    comp = pd.merge_asof(base, target.sort_values("timestamp"), on="timestamp", direction="nearest", tolerance=pd.Timedelta(seconds=5))
    comp = pd.merge_asof(comp.sort_values("timestamp"), meter.sort_values("timestamp"), on="timestamp", direction="nearest", tolerance=pd.Timedelta(seconds=2))
    comp = pd.merge_asof(comp.sort_values("timestamp"), dew.sort_values("timestamp"), on="timestamp", direction="nearest", tolerance=pd.Timedelta(seconds=2))

    scores = {"borne": 0.0, "véhicule": 0.0, "communication": 0.0}
    insights: list[str] = []
    evidence_table: list[dict] = []

    def add_evidence(row: pd.Series | None, impact: str, weight: float, comment: str, value: object = None) -> None:
        ts = row["timestamp"] if row is not None and "timestamp" in row else None
        src = row["source"] if row is not None and "source" in row else ""
        typ = row["event_type"] if row is not None and "event_type" in row else ""
        evidence_table.append(
            {
                "timestamp": ts.isoformat() if pd.notna(ts) else None,
                "source": src,
                "type": typ,
                "extracted_value": value,
                "impact": impact,
                "weight": weight,
                "comment": comment,
            }
        )
        if impact in scores:
            scores[impact] += weight

    # Borne evidence (strict)
    borne_rows = work[work["message"].str.contains("recalculated|published|maxpower|derating|curtail|gridcode.*limit|limit applied|restart|crash|fatal", case=False, na=False)]
    for _, r in borne_rows.head(8).iterrows():
        add_evidence(r, "borne", 1.5, "Preuve borne/configuration explicite.", r.get("message", "")[:120])
    if not borne_rows.empty:
        insights.append("Éléments explicites de limitation/recalcul détectés côté borne.")

    # Vehicle evidence requires setpoint + mismatch + no blocking borne evidence
    if comp["Ptarget"].notna().any():
        measured = comp["P_dewesoft"].combine_first(comp["P_meter"])
        mismatch = (comp["Ptarget"] - measured).abs() > 0.3 * comp["Ptarget"].abs().clip(lower=1.0)
        if mismatch.fillna(False).any() and borne_rows.empty:
            r = comp[mismatch.fillna(False)].iloc[0]
            add_evidence(r, "véhicule", 1.8, "Consigne non suivie sans blocage borne explicite.", {"Ptarget": r.get("Ptarget"), "P_measured": measured.loc[r.name]})
            insights.append("Consigne disponible mais puissance mesurée ne suit pas.")

    # Communication evidence
    comm_rows = work[
        (work["event_type"] == "timeout")
        | work["message"].str.contains("handshake|session error|protocol error|no response", case=False, na=False)
    ]
    for _, r in comm_rows.head(6).iterrows():
        add_evidence(r, "communication", 1.2, "Signal protocolaire/timeout de communication.", r.get("message", "")[:120])
    pcap_rows = work[work["source_group"].str.contains("netlogger", case=False, na=False)]
    if pcap_rows.empty:
        add_evidence(None, "communication", 0.8, "PCAP absent ou non exploité.")
        insights.append("PCAP absent/non exploité.")

    if not insights:
        insights.append("Aucune preuve forte; rester prudent.")

    rows = comp.head(200).to_dict(orient="records")
    return {"rows": rows, "insights": insights, "scores": scores, "evidence_table": evidence_table}

