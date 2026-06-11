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


def _empty_result(insight: str) -> dict:
    return {
        "rows": [],
        "insights": [insight],
        "scores": {"borne": 0.0, "vehicule": 0.0, "communication": 0.0},
        "evidence_table": [],
    }


def _merge_nearest(
    left: pd.DataFrame,
    right: pd.DataFrame,
    rename_map: dict[str, str],
    tolerance: pd.Timedelta,
) -> pd.DataFrame:
    merged = left.sort_values("timestamp").copy()

    if merged.empty or right.empty:
        for column in rename_map.values():
            if column not in merged.columns:
                merged[column] = pd.NA
        return merged

    overlap = [column for column in rename_map.values() if column in merged.columns]
    if overlap:
        merged = merged.drop(columns=overlap)

    payload = right.rename(columns=rename_map).sort_values("timestamp")
    payload = payload[[column for column in ["timestamp", *rename_map.values()] if column in payload.columns]]
    if payload.empty:
        for column in rename_map.values():
            if column not in merged.columns:
                merged[column] = pd.NA
        return merged

    try:
        result = pd.merge_asof(
            merged,
            payload,
            on="timestamp",
            direction="nearest",
            tolerance=tolerance,
        )
        for column in rename_map.values():
            if column not in result.columns:
                result[column] = pd.NA
        return result
    except Exception:
        for column in rename_map.values():
            if column not in merged.columns:
                merged[column] = pd.NA
        return merged


def compare_sources(session_df: pd.DataFrame) -> dict:
    if session_df.empty:
        return _empty_result("Donnees insuffisantes: timeline vide.")

    work = session_df.copy()
    work["timestamp"] = pd.to_datetime(work["timestamp"], utc=True, errors="coerce")
    work = work.dropna(subset=["timestamp"]).sort_values("timestamp")
    work["message"] = work.get("message", "").astype(str)
    work = work[~work["message"].str.lower().apply(_is_noise_message)]
    if work.empty:
        return _empty_result("Donnees insuffisantes: aucun evenement exploitable apres filtrage du bruit.")

    def _src_group(payload: object) -> str:
        if isinstance(payload, dict):
            return str(payload.get("source_group", ""))
        return ""

    work["source_group"] = work.get("payload", pd.Series([None] * len(work), index=work.index)).apply(_src_group)
    for column in ["Ptarget", "Qtarget", "P", "Q", "U", "frequency"]:
        if column not in work.columns:
            work[column] = pd.NA
        work[column] = pd.to_numeric(work[column], errors="coerce")

    meter = work[work["source_group"].str.contains("meter_dispatcher", case=False, na=False)][["timestamp", "P", "Q", "U", "frequency"]]
    dew = work[work["source_group"].str.contains("measure", case=False, na=False)][["timestamp", "P", "Q", "U", "frequency"]]
    target = work[work[["Ptarget", "Qtarget"]].notna().any(axis=1)][["timestamp", "Ptarget", "Qtarget"]]

    base = work[["timestamp", "source", "event_type", "message"]].drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
    if base.empty:
        return _empty_result("Donnees insuffisantes: aucune base temporelle exploitable.")

    comp = _merge_nearest(
        base,
        target,
        {"Ptarget": "Ptarget", "Qtarget": "Qtarget"},
        pd.Timedelta(seconds=5),
    )
    comp = _merge_nearest(
        comp,
        meter,
        {"P": "P_meter", "Q": "Q_meter", "U": "U_meter", "frequency": "frequency_meter"},
        pd.Timedelta(seconds=2),
    )
    comp = _merge_nearest(
        comp,
        dew,
        {"P": "P_dewesoft", "Q": "Q_dewesoft", "U": "U_dewesoft", "frequency": "frequency_dewesoft"},
        pd.Timedelta(seconds=2),
    )

    scores = {"borne": 0.0, "vehicule": 0.0, "communication": 0.0}
    insights: list[str] = []
    evidence_table: list[dict] = []

    def add_evidence(row: pd.Series | None, impact: str, weight: float, comment: str, value: object = None) -> None:
        timestamp = row["timestamp"] if row is not None and "timestamp" in row else None
        source = row["source"] if row is not None and "source" in row else ""
        event_type = row["event_type"] if row is not None and "event_type" in row else ""
        evidence_table.append(
            {
                "timestamp": timestamp.isoformat() if pd.notna(timestamp) else None,
                "source": source,
                "type": event_type,
                "extracted_value": value,
                "impact": impact,
                "weight": weight,
                "comment": comment,
            }
        )
        if impact in scores:
            scores[impact] += weight

    borne_rows = work[
        work["message"].str.contains(
            "recalculated|published|maxpower|derating|curtail|gridcode.*limit|limit applied|restart|crash|fatal",
            case=False,
            na=False,
        )
    ]
    for _, row in borne_rows.head(8).iterrows():
        add_evidence(row, "borne", 1.5, "Preuve borne ou configuration explicite.", row.get("message", "")[:120])
    if not borne_rows.empty:
        insights.append("Elements explicites de limitation ou recalcul detectes cote borne.")

    if comp["Ptarget"].notna().any():
        dew_values = comp["P_dewesoft"] if "P_dewesoft" in comp.columns else pd.Series(pd.NA, index=comp.index)
        meter_values = comp["P_meter"] if "P_meter" in comp.columns else pd.Series(pd.NA, index=comp.index)
        measured = dew_values.where(dew_values.notna(), meter_values)
        mismatch = (comp["Ptarget"] - measured).abs() > 0.3 * comp["Ptarget"].abs().clip(lower=1.0)
        if mismatch.fillna(False).any() and borne_rows.empty:
            row = comp[mismatch.fillna(False)].iloc[0]
            add_evidence(
                row,
                "vehicule",
                1.8,
                "Consigne non suivie sans blocage borne explicite.",
                {"Ptarget": row.get("Ptarget"), "P_measured": measured.loc[row.name]},
            )
            insights.append("Consigne disponible mais puissance mesuree ne suit pas.")

    comm_rows = work[
        (work["event_type"] == "timeout")
        | work["message"].str.contains("handshake|session error|protocol error|no response", case=False, na=False)
    ]
    for _, row in comm_rows.head(6).iterrows():
        add_evidence(row, "communication", 1.2, "Signal protocolaire ou timeout de communication.", row.get("message", "")[:120])

    pcap_rows = work[work["source_group"].str.contains("netlogger", case=False, na=False)]
    if pcap_rows.empty:
        add_evidence(None, "communication", 0.8, "Aucune trace PCAP exploitable dans la timeline.")
        insights.append("Aucune trace PCAP exploitable dans la timeline.")
    else:
        pcap_rst_rows = pcap_rows[
            pcap_rows["message"].str.contains("TCP resets detected", case=False, na=False)
        ]
        for _, row in pcap_rst_rows.head(3).iterrows():
            add_evidence(row, "communication", 1.6, "Resets TCP detectes dans le PCAP.", row.get("message", "")[:120])

        healthy_pcap_rows = pcap_rows[
            pcap_rows["message"].str.contains("V2G protocol markers detected|HomePlug / SLAC|without reset", case=False, na=False)
        ]
        if not healthy_pcap_rows.empty:
            insights.append("PCAP exploitable: marqueurs V2G / HomePlug / TCP observes.")

    if not insights:
        insights.append("Aucune preuve forte: rester prudent.")

    rows = comp.head(200).to_dict(orient="records")
    return {
        "rows": rows,
        "insights": insights,
        "scores": scores,
        "evidence_table": evidence_table,
    }
