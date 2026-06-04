"""Intelligent generic diagnostic engine for V2G sessions."""

from __future__ import annotations

import re

import pandas as pd

from analyzers.source_comparison import compare_sources as compare_sources_weighted


CAUSE_LABELS = {
    "borne": "borne",
    "vehicule": "vehicule",
    "communication": "communication",
    "indetermine": "indetermine",
}


def _build_simplified_timeline(session_df: pd.DataFrame) -> pd.DataFrame:
    """Reconstruct a compact timeline for diagnostic reasoning."""
    if session_df.empty:
        return session_df

    df = session_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")

    for column in ["Ptarget", "Qtarget", "P", "Q", "S", "U", "U_avg", "frequency", "frequency_Hz", "Pcalc", "Qcalc", "Smax", "derating"]:
        if column not in df.columns:
            df[column] = pd.NA
        df[column] = pd.to_numeric(df[column], errors="coerce")

    keep_cols = [
        "timestamp",
        "source",
        "event_type",
        "Ptarget",
        "Qtarget",
        "P",
        "Q",
        "S",
        "U",
        "U_avg",
        "frequency",
        "frequency_Hz",
        "Pcalc",
        "Qcalc",
        "Smax",
        "derating",
        "message",
        "payload",
    ]
    keep_cols = [column for column in keep_cols if column in df.columns]
    return df[keep_cols].copy()


def _source_group(row: pd.Series) -> str:
    payload = row.get("payload")
    if isinstance(payload, dict):
        group = payload.get("source_group")
        if isinstance(group, str):
            return group
    return str(row.get("source", "")).lower()


def _fmt_val(value: object) -> str:
    if pd.isna(value):
        return "n/a"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def _label_cause(value: str) -> str:
    return CAUSE_LABELS.get(str(value).lower(), str(value))


def _build_reasoning_blocks(simplified: pd.DataFrame, issues: list[str]) -> dict[str, list[str]]:
    blocks = {
        "A_requested": [],
        "B_station_computed": [],
        "C_sent_to_vehicle": [],
        "D_measured": [],
        "E_anomalies": [],
    }
    if simplified.empty:
        return blocks

    work = simplified.copy()
    work["source_group"] = work.apply(_source_group, axis=1)

    requested = work[work[["Ptarget", "Qtarget"]].notna().any(axis=1)].head(25)
    for _, row in requested.iterrows():
        blocks["A_requested"].append(
            f"{row['timestamp']} - demande Ptarget={_fmt_val(row.get('Ptarget'))} W "
            f"Qtarget={_fmt_val(row.get('Qtarget'))} var ({row.get('source')})"
        )

    request_keywords = re.compile(
        r"request\s*to\s*accept\s*setpoint|centralsetpoint|maxpower_w|charge\s*limit|discharge\s*limit|ocpp|ocpp_offline|cpd|\bev\b",
        re.IGNORECASE,
    )
    keyword_rows = work[work["message"].astype(str).str.contains(request_keywords, na=False)].head(25)
    for _, row in keyword_rows.iterrows():
        if len(blocks["A_requested"]) >= 25:
            break
        blocks["A_requested"].append(f"{row['timestamp']} - demande ou contrainte: {row.get('message')[:220]}")

    computed = work[
        work[["Pcalc", "Qcalc", "Smax", "derating"]].notna().any(axis=1)
        | (work["event_type"] == "power_limit")
    ].head(25)
    for _, row in computed.iterrows():
        blocks["B_station_computed"].append(
            f"{row['timestamp']} - calcul borne Pcalc={_fmt_val(row.get('Pcalc'))} "
            f"Qcalc={_fmt_val(row.get('Qcalc'))} Smax={_fmt_val(row.get('Smax'))} "
            f"derating={_fmt_val(row.get('derating'))} ({row.get('source')})"
        )

    published_keywords = re.compile(
        r"setpoint\s*is\s*recalculated\s*and\s*published|published|centralsetpoint|maxpower_w|limit|ocpp|cpd|ev",
        re.IGNORECASE,
    )
    published_rows = work[work["message"].astype(str).str.contains(published_keywords, na=False)].head(25)
    for _, row in published_rows.iterrows():
        if len(blocks["B_station_computed"]) >= 25:
            break
        blocks["B_station_computed"].append(f"{row['timestamp']} - publication borne: {row.get('message')[:220]}")

    sent = work[
        (work["event_type"].isin(["setpoint", "protocol_event"]))
        & (
            work["message"].astype(str).str.contains(
                "send|sent|publish|tx|transmit|iso15118|din70121|schedule|request",
                case=False,
                na=False,
            )
            | work["source_group"].astype(str).str.contains("charger_app|netlogger", case=False, na=False)
        )
    ].head(25)
    for _, row in sent.iterrows():
        blocks["C_sent_to_vehicle"].append(
            f"{row['timestamp']} - envoye au vehicule ({row.get('event_type')}): {row.get('message')[:220]}"
        )

    measured = work[work[["P", "Q", "S", "U", "U_avg", "frequency", "frequency_Hz"]].notna().any(axis=1)].head(25)
    for _, row in measured.iterrows():
        u_value = row.get("U") if not pd.isna(row.get("U")) else row.get("U_avg")
        f_value = row.get("frequency") if not pd.isna(row.get("frequency")) else row.get("frequency_Hz")
        blocks["D_measured"].append(
            f"{row['timestamp']} - mesure P={_fmt_val(row.get('P'))} W, "
            f"Q={_fmt_val(row.get('Q'))} var, U={_fmt_val(u_value)} V, "
            f"f={_fmt_val(f_value)} Hz ({row.get('source')})"
        )

    blocks["E_anomalies"].extend(issues)
    return blocks


def compare_sources(session_df: pd.DataFrame) -> dict:
    return compare_sources_weighted(session_df)


def run_diagnostic(session_df: pd.DataFrame) -> dict:
    """Return probable cause, confidence, justification, evidence and missing data.

    Rules:
      - setpoint sent but not followed => vehicule
      - internal limit / gridcodes => borne
      - protocol errors/timeouts => communication
      - insufficient data => indetermine
    """
    simplified = _build_simplified_timeline(session_df)

    result = {
        "cause_probable": "indetermine",
        "confidence_score": 20,
        "justification": "Donnees insuffisantes pour trancher.",
        "evidence": [],
        "missing_data": [],
    }

    if simplified.empty:
        result["missing_data"] = ["timeline vide"]
        result["issues"] = ["Indetermine: timeline vide."]
        result["blocks"] = _build_reasoning_blocks(simplified, result["issues"])
        result["conclusion"] = "Indetermine"
        result["confidence"] = "Faible"
        result["executive_summary"] = "Aucune donnee exploitable dans la timeline."
        return result

    missing = []
    for column in ["Ptarget", "P", "U"]:
        if column not in simplified.columns or simplified[column].dropna().empty:
            missing.append(column)
    result["missing_data"] = missing

    cross = compare_sources(session_df)
    scores = cross.get("scores", {})
    evidence_table = cross.get("evidence_table", [])

    issues: list[str] = []

    vehicle_signal = False
    setpoints = simplified[simplified[["Ptarget", "Qtarget"]].notna().any(axis=1)].dropna(subset=["Ptarget"])
    for _, row in setpoints.iterrows():
        t0 = row["timestamp"]
        target = row["Ptarget"]
        window = simplified[
            (simplified["timestamp"] >= t0)
            & (simplified["timestamp"] <= t0 + pd.Timedelta(seconds=60))
        ]
        measured = window["P"].dropna()
        if measured.empty:
            continue
        if abs(measured.iloc[-1] - target) > max(2.0, 0.3 * max(abs(target), 1.0)):
            vehicle_signal = True
            message = (
                f"Consigne Ptarget={target} a {t0.isoformat()} non suivie "
                f"(P mesure final={measured.iloc[-1]:.2f})."
            )
            result["evidence"].append(message)
            issues.append("Consigne envoyee mais P mesure ne suit pas.")
            break

    station_signal = (
        (simplified["event_type"] == "power_limit").any()
        or (simplified["event_type"] == "gridcodes").any()
        or simplified["message"].astype(str).str.contains("restart|crash|fatal", case=False, na=False).any()
    )
    if station_signal:
        result["evidence"].append("Limitations internes, GridCodes ou crash/restart detectes cote borne.")
        issues.append("Limitation ou evenement GridCode cote borne.")

    comm_signal = (
        (simplified["event_type"] == "timeout").any()
        or simplified["message"].astype(str).str.contains("handshake|session error|no response|protocol", case=False, na=False).any()
        or (simplified["event_type"] == "protocol_event").sum() == 0
    )
    if comm_signal:
        result["evidence"].append("Timeouts ou erreurs protocole/handshake observes.")
        issues.append("Timeout ou erreur protocolaire observe.")

    dew_available = False
    if "payload" in simplified.columns:
        dew_available = simplified["payload"].apply(
            lambda payload: isinstance(payload, dict) and "measure" in str(payload.get("source_group", "")).lower()
        ).any()

    best_cause = max(scores, key=scores.get) if scores else "indetermine"
    best_score = scores.get(best_cause, 0.0)
    second = sorted(scores.values(), reverse=True)[1] if len(scores) > 1 else 0.0

    if ("Ptarget" in missing) and (not dew_available):
        result["cause_probable"] = "indetermine"
        result["confidence_score"] = 30
        result["justification"] = "Consigne Ptarget non disponible et mesure Dewesoft absente."
    elif best_score < 1.5 or abs(best_score - second) < 0.8:
        result["cause_probable"] = "indetermine"
        result["confidence_score"] = 35 if not missing else 30
        result["justification"] = "Preuves insuffisantes ou contradictoires pour conclure."
    else:
        result["cause_probable"] = best_cause
        result["confidence_score"] = min(85, int(45 + best_score * 12))
        if best_cause == "vehicule":
            result["justification"] = (
                "La consigne est visible mais la puissance mesuree ne suit pas, "
                "sans preuve dominante d'un blocage cote borne."
            )
        elif best_cause == "borne":
            result["justification"] = "Preuves explicites de recalcul, limitation ou crash cote borne."
        else:
            result["justification"] = "Preuves protocolaires ou timeouts dominants cote communication."

    if missing:
        result["evidence"].append(f"Donnees manquantes: {', '.join(missing)}")
        issues.append(f"Donnees manquantes: {', '.join(missing)}")

    if result["cause_probable"] == "vehicule" and not dew_available:
        result["confidence_score"] = min(result["confidence_score"], 55)
        result["justification"] += " Dewesoft non exploite: confiance abaissee pour une conclusion cote vehicule."
        result["evidence"].append("Absence de Dewesoft exploitable: rester prudent pour une conclusion cote vehicule.")

    if not issues:
        issues.append("Aucune anomalie majeure detectee par les regles actuelles.")

    if vehicle_signal and result["cause_probable"] == "indetermine":
        result["evidence"].append("Ecart consigne/mesure observe, mais preuves encore trop faibles pour conclure.")

    blocks = _build_reasoning_blocks(simplified, issues)

    result["issues"] = issues
    result["blocks"] = blocks
    result["cross_analysis"] = cross
    result["evidence_table"] = evidence_table
    result["conclusion"] = _label_cause(result["cause_probable"]).capitalize()
    result["confidence"] = "Elevee" if result["confidence_score"] >= 75 else "Moyenne" if result["confidence_score"] >= 55 else "Faible"

    observation = (
        "Tension et frequence nominales, puissance active faible ou nulle."
        if "P" not in missing and "U" not in missing
        else "Observations physiques partielles."
    )
    recommendation = (
        "Extraire Ptarget depuis EnergyManager ou PCAP et importer Dewesoft pour renforcer le diagnostic."
        if ("Ptarget" in missing or not dew_available)
        else "Comparer finement les consignes et le protocole avec les mesures Dewesoft et le meter interne."
    )
    result["executive_summary"] = (
        f"Cause probable: {_label_cause(result['cause_probable'])}. "
        f"Confiance: {result['confidence']} ({result['confidence_score']}%). "
        f"Observation: {observation} "
        f"Preuves: {', '.join(cross.get('insights', [])[:3]) if cross.get('insights') else 'aucune forte'}. "
        f"Donnees manquantes: {', '.join(missing) if missing else 'aucune'}. "
        f"Recommandation: {recommendation}"
    )

    return result
