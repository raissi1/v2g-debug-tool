"""Generic V2G debug engine: reasoning blocks + probable cause inference."""

from __future__ import annotations

import pandas as pd

from analyzers.generic_debug import detect_behavior_issues


def _payload_series(timeline: pd.DataFrame, source_group: str | None = None) -> pd.DataFrame:
    frame = timeline.copy()
    if source_group and "payload" in frame.columns:
        frame = frame[frame["payload"].apply(lambda p: isinstance(p, dict) and p.get("source_group") == source_group)]
    return frame


def build_debug_blocks(timeline: pd.DataFrame) -> dict[str, list[str]]:
    blocks = {
        "A_requested": [],
        "B_station_computed": [],
        "C_sent_to_vehicle": [],
        "D_measured": [],
        "E_anomalies": [],
    }

    if timeline.empty:
        return blocks

    setpoints = timeline[timeline["event_type"] == "setpoint"]
    for _, row in setpoints.head(20).iterrows():
        p = row.get("Ptarget")
        q = row.get("Qtarget")
        blocks["A_requested"].append(f"{row.get('timestamp')} • setpoint demandé P={p} Q={q} ({row.get('source')})")

    em = _payload_series(timeline, "energy_manager")
    em_relevant = em[em["event_type"].isin(["setpoint", "power_limit", "gridcodes", "warning", "error"])]
    for _, row in em_relevant.head(20).iterrows():
        blocks["B_station_computed"].append(
            f"{row.get('timestamp')} • {row.get('event_type')} côté EnergyManager: {row.get('message')}"
        )

    ca = _payload_series(timeline, "charger_app")
    ca_relevant = ca[ca["event_type"].isin(["setpoint", "protocol_event", "session_event", "timeout"])]
    for _, row in ca_relevant.head(20).iterrows():
        blocks["C_sent_to_vehicle"].append(
            f"{row.get('timestamp')} • {row.get('event_type')} côté ChargerApp: {row.get('message')}"
        )

    measured = timeline[timeline["event_type"] == "physical_measurement"]
    for _, row in measured.head(20).iterrows():
        blocks["D_measured"].append(
            f"{row.get('timestamp')} • mesure P={row.get('P')} Q={row.get('Q')} U={row.get('U')} f={row.get('frequency', None)} ({row.get('source')})"
        )

    for issue in detect_behavior_issues(timeline):
        blocks["E_anomalies"].append(issue)

    return blocks


def infer_responsibility(timeline: pd.DataFrame, issues: list[str]) -> tuple[str, str, list[str], list[str]]:
    """Return (conclusion, confidence, evidence, missing_data)."""
    evidence: list[str] = []
    missing_data: list[str] = []
    text = " ".join(issues).lower()

    for col in ["Ptarget", "Qtarget", "P", "Q", "U", "frequency"]:
        if col not in timeline.columns or pd.to_numeric(timeline[col], errors="coerce").dropna().empty:
            missing_data.append(col)

    if "consigne envoyée mais p mesuré ne suit pas" in text:
        evidence.append("Consigne appliquée côté borne, mesure/réponse non conforme")
        return "Problème probable côté véhicule", "Moyenne", evidence, missing_data

    bore_signals = (
        (timeline["event_type"] == "power_limit").any()
        or (timeline["event_type"] == "gridcodes").any()
        or timeline["message"].astype(str).str.contains("restart|crash|fatal", case=False, na=False).any()
    )
    if bore_signals:
        evidence.append("Limitations/GridCodes/restart détectés côté borne")
        return "Problème probable borne / configuration interne", "Moyenne", evidence, missing_data

    comm_signals = (
        (timeline["event_type"] == "timeout").any()
        or timeline["message"].astype(str).str.contains("handshake|session error|no response", case=False, na=False).any()
        or (timeline["event_type"] == "protocol_event").sum() == 0
    )
    if comm_signals:
        evidence.append("Timeout/erreurs de session/protocole détectés")
        return "Problème probable communication", "Moyenne", evidence, missing_data

    if len(missing_data) >= 3:
        evidence.append("Données insuffisantes pour trancher")
        return "Indéterminé", "Faible", evidence, missing_data

    evidence.append("Aucune signature forte, corrélation incomplète")
    return "Indéterminé", "Faible", evidence, missing_data


def run_generic_diagnostic(timeline: pd.DataFrame) -> dict:
    issues = detect_behavior_issues(timeline)
    blocks = build_debug_blocks(timeline)
    conclusion, confidence, evidence, missing_data = infer_responsibility(timeline, issues)

    executive_summary = (
        f"Conclusion: {conclusion}. Confiance: {confidence}. "
        f"Anomalies majeures: {len(issues)}. Données manquantes: {', '.join(missing_data) if missing_data else 'aucune'}"
    )

    return {
        "issues": issues,
        "blocks": blocks,
        "conclusion": conclusion,
        "confidence": confidence,
        "evidence": evidence,
        "missing_data": missing_data,
        "executive_summary": executive_summary,
        "event_counts": timeline["event_type"].value_counts().to_dict() if not timeline.empty else {},
    }
