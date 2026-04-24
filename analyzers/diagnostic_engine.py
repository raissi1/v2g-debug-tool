"""Intelligent generic diagnostic engine for V2G sessions."""

from __future__ import annotations

import pandas as pd
import re


def _build_simplified_timeline(session_df: pd.DataFrame) -> pd.DataFrame:
    """Reconstruct a compact timeline for diagnostic reasoning."""
    if session_df.empty:
        return session_df

    df = session_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")

    for col in ["Ptarget", "Qtarget", "P", "Q", "S", "U", "U_avg", "frequency", "frequency_Hz", "Pcalc", "Qcalc", "Smax", "derating"]:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors="coerce")

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
    keep_cols = [c for c in keep_cols if c in df.columns]
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
            f"{row['timestamp']} • demande Ptarget={_fmt_val(row.get('Ptarget'))} W Qtarget={_fmt_val(row.get('Qtarget'))} var ({row.get('source')})"
        )

    request_keywords = re.compile(
        r"request\s*to\s*accept\s*setpoint|centralsetpoint|maxpower_w|charge\s*limit|discharge\s*limit|ocpp|ocpp_offline|cpd|\bev\b",
        re.IGNORECASE,
    )
    keyword_rows = work[work["message"].astype(str).str.contains(request_keywords, na=False)].head(25)
    for _, row in keyword_rows.iterrows():
        if len(blocks["A_requested"]) >= 25:
            break
        blocks["A_requested"].append(f"{row['timestamp']} • demande/contrainte: {row.get('message')[:220]}")

    computed = work[
        work[["Pcalc", "Qcalc", "Smax", "derating"]].notna().any(axis=1)
        | (work["event_type"] == "power_limit")
    ].head(25)
    for _, row in computed.iterrows():
        blocks["B_station_computed"].append(
            f"{row['timestamp']} • calcul borne Pcalc={_fmt_val(row.get('Pcalc'))} Qcalc={_fmt_val(row.get('Qcalc'))} Smax={_fmt_val(row.get('Smax'))} derating={_fmt_val(row.get('derating'))} ({row.get('source')})"
        )

    published_keywords = re.compile(
        r"setpoint\s*is\s*recalculated\s*and\s*published|published|centralsetpoint|maxpower_w|limit|ocpp|cpd|ev",
        re.IGNORECASE,
    )
    published_rows = work[work["message"].astype(str).str.contains(published_keywords, na=False)].head(25)
    for _, row in published_rows.iterrows():
        if len(blocks["B_station_computed"]) >= 25:
            break
        blocks["B_station_computed"].append(f"{row['timestamp']} • publication borne: {row.get('message')[:220]}")

    sent = work[
        (work["event_type"].isin(["setpoint", "protocol_event"]))
        & (
            work["message"].astype(str).str.contains("send|sent|publish|tx|transmit|iso15118|din70121|schedule|request", case=False, na=False)
            | work["source_group"].astype(str).str.contains("charger_app|netlogger", case=False, na=False)
        )
    ].head(25)
    for _, row in sent.iterrows():
        blocks["C_sent_to_vehicle"].append(
            f"{row['timestamp']} • envoyé EV ({row.get('event_type')}): {row.get('message')[:220]}"
        )

    measured = work[work[["P", "Q", "S", "U", "U_avg", "frequency", "frequency_Hz"]].notna().any(axis=1)].head(25)
    for _, row in measured.iterrows():
        u_value = row.get("U") if not pd.isna(row.get("U")) else row.get("U_avg")
        f_value = row.get("frequency") if not pd.isna(row.get("frequency")) else row.get("frequency_Hz")
        blocks["D_measured"].append(
            f"{row['timestamp']} • mesure P={_fmt_val(row.get('P'))} W, Q={_fmt_val(row.get('Q'))} var, U={_fmt_val(u_value)} V, f={_fmt_val(f_value)} Hz ({row.get('source')})"
        )

    blocks["E_anomalies"].extend(issues)
    return blocks


def run_diagnostic(session_df: pd.DataFrame) -> dict:
    """Return probable cause, confidence, justification, evidence and missing data.

    Rules:
      - setpoint sent but not followed => véhicule
      - internal limit / gridcodes => borne
      - protocol errors/timeouts => communication
      - insufficient data => indéterminé
    """
    simplified = _build_simplified_timeline(session_df)

    result = {
        "cause_probable": "indéterminé",
        "confidence_score": 20,
        "justification": "Données insuffisantes pour trancher.",
        "evidence": [],
        "missing_data": [],
    }

    if simplified.empty:
        result["missing_data"] = ["timeline vide"]
        result["issues"] = ["Indéterminé: timeline vide."]
        result["blocks"] = _build_reasoning_blocks(simplified, result["issues"])
        result["conclusion"] = "Indéterminé"
        result["confidence"] = "Faible"
        result["executive_summary"] = "Aucune donnée exploitable dans la timeline."
        return result

    missing = []
    for col in ["Ptarget", "P", "U"]:
        if col not in simplified.columns or simplified[col].dropna().empty:
            missing.append(col)
    result["missing_data"] = missing

    issues: list[str] = []

    # 1) vehicle suspect: setpoint changed, measured power does not follow.
    vehicle_signal = False
    setpoints = simplified[simplified[["Ptarget", "Qtarget"]].notna().any(axis=1)].dropna(subset=["Ptarget"])
    for _, row in setpoints.iterrows():
        t0 = row["timestamp"]
        target = row["Ptarget"]
        window = simplified[(simplified["timestamp"] >= t0) & (simplified["timestamp"] <= t0 + pd.Timedelta(seconds=60))]
        measured = window["P"].dropna()
        if measured.empty:
            continue
        if abs(measured.iloc[-1] - target) > max(2.0, 0.3 * max(abs(target), 1.0)):
            vehicle_signal = True
            msg = f"Consigne Ptarget={target} à {t0.isoformat()} non suivie (P mesuré final={measured.iloc[-1]:.2f})."
            result["evidence"].append(msg)
            issues.append("Consigne envoyée mais P mesuré ne suit pas.")
            break

    # 2) station/config signal.
    station_signal = (
        (simplified["event_type"] == "power_limit").any()
        or (simplified["event_type"] == "gridcodes").any()
        or simplified["message"].astype(str).str.contains("restart|crash|fatal", case=False, na=False).any()
    )
    if station_signal:
        result["evidence"].append("Limitations internes / GridCodes / crash-restart détectés côté station.")
        issues.append("Limitation ou événement GridCode côté borne.")

    # 3) communication signal.
    comm_signal = (
        (simplified["event_type"] == "timeout").any()
        or simplified["message"].astype(str).str.contains("handshake|session error|no response|protocol", case=False, na=False).any()
        or (simplified["event_type"] == "protocol_event").sum() == 0
    )
    if comm_signal:
        result["evidence"].append("Timeouts ou erreurs protocole/handshake observés.")
        issues.append("Timeout/erreur protocolaire observé.")

    # Decision priority by explicit rule strength.
    if vehicle_signal:
        result["cause_probable"] = "véhicule"
        result["confidence_score"] = 75 if not missing else 65
        result["justification"] = "La consigne a été envoyée mais la puissance mesurée ne suit pas suffisamment."
    elif station_signal:
        result["cause_probable"] = "borne"
        result["confidence_score"] = 72 if not missing else 60
        result["justification"] = "Des limitations internes/GridCodes/crash-restart indiquent un comportement côté borne/configuration."
    elif comm_signal:
        result["cause_probable"] = "communication"
        result["confidence_score"] = 70 if not missing else 55
        result["justification"] = "Des signaux de timeout/protocole suggèrent un problème de communication."
    else:
        result["cause_probable"] = "indéterminé"
        result["confidence_score"] = 30 if missing else 45
        result["justification"] = "Aucune signature forte détectée avec les règles génériques actuelles."

    if missing:
        result["evidence"].append(f"Données manquantes: {', '.join(missing)}")
        issues.append(f"Données manquantes: {', '.join(missing)}")

    if not issues:
        issues.append("Aucune anomalie majeure détectée par les règles actuelles.")

    blocks = _build_reasoning_blocks(simplified, issues)

    result["issues"] = issues
    result["blocks"] = blocks
    result["conclusion"] = result["cause_probable"].capitalize()
    result["confidence"] = "Élevée" if result["confidence_score"] >= 75 else "Moyenne" if result["confidence_score"] >= 55 else "Faible"
    result["executive_summary"] = (
        f"Cause probable: {result['cause_probable']} (confiance {result['confidence_score']}%). "
        f"Anomalies: {len(issues)}. Données manquantes: {', '.join(missing) if missing else 'aucune'}."
    )

    return result
