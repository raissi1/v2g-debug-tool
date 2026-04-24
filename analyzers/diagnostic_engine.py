"""Intelligent generic diagnostic engine for V2G sessions."""

from __future__ import annotations

import pandas as pd


def _build_simplified_timeline(session_df: pd.DataFrame) -> pd.DataFrame:
    """Reconstruct a compact timeline for diagnostic reasoning."""
    if session_df.empty:
        return session_df

    df = session_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")

    for col in ["Ptarget", "Qtarget", "P", "Q", "U", "frequency"]:
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
        "U",
        "frequency",
        "message",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    return df[keep_cols].copy()


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
        return result

    missing = []
    for col in ["Ptarget", "P", "U"]:
        if col not in simplified.columns or simplified[col].dropna().empty:
            missing.append(col)
    result["missing_data"] = missing

    # 1) vehicle suspect: setpoint changed, measured power does not follow.
    vehicle_signal = False
    setpoints = simplified[simplified["event_type"] == "setpoint"].dropna(subset=["Ptarget"])
    for _, row in setpoints.iterrows():
        t0 = row["timestamp"]
        target = row["Ptarget"]
        window = simplified[(simplified["timestamp"] >= t0) & (simplified["timestamp"] <= t0 + pd.Timedelta(seconds=60))]
        measured = window["P"].dropna()
        if measured.empty:
            continue
        if abs(measured.iloc[-1] - target) > max(2.0, 0.3 * max(abs(target), 1.0)):
            vehicle_signal = True
            result["evidence"].append(
                f"Consigne Ptarget={target} à {t0.isoformat()} non suivie (P mesuré final={measured.iloc[-1]:.2f})."
            )
            break

    # 2) station/config signal.
    station_signal = (
        (simplified["event_type"] == "power_limit").any()
        or (simplified["event_type"] == "gridcodes").any()
        or simplified["message"].astype(str).str.contains("restart|crash|fatal", case=False, na=False).any()
    )
    if station_signal:
        result["evidence"].append("Limitations internes / GridCodes / crash-restart détectés côté station.")

    # 3) communication signal.
    comm_signal = (
        (simplified["event_type"] == "timeout").any()
        or simplified["message"].astype(str).str.contains("handshake|session error|no response|protocol", case=False, na=False).any()
        or (simplified["event_type"] == "protocol_event").sum() == 0
    )
    if comm_signal:
        result["evidence"].append("Timeouts ou erreurs protocole/handshake observés.")

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

    return result
