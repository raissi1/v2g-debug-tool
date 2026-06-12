"""Rules-based V2G fault classifier layered on top of the generic diagnostic engine."""

from __future__ import annotations

from typing import Any

import pandas as pd


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _source_group(payload: object) -> str:
    if isinstance(payload, dict):
        return str(payload.get("source_group", "")).lower()
    return ""


def _has_source(df: pd.DataFrame, group_name: str) -> bool:
    if "payload" not in df.columns:
        return False
    return df["payload"].apply(lambda payload: group_name in _source_group(payload)).any()


def _ev(
    side: str,
    signal: str,
    value: object,
    message: str,
    *,
    timestamp: object = None,
    raw_log: str = "",
    details: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "side": side,
        "signal": signal,
        "value": "" if value is None else str(value),
        "message": message,
        "timestamp": str(timestamp) if timestamp is not None else None,
        "raw_log": raw_log[:240] if raw_log else "",
        "details": details or [],
    }


def _build_justification(cause: str, evidence: list[dict[str, Any]], has_dewesoft: bool) -> str:
    leading = [item["signal"] for item in evidence if item.get("side") == cause][:3]
    signals = ", ".join(leading) if leading else "signaux correles"

    if cause == "borne":
        return (
            f"Preuves dominantes cote borne ({signals}). "
            "La borne semble recalculer, limiter ou contraindre la session de facon autonome."
        )
    if cause == "vehicule":
        suffix = ""
        if not has_dewesoft:
            suffix = " La conclusion reste plus prudente sans Dewesoft CSV exploitable."
        return (
            f"Preuves dominantes cote vehicule ({signals}). "
            "La consigne est visible mais la reponse physique ne suit pas correctement."
            f"{suffix}"
        )
    if cause == "communication":
        return (
            f"Preuves dominantes cote communication ({signals}). "
            "Des anomalies protocole, timeouts ou indices PCAP perturbent la session."
        )
    return "Preuves insuffisantes ou contradictoires pour conclure fermement."


def _build_recommendations(cause: str, data_quality: dict[str, bool]) -> list[str]:
    recommendations: list[str] = []
    if not data_quality.get("dewesoft_csv"):
        recommendations.append("Convertir ou fournir un Dewesoft CSV pour renforcer la preuve physique.")
    if data_quality.get("dewesoft_conversion_pending"):
        recommendations.append("Finaliser la conversion des acquisitions Dewesoft brutes encore en attente.")
    if not data_quality.get("pcap"):
        recommendations.append("Ajouter une capture PCAP exploitable pour consolider la partie protocole.")

    if cause == "borne":
        recommendations.append("Verifier les logs EnergyManager autour des limitations, recalculs et GridCodes.")
        recommendations.append("Comparer les contraintes borne avec la consigne attendue sur la meme fenetre temporelle.")
    elif cause == "vehicule":
        recommendations.append("Comparer la consigne publiee avec la puissance mesuree Dewesoft sur 30 a 60 secondes.")
        recommendations.append("Verifier la puissance disponible declaree par le vehicule face a la consigne demandee.")
    elif cause == "communication":
        recommendations.append("Analyser les timeouts, les resets TCP et la sequence protocolaire autour du premier ecart.")
        recommendations.append("Verifier la continuite reseau entre publication de consigne et acquittement terrain.")
    else:
        recommendations.append("Recouper davantage les consignes, mesures et traces protocole avant de conclure.")

    return recommendations


def _numeric_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(index=df.index, dtype="float64")
    return pd.to_numeric(df[column], errors="coerce")


def classify_v2g_fault(session_df: pd.DataFrame, cross_analysis: dict | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {
        "cause": "indetermine",
        "confidence": "INDETERMINATE",
        "confidence_score": 20,
        "justification": "Donnees insuffisantes pour trancher.",
        "evidence": [],
        "borne_score": 0.0,
        "vehicule_score": 0.0,
        "communication_score": 0.0,
        "recommendations": [],
        "data_quality": {},
    }

    if session_df is None or session_df.empty:
        result["recommendations"] = ["Verifier que les fichiers de session ont bien ete detectes et parses."]
        return result

    df = session_df.copy()
    df["timestamp"] = pd.to_datetime(df.get("timestamp"), utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
    if df.empty:
        return result

    if "message" not in df.columns:
        df["message"] = ""
    df["message"] = df["message"].astype(str)

    has_energy_manager = _has_source(df, "energy_manager")
    has_meter_dispatcher = _has_source(df, "meter_dispatcher")
    has_dewesoft = _has_source(df, "measure")
    has_pcap = _has_source(df, "netlogger")
    has_charger_app = _has_source(df, "charger_app")
    dewesoft_conversion_pending = False
    if "payload" in df.columns:
        dewesoft_conversion_pending = bool(
            df["payload"].apply(
                lambda payload: isinstance(payload, dict) and bool(payload.get("conversion_required"))
            ).any()
        )
    result["data_quality"] = {
        "energy_manager": has_energy_manager,
        "meter_dispatcher": has_meter_dispatcher,
        "dewesoft_csv": has_dewesoft,
        "dewesoft_conversion_pending": dewesoft_conversion_pending,
        "pcap": has_pcap,
        "charger_app": has_charger_app,
    }

    evidence: list[dict[str, Any]] = []
    borne_score = 0.0
    vehicule_score = 0.0
    communication_score = 0.0

    if "event_type" not in df.columns:
        df["event_type"] = ""

    # Borne-side signals.
    borne_rows = df[
        df["event_type"].isin(["power_limit", "gridcodes", "error"])
        | df["message"].str.contains("limit|recalculated|derating|gridcode|fatal|restart", case=False, na=False)
    ]
    if not borne_rows.empty:
        row = borne_rows.iloc[0]
        borne_score += 2.2
        evidence.append(
            _ev(
                "borne",
                "borne_constraint_detected",
                row.get("event_type"),
                "Limitation, recalcul ou evenement borne explicite detecte.",
                timestamp=row.get("timestamp"),
                raw_log=row.get("message", ""),
            )
        )

    pcalc_rows = df[_numeric_series(df, "Pcalc").notna()]
    if not pcalc_rows.empty:
        row = pcalc_rows.iloc[0]
        borne_score += 1.4
        evidence.append(
            _ev(
                "borne",
                "recalculated_power",
                row.get("Pcalc"),
                "La borne a calcule une puissance interne Pcalc.",
                timestamp=row.get("timestamp"),
                raw_log=row.get("message", ""),
            )
        )

    # Vehicle-side signals from cross analysis.
    cross_rows = pd.DataFrame((cross_analysis or {}).get("rows", []))
    if not cross_rows.empty:
        for column in ["Ptarget", "P_dewesoft", "P_meter"]:
            if column in cross_rows.columns:
                cross_rows[column] = pd.to_numeric(cross_rows[column], errors="coerce")
        measured = pd.Series([float("nan")] * len(cross_rows), index=cross_rows.index, dtype="float64")
        if "P_dewesoft" in cross_rows.columns:
            measured = cross_rows["P_dewesoft"]
        if "P_meter" in cross_rows.columns:
            measured = measured.where(pd.notna(measured), cross_rows["P_meter"])
        if "Ptarget" in cross_rows.columns:
            mismatch = (cross_rows["Ptarget"] - measured).abs() > 0.25 * cross_rows["Ptarget"].abs().clip(lower=200.0)
            mismatch_rows = cross_rows[mismatch.fillna(False)]
            if not mismatch_rows.empty:
                row = mismatch_rows.iloc[0]
                vehicule_score += 3.0
                details = [
                    f"Ptarget={row.get('Ptarget')}",
                    f"P_dewesoft={row.get('P_dewesoft')}",
                    f"P_meter={row.get('P_meter')}",
                ]
                evidence.append(
                    _ev(
                        "vehicule",
                        "setpoint_not_followed",
                        "ecart de puissance",
                        "La puissance mesuree ne suit pas la consigne visible sur la meme fenetre temporelle.",
                        timestamp=row.get("timestamp"),
                        raw_log=str(row.get("message", "")),
                        details=details,
                    )
                )

    # Vehicle available discharge power mismatch.
    if "payload" in df.columns and "Ptarget" in df.columns:
        ptarget_series = pd.to_numeric(df["Ptarget"], errors="coerce").dropna()
        available_rows = df[
            df["payload"].apply(
                lambda payload: isinstance(payload, dict) and payload.get("AvailableDischargePower") is not None
            )
        ]
        if not available_rows.empty and not ptarget_series.empty:
            avail_series = available_rows["payload"].apply(
                lambda payload: _safe_float(payload.get("AvailableDischargePower")) if isinstance(payload, dict) else None
            ).dropna()
            if not avail_series.empty:
                available_power = abs(avail_series.median())
                requested_power = abs(ptarget_series.median())
                if requested_power > 0 and available_power < 0.7 * requested_power:
                    vehicule_score += 2.1
                    evidence.append(
                        _ev(
                            "vehicule",
                            "available_power_mismatch",
                            f"available={available_power:.0f}W vs target={requested_power:.0f}W",
                            "La puissance disponible declaree est sensiblement inferieure a la consigne demandee.",
                        )
                    )

    # Communication signals.
    timeout_rows = df[
        (df["event_type"].isin(["timeout", "warning", "protocol_event"]))
        & df["message"].str.contains("timeout|handshake|protocol|no response|reset", case=False, na=False)
    ]
    if not timeout_rows.empty:
        row = timeout_rows.iloc[0]
        communication_score += 2.4
        evidence.append(
            _ev(
                "communication",
                "protocol_timeout",
                row.get("event_type"),
                "Un timeout ou une anomalie protocolaire a ete detecte.",
                timestamp=row.get("timestamp"),
                raw_log=row.get("message", ""),
            )
        )

    pcap_rows = df[
        df["payload"].apply(lambda payload: isinstance(payload, dict) and payload.get("parser") == "pcap_generic")
    ] if "payload" in df.columns else pd.DataFrame()
    if not pcap_rows.empty:
        payload = pcap_rows.iloc[0]["payload"]
        resets = int(payload.get("pcap_tcp_rst_count", 0) or 0)
        if resets > 0:
            communication_score += 2.0
            evidence.append(
                _ev(
                    "communication",
                    "pcap_tcp_resets",
                    resets,
                    "Des resets TCP sont visibles dans le PCAP.",
                    timestamp=pcap_rows.iloc[0].get("timestamp"),
                    raw_log=pcap_rows.iloc[0].get("message", ""),
                )
            )
        elif payload.get("pcap_likely_v2g"):
            communication_score += 0.5
            evidence.append(
                _ev(
                    "communication",
                    "pcap_v2g_visible",
                    payload.get("pcap_top_ports"),
                    "Le PCAP montre un trafic V2G identifiable sans reset TCP.",
                    timestamp=pcap_rows.iloc[0].get("timestamp"),
                    raw_log=pcap_rows.iloc[0].get("message", ""),
                )
            )
        gap_events = payload.get("pcap_tls_gap_events_s") or []
        if gap_events:
            communication_score += 1.4
            evidence.append(
                _ev(
                    "communication",
                    "pcap_tls_gap",
                    gap_events[:3],
                    "Des gaps significatifs apparaissent dans le trafic TLS V2G.",
                    timestamp=pcap_rows.iloc[0].get("timestamp"),
                    raw_log=pcap_rows.iloc[0].get("message", ""),
                )
            )
        sdp_messages = payload.get("pcap_sdp_messages") or []
        if sdp_messages:
            first_sdp = sdp_messages[0]
            communication_score += 0.7
            evidence.append(
                _ev(
                    "communication",
                    "pcap_sdp_exchange",
                    first_sdp.get("message_type"),
                    "Le PCAP contient une negotiation SDP exploitable.",
                    timestamp=pcap_rows.iloc[0].get("timestamp"),
                    raw_log=pcap_rows.iloc[0].get("message", ""),
                    details=[
                        f"security={first_sdp.get('security')}",
                        f"port={first_sdp.get('server_port')}",
                    ],
                )
            )

    if dewesoft_conversion_pending and not has_dewesoft:
        vehicule_score += 0.4
        evidence.append(
            _ev(
                "vehicule",
                "dewesoft_conversion_pending",
                "conversion pending",
                "Des acquisitions Dewesoft brutes existent mais ne sont pas encore converties en CSV exploitable.",
            )
        )

    # Long timeline gaps can also support communication issues.
    if len(df) >= 2:
        gaps = df["timestamp"].diff().dt.total_seconds().dropna()
        large_gaps = gaps[gaps > 30]
        if not large_gaps.empty:
            communication_score += 1.0
            evidence.append(
                _ev(
                    "communication",
                    "timeline_gap",
                    f"{large_gaps.max():.0f}s",
                    "Une coupure temporelle importante a ete detectee dans la timeline.",
                )
            )

    scores = {
        "borne": round(borne_score, 2),
        "vehicule": round(vehicule_score, 2),
        "communication": round(communication_score, 2),
    }
    result["borne_score"] = scores["borne"]
    result["vehicule_score"] = scores["vehicule"]
    result["communication_score"] = scores["communication"]
    result["evidence"] = evidence

    sorted_scores = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    best_cause, best_score = sorted_scores[0]
    second_score = sorted_scores[1][1] if len(sorted_scores) > 1 else 0.0
    margin = best_score - second_score

    if best_score >= 3.0 and margin >= 1.0:
        confidence = "HIGH"
        confidence_score = min(90, int(55 + best_score * 8))
    elif best_score >= 1.5 and margin >= 0.5:
        confidence = "MEDIUM"
        confidence_score = min(75, int(42 + best_score * 10))
    elif best_score >= 0.5:
        confidence = "LOW"
        confidence_score = min(55, int(30 + best_score * 10))
    else:
        confidence = "INDETERMINATE"
        confidence_score = 20

    if best_score < 0.5:
        result["cause"] = "indetermine"
    else:
        result["cause"] = best_cause
    result["confidence"] = confidence
    result["confidence_score"] = confidence_score
    result["justification"] = _build_justification(result["cause"], evidence, has_dewesoft)
    result["recommendations"] = _build_recommendations(result["cause"], result["data_quality"])
    return result
