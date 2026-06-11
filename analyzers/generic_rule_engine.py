"""Generic reusable diagnostic rules for V2G sessions."""

from __future__ import annotations

import pandas as pd


def _base_rule(
    rule_id: str,
    title: str,
    category: str,
    status: str,
    severity: str,
    expected: str,
    observed: str,
    reason: str,
    timestamp: str | None = None,
    source: str | None = None,
) -> dict:
    return {
        "id": rule_id,
        "title": title,
        "category": category,
        "status": status,
        "severity": severity,
        "expected": expected,
        "observed": observed,
        "reason": reason,
        "timestamp": timestamp,
        "source": source,
    }


def evaluate_generic_rules(simplified: pd.DataFrame, cross_analysis: dict, dewesoft_status: dict[str, bool]) -> dict:
    rules: list[dict] = []

    if simplified.empty:
        rules.append(
            _base_rule(
                "timeline_presence",
                "Presence de timeline exploitable",
                "coverage",
                "fail",
                "high",
                "Une timeline exploitable doit etre disponible.",
                "Timeline vide.",
                "Aucun evenement exploitable n'a ete reconstruit.",
            )
        )
        return {
            "rules": rules,
            "summary": {"pass": 0, "fail": 1, "warn": 0, "unknown": 0},
        }

    work = simplified.copy()
    work["timestamp"] = pd.to_datetime(work["timestamp"], utc=True, errors="coerce")
    work = work.dropna(subset=["timestamp"]).sort_values("timestamp")

    cross_rows = pd.DataFrame(cross_analysis.get("rows", []))
    if not cross_rows.empty:
        cross_rows["timestamp"] = pd.to_datetime(cross_rows["timestamp"], utc=True, errors="coerce")
        cross_rows = cross_rows.dropna(subset=["timestamp"]).sort_values("timestamp")

    setpoints = work[work["Ptarget"].notna()] if "Ptarget" in work.columns else pd.DataFrame()
    if setpoints.empty:
        rules.append(
            _base_rule(
                "setpoint_visibility",
                "Visibilite des consignes",
                "setpoint",
                "unknown",
                "medium",
                "La session devrait contenir des consignes explicites Ptarget/Qtarget.",
                "Aucune consigne Ptarget exploitable detectee.",
                "Le moteur ne peut pas comparer proprement l'attendu et l'observe sans consigne visible.",
            )
        )
    else:
        if not cross_rows.empty and "Ptarget" in cross_rows.columns:
            measured = pd.Series(pd.NA, index=cross_rows.index, dtype="object")
            if "P_dewesoft" in cross_rows.columns:
                measured = cross_rows["P_dewesoft"]
            if "P_meter" in cross_rows.columns:
                measured = measured.where(pd.notna(measured), cross_rows["P_meter"])
            measured = pd.to_numeric(measured, errors="coerce")
            ptarget = pd.to_numeric(cross_rows["Ptarget"], errors="coerce")
            mismatch = (ptarget - measured).abs() > 0.3 * ptarget.abs().clip(lower=1.0)
            mismatch_rows = cross_rows[mismatch.fillna(False)]
            if not mismatch_rows.empty:
                row = mismatch_rows.iloc[0]
                rules.append(
                    _base_rule(
                        "setpoint_following",
                        "Suivi de la consigne active",
                        "setpoint",
                        "fail",
                        "high",
                        "La puissance mesuree devrait converger vers la consigne active.",
                        f"Ptarget={row.get('Ptarget')} ; P_meter={row.get('P_meter')} ; P_dewesoft={row.get('P_dewesoft')}",
                        "Un ecart significatif entre la consigne et la puissance mesuree a ete detecte.",
                        timestamp=row["timestamp"].isoformat() if pd.notna(row["timestamp"]) else None,
                        source=row.get("source"),
                    )
                )
            else:
                row = setpoints.iloc[0]
                rules.append(
                    _base_rule(
                        "setpoint_following",
                        "Suivi de la consigne active",
                        "setpoint",
                        "pass",
                        "medium",
                        "La puissance mesuree devrait converger vers la consigne active.",
                        "Aucun ecart significatif n'a ete trouve sur les points compares.",
                        "Les points compares ne montrent pas de deviation majeure entre consigne et mesure.",
                        timestamp=row["timestamp"].isoformat() if pd.notna(row["timestamp"]) else None,
                        source=row.get("source"),
                    )
                )

    station_rows = work[
        (work["event_type"].isin(["power_limit", "gridcodes", "error"]))
        | work["message"].astype(str).str.contains("recalculated|published|maxpower|derating|limit applied|crash|fatal", case=False, na=False)
    ]
    if station_rows.empty:
        rules.append(
            _base_rule(
                "station_internal_limits",
                "Indices de limitation cote borne",
                "borne",
                "pass",
                "low",
                "Aucun indice fort de limitation ou recalcul interne ne devrait apparaitre si la borne est saine.",
                "Aucun indice fort de limitation interne detecte.",
                "Les logs borne ne montrent pas de recalcul ou limitation explicite au premier niveau.",
            )
        )
    else:
        row = station_rows.iloc[0]
        rules.append(
            _base_rule(
                "station_internal_limits",
                "Indices de limitation cote borne",
                "borne",
                "warn",
                "high",
                "Les recalculs et limitations internes doivent etre expliques et coherents avec la consigne.",
                str(row.get("message", ""))[:220],
                "Des indices de limitation, recalcul ou evenement borne explicite ont ete detectes.",
                timestamp=row["timestamp"].isoformat() if pd.notna(row["timestamp"]) else None,
                source=row.get("source"),
            )
        )

    protocol_rows = work[
        (work["event_type"].isin(["timeout", "protocol_event", "warning"]))
        & work["message"].astype(str).str.contains("timeout|handshake|protocol|no response", case=False, na=False)
    ]
    if protocol_rows.empty:
        rules.append(
            _base_rule(
                "protocol_health",
                "Sante protocolaire",
                "communication",
                "pass",
                "medium",
                "Aucun timeout ni erreur protocolaire majeure ne devrait apparaitre.",
                "Aucun timeout critique remonte par les regles generiques.",
                "La communication ne montre pas d'anomalie critique evidente au premier niveau.",
            )
        )
    else:
        row = protocol_rows.iloc[0]
        rules.append(
            _base_rule(
                "protocol_health",
                "Sante protocolaire",
                "communication",
                "warn",
                "high",
                "La sequence protocolaire devrait etre complete et sans timeout critique.",
                str(row.get("message", ""))[:220],
                "Un timeout ou une erreur protocolaire a ete detecte.",
                timestamp=row["timestamp"].isoformat() if pd.notna(row["timestamp"]) else None,
                source=row.get("source"),
            )
        )

    if dewesoft_status.get("csv_available"):
        rules.append(
            _base_rule(
                "measurement_coverage",
                "Couverture des mesures physiques",
                "coverage",
                "pass",
                "medium",
                "Des mesures physiques exploitables devraient etre disponibles pour renforcer le verdict.",
                "Dewesoft CSV exploitable present.",
                "Le moteur dispose d'une mesure physique externe exploitable.",
            )
        )
    elif dewesoft_status.get("raw_detected"):
        rules.append(
            _base_rule(
                "measurement_coverage",
                "Couverture des mesures physiques",
                "coverage",
                "warn",
                "medium",
                "Des mesures physiques exploitables devraient etre disponibles pour renforcer le verdict.",
                "Acquisitions Dewesoft brutes detectees, mais non converties en CSV.",
                "Les mesures existent probablement, mais elles ne sont pas encore exploitables automatiquement.",
            )
        )
    else:
        rules.append(
            _base_rule(
                "measurement_coverage",
                "Couverture des mesures physiques",
                "coverage",
                "unknown",
                "medium",
                "Des mesures physiques exploitables devraient etre disponibles pour renforcer le verdict.",
                "Aucune mesure Dewesoft exploitable detectee.",
                "Le diagnostic reste plus faible sans mesure physique externe.",
            )
        )

    status_counts = {"pass": 0, "fail": 0, "warn": 0, "unknown": 0}
    for rule in rules:
        status_counts[rule["status"]] = status_counts.get(rule["status"], 0) + 1

    return {"rules": rules, "summary": status_counts}
