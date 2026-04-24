"""Generic analyzer producing first-level diagnostics from a session timeline."""

from __future__ import annotations

import pandas as pd


REQUIRED_COLUMNS = {"timestamp", "source", "event_type", "message", "payload"}
EXPECTED_SOURCES = {"charger_app", "energy_manager", "meter_dispatcher", "netlogger"}


def _top_messages(frame: pd.DataFrame, event_type: str, limit: int = 3) -> str:
    subset = frame[frame["event_type"] == event_type]
    if subset.empty:
        return "aucun"
    counts = subset["message"].value_counts().head(limit)
    return " | ".join(f"{msg[:120]} ({count})" for msg, count in counts.items())


def _extract_source_groups(frame: pd.DataFrame) -> set[str]:
    groups: set[str] = set()
    for payload in frame["payload"].tolist():
        if isinstance(payload, dict):
            group = payload.get("source_group") or payload.get("parser")
            if isinstance(group, str):
                groups.add(group)
    return groups


def detect_behavior_issues(timeline: pd.DataFrame) -> list[str]:
    """Detect first-level generic inconsistencies in physical behavior."""
    issues: list[str] = []
    if timeline.empty:
        return ["Indéterminé: aucune donnée exploitable."]

    work = timeline.copy()
    work["_ts"] = pd.to_datetime(work["timestamp"], utc=True, errors="coerce")
    work = work.sort_values("_ts", na_position="last")

    for col in ("Ptarget", "P", "AvailableDischargePower"):
        if col not in work.columns:
            work[col] = pd.NA
        work[col] = pd.to_numeric(work[col], errors="coerce")

    # Rule 1: setpoint changed but measured power does not follow.
    setpoint_rows = work[work["event_type"] == "setpoint"].dropna(subset=["_ts", "Ptarget"])
    for _, row in setpoint_rows.iterrows():
        t0 = row["_ts"]
        target = row["Ptarget"]
        if pd.isna(target):
            continue
        window = work[(work["_ts"] >= t0) & (work["_ts"] <= t0 + pd.Timedelta(seconds=60))]
        measured = window["P"].dropna()
        if measured.empty:
            continue
        if abs(measured.iloc[-1] - target) > max(2.0, 0.3 * max(abs(target), 1.0)):
            issues.append(
                "Consigne envoyée mais P mesuré ne suit pas (possible problème véhicule ou exécution consigne)."
            )
            break

    # Rule 2: internal limitation clues (borne side tendency).
    if (work["event_type"] == "power_limit").any():
        issues.append("Limitation de puissance détectée dans les logs (tendance borne).")
    else:
        candidate = work.dropna(subset=["AvailableDischargePower", "Ptarget"])
        if not candidate.empty and ((candidate["AvailableDischargePower"] + 1e-6) < candidate["Ptarget"].abs()).any():
            issues.append("AvailableDischargePower inférieur à la consigne (limitation interne probable).")

    # Rule 3: incoherent behavior.
    if "state" in work.columns:
        charging_rows = work[work["state"] == "charging"]
        if not charging_rows.empty and "P" in charging_rows.columns:
            low_power_ratio = (charging_rows["P"].abs() < 0.5).mean()
            if low_power_ratio > 0.7:
                issues.append("État 'charging' avec puissance quasi nulle: comportement incohérent.")

    # Rule 4: missing key data.
    missing_core = []
    for col in ("Ptarget", "P", "U"):
        if col not in work.columns or work[col].dropna().empty:
            missing_core.append(col)
    if missing_core:
        issues.append(f"Données manquantes ({', '.join(missing_core)}): diagnostic indéterminé.")

    if not issues:
        issues.append("Aucune anomalie majeure détectée par les règles génériques actuelles.")

    return issues


def summarize_session(timeline: pd.DataFrame) -> list[str]:
    """Return an improved generic V2G debug summary from a reconstructed timeline."""
    if timeline.empty:
        return ["Aucun événement détecté dans la session."]

    missing_cols = REQUIRED_COLUMNS - set(timeline.columns)
    if missing_cols:
        return [f"Timeline invalide: colonnes manquantes {sorted(missing_cols)}"]

    lines: list[str] = []
    lines.append(f"Nombre total d'événements (fenêtre utile): {len(timeline)}")

    ts = pd.to_datetime(timeline["timestamp"], utc=True, errors="coerce").dropna()
    if not ts.empty:
        lines.append(f"Plage temporelle utile: {ts.min().isoformat()} → {ts.max().isoformat()}")
        lines.append(f"Durée utile approximative: {ts.max() - ts.min()}")
    else:
        lines.append("Plage temporelle utile: timestamps non disponibles.")

    lines.append(f"Erreurs détectées: {_top_messages(timeline, 'error')}")
    lines.append(f"Warnings détectés: {_top_messages(timeline, 'warning')}")
    lines.append(f"Événements GridCodes: {_top_messages(timeline, 'gridcodes')}")
    lines.append(f"Changements de setpoint: {_top_messages(timeline, 'setpoint')}")
    lines.append(f"Limitations détectées: {_top_messages(timeline, 'power_limit')}")

    activity_counts = timeline["event_type"].value_counts()
    top_activity = activity_counts.index[0] if not activity_counts.empty else "inconnue"
    lines.append(f"Activité principale détectée: {top_activity}")

    available_sources = _extract_source_groups(timeline)
    missing_sources = sorted(EXPECTED_SOURCES - available_sources)
    lines.append(f"Sources disponibles: {', '.join(sorted(available_sources)) if available_sources else 'aucune'}")
    lines.append(f"Sources manquantes: {', '.join(missing_sources) if missing_sources else 'aucune'}")

    lines.append("Analyse comportementale:")
    for issue in detect_behavior_issues(timeline):
        lines.append(f"- {issue}")

    lines.append("Préparation diagnostic futur: événements taggés avec future_diagnostic_side=to_be_inferred")
    return lines
