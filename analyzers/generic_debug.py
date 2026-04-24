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
    lines.append(f"Changements de setpoint: {_top_messages(timeline, 'setpoint_change')}")
    lines.append(f"Limitations détectées: {_top_messages(timeline, 'power_limit')}")

    activity_counts = timeline["event_type"].value_counts()
    top_activity = activity_counts.index[0] if not activity_counts.empty else "inconnue"
    lines.append(f"Activité principale détectée: {top_activity}")

    available_sources = _extract_source_groups(timeline)
    missing_sources = sorted(EXPECTED_SOURCES - available_sources)
    lines.append(f"Sources disponibles: {', '.join(sorted(available_sources)) if available_sources else 'aucune'}")
    lines.append(f"Sources manquantes: {', '.join(missing_sources) if missing_sources else 'aucune'}")

    # Future diagnostic hook (borne vs véhicule) intentionally generic for now.
    lines.append("Préparation diagnostic futur: événements taggés avec future_diagnostic_side=to_be_inferred")

    return lines
