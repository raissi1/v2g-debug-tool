"""Generic analyzer producing first-level diagnostics from a session timeline."""

from __future__ import annotations

import pandas as pd


REQUIRED_COLUMNS = {"timestamp", "source", "event_type", "message", "payload"}


def _top_messages(frame: pd.DataFrame, event_type: str, limit: int = 3) -> str:
    subset = frame[frame["event_type"] == event_type]
    if subset.empty:
        return "aucun"
    counts = subset["message"].value_counts().head(limit)
    return " | ".join(f"{msg[:120]} ({count})" for msg, count in counts.items())


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

    lines.append(f"Erreurs principales: {_top_messages(timeline, 'error')}")
    lines.append(f"Warnings principaux: {_top_messages(timeline, 'warning')}")
    lines.append(f"Événements GridCodes: {_top_messages(timeline, 'gridcodes')}")
    lines.append(f"Changements de setpoint: {_top_messages(timeline, 'setpoint_change')}")
    lines.append(f"Limitations détectées: {_top_messages(timeline, 'power_limit')}")

    timeout_count = int((timeline["event_type"] == "timeout").sum())
    protocol_count = int((timeline["event_type"] == "protocol_event").sum())
    lines.append(f"Timeouts détectés: {timeout_count}")
    lines.append(f"Événements protocole détectés: {protocol_count}")

    event_types = timeline["event_type"].value_counts().to_dict()
    top_types = ", ".join(f"{name} ({count})" for name, count in list(event_types.items())[:8])
    lines.append(f"Répartition des types: {top_types}")

    return lines
