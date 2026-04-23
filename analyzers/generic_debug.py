"""Generic analyzer producing first-level diagnostics from a session timeline."""

from __future__ import annotations

import pandas as pd


REQUIRED_COLUMNS = {"timestamp", "source", "event_type", "message", "payload"}


def summarize_session(timeline: pd.DataFrame) -> list[str]:
    """Return a concise textual summary of a reconstructed session."""
    if timeline.empty:
        return ["Aucun événement détecté dans la session."]

    missing_cols = REQUIRED_COLUMNS - set(timeline.columns)
    if missing_cols:
        return [f"Timeline invalide: colonnes manquantes {sorted(missing_cols)}"]

    lines: list[str] = []
    lines.append(f"Nombre total d'événements: {len(timeline)}")

    sources = timeline["source"].value_counts().to_dict()
    top_sources = ", ".join(f"{name} ({count})" for name, count in list(sources.items())[:5])
    lines.append(f"Sources principales: {top_sources}")

    ts = pd.to_datetime(timeline["timestamp"], utc=True, errors="coerce").dropna()
    if not ts.empty:
        lines.append(f"Fenêtre temporelle: {ts.min().isoformat()} → {ts.max().isoformat()}")
        lines.append(f"Durée approximative: {ts.max() - ts.min()}")
    else:
        lines.append("Aucun timestamp exploitable trouvé.")

    event_types = timeline["event_type"].value_counts().to_dict()
    top_types = ", ".join(f"{name} ({count})" for name, count in list(event_types.items())[:5])
    lines.append(f"Types d'événements: {top_types}")

    return lines
