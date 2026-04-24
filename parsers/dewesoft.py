"""Dewesoft parsing entrypoint (CSV now, d7d/dxd placeholder for future converter)."""

from __future__ import annotations

from pathlib import Path

from core.models import Event
from parsers.dewesoft_csv import parse_dewesoft_csv


def convert_dewesoft_to_csv(path: Path) -> Path | None:
    """Placeholder converter hook for raw Dewesoft files (.d7d/.dxd)."""
    return None


def parse_dewesoft_file(path: Path) -> tuple[list[Event], str | None]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        events, _ = parse_dewesoft_csv(path)
        return events, None

    if suffix in {".d7d", ".dxd"}:
        _converted = convert_dewesoft_to_csv(path)
        event = Event(
            timestamp=None,
            source=path.name,
            event_type="warning",
            message=f"Dewesoft brut détecté, conversion requise ({path.suffix})",
            payload={
                "path": str(path),
                "parser": "dewesoft",
                "source_group": "measure",
                "conversion_required": True,
            },
        )
        return [event], "Dewesoft brut détecté, conversion requise"

    return [], None
