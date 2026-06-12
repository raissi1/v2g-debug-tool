"""Dewesoft parsing entrypoint with explicit source resolution states."""

from __future__ import annotations

from pathlib import Path

from core.models import Event
from parsers.dewesoft_csv import parse_dewesoft_csv
from parsers.dewesoft_resolver import RAW_SUFFIXES, convert_dewesoft_to_csv, resolve_dewesoft_source


def parse_dewesoft_file(path: Path) -> tuple[list[Event], str | None]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        events, _ = parse_dewesoft_csv(path)
        return events, None

    if suffix in RAW_SUFFIXES:
        resolution = resolve_dewesoft_source(path)
        converted = convert_dewesoft_to_csv(path)
        if converted is not None:
            event = Event(
                timestamp=None,
                source=path.name,
                event_type="session_event",
                message=str(resolution.get("message") or f"Dewesoft brut associe automatiquement au CSV {converted.name}"),
                payload={
                    "path": str(path),
                    "parser": "dewesoft",
                    "source_group": "measure",
                    "conversion_required": False,
                    "conversion_attempted": str(resolution.get("status")) == "conversion_required",
                    "converted_csv_path": str(converted),
                    "conversion_strategy": str(resolution.get("strategy", "sidecar_csv")),
                    "resolution_status": str(resolution.get("status", "sidecar_csv")),
                },
            )
            return [event], None

        event = Event(
            timestamp=None,
            source=path.name,
            event_type="warning",
            message=str(resolution.get("message") or f"Dewesoft brut detecte, conversion CSV requise ({path.suffix})"),
                payload={
                    "path": str(path),
                    "parser": "dewesoft",
                    "source_group": "measure",
                    "conversion_required": True,
                    "conversion_attempted": True,
                    "resolution_status": str(resolution.get("status", "conversion_required")),
                    "conversion_strategy": str(resolution.get("strategy", "missing_csv")),
                },
            )
        return [event], "Dewesoft brut detecte, conversion CSV requise"

    return [], None
