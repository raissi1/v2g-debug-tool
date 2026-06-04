"""Dewesoft parsing entrypoint with best-effort raw-to-CSV mapping."""

from __future__ import annotations

from pathlib import Path

from core.models import Event
from parsers.dewesoft_csv import parse_dewesoft_csv


RAW_SUFFIXES = {".d7d", ".dxd", ".dmd"}


def convert_dewesoft_to_csv(path: Path) -> Path | None:
    """Best-effort converter hook for raw Dewesoft files.

    True binary conversion is not implemented because Dewesoft raw formats are proprietary.
    For now we auto-resolve to an existing sibling CSV when present.
    """
    same_dir_csvs = sorted(path.parent.glob("*.csv"))
    same_stem = [candidate for candidate in same_dir_csvs if candidate.stem == path.stem]
    if same_stem:
        return same_stem[0]

    if len(same_dir_csvs) == 1:
        return same_dir_csvs[0]

    normalized_stem = path.stem.lower().replace(" ", "").replace("-", "").replace("_", "")
    fuzzy = []
    for candidate in same_dir_csvs:
        candidate_stem = candidate.stem.lower().replace(" ", "").replace("-", "").replace("_", "")
        if normalized_stem and (normalized_stem in candidate_stem or candidate_stem in normalized_stem):
            fuzzy.append(candidate)
    if len(fuzzy) == 1:
        return fuzzy[0]

    return None


def parse_dewesoft_file(path: Path) -> tuple[list[Event], str | None]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        events, _ = parse_dewesoft_csv(path)
        return events, None

    if suffix in RAW_SUFFIXES:
        converted = convert_dewesoft_to_csv(path)
        if converted is not None:
            event = Event(
                timestamp=None,
                source=path.name,
                event_type="session_event",
                message=f"Dewesoft brut associe automatiquement au CSV {converted.name}",
                payload={
                    "path": str(path),
                    "parser": "dewesoft",
                    "source_group": "measure",
                    "conversion_required": False,
                    "converted_csv_path": str(converted),
                    "conversion_strategy": "sidecar_csv",
                },
            )
            return [event], None

        event = Event(
            timestamp=None,
            source=path.name,
            event_type="warning",
            message=f"Dewesoft brut detecte, conversion CSV requise ({path.suffix})",
            payload={
                "path": str(path),
                "parser": "dewesoft",
                "source_group": "measure",
                "conversion_required": True,
            },
        )
        return [event], "Dewesoft brut detecte, conversion CSV requise"

    return [], None
