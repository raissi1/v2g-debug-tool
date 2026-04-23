"""Core data models used across the V2G debug tool."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class DetectedFiles:
    """Represents the set of files detected in a session directory."""

    root: Path
    logs: list[Path] = field(default_factory=list)
    pcaps: list[Path] = field(default_factory=list)
    measures: list[Path] = field(default_factory=list)
    others: list[Path] = field(default_factory=list)

    def to_summary(self) -> dict[str, Any]:
        return {
            "root": str(self.root),
            "logs": [str(p) for p in self.logs],
            "pcaps": [str(p) for p in self.pcaps],
            "measures": [str(p) for p in self.measures],
            "others": [str(p) for p in self.others],
        }


@dataclass(slots=True)
class Event:
    """Canonical timeline event shared by all parsers."""

    timestamp: datetime | None
    source: str
    event_type: str
    message: str
    payload: dict[str, Any] = field(default_factory=dict)
