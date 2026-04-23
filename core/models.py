"""Core data models used across the V2G debug tool."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class DetectedFiles:
    """Represents the set of files detected in a session directory."""

    root: Path
    energy_manager: list[Path] = field(default_factory=list)
    charger_app: list[Path] = field(default_factory=list)
    iotc_meter_dispatcher: list[Path] = field(default_factory=list)

    pcaps: list[Path] = field(default_factory=list)
    measures: list[Path] = field(default_factory=list)
    logs: list[Path] = field(default_factory=list)
    others: list[Path] = field(default_factory=list)

    def all_text_logs(self) -> list[Path]:
        """Return deduplicated list of text logs to parse into timeline events."""
        ordered = [*self.energy_manager, *self.charger_app, *self.iotc_meter_dispatcher, *self.logs]
        seen: set[Path] = set()
        unique: list[Path] = []
        for path in ordered:
            if path not in seen:
                unique.append(path)
                seen.add(path)
        return unique

    def to_summary(self) -> dict[str, Any]:
        return {
            "root": str(self.root),
            "energy_manager": [str(p) for p in self.energy_manager],
            "charger_app": [str(p) for p in self.charger_app],
            "iotc_meter_dispatcher": [str(p) for p in self.iotc_meter_dispatcher],
            "pcaps": [str(p) for p in self.pcaps],
            "measures": [str(p) for p in self.measures],
            "logs": [str(p) for p in self.logs],
            "others": [str(p) for p in self.others],
        }


@dataclass
class Event:
    """Canonical timeline event shared by all parsers."""

    timestamp: datetime | None
    source: str
    event_type: str
    message: str
    payload: dict[str, Any] = field(default_factory=dict)
