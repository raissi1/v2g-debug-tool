"""Core data models used across the V2G debug tool."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class DetectedFiles:
    """Represents the set of relevant files detected in a session package."""

    root: Path
    aux_root: Path | None = None

    charger_app: list[Path] = field(default_factory=list)
    energy_manager: list[Path] = field(default_factory=list)
    iotc_meter_dispatcher: list[Path] = field(default_factory=list)
    netlogger_pcaps: list[Path] = field(default_factory=list)
    netlogger_logs: list[Path] = field(default_factory=list)

    generic_logs: list[Path] = field(default_factory=list)
    generic_pcaps: list[Path] = field(default_factory=list)

    dewesoft_csv: list[Path] = field(default_factory=list)
    dewesoft_raw: list[Path] = field(default_factory=list)  # .d7d/.dxd (conversion required)

    ignored_files: list[Path] = field(default_factory=list)

    def all_text_logs(self) -> list[Path]:
        """Return deduplicated list of text logs to parse into timeline events."""
        ordered = [
            *self.charger_app,
            *self.energy_manager,
            *self.iotc_meter_dispatcher,
            *self.netlogger_logs,
            *self.generic_logs,
        ]
        seen: set[Path] = set()
        unique: list[Path] = []
        for path in ordered:
            if path not in seen:
                unique.append(path)
                seen.add(path)
        return unique

    @property
    def pcaps(self) -> list[Path]:
        return [*self.netlogger_pcaps, *self.generic_pcaps]

    @property
    def measures(self) -> list[Path]:
        return [*self.dewesoft_csv, *self.dewesoft_raw]

    def to_summary(self) -> dict[str, Any]:
        return {
            "root": str(self.root),
            "aux_root": str(self.aux_root) if self.aux_root else None,
            "charger_app": [str(p) for p in self.charger_app],
            "energy_manager": [str(p) for p in self.energy_manager],
            "iotc_meter_dispatcher": [str(p) for p in self.iotc_meter_dispatcher],
            "netlogger_pcaps": [str(p) for p in self.netlogger_pcaps],
            "netlogger_logs": [str(p) for p in self.netlogger_logs],
            "generic_logs": [str(p) for p in self.generic_logs],
            "generic_pcaps": [str(p) for p in self.generic_pcaps],
            "dewesoft_csv": [str(p) for p in self.dewesoft_csv],
            "dewesoft_raw": [str(p) for p in self.dewesoft_raw],
            "ignored_files": [str(p) for p in self.ignored_files],
        }


@dataclass
class Event:
    """Canonical timeline event shared by all parsers."""

    timestamp: datetime | None
    source: str
    event_type: str
    message: str
    payload: dict[str, Any] = field(default_factory=dict)
