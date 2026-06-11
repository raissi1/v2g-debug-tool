"""Core data models used across the V2G debug tool."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class DetectedAssetStatus:
    """Represents the processing state of one detected asset."""

    path: Path
    family: str
    status: str
    detail: str
    related_path: Path | None = None

    def to_summary(self) -> dict[str, Any]:
        return {
            "path": str(self.path),
            "family": self.family,
            "status": self.status,
            "detail": self.detail,
            "related_path": str(self.related_path) if self.related_path else None,
        }


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
    dewesoft_raw: list[Path] = field(default_factory=list)  # .d7d/.dxd/.dmd (conversion required)
    supporting_images: list[Path] = field(default_factory=list)

    ignored_files: list[Path] = field(default_factory=list)
    asset_statuses: list[DetectedAssetStatus] = field(default_factory=list)

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

    def _build_measure_statuses(self) -> list[DetectedAssetStatus]:
        if self.asset_statuses:
            return list(self.asset_statuses)

        statuses: list[DetectedAssetStatus] = []
        try:
            from parsers.dewesoft_resolver import resolve_dewesoft_source
        except Exception:
            resolve_dewesoft_source = None

        for path in self.dewesoft_csv:
            statuses.append(
                DetectedAssetStatus(
                    path=path,
                    family="dewesoft",
                    status="csv_ready",
                    detail="Export CSV exploitable detecte.",
                )
            )

        for path in self.dewesoft_raw:
            if resolve_dewesoft_source is None:
                statuses.append(
                    DetectedAssetStatus(
                        path=path,
                        family="dewesoft",
                        status="conversion_required",
                        detail="Acquisition brute detectee, conversion CSV requise.",
                    )
                )
                continue

            resolution = resolve_dewesoft_source(path)
            statuses.append(
                DetectedAssetStatus(
                    path=path,
                    family="dewesoft",
                    status=str(resolution.get("status", "conversion_required")),
                    detail=str(resolution.get("message", "Acquisition Dewesoft detectee.")),
                    related_path=Path(resolution["resolved_csv_path"]) if resolution.get("resolved_csv_path") else None,
                )
            )

        self.asset_statuses = statuses
        return list(statuses)

    def _coverage_summary(self) -> dict[str, Any]:
        statuses = self._build_measure_statuses()
        by_status: dict[str, int] = {}
        for asset in statuses:
            by_status[asset.status] = by_status.get(asset.status, 0) + 1

        return {
            "dewesoft": {
                "csv_ready": by_status.get("csv_ready", 0),
                "sidecar_csv": by_status.get("sidecar_csv", 0),
                "conversion_required": by_status.get("conversion_required", 0),
                "total_assets": len(statuses),
            },
            "sources": {
                "text_logs": len(self.all_text_logs()),
                "pcaps": len(self.pcaps),
                "measures": len(self.measures),
                "images": len(self.supporting_images),
            },
        }

    def to_summary(self) -> dict[str, Any]:
        all_pcaps = [str(p) for p in self.pcaps]
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
            "pcaps": all_pcaps,
            "dewesoft_csv": [str(p) for p in self.dewesoft_csv],
            "dewesoft_raw": [str(p) for p in self.dewesoft_raw],
            "supporting_images": [str(p) for p in self.supporting_images],
            "ignored_files": [str(p) for p in self.ignored_files],
            "asset_statuses": [status.to_summary() for status in self._build_measure_statuses()],
            "coverage": self._coverage_summary(),
        }


@dataclass
class Event:
    """Canonical timeline event shared by all parsers."""

    timestamp: datetime | None
    source: str
    event_type: str
    message: str
    payload: dict[str, Any] = field(default_factory=dict)
