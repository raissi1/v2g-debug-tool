"""Resolve Dewesoft assets into analysis-ready or conversion-required states."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path


RAW_SUFFIXES = {".d7d", ".dxd", ".dmd"}


def _normalize_stem(value: str) -> str:
    return value.lower().replace(" ", "").replace("-", "").replace("_", "")


def _find_sidecar_csv(path: Path) -> Path | None:
    same_dir_csvs = sorted(path.parent.glob("*.csv"))
    same_stem = [candidate for candidate in same_dir_csvs if candidate.stem == path.stem]
    if same_stem:
        return same_stem[0]

    if len(same_dir_csvs) == 1:
        return same_dir_csvs[0]

    normalized_stem = _normalize_stem(path.stem)
    fuzzy: list[Path] = []
    for candidate in same_dir_csvs:
        candidate_stem = _normalize_stem(candidate.stem)
        if normalized_stem and (normalized_stem in candidate_stem or candidate_stem in normalized_stem):
            fuzzy.append(candidate)
    if len(fuzzy) == 1:
        return fuzzy[0]

    return None


def _default_converter_script() -> Path:
    return Path(__file__).resolve().parents[1] / "scripts" / "convert_dewesoft_session.ps1"


def _run_external_converter(path: Path) -> None:
    converter = os.environ.get("V2G_DEWESOFT_CONVERTER", "").strip()
    if not converter:
        default_script = _default_converter_script()
        if default_script.exists():
            converter = str(default_script)
        else:
            return

    converter_path = Path(converter)
    try:
        if converter_path.suffix.lower() == ".ps1":
            if not converter_path.exists():
                return
            subprocess.run(
                [
                    "powershell",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(converter_path),
                    "-SessionRoot",
                    str(path.parent),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            return

        subprocess.run(
            [converter, str(path)],
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception:
        return


def resolve_dewesoft_source(path: Path) -> dict[str, str | None]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return {
            "status": "csv_ready",
            "resolved_csv_path": str(path),
            "message": "Export CSV Dewesoft exploitable detecte.",
            "strategy": "csv_direct",
        }

    if suffix in RAW_SUFFIXES:
        sidecar = _find_sidecar_csv(path)
        if sidecar is not None:
            return {
                "status": "sidecar_csv",
                "resolved_csv_path": str(sidecar),
                "message": f"Acquisition brute associee automatiquement au CSV {sidecar.name}.",
                "strategy": "sidecar_csv",
            }
        return {
            "status": "conversion_required",
            "resolved_csv_path": None,
            "message": f"Acquisition brute detectee ({path.suffix}), conversion CSV requise.",
            "strategy": "missing_csv",
        }

    return {
        "status": "unsupported",
        "resolved_csv_path": None,
        "message": f"Format Dewesoft non supporte: {path.suffix}",
        "strategy": "unsupported",
    }


def convert_dewesoft_to_csv(path: Path) -> Path | None:
    resolution = resolve_dewesoft_source(path)
    if resolution.get("resolved_csv_path"):
        return Path(str(resolution["resolved_csv_path"]))
    if path.suffix.lower() in RAW_SUFFIXES:
        _run_external_converter(path)
        resolution = resolve_dewesoft_source(path)
        if resolution.get("resolved_csv_path"):
            return Path(str(resolution["resolved_csv_path"]))
    return None
