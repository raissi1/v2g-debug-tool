"""Resolve Dewesoft assets into analysis-ready or conversion-required states."""

from __future__ import annotations

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
    return None
