"""Generic diagnostic engine combining setpoints, measurements and protocol/errors."""

from __future__ import annotations

import pandas as pd

from analyzers.generic_debug import detect_behavior_issues


def infer_responsibility(timeline: pd.DataFrame, issues: list[str]) -> str:
    text = " ".join(issues).lower()
    if "limitation" in text and "borne" in text:
        return "Probable côté borne"
    if "consigne envoyée mais p mesuré ne suit pas" in text:
        return "Probable côté véhicule"

    if not timeline.empty:
        protocol_count = int((timeline["event_type"] == "protocol_event").sum()) if "event_type" in timeline else 0
        error_count = int((timeline["event_type"] == "error").sum()) if "event_type" in timeline else 0
        if protocol_count > 0 and error_count == 0:
            return "Probable côté communication"

    return "Indéterminé"


def run_generic_diagnostic(timeline: pd.DataFrame) -> dict:
    issues = detect_behavior_issues(timeline)
    conclusion = infer_responsibility(timeline, issues)

    return {
        "issues": issues,
        "conclusion": conclusion,
        "event_counts": timeline["event_type"].value_counts().to_dict() if not timeline.empty else {},
    }
