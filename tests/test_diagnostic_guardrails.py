import pandas as pd

from analyzers.diagnostic_engine import run_diagnostic


def test_indeterminate_when_ptarget_and_dewesoft_missing() -> None:
    frame = pd.DataFrame(
        [
            {
                "timestamp": "2026-01-01T00:00:00Z",
                "source": "meter.log",
                "event_type": "measurement",
                "message": "Slice: meter sample",
                "payload": {"source_group": "meter_dispatcher"},
                "P": 0.0,
                "U": 230.0,
                "frequency": 50.0,
            },
            {
                "timestamp": "2026-01-01T00:00:01Z",
                "source": "EnergyManager.log",
                "event_type": "gridcodes",
                "message": "GridCodes informational event",
                "payload": {"source_group": "energy_manager"},
            },
        ]
    )
    result = run_diagnostic(frame)
    assert result["cause_probable"] == "indéterminé"
    assert result["confidence_score"] <= 35
    assert "Ptarget" in ",".join(result["missing_data"])


def test_structured_evidence_table_is_present() -> None:
    frame = pd.DataFrame(
        [
            {
                "timestamp": "2026-01-01T00:00:00Z",
                "source": "EnergyManager.log",
                "event_type": "timeout",
                "message": "protocol timeout handshake",
                "payload": {"source_group": "energy_manager"},
            }
        ]
    )
    result = run_diagnostic(frame)
    assert "evidence_table" in result
    assert isinstance(result["evidence_table"], list)

