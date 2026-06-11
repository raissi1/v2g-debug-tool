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
    assert result["cause_probable"] == "indetermine"
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
    assert "first_divergence" in result
    assert isinstance(result["first_divergence"], dict)
    assert "generic_rules" in result
    assert isinstance(result["generic_rules"], list)
    assert "generic_rule_summary" in result
    assert isinstance(result["generic_rule_summary"], dict)


def test_raw_dewesoft_is_not_reported_as_absent() -> None:
    frame = pd.DataFrame(
        [
            {
                "timestamp": "2026-01-01T00:00:00Z",
                "source": "session.dxd",
                "event_type": "warning",
                "message": "Dewesoft brut detecte, conversion requise (.dxd)",
                "payload": {
                    "source_group": "measure",
                    "conversion_required": True,
                },
            }
        ]
    )
    result = run_diagnostic(frame)
    assert "brut" in result["justification"].lower() or any("brut" in item.lower() for item in result["evidence"])


def test_first_divergence_detects_setpoint_mismatch() -> None:
    frame = pd.DataFrame(
        [
            {
                "timestamp": "2026-01-01T00:00:00Z",
                "source": "EnergyManager.log",
                "event_type": "setpoint",
                "message": "setpoint Ptarget=10000",
                "payload": {"source_group": "energy_manager"},
                "Ptarget": 10000.0,
            },
            {
                "timestamp": "2026-01-01T00:00:01Z",
                "source": "meter.log",
                "event_type": "measurement",
                "message": "meter power sample",
                "payload": {"source_group": "meter_dispatcher"},
                "P": 5000.0,
                "U": 230.0,
                "frequency": 50.0,
            },
        ]
    )

    result = run_diagnostic(frame)
    divergence = result["first_divergence"]
    assert divergence["category"] == "consigne_non_suivie"
    assert divergence["timestamp"] is not None
    setpoint_rule = next(rule for rule in result["generic_rules"] if rule["id"] == "setpoint_following")
    assert setpoint_rule["status"] == "fail"
    assert result["generic_rule_summary"]["fail"] >= 1
