import pandas as pd

from analyzers.diagnostic_engine import run_diagnostic
from analyzers.v2g_fault_classifier import classify_v2g_fault


def test_classifier_detects_station_side_constraints() -> None:
    frame = pd.DataFrame(
        [
            {
                "timestamp": "2026-01-01T00:00:00Z",
                "source": "EnergyManager.log",
                "event_type": "power_limit",
                "message": "Setpoint recalculated due to gridcode derating limit",
                "payload": {"source_group": "energy_manager"},
                "Pcalc": 7200.0,
            }
        ]
    )

    result = classify_v2g_fault(frame, {})
    assert result["cause"] == "borne"
    assert result["borne_score"] > result["vehicule_score"]
    assert result["borne_score"] > result["communication_score"]


def test_classifier_detects_vehicle_side_setpoint_mismatch() -> None:
    frame = pd.DataFrame(
        [
            {
                "timestamp": "2026-01-01T00:00:00Z",
                "source": "EnergyManager.log",
                "event_type": "setpoint",
                "message": "publish target",
                "payload": {"source_group": "energy_manager"},
                "Ptarget": 10000.0,
            },
            {
                "timestamp": "2026-01-01T00:00:10Z",
                "source": "PU.csv",
                "event_type": "measurement",
                "message": "dew sample",
                "payload": {"source_group": "measure"},
                "P": 2400.0,
            },
        ]
    )
    cross = {
        "rows": [
            {
                "timestamp": "2026-01-01T00:00:10Z",
                "Ptarget": 10000.0,
                "P_dewesoft": 2400.0,
                "P_meter": 2500.0,
                "message": "target not followed",
            }
        ]
    }

    result = classify_v2g_fault(frame, cross)
    assert result["cause"] == "vehicule"
    assert result["vehicule_score"] >= 3.0


def test_run_diagnostic_exposes_v2g_classification() -> None:
    frame = pd.DataFrame(
        [
            {
                "timestamp": "2026-01-01T00:00:00Z",
                "source": "netlog.pcap",
                "event_type": "timeout",
                "message": "protocol timeout reset no response",
                "payload": {
                    "source_group": "netlogger",
                    "parser": "pcap_generic",
                    "pcap_tcp_rst_count": 3,
                    "pcap_likely_v2g": True,
                    "pcap_top_ports": [15118],
                },
            }
        ]
    )

    result = run_diagnostic(frame)
    assert "v2g_classification" in result
    assert result["v2g_classification"]["communication_score"] > 0
