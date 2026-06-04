from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from analyzers.diagnostic_engine import compare_sources
from utils.file_detector import detect_session_files


def test_dewesoft_detection_in_expected_folders() -> None:
    with TemporaryDirectory() as td:
        root = Path(td)
        (root / "Acquisitions").mkdir()
        (root / "Dewesoft").mkdir()
        (root / "Measures").mkdir()
        (root / "Acquisitions" / "measure.csv").write_text("time,P\n0,1\n")
        (root / "Dewesoft" / "session.d7d").write_text("x")
        (root / "Measures" / "session.dxd").write_text("x")

        detected = detect_session_files(root)
        assert len(detected.dewesoft_csv) == 1
        assert len(detected.dewesoft_raw) == 2


def test_compare_sources_builds_cross_insights() -> None:
    frame = pd.DataFrame(
        [
            {
                "timestamp": "2026-01-01T00:00:00Z",
                "source": "EnergyManager.log",
                "event_type": "setpoint",
                "message": "setpoint",
                "payload": {"source_group": "energy_manager"},
                "Ptarget": 10000,
            },
            {
                "timestamp": "2026-01-01T00:00:01Z",
                "source": "iotc-meter.log",
                "event_type": "measurement",
                "message": "meter",
                "payload": {"source_group": "meter_dispatcher"},
                "P": 6000,
                "Q": 0,
                "U": 230,
                "frequency": 50,
            },
        ]
    )
    cross = compare_sources(frame)
    assert cross["rows"]
    assert any("vehicule" in message.lower() or "pcap" in message.lower() for message in cross["insights"])


def test_compare_sources_survives_noise_only_timeline() -> None:
    frame = pd.DataFrame(
        [
            {
                "timestamp": "2026-01-01T00:00:00Z",
                "source": "EnergyManager.log",
                "event_type": "info",
                "message": "keep alive",
                "payload": {"source_group": "energy_manager"},
            },
            {
                "timestamp": "2026-01-01T00:00:01Z",
                "source": "meter.log",
                "event_type": "info",
                "message": "queue created",
                "payload": {"source_group": "meter_dispatcher"},
            },
        ]
    )

    cross = compare_sources(frame)

    assert cross["rows"] == []
    assert isinstance(cross["insights"], list)
    assert cross["scores"] == {"borne": 0.0, "vehicule": 0.0, "communication": 0.0}
    assert cross["evidence_table"] == []
