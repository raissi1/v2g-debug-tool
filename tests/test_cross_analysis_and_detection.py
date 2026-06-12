from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

from analyzers.diagnostic_engine import compare_sources
from parsers.dewesoft import convert_dewesoft_to_csv
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


def test_real_world_aquisitions_layout_is_detected() -> None:
    with TemporaryDirectory() as td:
        root = Path(td)
        target = root / "CES-1354" / "Aquisitions" / "1ph" / "limitation"
        target.mkdir(parents=True)
        (target / "Primara_20260323_152541.csv").write_text("time,P\n0,1\n")
        (target / "Primara_20260323_152541.dmd").write_text("x")
        (target / "Primara_20260323_152541_screenshot_20260323_153148.png").write_text("x")
        (root / "CES-1354" / "Aquisitions" / "PU-st01-tri.d7d").write_text("x")

        detected = detect_session_files(root)
        assert len(detected.dewesoft_csv) == 1
        assert len(detected.dewesoft_raw) == 2
        assert len(detected.supporting_images) == 1


def test_raw_dewesoft_prefers_sidecar_csv_when_present() -> None:
    with TemporaryDirectory() as td:
        root = Path(td)
        raw = root / "Primara_20260323_152541.dmd"
        csv = root / "Primara_20260323_152541.csv"
        raw.write_text("x")
        csv.write_text("time,P\n0,1\n")

        converted = convert_dewesoft_to_csv(raw)
        assert converted == csv


def test_detected_summary_exposes_dewesoft_coverage_statuses() -> None:
    with TemporaryDirectory() as td:
        root = Path(td)
        acquisitions = root / "Aquisitions"
        acquisitions.mkdir()
        csv = acquisitions / "Primara_1.csv"
        raw_with_csv = acquisitions / "Primara_1.dmd"
        raw_without_csv = acquisitions / "Primara_2.d7d"
        csv.write_text("time,P\n0,1\n")
        raw_with_csv.write_text("x")
        raw_without_csv.write_text("x")

        detected = detect_session_files(root)
        summary = detected.to_summary()

        coverage = summary["coverage"]["dewesoft"]
        assert coverage["csv_ready"] == 1
        assert coverage["sidecar_csv"] == 1
        assert coverage["conversion_required"] == 1
        assert len(summary["asset_statuses"]) == 3


def test_raw_dewesoft_can_use_configured_external_converter() -> None:
    with TemporaryDirectory() as td:
        root = Path(td)
        raw = root / "Primara_3.d7d"
        raw.write_text("x")
        script = root / "fake_converter.ps1"
        script.write_text("Write-Host 'fake converter'")
        expected_csv = root / "Primara_3.csv"

        def _fake_run(*args, **kwargs):
            expected_csv.write_text("time,P\n0,1\n")
            return None

        with patch("parsers.dewesoft_resolver.subprocess.run", side_effect=_fake_run):
            with patch.dict("os.environ", {"V2G_DEWESOFT_CONVERTER": str(script)}):
                converted = convert_dewesoft_to_csv(raw)

        assert converted == expected_csv


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
