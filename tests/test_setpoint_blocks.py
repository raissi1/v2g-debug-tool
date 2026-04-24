import pandas as pd

from analyzers.diagnostic_engine import run_diagnostic


def test_requested_and_published_setpoints_are_filled_from_textual_logs() -> None:
    frame = pd.DataFrame(
        [
            {
                "timestamp": "2026-01-01T00:00:00Z",
                "source": "EnergyManager.log",
                "event_type": "log_line",
                "message": "Request to accept setpoint CentralSetpoint maxPower_W=11000 source OCPP",
                "payload": {"source_group": "energy_manager"},
            },
            {
                "timestamp": "2026-01-01T00:00:01Z",
                "source": "GridCodes.log",
                "event_type": "log_line",
                "message": "Setpoint is recalculated and published maxPower_W=9500 source CPD",
                "payload": {"source_group": "energy_manager"},
            },
            {
                "timestamp": "2026-01-01T00:00:02Z",
                "source": "ChargerApp.log",
                "event_type": "log_line",
                "message": "Charge limit from EV applied",
                "payload": {"source_group": "charger_app"},
            },
        ]
    )

    result = run_diagnostic(frame)
    requested = result["blocks"]["A_requested"]
    published = result["blocks"]["B_station_computed"]

    assert requested, "A. Ce qui a été demandé ne doit pas être vide"
    assert published, "B. Ce que la borne a calculé/publié ne doit pas être vide"
    assert any("setpoint" in line.lower() or "maxpower_w" in line.lower() for line in requested)
    assert any("published" in line.lower() or "publication borne" in line.lower() for line in published)

