from pathlib import Path
from tempfile import TemporaryDirectory

from parsers.dewesoft_csv import parse_dewesoft_csv


def test_dewesoft_csv_normalized_aliases_are_populated() -> None:
    with TemporaryDirectory() as td:
        path = Path(td) / "dew.csv"
        path.write_text("temps,puissance active,puissance reactive,tension,freq,courant\n2026-01-01T00:00:00Z,10,2,230,50,1\n")

        events, normalized = parse_dewesoft_csv(path)

        assert events
        payload = events[0].payload
        assert payload["P_dewesoft_W"] == 10
        assert payload["Q_dewesoft_var"] == 2
        assert payload["U_dewesoft_V"] == 230
        assert payload["frequency_dewesoft_Hz"] == 50
        assert payload["I_dewesoft_A"] == 1
        assert "P_dewesoft_W" in normalized.columns
        assert "frequency_dewesoft_Hz" in normalized.columns


def test_dewesoft_csv_supports_real_d7d_export_headers() -> None:
    with TemporaryDirectory() as td:
        path = Path(td) / "PU-st01-tri_d7d_output.csv"
        path.write_text(
            "Time;UL1;IL1;Frequency;P;Q;S\n"
            "0.2048;230.5;23.37;50.02;5436.37;590.74;5468.37\n"
        )

        events, normalized = parse_dewesoft_csv(path)

        assert events
        payload = events[0].payload
        assert payload["P_dewesoft_W"] == 5436.37
        assert payload["Q_dewesoft_var"] == 590.74
        assert payload["U_dewesoft_V"] == 230.5
        assert payload["I_dewesoft_A"] == 23.37
        assert payload["frequency_dewesoft_Hz"] == 50.02
        assert payload["S_VA"] == 5468.37
        assert normalized.iloc[0]["P_dewesoft_W"] == 5436.37
        assert normalized.iloc[0]["U_dewesoft_V"] == 230.5
        assert normalized.iloc[0]["I_dewesoft_A"] == 23.37
        assert normalized.iloc[0]["S_VA"] == 5468.37

