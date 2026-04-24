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

