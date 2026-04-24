from parsers.meter_dispatcher import (
    _extract_physical_signals,
    milliamps_to_amps,
    millihertz_to_hertz,
    millivolts_to_volts,
)


def test_unit_conversion_helpers_are_pure_and_correct() -> None:
    assert millivolts_to_volts(229900) == 229.9
    assert millihertz_to_hertz(50060) == 50.06
    assert milliamps_to_amps(1000) == 1.0
    assert millivolts_to_volts(0) == 0.0
    assert millihertz_to_hertz(0) == 0.0
    assert milliamps_to_amps(0) == 0.0
    assert millivolts_to_volts(None) is None
    assert millihertz_to_hertz(None) is None
    assert milliamps_to_amps(None) is None


def test_slice_parser_keeps_zero_values_and_converts_units() -> None:
    line = (
        '2026-01-01T00:00:00Z Slice: {"POWER_ACTIVE_W":0,"POWER_REACTIVE_var":0,'
        '"POWER_APPARENT_VA":0,"VOLTAGE_RMS_mV":229900,"CURRENT_RMS_mA":0,'
        '"FREQUENCY_mHz":50060}'
    )
    signals = _extract_physical_signals(line)

    assert signals["P"] == 0.0
    assert signals["Q"] == 0.0
    assert signals["S"] == 0.0
    assert signals["U_V"] == 229.9
    assert signals["I_A"] == 0.0
    assert signals["frequency_Hz"] == 50.06
    assert signals["is_slice_measurement"] is True

