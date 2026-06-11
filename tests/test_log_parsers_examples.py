from parsers.charger_app import _extract_physical_signals as extract_charger_signals
from parsers.energy_manager import _extract_physical_signals as extract_energy_signals


def test_energy_manager_extracts_limits_and_setpoints_from_realistic_lines() -> None:
    limit_line = (
        "2026-03-12 10:24:21.487 [I] EnergyManager: EnergyManagerAPI::LimitsGranted: "
        "{ { Connector: 0, mode (i2p2_Offline): DISCHARGING, Charge: { maxPower_W (i2p2_Offline_CalcFromCurrent): 22080, "
        "maxCurrentPh1_mA (i2p2_Offline): 32000,}, Discharge: { maxPower_W (i2p2_Offline): -11000, maxCurrent_mA "
        "(i2p2_Offline_CalcFromPower): -15942,}, chargeEnergyAvailable: true, dischargeEnergyAvailable: true}, }"
    )
    setpoint_line = (
        "2026-03-12 10:24:21.492 [I] GridCodes: Setpoint published: { Source: EvseID: Undefined, "
        "V2XMode: NotSet, { P: {power_W: -6000} }, { Q: {power_W: 500, load_mA: 0} }}"
    )

    limit_signals = extract_energy_signals(limit_line)
    setpoint_signals = extract_energy_signals(setpoint_line)

    assert limit_signals["AvailableDischargePower"] == -11000.0
    assert limit_signals["AvailableChargePower"] == 22080.0
    assert limit_signals["maxCurrent_mA"] == 32000.0
    assert setpoint_signals["Ptarget"] == -6000.0
    assert setpoint_signals["Qtarget"] == 500.0


def test_charger_app_extracts_status_limits_and_meter_values() -> None:
    status_line = (
        "2026-02-27 11:56:10.530 [I] EVPLCCom-App: [1] [HLCStateMachine] Got event "
        "AcHlcV2gEvents::ChargingStatusReq, departureTime: 1772425858, chargeLimits: maxPower_W: {ph1: 7350} "
        "minPower_W: {ph1: 0} , dischargeLimits: maxPower_W: {ph1: -7300} minPower_W: {ph1: 0} , "
        "evEnergy: targetEnergyRequest_Ws: 230760000, maxEnergyRequest_Ws: 250200000, minEnergyRequest_Ws: -12600000, "
        "evActivePowerPresent_W: ph1: -7000, evReactivePowerPresent_W: ph1: 200, evV2xRequest: {maxV2xEnergyRequest_Ws: 179640000, "
        "minV2xEnergyRequest_Ws: -12600000 }, evPresentSoC: 25"
    )
    meter_line = (
        "2026-02-27 11:56:10.539 [I] EVPLCCom-App: [1] [HLCStateMachine] Got event "
        "HlcEvents::MeterData, power_W: ph1: -6882, load_mA: ph1: 29852, voltage_mV ph1: 232185 in state HLCStateMachineReactor"
    )

    status_signals = extract_charger_signals(status_line)
    meter_signals = extract_charger_signals(meter_line)

    assert status_signals["evActivePowerPresent_W"] == -7000.0
    assert status_signals["evReactivePowerPresent_var"] == 200.0
    assert status_signals["chargeLimitPower_W"] == 7350.0
    assert status_signals["AvailableDischargePower"] == -7300.0
    assert meter_signals["P"] == -6882.0
    assert meter_signals["I_A"] == 29.852
    assert meter_signals["U"] == 232.185
