"""iotc-meter-dispatcher parser for generic V2G debugging."""

from __future__ import annotations

import ast
import gzip
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Iterable

from core.models import Event

ISO_TS_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?")
NUMBER = r"([-+]?\d+(?:[\.,]\d+)?)"

EVENT_PATTERNS: list[tuple[str, tuple[str, ...]]] = [
    ("error", (" error ", "exception", "failed", "decode error", "invalid")),
    ("warning", (" warning ", "warn", "drift", "outlier")),
    ("timeout", ("timeout", "timed out", "stale", "no sample")),
    ("power_limit", ("power limit", "clamp", "limited", "cap", "max power")),
    ("gridcodes", ("gridcode", "grid code", "frequency event", "voltage event")),
    ("protocol_event", ("modbus", "mqtt", "publish", "packet", "frame")),
    ("session_event", ("session start", "session stop", "sampling started", "sampling stopped")),
]

PHYSICAL_PATTERNS = {
    "Ptarget": [re.compile(rf"(?:ptarget|p_target|requested\s*power|target\s*power)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "Qtarget": [re.compile(rf"(?:qtarget|q_target|requested\s*reactive\s*power|target\s*reactive)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "P": [re.compile(rf"(?:p\s*meas(?:ured)?|measured\s*power|active\s*power)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "Q": [re.compile(rf"(?:q\s*meas(?:ured)?|reactive\s*power)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "U": [re.compile(rf"(?:voltage|tension|\bu\b)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "frequency": [re.compile(rf"(?:freq(?:uency)?|\bhz\b)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
    "AvailableDischargePower": [re.compile(rf"(?:availabledischargepower|available\s*discharge\s*power)\D{{0,12}}{NUMBER}", re.IGNORECASE)],
}
STATE_PATTERNS = [
    ("start", re.compile(r"session\s*start|start\s*session|sampling\s*started", re.IGNORECASE)),
    ("stop", re.compile(r"session\s*stop|stop\s*session|sampling\s*stopped", re.IGNORECASE)),
    ("charging", re.compile(r"\bcharging\b", re.IGNORECASE)),
]


def _open_text(path: Path):
    if path.name.lower().endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="ignore")
    return path.open("r", encoding="utf-8", errors="ignore")


def _to_float(raw: str) -> float | None:
    try:
        return float(raw.replace(",", "."))
    except ValueError:
        return None


def millivolts_to_volts(value_mv: float | int | None) -> float | None:
    if value_mv is None:
        return None
    return float(value_mv) / 1000.0


def milliamps_to_amps(value_ma: float | int | None) -> float | None:
    if value_ma is None:
        return None
    return float(value_ma) / 1000.0


def millihertz_to_hertz(value_mhz: float | int | None) -> float | None:
    if value_mhz is None:
        return None
    return float(value_mhz) / 1000.0


def _extract_physical_signals(line: str) -> dict[str, float | str]:
    signals: dict[str, float | str] = {}
    for key, patterns in PHYSICAL_PATTERNS.items():
        for pattern in patterns:
            m = pattern.search(line)
            if m:
                value = _to_float(m.group(1))
                if value is not None:
                    signals[key] = value
                    break


    # Slice parser for JSON payloads after "Slice:".
    slice_match = re.search(r"slice\s*:\s*(\{.*\})", line, re.IGNORECASE)
    if slice_match:
        payload_raw = slice_match.group(1).strip()
        data: dict | None = None
        try:
            loaded = json.loads(payload_raw)
            if isinstance(loaded, dict):
                data = loaded
        except json.JSONDecodeError:
            try:
                loaded = ast.literal_eval(payload_raw)
                if isinstance(loaded, dict):
                    data = loaded
            except (ValueError, SyntaxError):
                data = None

        if data:
            def _num(key: str) -> float | None:
                raw = data.get(key)
                if raw is None:
                    return None
                return _to_float(str(raw))

            p = _num("POWER_ACTIVE_W")
            q = _num("POWER_REACTIVE_var")
            s = _num("POWER_APPARENT_VA")
            u_mv = _num("VOLTAGE_RMS_mV")
            i_ma = _num("CURRENT_RMS_mA")
            ua_mv = _num("VOLTAGE_RMS_PHASE_A_mV")
            ub_mv = _num("VOLTAGE_RMS_PHASE_B_mV")
            uc_mv = _num("VOLTAGE_RMS_PHASE_C_mV")
            ia_ma = _num("CURRENT_RMS_PHASE_A_mA")
            ib_ma = _num("CURRENT_RMS_PHASE_B_mA")
            ic_ma = _num("CURRENT_RMS_PHASE_C_mA")
            f_mhz = _num("FREQUENCY_mHz")

            if p is not None:
                signals["P"] = p
                signals["P_W"] = p
            if q is not None:
                signals["Q"] = q
                signals["Q_var"] = q
            if s is not None:
                signals["S"] = s
                signals["S_VA"] = s

            if u_mv is not None:
                signals["U"] = millivolts_to_volts(u_mv)
                signals["U_V"] = signals["U"]
            if i_ma is not None:
                signals["I_A"] = milliamps_to_amps(i_ma)

            voltages: list[float] = []
            if ua_mv is not None:
                signals["U_phase_A"] = millivolts_to_volts(ua_mv)
                signals["U_phase_A_V"] = signals["U_phase_A"]
                voltages.append(signals["U_phase_A"])
            if ub_mv is not None:
                signals["U_phase_B"] = millivolts_to_volts(ub_mv)
                signals["U_phase_B_V"] = signals["U_phase_B"]
                voltages.append(signals["U_phase_B"])
            if uc_mv is not None:
                signals["U_phase_C"] = millivolts_to_volts(uc_mv)
                signals["U_phase_C_V"] = signals["U_phase_C"]
                voltages.append(signals["U_phase_C"])
            if voltages:
                signals["U_avg"] = sum(voltages) / len(voltages)
                signals["U_avg_V"] = signals["U_avg"]
                signals["U"] = signals["U_avg"]
                signals["U_V"] = signals["U"]

            if ia_ma is not None:
                signals["I_phase_A"] = milliamps_to_amps(ia_ma)
                signals["I_phase_A_A"] = signals["I_phase_A"]
            if ib_ma is not None:
                signals["I_phase_B"] = milliamps_to_amps(ib_ma)
                signals["I_phase_B_A"] = signals["I_phase_B"]
            if ic_ma is not None:
                signals["I_phase_C"] = milliamps_to_amps(ic_ma)
                signals["I_phase_C_A"] = signals["I_phase_C"]

            if f_mhz is not None:
                signals["frequency_Hz"] = millihertz_to_hertz(f_mhz)
                signals["frequency"] = signals["frequency_Hz"]

            signals["is_slice_measurement"] = True

    for state_name, pattern in STATE_PATTERNS:
        if pattern.search(line):
            signals["state"] = state_name
            break

    return signals


def _physical_event_type(signals: dict[str, float | str]) -> str | None:
    if signals.get("is_slice_measurement"):
        return "measurement"
    if "state" in signals:
        return "state_change"
    if "Ptarget" in signals or "Qtarget" in signals:
        return "setpoint"
    if any(k in signals for k in ("P", "Q", "U", "AvailableDischargePower")):
        return "physical_measurement"
    return None


def _parse_timestamp(line: str) -> datetime | None:
    match = ISO_TS_PATTERN.search(line)
    if not match:
        return None
    raw = match.group(0)
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _classify_event(line: str) -> str:
    normalized = f" {line.lower()} "
    for event_type, keywords in EVENT_PATTERNS:
        if any(keyword in normalized for keyword in keywords):
            return event_type
    return "log_line"


def parse_meter_dispatcher(path: Path) -> Iterable[Event]:
    with _open_text(path) as stream:
        for idx, line in enumerate(stream, 1):
            text = line.strip()
            if not text:
                continue

            signals = _extract_physical_signals(text)
            base_event_type = _classify_event(text)
            event_type = _physical_event_type(signals) or base_event_type

            payload = {
                "line": idx,
                "path": str(path),
                "parser": "meter_dispatcher",
                "source_group": "meter_dispatcher",
                "future_diagnostic_side": "to_be_inferred",
                "base_event_type": base_event_type,
            }
            payload.update(signals)

            yield Event(
                timestamp=_parse_timestamp(text),
                source=path.name,
                event_type=event_type,
                message=text,
                payload=payload,
            )
