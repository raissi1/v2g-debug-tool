"""Lightweight generic PCAP parser for V2G session evidence."""

from __future__ import annotations

import struct
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from core.models import Event

LINKTYPE_ETHERNET = 1
LINKTYPE_LINUX_SLL = 113

ETHERTYPE_HOMEPLUG = 0x88E1
ETHERTYPE_IPV4 = 0x0800
ETHERTYPE_IPV6 = 0x86DD
V2GTP_VERSION = (0x01, 0xFE)
TLS_RECORD_APPDATA = 0x17
TLS_VERSIONS = {0x0301, 0x0302, 0x0303, 0x0304}
PCAP_GAP_THRESHOLD_S = 10.0
PCAP_PHASE_BREAK_S = 3.0

KEYWORD_PATTERNS: tuple[bytes, ...] = (
    b"V2G",
    b"Renault",
    b"CLIENT_RANDOM",
    b"ISO15118",
    b"SessionSetup",
    b"ChargingStatus",
    b"HomePlug",
    b"SLAC",
    b"DIN",
    b"TLS",
)

SDP_MESSAGE_TYPES = {
    0x9000: "SDP_REQUEST",
    0x9001: "SDP_RESPONSE",
    0x8001: "SDP_REQUEST_ALT",
    0x8002: "EXI_REQUEST",
    0x9002: "EXI_RESPONSE",
}

SDP_SECURITY = {
    0x00: "TLS",
    0x10: "NO_TLS",
}


@dataclass
class PacketSummary:
    timestamp: datetime
    ethertype: int | None
    transport: str | None = None
    src_port: int | None = None
    dst_port: int | None = None
    tcp_flags: int | None = None
    payload: bytes = b""


def _is_tls_appdata(payload: bytes) -> bool:
    return len(payload) >= 5 and payload[0] == TLS_RECORD_APPDATA and ((payload[1] << 8) | payload[2]) in TLS_VERSIONS


def _parse_sdp_payload(payload: bytes) -> dict[str, object] | None:
    if len(payload) < 8:
        return None
    if (payload[0], payload[1]) != V2GTP_VERSION:
        return None

    message_type = (payload[2] << 8) | payload[3]
    info: dict[str, object] = {
        "message_type": SDP_MESSAGE_TYPES.get(message_type, f"V2GTP_0x{message_type:04x}"),
        "message_type_code": f"0x{message_type:04x}",
        "payload_length": int.from_bytes(payload[4:8], "big"),
    }
    if message_type == 0x9001 and len(payload) >= 28:
        info["server_port"] = int.from_bytes(payload[24:26], "big")
        info["security"] = SDP_SECURITY.get(payload[26], f"0x{payload[26]:02x}")
        info["transport"] = "TCP" if payload[27] == 0x00 else f"0x{payload[27]:02x}"
    return info


def _tls_appdata_times(packets: list[PacketSummary]) -> list[datetime]:
    return [packet.timestamp for packet in packets if packet.transport == "tcp" and _is_tls_appdata(packet.payload)]


def _build_phase_rows(timestamps: list[datetime]) -> list[tuple[datetime, datetime, int, float, str]]:
    if len(timestamps) < 2:
        return []

    phases: list[list[datetime]] = [[timestamps[0]]]
    for current in timestamps[1:]:
        previous = phases[-1][-1]
        if (current - previous).total_seconds() > PCAP_PHASE_BREAK_S:
            phases.append([current])
        else:
            phases[-1].append(current)

    rows: list[tuple[datetime, datetime, int, float, str]] = []
    for index, phase in enumerate(phases):
        start = phase[0]
        end = phase[-1]
        duration_s = max(0.0, (end - start).total_seconds())
        count = len(phase)
        if index == 0 and duration_s < 20:
            phase_type = "tls_handshake_or_authorization"
        elif count >= 6 and duration_s > 1:
            phase_type = "active_energy_exchange"
        elif duration_s < 15:
            phase_type = "session_transition_or_stop"
        else:
            phase_type = "v2g_session_phase"
        rows.append((start, end, count, duration_s, phase_type))
    return rows


def _read_pcap_header(blob: bytes) -> tuple[str, int]:
    if len(blob) < 24:
        raise ValueError("PCAP too short")

    magic = blob[:4]
    if magic in {b"\xd4\xc3\xb2\xa1", b"\x4d\x3c\xb2\xa1"}:
        endian = "<"
    elif magic in {b"\xa1\xb2\xc3\xd4", b"\xa1\xb2\x3c\x4d"}:
        endian = ">"
    else:
        raise ValueError("Unsupported PCAP format")

    network = struct.unpack(f"{endian}I", blob[20:24])[0]
    return endian, int(network)


def _iter_packets(blob: bytes, endian: str, linktype: int) -> list[PacketSummary]:
    packets: list[PacketSummary] = []
    offset = 24

    while offset + 16 <= len(blob):
        ts_sec, ts_usec, incl_len, _orig_len = struct.unpack(f"{endian}IIII", blob[offset : offset + 16])
        offset += 16
        if offset + incl_len > len(blob):
            break

        raw = blob[offset : offset + incl_len]
        offset += incl_len

        summary = _parse_packet(
            raw=raw,
            timestamp=datetime.fromtimestamp(ts_sec + (ts_usec / 1_000_000.0), tz=timezone.utc),
            linktype=linktype,
        )
        if summary is not None:
            packets.append(summary)

    return packets


def _parse_packet(raw: bytes, timestamp: datetime, linktype: int) -> PacketSummary | None:
    if linktype == LINKTYPE_LINUX_SLL:
        if len(raw) < 16:
            return None
        ethertype = struct.unpack("!H", raw[14:16])[0]
        payload = raw[16:]
    elif linktype == LINKTYPE_ETHERNET:
        if len(raw) < 14:
            return None
        ethertype = struct.unpack("!H", raw[12:14])[0]
        payload = raw[14:]
    else:
        return PacketSummary(timestamp=timestamp, ethertype=None, payload=raw)

    packet = PacketSummary(timestamp=timestamp, ethertype=ethertype, payload=payload)

    if ethertype == ETHERTYPE_IPV4 and len(payload) >= 20:
        ihl = (payload[0] & 0x0F) * 4
        if len(payload) < ihl:
            return packet
        proto = payload[9]
        transport_payload = payload[ihl:]
        _parse_transport(packet, proto, transport_payload)
    elif ethertype == ETHERTYPE_IPV6 and len(payload) >= 40:
        next_header = payload[6]
        transport_payload = payload[40:]
        _parse_transport(packet, next_header, transport_payload)

    return packet


def _parse_transport(packet: PacketSummary, proto: int, payload: bytes) -> None:
    if proto == 6 and len(payload) >= 20:
        packet.transport = "tcp"
        packet.src_port, packet.dst_port = struct.unpack("!HH", payload[:4])
        packet.tcp_flags = payload[13]
        packet.payload = payload[(payload[12] >> 4) * 4 :] if len(payload) >= (payload[12] >> 4) * 4 else b""
    elif proto == 17 and len(payload) >= 8:
        packet.transport = "udp"
        packet.src_port, packet.dst_port = struct.unpack("!HH", payload[:4])
        packet.payload = payload[8:]


def _extract_ascii_markers(blob: bytes, limit: int = 8) -> list[str]:
    found: list[str] = []
    lowered = blob.lower()
    for pattern in KEYWORD_PATTERNS:
        index = lowered.find(pattern.lower())
        if index >= 0:
            found.append(pattern.decode("ascii", "ignore"))

    # Add a few human-readable subjects/labels if present.
    current = bytearray()
    extras: list[str] = []
    for byte in blob:
        if 32 <= byte < 127:
            current.append(byte)
        else:
            if len(current) >= 12:
                extras.append(current.decode("ascii", "ignore"))
            current = bytearray()
    if len(current) >= 12:
        extras.append(current.decode("ascii", "ignore"))

    for text in extras:
        if any(token in text for token in ("V2G", "Renault", "Powerbox", "Connected Car", "Vehicles SubCA")):
            found.append(text[:120])
        if len(found) >= limit:
            break

    seen: list[str] = []
    for item in found:
        if item not in seen:
            seen.append(item)
    return seen[:limit]


def parse_pcap_file(path: Path) -> list[Event]:
    blob = path.read_bytes()
    endian, linktype = _read_pcap_header(blob)
    packets = _iter_packets(blob, endian, linktype)

    if not packets:
        return [
            Event(
                timestamp=datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc),
                source=path.name,
                event_type="warning",
                message="PCAP detected but no packet could be parsed.",
                payload={
                    "path": str(path),
                    "parser": "pcap_generic",
                    "source_group": "netlogger",
                    "future_diagnostic_side": "to_be_inferred",
                    "pcap_link_type": linktype,
                },
            )
        ]

    ethertypes = Counter(packet.ethertype for packet in packets if packet.ethertype is not None)
    transports = Counter(packet.transport for packet in packets if packet.transport)
    ports = Counter(
        port
        for packet in packets
        for port in (packet.src_port, packet.dst_port)
        if isinstance(port, int)
    )
    tcp_resets = sum(1 for packet in packets if packet.tcp_flags is not None and packet.tcp_flags & 0x04)
    tcp_syn = sum(1 for packet in packets if packet.tcp_flags is not None and packet.tcp_flags & 0x02)
    tcp_fin = sum(1 for packet in packets if packet.tcp_flags is not None and packet.tcp_flags & 0x01)
    markers = _extract_ascii_markers(blob[: min(len(blob), 1_000_000)])
    tls_appdata_ts = _tls_appdata_times(packets)
    tls_phase_rows = _build_phase_rows(tls_appdata_ts)
    sdp_pairs = [
        (packet, parsed)
        for packet in packets
        if packet.transport == "udp" and 15118 in (packet.src_port, packet.dst_port)
        for parsed in [_parse_sdp_payload(packet.payload)]
        if parsed is not None
    ]
    sdp_messages = [parsed for _packet, parsed in sdp_pairs]

    first_ts = packets[0].timestamp
    last_ts = packets[-1].timestamp
    duration_s = max(0.0, (last_ts - first_ts).total_seconds())
    has_homeplug = ethertypes.get(ETHERTYPE_HOMEPLUG, 0) > 0
    has_ipv6 = ethertypes.get(ETHERTYPE_IPV6, 0) > 0
    has_tcp = transports.get("tcp", 0) > 0
    likely_v2g = has_homeplug or any("V2G" in marker or "SLAC" in marker for marker in markers) or bool(sdp_messages) or bool(tls_appdata_ts)
    top_ports = [port for port, _count in ports.most_common(8)]
    candidate_v2g_ports = [port for port in top_ports if int(port) > 1024 and port != 15118]
    tls_gaps_s: list[float] = []
    for previous, current in zip(tls_appdata_ts, tls_appdata_ts[1:]):
        gap_s = (current - previous).total_seconds()
        if gap_s >= PCAP_GAP_THRESHOLD_S:
            tls_gaps_s.append(round(gap_s, 3))

    summary_message = (
        f"PCAP analysed: {len(packets)} packets, linktype={linktype}, "
        f"ethertypes={dict(sorted((hex(k), v) for k, v in ethertypes.items()))}, "
        f"transports={dict(transports)}, top_ports={[port for port, _count in ports.most_common(4)]}, "
        f"tcp_resets={tcp_resets}."
    )
    payload = {
        "path": str(path),
        "parser": "pcap_generic",
        "source_group": "netlogger",
        "future_diagnostic_side": "to_be_inferred",
        "pcap_packet_count": len(packets),
        "pcap_link_type": linktype,
        "pcap_duration_s": duration_s,
        "pcap_ethertypes": {hex(k): v for k, v in ethertypes.items()},
        "pcap_transports": dict(transports),
        "pcap_top_ports": top_ports,
        "pcap_v2g_candidate_ports": candidate_v2g_ports,
        "pcap_tcp_syn_count": tcp_syn,
        "pcap_tcp_fin_count": tcp_fin,
        "pcap_tcp_rst_count": tcp_resets,
        "pcap_markers": markers,
        "pcap_has_homeplug": has_homeplug,
        "pcap_has_ipv6": has_ipv6,
        "pcap_has_tcp": has_tcp,
        "pcap_likely_v2g": likely_v2g,
        "pcap_sdp_message_count": len(sdp_messages),
        "pcap_sdp_messages": sdp_messages[:6],
        "pcap_tls_appdata_count": len(tls_appdata_ts),
        "pcap_tls_gap_events_s": tls_gaps_s[:10],
        "pcap_tls_phase_count": len(tls_phase_rows),
    }

    events = [
        Event(
            timestamp=first_ts,
            source=path.name,
            event_type="protocol_event",
            message=summary_message,
            payload=payload,
        )
    ]

    if likely_v2g:
        events.append(
            Event(
                timestamp=first_ts,
                source=path.name,
                event_type="protocol_event",
                message="V2G protocol markers detected in PCAP.",
                payload={**payload, "pcap_markers": markers},
            )
        )

    if has_homeplug:
        events.append(
            Event(
                timestamp=first_ts,
                source=path.name,
                event_type="protocol_event",
                message="HomePlug / SLAC level traffic detected in PCAP.",
                payload=payload,
            )
        )

    if candidate_v2g_ports:
        events.append(
            Event(
                timestamp=first_ts,
                source=path.name,
                event_type="protocol_event",
                message=f"Potential negotiated V2G TCP ports detected: {candidate_v2g_ports[:4]}.",
                payload=payload,
            )
        )

    for packet, sdp_info in sdp_pairs[:4]:
        if sdp_info["message_type"] == "SDP_RESPONSE":
            message = (
                f"SDP response observed: port={sdp_info.get('server_port')} "
                f"security={sdp_info.get('security')} transport={sdp_info.get('transport')}."
            )
        else:
            message = f"{sdp_info['message_type']} observed on UDP/15118."
        events.append(
            Event(
                timestamp=packet.timestamp,
                source=path.name,
                event_type="protocol_event",
                message=message,
                payload={**payload, "pcap_sdp_current": sdp_info},
            )
        )

    for start, end, count, phase_duration_s, phase_type in tls_phase_rows[:5]:
        events.append(
            Event(
                timestamp=start,
                source=path.name,
                event_type="session_event",
                message=(
                    f"V2G TLS phase detected: {phase_type}, "
                    f"{count} application-data packets over {phase_duration_s:.1f}s."
                ),
                payload={
                    **payload,
                    "pcap_phase_type": phase_type,
                    "pcap_phase_start": start.isoformat(),
                    "pcap_phase_end": end.isoformat(),
                    "pcap_phase_duration_s": round(phase_duration_s, 3),
                    "pcap_phase_packet_count": count,
                },
            )
        )

    for previous, current in zip(tls_appdata_ts, tls_appdata_ts[1:]):
        gap_s = (current - previous).total_seconds()
        if gap_s >= PCAP_GAP_THRESHOLD_S:
            events.append(
                Event(
                    timestamp=previous,
                    source=path.name,
                    event_type="timeout" if gap_s >= 20 else "warning",
                    message=f"V2G TLS traffic gap detected: {gap_s:.1f}s without encrypted application traffic.",
                    payload={
                        **payload,
                        "pcap_gap_s": round(gap_s, 3),
                        "pcap_gap_start": previous.isoformat(),
                        "pcap_gap_end": current.isoformat(),
                    },
                )
            )

    if tcp_resets > 0:
        events.append(
            Event(
                timestamp=last_ts,
                source=path.name,
                event_type="warning",
                message=f"TCP resets detected in PCAP: {tcp_resets}.",
                payload=payload,
            )
        )
    elif has_tcp:
        events.append(
            Event(
                timestamp=last_ts,
                source=path.name,
                event_type="protocol_event",
                message="TCP session observed in PCAP without reset.",
                payload=payload,
            )
        )

    return events
