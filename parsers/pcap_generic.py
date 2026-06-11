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


@dataclass
class PacketSummary:
    timestamp: datetime
    ethertype: int | None
    transport: str | None = None
    src_port: int | None = None
    dst_port: int | None = None
    tcp_flags: int | None = None
    payload: bytes = b""


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

    first_ts = packets[0].timestamp
    last_ts = packets[-1].timestamp
    duration_s = max(0.0, (last_ts - first_ts).total_seconds())
    has_homeplug = ethertypes.get(ETHERTYPE_HOMEPLUG, 0) > 0
    has_ipv6 = ethertypes.get(ETHERTYPE_IPV6, 0) > 0
    has_tcp = transports.get("tcp", 0) > 0
    likely_v2g = has_homeplug or any("V2G" in marker or "SLAC" in marker for marker in markers)

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
        "pcap_top_ports": [port for port, _count in ports.most_common(8)],
        "pcap_tcp_syn_count": tcp_syn,
        "pcap_tcp_fin_count": tcp_fin,
        "pcap_tcp_rst_count": tcp_resets,
        "pcap_markers": markers,
        "pcap_has_homeplug": has_homeplug,
        "pcap_has_ipv6": has_ipv6,
        "pcap_has_tcp": has_tcp,
        "pcap_likely_v2g": likely_v2g,
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
