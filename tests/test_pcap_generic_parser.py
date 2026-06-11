from pathlib import Path
from tempfile import TemporaryDirectory
import struct

from parsers.pcap_generic import parse_pcap_file


def _write_test_pcap(path: Path) -> None:
    global_header = struct.pack(
        "<IHHIIII",
        0xA1B2C3D4,
        2,
        4,
        0,
        0,
        65535,
        113,
    )

    # Linux cooked header with HomePlug ethertype.
    sll_header = bytes.fromhex("000100010006f896fe0bcd2d000088e1")
    homeplug_payload = b"SLAC V2G HomePlug"
    pkt1 = sll_header + homeplug_payload
    rec1 = struct.pack("<IIII", 1_700_000_000, 0, len(pkt1), len(pkt1)) + pkt1

    # Linux cooked header with IPv6 + TCP, including CLIENT_RANDOM marker.
    sll_ipv6 = bytes.fromhex("000400010006f896fe0bcd2d000086dd")
    ipv6_header = bytearray(40)
    ipv6_header[0] = 0x60
    payload = b"CLIENT_RANDOM V2G Renault"
    tcp_header = bytearray(20)
    struct.pack_into("!HH", tcp_header, 0, 49153, 43153)
    tcp_header[12] = 0x50
    tcp_header[13] = 0x02
    transport = bytes(tcp_header) + payload
    struct.pack_into("!H", ipv6_header, 4, len(transport))
    ipv6_header[6] = 6
    ipv6_header[7] = 64
    pkt2 = sll_ipv6 + bytes(ipv6_header) + transport
    rec2 = struct.pack("<IIII", 1_700_000_001, 0, len(pkt2), len(pkt2)) + pkt2

    path.write_bytes(global_header + rec1 + rec2)


def test_parse_pcap_file_extracts_v2g_markers_and_ports() -> None:
    with TemporaryDirectory() as td:
        path = Path(td) / "sample.pcap"
        _write_test_pcap(path)

        events = parse_pcap_file(path)

        assert len(events) >= 3
        summary = events[0]
        assert summary.event_type == "protocol_event"
        assert summary.payload["pcap_packet_count"] == 2
        assert summary.payload["pcap_link_type"] == 113
        assert summary.payload["pcap_has_homeplug"] is True
        assert summary.payload["pcap_has_ipv6"] is True
        assert summary.payload["pcap_has_tcp"] is True
        assert summary.payload["pcap_likely_v2g"] is True
        assert 49153 in summary.payload["pcap_top_ports"]
        assert 43153 in summary.payload["pcap_top_ports"]
        assert any("V2G" in marker for marker in summary.payload["pcap_markers"])
        assert any(event.message.startswith("HomePlug / SLAC") for event in events)
