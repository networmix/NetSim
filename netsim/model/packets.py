"""Packet header objects, flow keys, packet templates and wire sizes.

Headers are frozen records with fields, never byte layouts: the simulator
passes objects and only *accounts* for sizes (RFC 791, RFC 8200, RFC 8754
field sets; IEEE 802.3 framing overhead).
"""

from __future__ import annotations

import struct
from typing import Any, NamedTuple

from netsim.model.addressing import IPV4, IPV6, AddressFamily
from netsim.model.state import record

ETHERTYPE_IPV4 = 0x0800
ETHERTYPE_IPV6 = 0x86DD
ETHERTYPE_ARP = 0x0806

PROTO_TCP = 6
PROTO_UDP = 17
PROTO_IPV4 = 4  # IPv4-in-IPv6 (RFC 8986 H.Encaps inner IPv4)
PROTO_IPV6 = 41
PROTO_ROUTING = 43

ETHERNET_HEADER = 14
IPV4_HEADER = 20
IPV6_HEADER = 40
SRH_BASE = 8
SRH_ENTRY = 16

# Packet stage metadata used by the action program.
ORIGINATED = 0
TRANSIT = 1
AFTER_ENCAP = 2
AFTER_DECAP = 3


@record
class L4Header:
    sport: int
    dport: int


@record
class SRH:
    """Segment Routing Header (RFC 8754 §2); entries in RFC order,
    index 0 is the last segment. ``validate`` allows the reduced form
    ``segments_left == last_entry + 1`` (RFC 8754 §4.3.1)."""

    entries: tuple[int, ...]
    segments_left: int
    last_entry: int
    flags: int = 0
    tag: int = 0

    def validate(self) -> str | None:
        n = len(self.entries)
        if n == 0 or self.last_entry != n - 1:
            return 'SRH_MALFORMED'
        if self.segments_left < 0 or self.segments_left > self.last_entry + 1:
            return 'SRH_MALFORMED'
        return None


@record
class IPv4Packet:
    src: int
    dst: int
    protocol: int
    ttl: int = 64
    dscp: int = 0
    ecn: int = 0
    payload: Any = None
    payload_size: int = 0

    af = IPV4

    @property
    def header_bytes(self) -> int:
        return IPV4_HEADER


@record
class IPv6Packet:
    src: int
    dst: int
    next_header: int
    hop_limit: int = 64
    traffic_class: int = 0
    flow_label: int = 0
    srh: SRH | None = None
    payload: Any = None
    payload_size: int = 0

    af = IPV6

    @property
    def dscp(self) -> int:
        return self.traffic_class >> 2

    @property
    def header_bytes(self) -> int:
        n = IPV6_HEADER
        if self.srh is not None:
            n += SRH_BASE + SRH_ENTRY * len(self.srh.entries)
        return n


IPPacket = IPv4Packet | IPv6Packet


@record
class EthernetFrame:
    dst: int
    src: int
    ethertype: int
    payload: Any
    vlan: int | None = None


def ip_bytes(packet: IPPacket) -> int:
    """Bytes of the IP packet as transmitted: every header down the stack plus payload."""
    total = 0
    p: Any = packet
    while isinstance(p, (IPv4Packet, IPv6Packet)):
        total += p.header_bytes
        if isinstance(p.payload, (IPv4Packet, IPv6Packet)):
            p = p.payload
        else:
            total += p.payload_size
            break
    return total


def frame_bytes(packet: IPPacket) -> int:
    """Ethernet frame bytes: 14-byte header plus the IP stack; FCS,
    preamble and inter-frame gap are excluded explicitly."""
    return ETHERNET_HEADER + ip_bytes(packet)


class FlowKey(NamedTuple):
    """The fields hashing may use, all ints; ``to_bytes`` is a fixed
    42-byte canonical encoding."""

    af: int
    src: int
    dst: int
    proto: int
    sport: int
    dport: int
    flow_label: int

    @classmethod
    def from_packet(cls, packet: IPPacket) -> FlowKey:
        payload = packet.payload
        sport = dport = 0
        if isinstance(payload, L4Header):
            sport, dport = payload.sport, payload.dport
        if isinstance(packet, IPv4Packet):
            return cls(IPV4, packet.src, packet.dst, packet.protocol, sport, dport, 0)
        return cls(
            IPV6,
            packet.src,
            packet.dst,
            packet.next_header,
            sport,
            dport,
            packet.flow_label,
        )

    def to_bytes(self) -> bytes:
        return (
            struct.pack(
                '>BBHHI', self.af, self.proto, self.sport, self.dport, self.flow_label
            )
            + self.src.to_bytes(16, 'big')
            + self.dst.to_bytes(16, 'big')
        )


@record
class PacketTemplate:
    """Deterministic packet description for a demand or microflow.

    A microflow index maps into the source port (``sport + i`` mod 65536)
    before hashing, so the index is never an implicit wire field.
    """

    af: int
    src: int
    dst: int
    protocol: int = PROTO_UDP
    sport: int = 49152
    dport: int = 4789
    dscp: int = 0
    flow_label: int = 0
    payload_size: int = 1000
    ttl: int = 64
    hop_limit: int = 64

    def to_packet(self, index: int = 0) -> IPPacket:
        sport = (self.sport + index) & 0xFFFF
        l4 = L4Header(sport, self.dport)
        if self.af == IPV4:
            return IPv4Packet(
                self.src,
                self.dst,
                self.protocol,
                self.ttl,
                self.dscp,
                0,
                l4,
                self.payload_size,
            )
        return IPv6Packet(
            self.src,
            self.dst,
            self.protocol,
            self.hop_limit,
            self.dscp << 2,
            self.flow_label,
            None,
            l4,
            self.payload_size,
        )

    def flow_key(self, index: int = 0) -> FlowKey:
        return FlowKey.from_packet(self.to_packet(index))


def af_of_packet(packet: IPPacket) -> AddressFamily:
    return IPV4 if isinstance(packet, IPv4Packet) else IPV6
