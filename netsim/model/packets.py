"""Packet header objects, flow keys, packet templates and wire sizes.

Headers are frozen records with fields, never byte layouts: the simulator
passes objects and only *accounts* for sizes (RFC 791, RFC 8200, RFC 8754
field sets; IEEE 802.3 framing overhead).
"""

from __future__ import annotations

import struct
from collections.abc import Sequence
from dataclasses import replace
from typing import Any, NamedTuple

from netsim.model.addressing import IPV4, IPV6, AddressFamily
from netsim.model.srv6 import H_ENCAPS, H_ENCAPS_RED
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
    ``segments_left == last_entry + 1`` (RFC 8754 §4.3.1.1).

    The following header is stored in ``IPv6Packet.payload``; ``next_header``
    identifies it. Its UDP default preserves the five positional arguments.
    TLVs are not represented by this fixed-size Gate B header model.
    """

    entries: tuple[int, ...]
    segments_left: int
    last_entry: int
    flags: int = 0
    tag: int = 0
    next_header: int = PROTO_UDP

    def validate(self) -> str | None:
        n = len(self.entries)
        # RFC 8754 §2: Hdr Ext Len is 8 bits, with two units per entry.
        if not 1 <= n <= 127 or self.last_entry != n - 1:
            return 'SRH_MALFORMED'
        if self.segments_left < 0 or self.segments_left > self.last_entry + 1:
            return 'SRH_MALFORMED'
        if (
            not 0 <= self.flags <= 255
            or not 0 <= self.tag <= 65535
            or not 0 <= self.next_header <= 255
            or any(not 0 <= sid < 1 << 128 for sid in self.entries)
        ):
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
    """IPv6 header and optional SRH, followed by ``payload``.

    With ``srh`` present, ``next_header`` must be 43. Constructors permit
    malformed records for probes; ``validate_chain`` detects disagreement.
    """

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
            n += srh_bytes(self.srh)
        return n


IPPacket = IPv4Packet | IPv6Packet


@record
class EthernetFrame:
    dst: int
    src: int
    ethertype: int
    payload: Any
    vlan: int | None = None


def srh_bytes(srh: SRH) -> int:
    """RFC 8754 §2 fixed header and Segment List (no TLVs in Gate B)."""
    return SRH_BASE + SRH_ENTRY * len(srh.entries)


def validate_chain(packet: IPPacket) -> str | None:
    """Check every typed header link and SRH; opaque transport data is allowed.

    The SRH occupies the ``srh`` field, not ``payload``. Validation does not
    rewrite malformed probes or enforce behavior-specific SL/TTL checks.
    """
    current = packet
    while True:
        if isinstance(current, IPv6Packet):
            protocol = current.next_header
            if current.srh is not None:
                if protocol != PROTO_ROUTING or current.srh.validate() is not None:
                    return 'SRH_MALFORMED'
                protocol = current.srh.next_header
        else:
            protocol = current.protocol
        payload = current.payload
        if not 0 <= protocol <= 255 or protocol == PROTO_ROUTING:
            return 'SRH_MALFORMED'
        if isinstance(payload, IPv4Packet):
            if protocol != PROTO_IPV4:
                return 'SRH_MALFORMED'
        elif isinstance(payload, IPv6Packet):
            if protocol != PROTO_IPV6:
                return 'SRH_MALFORMED'
        else:
            if (
                protocol in (PROTO_IPV4, PROTO_IPV6)
                or isinstance(payload, SRH)
                or isinstance(payload, L4Header)
                and protocol not in (PROTO_TCP, PROTO_UDP)
            ):
                return 'SRH_MALFORMED'
            return None
        current = payload


def encapsulate(
    inner: IPPacket,
    entries: Sequence[int],
    *,
    behavior: int,
    source: int,
    hop_limit: int,
    flow_label: int,
    transit: bool,
    flags: int = 0,
    tag: int = 0,
    tlvs: tuple[bytes, ...] = (),
) -> IPv6Packet:
    """RFC 8986 §§5.1-5.2 encapsulation, with entries in forwarding order.

    Errors are ``ValueError(reason)`` for the action interpreter to model as
    drops. TLVs require a variable-size header model and are rejected explicitly.
    Reduced one-entry lists carrying flags/tag use a full one-entry SRH so
    Last Entry remains representable. Originated inner packets retain identity.
    """
    if behavior not in (H_ENCAPS, H_ENCAPS_RED):
        raise ValueError('UNSUPPORTED_BEHAVIOR')
    if not entries:
        raise ValueError('EMPTY_LIST')
    if tlvs:
        raise ValueError('SRH_TLV_UNSUPPORTED')
    if validate_chain(inner) is not None:
        raise ValueError('SRH_MALFORMED')
    if isinstance(inner.payload, (IPv4Packet, IPv6Packet)):
        raise ValueError('NESTED_ENCAP_UNSUPPORTED')
    if any(not 0 <= sid < 1 << 128 for sid in entries):
        raise ValueError('SRH_MALFORMED')
    protocol = PROTO_IPV4 if isinstance(inner, IPv4Packet) else PROTO_IPV6
    # RFC 8986 §5.1 S05 / RFC 2473: the headend consumes one transit hop
    # on the inner header, never a hop on the newly initialized outer header.
    if transit:
        ttl = inner.ttl if isinstance(inner, IPv4Packet) else inner.hop_limit
        if ttl <= 1:
            raise ValueError('TTL_EXPIRED')
        inner = (
            replace(inner, ttl=ttl - 1)
            if isinstance(inner, IPv4Packet)
            else replace(inner, hop_limit=ttl - 1)
        )
    srh = None
    n = len(entries)
    if behavior == H_ENCAPS or n > 1 or flags or tag:
        listed = entries[1:] if behavior == H_ENCAPS_RED and n > 1 else entries
        srh = SRH(tuple(reversed(listed)), n - 1, len(listed) - 1, flags, tag, protocol)
        if srh.validate() is not None:
            raise ValueError('SRH_MALFORMED')
    return IPv6Packet(
        source,
        entries[0],
        PROTO_ROUTING if srh is not None else protocol,
        hop_limit=hop_limit,
        traffic_class=inner.dscp << 2,
        flow_label=flow_label,
        srh=srh,
        payload=inner,
    )


def decapsulate(outer: IPPacket) -> IPPacket:
    """Remove one outer IPv6/SRH stack, preserving the inner object and TTL.

    The caller enforces the active SID's SL and flavor rules before calling.
    """
    if validate_chain(outer) is not None:
        raise ValueError('SRH_MALFORMED')
    if not isinstance(outer, IPv6Packet) or not isinstance(
        outer.payload, (IPv4Packet, IPv6Packet)
    ):
        raise ValueError('UPPER_LAYER_NOT_ALLOWED')
    return outer.payload


def outer_stack_bytes(packet: IPPacket) -> int:
    """Tunnel overhead outside the innermost IP packet, excluding Ethernet.

    Plain IP is 0; IPv6 encapsulation adds 40 plus its SRH, if present.
    ``ip_bytes`` includes the innermost IP header and payload as well.
    """
    total = 0
    while isinstance(packet.payload, (IPv4Packet, IPv6Packet)):
        total += packet.header_bytes
        packet = packet.payload
    return total


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
        """Outer IP fields, with ports reached through its optional SRH.

        ``proto`` is the outer header's protocol/Next Header (43 with SRH).
        Stop at an inner IP header: its fields contribute only via the outer
        flow label derived at encapsulation, never by inspecting inner ports.
        """
        payload = packet.payload
        protocol = (
            packet.protocol if isinstance(packet, IPv4Packet) else packet.next_header
        )
        transport = protocol
        if isinstance(packet, IPv6Packet) and protocol == PROTO_ROUTING:
            if packet.srh is not None:
                transport = packet.srh.next_header
        sport = dport = 0
        if transport in (PROTO_TCP, PROTO_UDP) and isinstance(payload, L4Header):
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
    before hashing, so the index is never an implicit wire field. ``srh``
    and ``inner`` optionally describe a probe's header stack. Next Header
    fields are derived from that stack; its innermost template owns the
    transport ports and payload size.
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
    srh: SRH | None = None
    inner: PacketTemplate | None = None

    def to_packet(self, index: int = 0) -> IPPacket:
        sport = (self.sport + index) & 0xFFFF
        l4 = L4Header(sport, self.dport)
        payload = self.inner.to_packet(index) if self.inner is not None else l4
        protocol = self.protocol
        if isinstance(payload, IPv4Packet):
            protocol = PROTO_IPV4
        elif isinstance(payload, IPv6Packet):
            protocol = PROTO_IPV6
        payload_size = self.payload_size if self.inner is None else 0
        if self.af == IPV4:
            if self.srh is not None:
                raise ValueError('SRH_REQUIRES_IPV6')
            return IPv4Packet(
                self.src,
                self.dst,
                protocol,
                self.ttl,
                self.dscp,
                0,
                payload,
                payload_size,
            )
        srh = self.srh
        if srh is not None and srh.next_header != protocol:
            srh = replace(srh, next_header=protocol)
        return IPv6Packet(
            self.src,
            self.dst,
            PROTO_ROUTING if srh is not None else protocol,
            self.hop_limit,
            self.dscp << 2,
            self.flow_label,
            srh,
            payload,
            payload_size,
        )

    def flow_key(self, index: int = 0) -> FlowKey:
        return FlowKey.from_packet(self.to_packet(index))


def af_of_packet(packet: IPPacket) -> AddressFamily:
    return IPV4 if isinstance(packet, IPv4Packet) else IPV6
