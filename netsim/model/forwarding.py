"""Data-plane records: adjacencies, next-hop groups, FIB entries, neighbors."""

from __future__ import annotations

from dataclasses import field
from typing import Any

from netsim.model.links import (  # noqa: F401  (re-exported drop reasons)
    LINK_DOWN,
    RX_DOWN,
)
from netsim.model.lpm import FrozenPrefixTable
from netsim.model.state import PMap, empty_pmap, record

# FIB actions
FORWARD = 1
RECEIVE = 2
DROP_BLACKHOLE = 3
DROP_UNREACHABLE = 4
DROP_PROHIBIT = 5
SRV6_LOCAL = 6

DROP_ACTIONS = frozenset({DROP_BLACKHOLE, DROP_UNREACHABLE, DROP_PROHIBIT})

# Drop reasons (strings so traces and reports read naturally).
IFACE_DOWN = 'IFACE_DOWN'
MAC_MISMATCH = 'MAC_MISMATCH'
UNSUPPORTED_ETHERTYPE = 'UNSUPPORTED_ETHERTYPE'
VLAN_UNSUPPORTED = 'VLAN_UNSUPPORTED'
NO_ROUTE = 'NO_ROUTE'
TTL_EXPIRED = 'TTL_EXPIRED'
EGRESS_DOWN = 'EGRESS_DOWN'
ADJ_UNRESOLVED = 'ADJ_UNRESOLVED'
MTU_EXCEEDED = 'MTU_EXCEEDED'
LOOP = 'LOOP'
CONGESTION = 'CONGESTION'
POLICY_DOWN = 'POLICY_DOWN'
NO_SOURCE = 'NO_SOURCE'

ACTION_DROP_REASON = {
    DROP_BLACKHOLE: 'DROP_BLACKHOLE',
    DROP_UNREACHABLE: 'DROP_UNREACHABLE',
    DROP_PROHIBIT: 'DROP_PROHIBIT',
}

Prefix = tuple[int, int]
"""``(network_int, prefix_len)``."""


@record
class Adjacency:
    """One resolved forwarding leg."""

    interface: str
    nexthop: int | None
    mac: int | None
    weight: int = 1
    encap: Any = None
    af: int | None = None
    """Family of ``nexthop`` when it differs from the entry's (RFC 8950)."""


@record
class NexthopGroup:
    id: int
    adjacencies: tuple[Adjacency, ...]
    total_weight: int

    @property
    def weights(self) -> tuple[int, ...]:
        return tuple(a.weight for a in self.adjacencies)


@record
class DependsOn:
    """Everything the resolver consulted, including failed lookups."""

    prefixes: tuple[tuple[int, int, int], ...] = ()
    """``(af, network_int, prefix_len)`` of RIB prefixes queried."""
    lookups: tuple[tuple[int, int], ...] = ()
    """``(af, address_int)`` of recursive lookups performed (hits and misses)."""
    interfaces: tuple[str, ...] = ()


@record
class FibEntry:
    prefix: Prefix
    action: int
    group_id: int | None = None
    contributing: tuple[Any, ...] = ()
    """Row keys of the rows that produced this entry."""
    depends_on: DependsOn = field(default_factory=DependsOn)
    program: Any = None
    """SR action program (Gate B); ``None`` for plain forwarding."""


@record
class Fib:
    af: int
    version: int
    entries: FrozenPrefixTable[FibEntry]
    groups: PMap[int, NexthopGroup] = field(default_factory=empty_pmap)

    def lookup(self, addr: int) -> FibEntry | None:
        match = self.entries.lookup(addr)
        return None if match is None else match[2]

    def group(self, entry: FibEntry) -> NexthopGroup | None:
        return None if entry.group_id is None else self.groups[entry.group_id]


NeighborKey = tuple[str, int]
"""``(interface name, address_int)``; link-local entries are scoped by the interface."""


@record
class NeighborTable:
    entries: PMap[NeighborKey, int] = field(default_factory=empty_pmap)
    """Neighbor address on an interface → MAC (numbered peers and link-local)."""
    peers: PMap[str, int] = field(default_factory=empty_pmap)
    """Interface → MAC of its point-to-point peer (interface-only next-hops)."""

    def mac(self, interface: str, address: int) -> int | None:
        return self.entries.get((interface, address))

    def peer_mac(self, interface: str) -> int | None:
        return self.peers.get(interface)


# ---------------------------------------------------------------------------
# Action program (plain IP in Gate A)
# ---------------------------------------------------------------------------

from typing import Protocol  # noqa: E402

from netsim.model.hashing import BalancerKind, LoadBalancer  # noqa: E402
from netsim.model.packets import (  # noqa: E402
    ETHERTYPE_IPV4,
    ETHERTYPE_IPV6,
    ORIGINATED,
    TRANSIT,
    EthernetFrame,
    FlowKey,
    IPPacket,
    IPv4Packet,
    IPv6Packet,
    ip_bytes,
)

TRANSMIT = 'TRANSMIT'
DELIVER = 'DELIVER'
DROP = 'DROP'
SRV6_UNSUPPORTED = 'SRV6_UNSUPPORTED'


class DeviceView(Protocol):
    """What the forward step may ask about one device."""

    name: str

    def enabled(self) -> bool: ...

    def fast_failover(self) -> bool: ...

    def interface(self, name: str) -> Any: ...

    def fib(self, af: int) -> Fib | None: ...

    def neighbor_mac(self, interface: str, address: int) -> int | None: ...

    def load_balancer(self, kind: int) -> LoadBalancer: ...

    def active_members(self, port_channel: str) -> tuple[str, ...]: ...

    def link_for(self, interface: str) -> tuple[Any, tuple[str, str]] | None:
        """``(LinkNode, peer endpoint)`` for an Ethernet, or ``None``."""
        ...

    def endpoint_usable(self, endpoint: tuple[str, str]) -> tuple[bool, bool]:
        """``(admin up, device enabled)`` of a remote endpoint."""
        ...


@record
class StepResult:
    outcome: str
    reason: str | None = None
    egress: str | None = None
    member: str | None = None
    link_id: str | None = None
    edge_id: int | None = None
    peer: tuple[str, str] | None = None
    packet: Any = None
    """Packet as transmitted (TTL decremented) or delivered."""
    mac_dst: int | None = None
    mac_src: int | None = None


def _l3_usable(node: Any, af: int) -> bool:
    from netsim.model.interfaces import l3_usable

    return l3_usable(node, af)


def leg_live(node: Any, af: int) -> bool:
    """What a data plane with fast failover sees for a next-hop group leg:
    the egress is usable for *af* and, for an Ethernet port, its raw carrier
    (link state as the hardware sees it, before any debounce) is up."""
    from netsim.model.interfaces import EthernetNode, l3_usable

    if node is None or not l3_usable(node, af):
        return False
    if isinstance(node, EthernetNode) and not node.oper.carrier_raw:
        return False
    return True


def live_legs(
    view: DeviceView, group: NexthopGroup, af: int
) -> tuple[tuple[Adjacency, ...], tuple[int, ...]]:
    """Legs (and their weights) that fast failover keeps; the whole group
    when it is off or when no leg is live (then the ordinary drop applies)."""
    if not view.fast_failover():
        return group.adjacencies, group.weights
    live = [
        (a, w)
        for a, w in zip(group.adjacencies, group.weights, strict=True)
        if leg_live(view.interface(a.interface), af)
    ]
    if not live or len(live) == len(group.adjacencies):
        return group.adjacencies, group.weights
    return tuple(a for a, _ in live), tuple(w for _, w in live)


def forward_ip(
    view: DeviceView, packet: IPPacket, ingress: str | None, stage: int
) -> StepResult:
    """LOOKUP → TTL → SELECT_GROUP → MTU → SELECT_MEMBER → TRANSMIT."""
    from netsim.model.interfaces import OperState, PortChannelNode

    af = 4 if isinstance(packet, IPv4Packet) else 6
    fib = view.fib(af)
    entry = fib.lookup(packet.dst) if fib is not None else None
    if entry is None:
        return StepResult(DROP, NO_ROUTE)
    if entry.action == RECEIVE:
        return StepResult(DELIVER, packet=packet)
    if entry.action in DROP_ACTIONS:
        return StepResult(DROP, ACTION_DROP_REASON[entry.action])
    if entry.action == SRV6_LOCAL:
        return StepResult(DROP, SRV6_UNSUPPORTED)
    # TTL: transit packets only (RFC 1812 §4.2.2.9 for receive is handled above).
    if stage == TRANSIT:
        ttl = packet.ttl if isinstance(packet, IPv4Packet) else packet.hop_limit
        if ttl <= 1:
            return StepResult(DROP, TTL_EXPIRED)
        packet = (
            dataclasses.replace(packet, ttl=ttl - 1)
            if isinstance(packet, IPv4Packet)
            else dataclasses.replace(packet, hop_limit=ttl - 1)
        )
    group = fib.group(entry) if fib is not None else None
    if group is None or not group.adjacencies:
        return StepResult(DROP, NO_ROUTE)
    key = FlowKey.from_packet(packet)
    ecmp = view.load_balancer(BalancerKind.ECMP)
    legs, weights = live_legs(view, group, af)
    adj = legs[ecmp.select(key, weights)]
    egress = view.interface(adj.interface)
    if egress is None or not _l3_usable(egress, af):
        return StepResult(DROP, EGRESS_DOWN, egress=adj.interface)
    if adj.mac is None:
        return StepResult(DROP, ADJ_UNRESOLVED, egress=adj.interface)
    if ip_bytes(packet) > egress.config.mtu:
        return StepResult(DROP, MTU_EXCEEDED, egress=adj.interface)
    member = adj.interface
    if isinstance(egress, PortChannelNode):
        members = view.active_members(adj.interface)
        if not members:
            return StepResult(DROP, EGRESS_DOWN, egress=adj.interface)
        lag = view.load_balancer(BalancerKind.AGGREGATE_PORT)
        member = members[lag.select(key, (1,) * len(members))]
    tx = view.link_for(member)
    if tx is None:
        return StepResult(DROP, EGRESS_DOWN, egress=adj.interface, member=member)
    link, peer = tx
    member_node = view.interface(member)
    rx_admin, rx_enabled = view.endpoint_usable(peer)
    from netsim.model.interfaces import AdminState
    from netsim.model.links import transfer_blocked

    blocked = transfer_blocked(
        link.oper.state,
        member_node.config.admin == AdminState.UP,
        view.enabled(),
        rx_admin,
        rx_enabled,
    )
    if blocked is not None:
        return StepResult(
            DROP,
            blocked,
            egress=adj.interface,
            member=member,
            link_id=link.id,
            edge_id=link.edge_id((view.name, member)),
            peer=peer,
            packet=packet,
        )
    if member_node.oper.oper != OperState.UP:
        return StepResult(DROP, EGRESS_DOWN, egress=adj.interface, member=member)
    return StepResult(
        TRANSMIT,
        egress=adj.interface,
        member=member if member != adj.interface else None,
        link_id=link.id,
        edge_id=link.edge_id((view.name, member)),
        peer=peer,
        packet=packet,
        mac_dst=adj.mac,
        mac_src=egress.mac,
    )


def receive_frame(view: DeviceView, ingress: str, frame: EthernetFrame) -> StepResult:
    """L2 checks, then the IP step as a transit packet."""
    from netsim.model.interfaces import EthernetNode, OperState

    node = view.interface(ingress)
    if node is None or node.oper.oper != OperState.UP:
        return StepResult(DROP, IFACE_DOWN)
    logical = node
    if isinstance(node, EthernetNode) and node.config.aggregate_id is not None:
        logical = view.interface(node.config.aggregate_id)
    if frame.vlan is not None:
        return StepResult(DROP, VLAN_UNSUPPORTED)
    if frame.dst != logical.mac and not ((frame.dst >> 40) & 1):
        return StepResult(DROP, MAC_MISMATCH)
    if frame.ethertype not in (ETHERTYPE_IPV4, ETHERTYPE_IPV6) or not isinstance(
        frame.payload, (IPv4Packet, IPv6Packet)
    ):
        return StepResult(DROP, UNSUPPORTED_ETHERTYPE)
    return forward_ip(view, frame.payload, ingress, TRANSIT)


@record
class Hop:
    device: str
    ingress: str | None
    egress: str | None
    member: str | None
    link_id: str | None
    edge_id: int | None
    packet: Any
    outcome: str
    reason: str | None


@record
class Trace:
    hops: tuple[Hop, ...]
    outcome: str
    reason: str | None

    @property
    def path(self) -> tuple[str, ...]:
        return tuple(h.device for h in self.hops)


def trace(
    views: Callable[[str], DeviceView],
    device: str,
    packet: IPPacket,
    max_hops: int = 64,
) -> Trace:
    """Walk a packet from *device* (originated) until delivered or dropped."""
    hops: list[Hop] = []
    current = device
    ingress: str | None = None
    frame: EthernetFrame | None = None
    for _ in range(max_hops):
        view = views(current)
        if frame is None:
            result = forward_ip(view, packet, None, ORIGINATED)
        else:
            assert ingress is not None
            result = receive_frame(view, ingress, frame)
        hops.append(
            Hop(
                current,
                ingress,
                result.egress,
                result.member,
                result.link_id,
                result.edge_id,
                result.packet,
                result.outcome,
                result.reason,
            )
        )
        if result.outcome != TRANSMIT:
            return Trace(tuple(hops), result.outcome, result.reason)
        assert result.peer is not None and result.packet is not None
        packet = result.packet
        ethertype = ETHERTYPE_IPV4 if isinstance(packet, IPv4Packet) else ETHERTYPE_IPV6
        frame = EthernetFrame(
            result.mac_dst or 0, result.mac_src or 0, ethertype, packet
        )
        current, ingress = result.peer
    return Trace(tuple(hops), DROP, LOOP)


import dataclasses  # noqa: E402
from typing import Callable  # noqa: E402
