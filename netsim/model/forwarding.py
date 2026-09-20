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
    sid: Any = field(default=None, repr=False)
    """LocalSid for SRV6_LOCAL; hidden from repr to preserve plain-IP fingerprints."""


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

from netsim.model.hashing import (  # noqa: E402
    BalancerKind,
    LoadBalancer,
    flow_label_for,
)
from netsim.model.packets import (  # noqa: E402
    AFTER_DECAP,
    AFTER_ENCAP,
    ETHERTYPE_IPV4,
    ETHERTYPE_IPV6,
    ORIGINATED,
    TRANSIT,
    EthernetFrame,
    FlowKey,
    IPPacket,
    IPv4Packet,
    IPv6Packet,
    decapsulate,
    encapsulate,
    ip_bytes,
    validate_chain,
)

TRANSMIT = 'TRANSMIT'
DELIVER = 'DELIVER'
DROP = 'DROP'
SRV6_UNSUPPORTED = 'SRV6_UNSUPPORTED'

from netsim.model.srv6 import (  # noqa: E402
    END,
    END_DT46,
    END_X,
    NEXT_CSID,
    PSP,
    SID_UNKNOWN,
    SRH_MALFORMED,
    SRH_SL_NONZERO,
    UPPER_LAYER_NOT_ALLOWED,
    USD,
    LocalSid,
    Srv6Encap,
)
from netsim.model.srv6_compress import csid_arg, shift_csid  # noqa: E402

RELOOKUP = 'RELOOKUP'
CROSS_CONNECT = 'CROSS_CONNECT'
DECAP_LOOKUP = 'DECAP_LOOKUP'


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


@record
class SidResult:
    """A pure local action's explicit continuation, shared with the resolver."""

    action: str
    packet: IPPacket | None = None
    adjacency: Adjacency | None = None
    reason: str | None = None


def local_sid(packet: IPPacket, sid: LocalSid | None) -> SidResult:
    """RFC 8986 sections 4.1, 4.2, 4.8, 4.16; RFC 9800 section 4.1.

    Does no FIB, neighbor or interface lookup. In particular End.X returns
    CROSS_CONNECT even if its updated DA matches another local SID.
    """
    if validate_chain(packet) is not None or not isinstance(packet, IPv6Packet):
        return SidResult(DROP, reason=SRH_MALFORMED)
    if sid is None or sid.behavior not in (END, END_X, END_DT46):
        return SidResult(DROP, reason=SRV6_UNSUPPORTED)
    if sid.flavors & ~(PSP | USD | NEXT_CSID) or (
        sid.behavior == END_DT46 and sid.flavors & (PSP | USD)
    ):
        return SidResult(DROP, reason=SRV6_UNSUPPORTED)
    cross = sid.behavior == END_X
    adjacency = (
        Adjacency(sid.interface, sid.nexthop, None, af=6)
        if cross and sid.interface is not None
        else None
    )
    if cross and adjacency is None:
        return SidResult(DROP, reason=ADJ_UNRESOLVED)
    if sid.flavors & NEXT_CSID:
        # RFC 9800 section 4.1.1 N01-N09 runs even without an SRH, before SL.
        try:
            arg = csid_arg(packet.dst, sid.structure)
        except ValueError:
            return SidResult(DROP, reason=SRH_MALFORMED)
        if arg:
            if sid.behavior == END_DT46:
                return SidResult(DROP, reason=SRH_MALFORMED)
            if packet.hop_limit <= 1:
                return SidResult(DROP, reason=TTL_EXPIRED)
            try:
                da, _ = shift_csid(packet.dst, sid.structure)
            except ValueError:
                return SidResult(DROP, reason=SRH_MALFORMED)
            packet = dataclasses.replace(packet, dst=da, hop_limit=packet.hop_limit - 1)
            return SidResult(CROSS_CONNECT if cross else RELOOKUP, packet, adjacency)
    srh = packet.srh
    sl = 0 if srh is None else srh.segments_left
    if sid.behavior == END_DT46:
        if sl != 0:
            return SidResult(DROP, reason=SRH_SL_NONZERO)
    elif sl != 0:
        assert srh is not None
        if packet.hop_limit <= 1:
            return SidResult(DROP, reason=TTL_EXPIRED)
        # RFC 8754 section 4.3.1 permits reduced SRH SL = LE + 1.
        sl -= 1
        pop = sl == 0 and bool(sid.flavors & PSP)
        packet = dataclasses.replace(
            packet,
            dst=srh.entries[sl],
            hop_limit=packet.hop_limit - 1,
            srh=None if pop else dataclasses.replace(srh, segments_left=sl),
            next_header=srh.next_header if pop else packet.next_header,
        )
        return SidResult(CROSS_CONNECT if cross else RELOOKUP, packet, adjacency)
    elif not sid.flavors & USD:
        return SidResult(DROP, reason=UPPER_LAYER_NOT_ALLOWED)
    try:
        inner = decapsulate(packet)
    except ValueError as error:
        return SidResult(DROP, reason=str(error))
    return SidResult(CROSS_CONNECT if cross else DECAP_LOOKUP, inner, adjacency)


def steer(
    view: DeviceView,
    packet: IPPacket,
    ingress: str | None,
    stage: int,
) -> Srv6Encap | None:
    """Gate B2 extension point; called once on ingress and after decapsulation."""
    return None


def srv6_settings(view: DeviceView, encap: Srv6Encap) -> tuple[int | None, int, int]:
    """Read the immutable view's SR source, hop limit and domain hash seed.

    G1's _View owns ``dev`` and ``state``. Keeping this adapter here avoids
    editing network.py while the other slice extends its view API.
    """
    from netsim.model.interfaces import LoopbackNode

    dev = getattr(view, 'dev', None)
    config = getattr(dev, 'config', None)
    source = (
        encap.source
        if encap.source is not None
        else getattr(config, 'srv6_source', None)
    )
    if source is None and dev is not None:
        candidates = [
            addr[0]
            for _, node in dev.interfaces.sorted_items()
            if isinstance(node, LoopbackNode)
            for addr in node.config.ipv6
        ]
        source = min(candidates) if candidates else None
    # _View currently exposes no network seed. The default label domain is 0;
    # an extended view can supply flow_label_seed without using the device's
    # independent ECMP seed (which would change a flow's label between headends).
    seed = getattr(view, 'flow_label_seed', 0)
    return source, getattr(config, 'srv6_hop_limit', 64), seed


def _bind_adjacency(view: DeviceView, adj: Adjacency) -> Adjacency:
    if adj.nexthop is not None:
        mac = view.neighbor_mac(adj.interface, adj.nexthop)
    else:
        dev = getattr(view, 'dev', None)
        neighbors = getattr(dev, 'neighbors', None)
        mac = neighbors.peer_mac(adj.interface) if neighbors is not None else None
    return dataclasses.replace(adj, mac=mac)


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
    view: DeviceView,
    packet: IPPacket,
    ingress: str | None,
    stage: int,
    *,
    encap: Srv6Encap | None = None,
) -> StepResult:
    """Execute the action program, shared by trace and Simulation.send.

    ``encap`` supplies an uncompiled ENCAP action (also useful to steering).
    Compiled FIB legs retain the same Srv6Encap and execute its transformations
    before selecting a resolved leg using the resulting outer flow key.
    """
    compiled: NexthopGroup | None = None
    sr = stage in (AFTER_ENCAP, AFTER_DECAP) or (
        isinstance(packet, IPv6Packet)
        and (
            packet.srh is not None
            or isinstance(packet.payload, (IPv4Packet, IPv6Packet))
        )
    )
    do_steer = stage in (ORIGINATED, TRANSIT, AFTER_DECAP)
    # Bounds same-device ENCAP -> DECAP -> ENCAP cycles independently of TTL.
    for _ in range(256):
        if do_steer:
            if encap is None:
                encap = steer(view, packet, ingress, stage)
            do_steer = False
        if encap is not None:
            sr = True
            try:
                source, hop_limit, seed = srv6_settings(view, encap)
                if source is None:
                    return StepResult(DROP, NO_SOURCE)
                packet = encapsulate(
                    packet,
                    encap.entries,
                    behavior=encap.behavior,
                    source=source,
                    hop_limit=hop_limit,
                    flow_label=flow_label_for(FlowKey.from_packet(packet), seed),
                    transit=stage == TRANSIT,
                )
            except ValueError as error:
                return StepResult(DROP, str(error))
            stage = AFTER_ENCAP
            encap = None
        af = 4 if isinstance(packet, IPv4Packet) else 6
        fib = view.fib(af)
        entry = fib.lookup(packet.dst) if fib is not None else None
        if entry is None:
            return StepResult(DROP, NO_ROUTE)
        if entry.action == RECEIVE:
            # RFC 8754 section 4.3.2: an ordinary local interface cannot consume
            # an active segment. Only a valid chain with SL=0 can be delivered.
            if isinstance(packet, IPv6Packet) and (
                validate_chain(packet) is not None
                or (packet.srh is not None and packet.srh.segments_left != 0)
            ):
                return StepResult(DROP, SRH_MALFORMED)
            return StepResult(DELIVER, packet=packet)
        if entry.action in DROP_ACTIONS:
            reason = ACTION_DROP_REASON[entry.action]
            if entry.action == DROP_UNREACHABLE and any(
                row[2].name == 'srv6-local' for row in entry.contributing
            ):
                reason = SID_UNKNOWN
            return StepResult(DROP, reason)
        if entry.action == SRV6_LOCAL:
            sr = True
            result = local_sid(packet, entry.sid)
            if result.action in (DROP, DELIVER):
                return StepResult(result.action, result.reason, packet=result.packet)
            assert result.packet is not None
            packet = result.packet
            if result.action == CROSS_CONNECT:
                assert result.adjacency is not None
                return _transmit(
                    view, packet, _bind_adjacency(view, result.adjacency), sr=True
                )
            if result.action == DECAP_LOOKUP:
                stage, compiled, do_steer = AFTER_DECAP, None, True
            else:
                stage = AFTER_ENCAP  # local behavior already paid its hop-limit cost
            continue
        group = compiled if compiled is not None else fib.group(entry) if fib else None
        if group is None or not group.adjacencies:
            return StepResult(DROP, NO_ROUTE)
        if compiled is None and any(a.encap is not None for a in group.adjacencies):
            # Gate B1 has one list; choose distinct transformations before underlay
            # ECMP. The per-flow/policy slice can supply weighted list choice here.
            choices = tuple(dict.fromkeys(a.encap for a in group.adjacencies))
            if len(choices) != 1 or not isinstance(choices[0], Srv6Encap):
                return StepResult(DROP, SRV6_UNSUPPORTED)
            encap = choices[0]
            compiled = group
            continue
        # RFC 1812 section 4.2.2.9: transit only, never after local SID or decap.
        if stage == TRANSIT:
            ttl = packet.ttl if isinstance(packet, IPv4Packet) else packet.hop_limit
            if ttl <= 1:
                return StepResult(DROP, TTL_EXPIRED)
            packet = (
                dataclasses.replace(packet, ttl=ttl - 1)
                if isinstance(packet, IPv4Packet)
                else dataclasses.replace(packet, hop_limit=ttl - 1)
            )
        key = FlowKey.from_packet(packet)
        legs, weights = live_legs(view, group, af)
        adj = legs[view.load_balancer(BalancerKind.ECMP).select(key, weights)]
        return _transmit(view, packet, adj, sr=sr)
    return StepResult(DROP, LOOP)


def _transmit(
    view: DeviceView,
    packet: IPPacket,
    adj: Adjacency,
    *,
    sr: bool = False,
) -> StepResult:
    """Shared egress checks; a CROSS_CONNECT enters here without a DA lookup."""
    from netsim.model.interfaces import OperState, PortChannelNode

    af = 4 if isinstance(packet, IPv4Packet) else 6
    key = FlowKey.from_packet(packet)
    egress = view.interface(adj.interface)
    if egress is None or not _l3_usable(egress, af):
        return StepResult(DROP, EGRESS_DOWN, egress=adj.interface)
    if adj.mac is None:
        return StepResult(DROP, ADJ_UNRESOLVED, egress=adj.interface)
    # Preserve Gate A's MTU-before-member drop precedence for plain IP.
    if not sr and ip_bytes(packet) > egress.config.mtu:
        return StepResult(DROP, MTU_EXCEEDED, egress=adj.interface)
    member = adj.interface
    if isinstance(egress, PortChannelNode):
        members = view.active_members(adj.interface)
        if not members:
            return StepResult(DROP, EGRESS_DOWN, egress=adj.interface)
        lag = view.load_balancer(BalancerKind.AGGREGATE_PORT)
        member = members[lag.select(key, (1,) * len(members))]
    if sr and ip_bytes(packet) > egress.config.mtu:
        return StepResult(DROP, MTU_EXCEEDED, egress=adj.interface)
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
