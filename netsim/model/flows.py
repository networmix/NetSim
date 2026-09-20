"""Flow placement: demands become per-edge offered, carried and dropped
rates by walking each forwarding-equivalence class over the FIBs.

FLUID demands with identical forwarding (same family, destination, DSCP
and packet template) share one class walk: destination-based forwarding
is linear in inflow, so a class is walked once with all its sources and
each demand's result is its rate times the class fractions. HASH demands
execute the forward step per microflow. Loops are handled per strongly
connected component: traffic that stays inside a cyclic component is
charged on the entering edge and dropped as ``LOOP``; traffic leaving it
continues. Physical failures (``LINK_DOWN`` / ``RX_DOWN``) and MTU are
checked per edge, so stale routing during debounce shows as drops.
"""

from __future__ import annotations

import dataclasses
from collections import defaultdict
from dataclasses import field
from fractions import Fraction
from typing import Any, Iterator, NamedTuple, Protocol

from netsim.model import derive
from netsim.model import forwarding as fw
from netsim.model.addressing import IPV4, IPV6, to_int
from netsim.model.hashing import BalancerKind
from netsim.model.interfaces import AdminState, OperState, PortChannelNode, l3_usable
from netsim.model.links import transfer_blocked
from netsim.model.packets import (
    AFTER_DECAP,
    AFTER_ENCAP,
    ETHERNET_HEADER,
    ETHERTYPE_IPV4,
    ETHERTYPE_IPV6,
    IPV4_HEADER,
    IPV6_HEADER,
    ORIGINATED,
    SRH,
    SRH_BASE,
    SRH_ENTRY,
    TRANSIT,
    EthernetFrame,
    IPv4Packet,
    IPv6Packet,
    L4Header,
    PacketTemplate,
    frame_bytes,
    ip_bytes,
)
from netsim.model.srv6 import NESTED_ENCAP_UNSUPPORTED, PolicyRef
from netsim.model.state import (
    FloatArray,
    NetworkState,
    PMap,
    diff_pmap,
    empty_pmap,
    record,
)

FLUID = 1
HASH = 2

UNCONSTRAINED = 1
LOSSY = 2


@record
class Demand:
    id: str
    source: str
    dst: int
    af: int
    rate: float
    """IP-payload bit/s."""
    template: PacketTemplate | None = None
    payload_size: int = 1000
    mode: int = FLUID
    dscp: int = 0
    flows: int = 1
    priority: int = 0
    tag: str | None = None
    steer: PolicyRef | None = None

    def __post_init__(self) -> None:
        if (
            self.rate < 0
            or (self.template is None and self.payload_size <= 0)
            or self.flows < 1
        ):
            raise ValueError('demand needs rate >= 0, payload_size > 0, flows >= 1')
        if self.template is not None and payload_bytes(self.template) <= 0:
            raise ValueError('template needs payload_size > 0')

    @property
    def class_key(self) -> tuple:
        base = (self.af, self.dst, self.dscp, self.payload_size)
        if self.template is None and self.steer is None:
            return base  # Preserve Gate A cache keys exactly.
        template = _ordered_value(self.template) if self.template is not None else ()
        steer = (
            (self.steer.color, self.steer.endpoint) if self.steer is not None else ()
        )
        return (*base, template, steer)


def payload_bytes(template: PacketTemplate) -> int:
    """One authoritative rate denominator: payload of the innermost IP header."""
    while template.inner is not None:
        template = template.inner
    return template.payload_size


def _ordered_value(value: Any) -> tuple:
    """Explicit tags keep optional headers/records totally ordered, deterministically."""
    if value is None:
        return (0,)
    if isinstance(value, int):
        return (1, value)
    if isinstance(value, tuple):
        return (2, tuple(_ordered_value(item) for item in value))
    if isinstance(value, (PacketTemplate, SRH)):
        return (
            3 if isinstance(value, PacketTemplate) else 4,
            tuple(
                _ordered_value(getattr(value, f.name))
                for f in dataclasses.fields(value)
            ),
        )
    raise TypeError(f'unsupported packet-template field: {type(value).__name__}')


def make_demand(id: str, source: str, dst: str, rate: float, **kw: Any) -> Demand:
    dst_int, af = to_int(dst)
    return Demand(id, source, dst_int, af, float(rate), **kw)


@record
class SourceResult:
    """One source's exact share of a class walk, as fractions of that
    source's unit rate."""

    edges: tuple[tuple[int, Fraction], ...]
    carried: tuple[tuple[int, Fraction], ...]
    delivered: Fraction
    drops: tuple[tuple[str, str, Fraction], ...]
    transmissions: tuple[_Transmission, ...] = field(default=(), repr=False)
    policy_outcomes: tuple[tuple[str, int, int, str, str, Fraction], ...] = field(
        default=(), repr=False
    )
    """Internal accounting metadata; omitted from the stable Gate A repr."""


@record
class ClassResult:
    """Unit result of one class walk: fractions of a unit payload rate."""

    edges: tuple[tuple[int, Fraction], ...]
    """``(edge_id, fraction of the class inflow attempted on the edge)`` per source-weighted unit."""
    carried: tuple[tuple[int, Fraction], ...]
    delivered: Fraction
    drops: tuple[tuple[str, str, Fraction], ...]
    """``(reason, device or edge id, fraction)``."""
    visited: tuple[str, ...]
    """Devices consulted by the walk; validity is checked against the report's dependency table."""
    sources: tuple[tuple[str, Fraction], ...] = ()
    per_source: tuple[tuple[str, SourceResult], ...] = ()
    """Exact per-source results, filled only when the class has several
    sources and loses traffic (otherwise every source is fully delivered)."""
    transmissions: tuple[_Transmission, ...] = field(default=(), repr=False)
    policy_outcomes: tuple[tuple[str, int, int, str, str, Fraction], ...] = field(
        default=(), repr=False
    )
    """Each leg's attempted/carried payload fraction and transmitted frame bytes.
    Packet rate on a leg is ``fraction * payload_rate / (8 * payload_size)``.
    Repeated uses of a physical edge remain separate transmissions."""
    cacheable: bool = field(default=True, repr=False)
    """False for injected steps whose external inputs have no dependency token."""


@record
class PolicyDelivery:
    device: str
    color: int
    endpoint: int
    delivered: float
    drops: tuple[tuple[str, str, float], ...] = ()


@record
class DemandResult:
    demand: str
    delivered: float
    """Payload bit/s delivered."""
    drops: tuple[tuple[str, str, float], ...]
    edges: tuple[tuple[int, float], ...] = ()
    """``(edge_id, payload bit/s attempted)`` in FULL detail; empty when the
    demand shares a forwarding class with other sources, because the class
    walk does not attribute edges per source."""
    policies: tuple[PolicyDelivery, ...] = field(default=(), repr=False)


@record
class PlacementReport:
    version: int
    model: int
    edge_count: int
    offered: FloatArray
    """Wire bit/s attempted per directed edge (``2*link.index + direction``)."""
    carried: FloatArray
    dropped: FloatArray
    """Payload bit/s dropped on the edge (physical, MTU, loop, congestion)."""
    capacity: FloatArray
    edge_links: PMap[int, str] = field(default_factory=empty_pmap)
    demands: PMap[str, DemandResult] = field(default_factory=empty_pmap)
    classes: PMap[tuple, ClassResult] = field(default_factory=empty_pmap)
    deps: PMap[str, DepToken] = field(default_factory=empty_pmap)
    """Per-device dependency tokens the cached classes were computed against."""
    delivered_total: float = 0.0
    dropped_by_reason: PMap[str, float] = field(default_factory=empty_pmap)

    # -- views ----------------------------------------------------------------

    def utilization(self, edge_id: int) -> float:
        cap = self.capacity[edge_id]
        return self.carried[edge_id] / cap if cap > 0 else float('inf')

    def oversubscribed(self) -> list[tuple[int, float]]:
        out = [
            (e, self.utilization(e))
            for e in range(self.edge_count)
            if self.capacity[e] > 0 and self.offered[e] > self.capacity[e]
        ]
        return sorted(out, key=lambda x: (-x[1], x[0]))

    def bottlenecks(self, k: int = 5) -> list[tuple[int, float]]:
        out = [
            (e, self.utilization(e))
            for e in range(self.edge_count)
            if self.capacity[e] > 0
        ]
        return sorted(out, key=lambda x: (-x[1], x[0]))[:k]

    def oversubscribed_count(self) -> int:
        cap, off = self.capacity.view(), self.offered.view()
        return sum(1 for e in range(self.edge_count) if cap[e] > 0 and off[e] > cap[e])

    def max_utilization(self) -> float:
        return max(
            (
                self.utilization(e)
                for e in range(self.edge_count)
                if self.capacity[e] > 0
            ),
            default=0.0,
        )


# ---------------------------------------------------------------------------
# Edge index
# ---------------------------------------------------------------------------


def _edge_tables(state: NetworkState) -> tuple[int, dict[int, str], list[float]]:
    n = 0
    links: dict[int, str] = {}
    caps: list[float] = []
    for link in state.links.values():
        n = max(n, 2 * link.index + 2)
    caps = [0.0] * n
    for lid, link in state.links.items():
        cap = derive.link_capacity(state, link)
        for d in (0, 1):
            links[2 * link.index + d] = lid
            caps[2 * link.index + d] = cap
    return n, links, caps


def _frame_bytes(af: int, payload_size: int) -> int:
    return ETHERNET_HEADER + (IPV4_HEADER if af == IPV4 else IPV6_HEADER) + payload_size


# ---------------------------------------------------------------------------
# Class walk (FLUID)
# ---------------------------------------------------------------------------


class _PacketState(NamedTuple):
    """Forwarding identity, excluding TTL (FLUID uses SCC loop-cut).

    Plain IP has only an inner family. SR steps must retain the complete
    remaining SID/action continuation, not just the active DA: equal-sized
    SRHs can have different tails. ``stage`` distinguishes steering after
    DECAP from initial ingress; it is never a monotonically growing hop id.
    Tuple ordering makes vertex and leg ordering stable without hash/id order.
    """

    inner_af: int
    outer: bool = False
    srh_entries: int = 0
    active_da: int = 0
    continuation: tuple[int, ...] = ()
    stage: int = ORIGINATED
    policies: tuple[tuple[str, int, int], ...] = ()
    wire: tuple = ()

    def encap(
        self, active_da: int, srh_entries: int = 0, continuation: tuple[int, ...] = ()
    ) -> _PacketState:
        """Push the Gate B outer IPv6 stack; the SR step supplies its continuation."""
        if self.outer:
            raise ValueError(NESTED_ENCAP_UNSUPPORTED)
        if srh_entries < 0:
            raise ValueError('SRH entry count must be non-negative')
        return _PacketState(
            self.inner_af,
            True,
            srh_entries,
            active_da,
            continuation,
            AFTER_ENCAP,
            self.policies,
        )

    def decap(self) -> _PacketState:
        """Pop the outer stack and expose the inner steering context."""
        return (
            _PacketState(self.inner_af, stage=AFTER_DECAP, policies=self.policies)
            if self.outer
            else self
        )

    def frame_bytes(self, payload_size: int) -> int:
        # RFC 8200 section 3 and RFC 8754 section 2: outer 40 + SRH 8 + 16*n.
        if self.wire:
            return frame_bytes(_packet_from_token(self.wire))
        size = _frame_bytes(self.inner_af, payload_size)
        if self.outer:
            size += IPV6_HEADER
            if self.srh_entries:
                size += SRH_BASE + SRH_ENTRY * self.srh_entries
        return size


_Vertex = tuple[str, _PacketState]


class _Transmission(NamedTuple):
    edge_id: int
    frame_bytes: int
    offered: Fraction
    carried: Fraction


@record
class _Edge:
    edge_id: int
    peer: str
    fraction: Fraction
    blocked: str | None
    """Physical or MTU reason, or ``None``."""
    frame_bytes: int
    packet_state: _PacketState
    """Successor state after this transmission's complete local action program."""


_Decision = tuple[str, Any, list[_Edge], list[tuple[str, Fraction]]]


class _Step(Protocol):
    def __call__(
        self,
        state: NetworkState,
        device: str,
        packet_state: _PacketState,
        af: int,
        dst: int,
        payload_size: int,
        *,
        template: PacketTemplate | None,
        steer: PolicyRef | None,
        dscp: int,
    ) -> _Decision:
        """Expand local actions into ordered physical legs and node drops.

        ENCAP/DECAP steps transform the state before egress resolution, using
        ``encap``/``decap`` and then ``_egress_edges`` (or a cross-connect).
        Each returned edge carries the transmitted size and successor state.
        The step owns local-action validation and steering consumption; the
        walk owns SCCs, physical-leg accounting and capacity allocation.
        """
        ...


def _egress_edges(
    state: NetworkState,
    device: str,
    packet_state: _PacketState,
    af: int,
    dst: int,
    payload_size: int,
    *,
    template: PacketTemplate | None = None,
    steer: PolicyRef | None = None,
    dscp: int = 0,
) -> _Decision:
    """FIB decision at *device* for *dst*: ``(action, entry, edges, node_drops)``."""
    if steer is not None and packet_state.stage == ORIGINATED:
        return fw.DROP, None, [], [(fw.SRV6_UNSUPPORTED, Fraction(1))]
    if packet_state.outer:
        af, dst = IPV6, packet_state.active_da
    frame_len = packet_state.frame_bytes(payload_size)
    ip_len = frame_len - ETHERNET_HEADER
    dev = state.devices[device]
    fib = dev.fibs.get(af)
    entry = fib.lookup(dst) if fib is not None else None
    if entry is None:
        return fw.DROP, None, [], [(fw.NO_ROUTE, Fraction(1))]
    if entry.action == fw.RECEIVE:
        return fw.DELIVER, entry, [], []
    if entry.action in fw.DROP_ACTIONS:
        return fw.DROP, entry, [], [(fw.ACTION_DROP_REASON[entry.action], Fraction(1))]
    if entry.action != fw.FORWARD:
        return fw.DROP, entry, [], [(fw.SRV6_UNSUPPORTED, Fraction(1))]
    group = fib.group(entry) if fib is not None else None
    if group is None or not group.adjacencies:
        return fw.DROP, entry, [], [(fw.NO_ROUTE, Fraction(1))]
    edges: list[_Edge] = []
    drops: list[tuple[str, Fraction]] = []
    legs = group.adjacencies
    if dev.config.fast_failover:
        # Data-plane pruning: dead legs get no share, live legs are renormalized.
        live = tuple(
            a for a in legs if fw.leg_live(dev.interfaces.get(a.interface), af)
        )
        if live:
            legs = live
    total = sum(a.weight for a in legs)
    for adj in legs:
        share = Fraction(adj.weight, total)
        egress = dev.interfaces.get(adj.interface)
        if egress is None or not l3_usable(egress, af):
            drops.append((fw.EGRESS_DOWN, share))
            continue
        if adj.mac is None:
            drops.append((fw.ADJ_UNRESOLVED, share))
            continue
        if ip_len > egress.config.mtu:
            drops.append((fw.MTU_EXCEEDED, share))
            continue
        if isinstance(egress, PortChannelNode):
            members = sorted(n for n, m in egress.oper.members.items() if m.active)
            if not members:
                drops.append((fw.EGRESS_DOWN, share))
                continue
            member_share = share / len(members)
        else:
            members = [adj.interface]
            member_share = share
        for member in members:
            link = derive.link_of(state, device, member)
            mnode = dev.interfaces[member]
            if link is None:
                drops.append((fw.EGRESS_DOWN, member_share))
                continue
            peer = link.other((device, member))
            pdev = state.devices.get(peer[0])
            pnode = pdev.interfaces.get(peer[1]) if pdev is not None else None
            blocked = transfer_blocked(
                link.oper.state,
                mnode.config.admin == AdminState.UP,
                dev.config.enabled,
                pnode is not None and pnode.config.admin == AdminState.UP,
                pdev is not None and pdev.config.enabled,
            )
            if blocked is None and mnode.oper.oper != OperState.UP:
                blocked = fw.EGRESS_DOWN
            edges.append(
                _Edge(
                    link.edge_id((device, member)),
                    peer[0],
                    member_share,
                    blocked,
                    frame_len,
                    packet_state,
                )
            )
    return fw.TRANSMIT, entry, edges, drops


class _Observed(NamedTuple):
    entry: Any
    terminals: tuple[tuple[str, Fraction, tuple[tuple[str, int, int], ...]], ...]


class _Branch(Exception):
    def __init__(self, weights: tuple[int, ...]) -> None:
        self.weights = weights


def _forward_edges(
    state: NetworkState,
    device: str,
    packet_state: _PacketState,
    af: int,
    dst: int,
    payload_size: int,
    *,
    template: PacketTemplate | None = None,
    steer: PolicyRef | None = None,
    dscp: int = 0,
) -> _Decision:
    """Enumerate the interpreter's weighted decisions, preserving packet state.

    The packet interpreter remains the only implementation of local actions.
    Selection is pure: each branch replays a finite prefix of choices. TTL is
    intentionally absent from FLUID identity; the walk cuts cycles by SCC.
    """
    from netsim.model.network import _View

    view = _View(state, device)
    fib = view.fib(6 if packet_state.outer else af)
    entry = (
        fib.lookup(packet_state.active_da if packet_state.outer else dst)
        if fib
        else None
    )
    group = fib.group(entry) if fib and entry else None
    sr_fib = view.fib(6)
    if not (
        packet_state.wire
        or packet_state.outer
        or steer is not None
        or sr_fib
        and sr_fib.steering
        or entry
        and (entry.action == fw.SRV6_LOCAL or isinstance(entry.program, PolicyRef))
        or group
        and any(a.encap is not None for a in group.adjacencies)
    ):
        decision = _egress_edges(
            state,
            device,
            packet_state,
            af,
            dst,
            payload_size,
            template=template,
            dscp=dscp,
        )
        if packet_state.policies:
            action, entry, edges, losses = decision
            terms = (
                ((fw.DELIVER, Fraction(1), packet_state.policies),)
                if action == fw.DELIVER
                else tuple(
                    (reason, fraction, packet_state.policies)
                    for reason, fraction in losses
                )
            )
            return action, _Observed(entry, terms), edges, losses
        return decision
    inner = (
        template
        or PacketTemplate(
            af,
            _source_address(state, device, af),
            dst,
            payload_size=payload_size,
            dscp=dscp,
        )
    ).to_packet()
    packet = inner
    if packet_state.wire:
        packet = _packet_from_token(packet_state.wire)
    elif packet_state.outer:
        srh = None
        if packet_state.srh_entries:
            sl, le, nh, flags, tag, *entries = packet_state.continuation
            srh = SRH(tuple(entries), sl, le, flags, tag, nh)
        packet = IPv6Packet(
            0,
            packet_state.active_da,
            43 if srh else 4 if af == 4 else 41,
            payload=inner,
            srh=srh,
            traffic_class=dscp << 2,
            hop_limit=255,
        )
    branches: list[tuple[tuple[int, ...], Fraction]] = [((), Fraction(1))]
    terminals = []
    edges: list[_Edge] = []
    drops: list[tuple[str, Fraction]] = []
    while branches:
        choices, share = branches.pop()
        cursor = 0

        def select(_domain, _key, weights, choices=choices):
            nonlocal cursor
            if len(weights) == 1:
                return 0
            if cursor == len(choices):
                raise _Branch(weights)
            choice = choices[cursor]
            cursor += 1
            return choice

        observed: list[tuple[int, int]] = []
        try:
            result = fw.forward_ip(
                view,
                packet,
                None,
                packet_state.stage,
                policy=steer if packet_state.stage == ORIGINATED else None,
                select=select,
                fluid=True,
                policies=observed,
            )
        except _Branch as branch:
            total = sum(branch.weights)
            for i in reversed(range(len(branch.weights))):
                branches.append(
                    (choices + (i,), share * Fraction(branch.weights[i], total))
                )
            continue
        history = tuple(
            sorted(set(packet_state.policies) | {(device, c, e) for c, e in observed})
        )
        if result.edge_id is None:
            reason = (
                fw.DELIVER
                if result.outcome == fw.DELIVER
                else result.reason or fw.NO_ROUTE
            )
            drops.append((reason, share))
            terminals.append((reason, share, history))
            continue
        assert result.packet is not None and result.peer is not None
        output = result.packet
        successor = _state_from_packet(output, TRANSIT, history)
        edges.append(
            _Edge(
                result.edge_id,
                result.peer[0],
                share,
                result.reason if result.outcome == fw.DROP else None,
                frame_bytes(output),
                successor,
            )
        )
    return fw.TRANSMIT, _Observed(entry, tuple(terminals)), edges, drops


def _bounded_fraction(value: float) -> Fraction:
    """The exact fraction of a non-negative float (floats are dyadic
    rationals, so this is always representable). Nothing is rounded to a
    coarse denominator: a tiny share beside a large one stays nonzero and
    per-demand conservation holds to floating-point precision."""
    exact = Fraction(value)
    return exact if exact > 0 else Fraction(0)


def _share(rate: float, total: Fraction) -> Fraction:
    """``rate / total`` as an exact fraction; the shares of one class sum to
    exactly one when *total* is the exact sum of the rates."""
    return Fraction(rate) / total if total > 0 else Fraction(0)


def _edge_loc(edge_id: int) -> str:
    """Drop location for a directed edge; device names cannot contain ``:``."""
    return f'edge:{edge_id}'


def edge_of_location(where: str) -> int | None:
    """Edge id of a drop location, or ``None`` for a device location."""
    return int(where[5:]) if where.startswith('edge:') else None


def _tarjan(
    nodes: list[_Vertex], succ: dict[_Vertex, list[_Vertex]]
) -> list[list[_Vertex]]:
    """Strongly connected components in reverse topological order (Tarjan),
    with an explicit stack so a forwarding chain of any length works."""
    index: dict[_Vertex, int] = {}
    low: dict[_Vertex, int] = {}
    on_stack: set[_Vertex] = set()
    stack: list[_Vertex] = []
    comps: list[list[_Vertex]] = []
    counter = 0
    for root in nodes:
        if root in index:
            continue
        index[root] = low[root] = counter
        counter += 1
        stack.append(root)
        on_stack.add(root)
        work: list[tuple[_Vertex, Iterator[_Vertex]]] = [
            (root, iter(succ.get(root, ())))
        ]
        while work:
            v, it = work[-1]
            descended = False
            for w in it:
                if w not in index:
                    index[w] = low[w] = counter
                    counter += 1
                    stack.append(w)
                    on_stack.add(w)
                    work.append((w, iter(succ.get(w, ()))))
                    descended = True
                    break
                if w in on_stack:
                    low[v] = min(low[v], index[w])
            if descended:
                continue
            work.pop()
            if work:
                u = work[-1][0]
                low[u] = min(low[u], low[v])
            if low[v] == index[v]:
                comp = []
                while True:
                    w = stack.pop()
                    on_stack.discard(w)
                    comp.append(w)
                    if w == v:
                        break
                comps.append(sorted(comp))
    return comps


def walk_class(
    state: NetworkState,
    af: int,
    dst: int,
    payload_size: int,
    sources: dict[str, Fraction],
    residual: list[float] | None = None,
    scale: float = 1.0,
    *,
    template: PacketTemplate | None = None,
    steer: PolicyRef | None = None,
    step: _Step | None = None,
    dscp: int = 0,
) -> ClassResult:
    """Walk one class, propagating original payload fractions (packet rate).

    With ``residual`` (LOSSY), clip each transmission's wire load scaled by
    the demand's payload rate. Deterministic leg order is reverse Tarjan
    component order (roots/successors sorted by vertex), sorted vertices
    within a component, then the step's edge order. The plain step preserves
    FIB adjacency order and sorted bundle members. SR steps must emit policy
    lists by descending weight, then name, and legs in chain order.

    ``step`` is the SR expansion seam. It must be a pure finite derivation of
    the immutable snapshot, including complete continuation in every vertex.
    """
    if template is not None:
        af, dst, payload_size = template.af, template.dst, payload_bytes(template)
        dscp = template.dscp
    initial = (
        _state_from_packet(template.to_packet(), ORIGINATED)
        if template is not None
        and (template.inner is not None or template.srh is not None)
        else _PacketState(af)
    )
    expand = _forward_edges if step is None else step
    decisions: dict[_Vertex, _Decision] = {}
    succ: dict[_Vertex, list[_Vertex]] = {}
    order: list[_Vertex] = []
    frontier = [(d, initial) for d in sorted(sources)]
    seen: set[_Vertex] = set()
    while frontier:
        v = frontier.pop()
        d, packet_state = v
        if v in seen or d not in state.devices:
            continue
        seen.add(v)
        order.append(v)
        dec = expand(
            state,
            d,
            packet_state,
            af,
            dst,
            payload_size,
            template=template,
            steer=steer,
            dscp=dscp,
        )
        decisions[v] = dec
        nxt = sorted({(e.peer, e.packet_state) for e in dec[2] if e.blocked is None})
        succ[v] = nxt
        frontier.extend(n for n in nxt if n not in seen)
    comps = _tarjan(sorted(order), succ)
    comp_of: dict[_Vertex, int] = {}
    cyclic: set[int] = set()
    for i, comp in enumerate(comps):
        for v in comp:
            comp_of[v] = i
        if len(comp) > 1 or (comp and comp[0] in succ.get(comp[0], ())):
            cyclic.add(i)
    inflow: dict[_Vertex, Fraction] = defaultdict(Fraction)
    for s, f in sources.items():
        inflow[(s, initial)] += f
    edges: dict[int, Fraction] = defaultdict(Fraction)
    carried: dict[int, Fraction] = defaultdict(Fraction)
    transmissions: list[_Transmission] = []
    zero = Fraction(0)
    policy_outcomes: dict[tuple[str, int, int, str, str], Fraction] = defaultdict(
        Fraction
    )

    def observe(history, reason, where, fraction):
        for device, color, endpoint in history:
            policy_outcomes[(device, color, endpoint, reason, where)] += fraction

    delivered = zero
    drops: dict[tuple[str, str], Fraction] = defaultdict(Fraction)
    # Tarjan yields components in reverse topological order: process from the last.
    for ci in range(len(comps) - 1, -1, -1):
        for v in comps[ci]:
            q = inflow.get(v, zero)
            if q == 0:
                continue
            action, _entry, egress, node_drops = decisions[v]
            if isinstance(_entry, _Observed):
                for reason, fraction, history in _entry.terminals:
                    observe(history, reason, v[0], q * fraction)
            if action == fw.DELIVER:
                delivered += q
                continue
            for reason, f in node_drops:
                if reason == fw.DELIVER:
                    delivered += q * f
                else:
                    drops[(reason, v[0])] += q * f
            if action == fw.DROP:
                continue
            for e in egress:
                share = q * e.fraction
                edges[e.edge_id] += share
                successor = (e.peer, e.packet_state)
                reason = e.blocked
                if reason is None and comp_of.get(successor) == ci and ci in cyclic:
                    reason = fw.LOOP
                if reason is not None:
                    drops[(reason, _edge_loc(e.edge_id))] += share
                    observe(
                        e.packet_state.policies, reason, _edge_loc(e.edge_id), share
                    )
                    transmissions.append(
                        _Transmission(e.edge_id, e.frame_bytes, share, zero)
                    )
                    continue
                if residual is not None:
                    wire_per_payload = e.frame_bytes / payload_size
                    wire_cap = residual[e.edge_id]
                    attempted = float(share) * scale * wire_per_payload
                    if attempted > wire_cap:
                        carried_share = (
                            _bounded_fraction(
                                max(wire_cap, 0.0) / (scale * wire_per_payload)
                            )
                            if scale > 0
                            else zero
                        )
                        carried_share = min(carried_share, share)
                        drops[(fw.CONGESTION, _edge_loc(e.edge_id))] += (
                            share - carried_share
                        )
                        observe(
                            e.packet_state.policies,
                            fw.CONGESTION,
                            _edge_loc(e.edge_id),
                            share - carried_share,
                        )
                        residual[e.edge_id] = 0.0
                    else:
                        carried_share = share
                        residual[e.edge_id] = wire_cap - attempted
                else:
                    carried_share = share
                carried[e.edge_id] += carried_share
                transmissions.append(
                    _Transmission(e.edge_id, e.frame_bytes, share, carried_share)
                )
                inflow[successor] += carried_share
    return ClassResult(
        tuple(sorted(edges.items())),
        tuple(sorted(carried.items())),
        delivered,
        tuple(sorted((r, w, f) for (r, w), f in drops.items())),
        tuple(sorted({d for d, _packet_state in order})),
        tuple(sorted(sources.items())),
        policy_outcomes=tuple(
            (*key, value) for key, value in sorted(policy_outcomes.items())
        ),
        transmissions=tuple(transmissions),
        cacheable=step is None,
    )


@record
class DepToken:
    """What a class walk depends on at one device, as references compared
    by identity: the device's config, interfaces, FIBs and load balancers,
    and per link the link node plus the peer endpoint's device config and
    interface node (``_egress_edges`` reads those for physical availability
    and capacity). Validity checks cost O(visited devices); nothing is copied."""

    config: Any
    interfaces: Any
    fibs: Any
    load_balancers: Any
    links: tuple[tuple[Any, Any, Any], ...]
    """``(link node, peer device config, peer interface node)`` per attached link."""

    def same(self, other: DepToken) -> bool:
        if not (
            self.config is other.config
            and self.interfaces is other.interfaces
            and self.fibs is other.fibs
            and self.load_balancers is other.load_balancers
            and len(self.links) == len(other.links)
        ):
            return False
        for (a, ac, an), (b, bc, bn) in zip(self.links, other.links, strict=True):
            if a is not b or ac is not bc or an is not bn:
                return False
        return True


def dep_token(state: NetworkState, device: str) -> DepToken:
    dev = state.devices[device]
    links: list[tuple[Any, Any, Any]] = []
    for name, _node in dev.interfaces.sorted_items():
        link = derive.link_of(state, device, name)
        if link is None:
            continue
        peer = link.other((device, name))
        pdev = state.devices.get(peer[0])
        pnode = pdev.interfaces.get(peer[1]) if pdev is not None else None
        links.append((link, pdev.config if pdev is not None else None, pnode))
    return DepToken(
        dev.config, dev.interfaces, dev.fibs, dev.load_balancers, tuple(links)
    )


# ---------------------------------------------------------------------------
# HASH walk
# ---------------------------------------------------------------------------


def walk_hash(
    state: NetworkState,
    demand: Demand,
    views: Any,
    max_hops: int = 64,
    residual: list[float] | None = None,
    *,
    wire: tuple[dict[int, float], dict[int, float]] | None = None,
    policy_outcomes: dict[tuple[str, int, int, str, str], float] | None = None,
) -> tuple[dict[int, float], dict[int, float], float, dict[tuple[str, str], float]]:
    """Per microflow forward-step execution; returns edge offered/carried
    payload rates, delivered payload rate and drops. With ``residual``
    (LOSSY) each hop is clipped against the remaining wire capacity. Optional
    ``wire`` accumulators receive the corresponding per-hop wire rates."""
    template = demand.template or PacketTemplate(
        demand.af,
        _source_address(state, demand.source, demand.af),
        demand.dst,
        dscp=demand.dscp,
        payload_size=demand.payload_size,
    )
    payload_size = payload_bytes(template)
    per_flow = demand.rate / demand.flows
    offered: dict[int, float] = defaultdict(float)
    carried: dict[int, float] = defaultdict(float)
    wire_offered: dict[tuple[int, int], float] = defaultdict(float)
    wire_carried: dict[tuple[int, int], float] = defaultdict(float)
    delivered = 0.0
    drops: dict[tuple[str, str], float] = defaultdict(float)
    for i in range(demand.flows):
        packet = template.to_packet(i)
        current = demand.source
        ingress: str | None = None
        frame: EthernetFrame | None = None
        outcome = fw.DROP
        reason: str | None = fw.LOOP
        where = current
        rate = per_flow  # payload bit/s still flowing on this microflow
        history: set[tuple[str, int, int]] = set()
        for _ in range(max_hops):
            view = views(current)
            observed: list[tuple[int, int]] = []
            if frame is None:
                res = (
                    fw.forward_ip(
                        view,
                        packet,
                        None,
                        ORIGINATED,
                        policy=demand.steer,
                        policies=observed,
                    )
                    if policy_outcomes is not None or demand.steer is not None
                    else fw.forward_ip(view, packet, None, ORIGINATED)
                )
            else:
                assert ingress is not None
                res = (
                    fw.receive_frame(view, ingress, frame, policies=observed)
                    if policy_outcomes is not None
                    else fw.receive_frame(view, ingress, frame)
                )
            history.update((current, c, e) for c, e in observed)
            where = current
            wire_per_payload = 0.0
            frame_len = 0
            if res.edge_id is not None:
                assert res.packet is not None
                frame_len = frame_bytes(res.packet)
                wire_per_payload = frame_len / payload_size
                offered[res.edge_id] += rate
                if wire is not None:
                    wire_offered[(res.edge_id, frame_len)] += rate
            if res.outcome != fw.TRANSMIT:
                outcome, reason = res.outcome, res.reason
                if res.reason in (fw.LINK_DOWN, fw.RX_DOWN) and res.edge_id is not None:
                    where = _edge_loc(res.edge_id)
                break
            assert (
                res.edge_id is not None
                and res.peer is not None
                and res.packet is not None
            )
            if residual is not None:
                wire_cap = residual[res.edge_id]
                attempted = rate * wire_per_payload
                if attempted > wire_cap:
                    fits = max(wire_cap, 0.0) / wire_per_payload
                    drops[(fw.CONGESTION, _edge_loc(res.edge_id))] += rate - fits
                    if policy_outcomes is not None:
                        for dev, c, e in history:
                            k = (dev, c, e, fw.CONGESTION, _edge_loc(res.edge_id))
                            policy_outcomes[k] = (
                                policy_outcomes.get(k, 0.0) + rate - fits
                            )
                    residual[res.edge_id] = 0.0
                    rate = fits
                else:
                    residual[res.edge_id] = wire_cap - attempted
            carried[res.edge_id] += rate
            if wire is not None:
                wire_carried[(res.edge_id, frame_len)] += rate
            packet = res.packet
            ethertype = (
                ETHERTYPE_IPV4 if isinstance(packet, IPv4Packet) else ETHERTYPE_IPV6
            )
            frame = EthernetFrame(res.mac_dst or 0, res.mac_src or 0, ethertype, packet)
            current, ingress = res.peer
        if policy_outcomes is not None:
            for dev, c, e in history:
                k = (
                    dev,
                    c,
                    e,
                    fw.DELIVER if outcome == fw.DELIVER else reason or fw.LOOP,
                    where,
                )
                policy_outcomes[k] = policy_outcomes.get(k, 0.0) + rate
        if outcome == fw.DELIVER:
            delivered += rate
        else:
            drops[(reason or fw.LOOP, where)] += rate
    if wire is not None:
        # Aggregate equal-sized transmissions before scaling, preserving the
        # plain-IP arithmetic even for many microflows sharing an edge.
        for rates, output in zip((wire_offered, wire_carried), wire, strict=True):
            for (edge, size), payload_rate in sorted(rates.items()):
                output[edge] = output.get(edge, 0.0) + payload_rate * (
                    size / payload_size
                )
    return offered, carried, delivered, drops


def _source_address(state: NetworkState, device: str, af: int) -> int:
    from netsim.model.interfaces import LoopbackNode

    dev = state.devices[device]
    for node in dev.interfaces.values():
        if isinstance(node, LoopbackNode):
            addrs = node.config.ipv4 if af == IPV4 else node.config.ipv6
            if addrs:
                return addrs[0][0]
    for node in dev.interfaces.values():
        addrs = node.config.ipv4 if af == IPV4 else node.config.ipv6
        if addrs:
            return addrs[0][0]
    return 0


# ---------------------------------------------------------------------------
# derive_placement
# ---------------------------------------------------------------------------


def _wire_rates(
    result: ClassResult | SourceResult,
    payload_size: int,
    rate: float,
    weight: float = 1.0,
) -> tuple[dict[int, float], dict[int, float]]:
    """Scale each transmission's packet rate by its own frame length.

    When all transmissions have the same framing, aggregate the exact
    fractions first, preserving Gate A's floating-point arithmetic and bytes.
    """
    frames = {t.frame_bytes for t in result.transmissions}
    if len(frames) == 1:
        wire = frames.pop() / payload_size
        return (
            {e: float(fr) * rate * weight * wire for e, fr in result.edges},
            {e: float(fr) * rate * weight * wire for e, fr in result.carried},
        )
    offered: dict[int, float] = defaultdict(float)
    carried: dict[int, float] = defaultdict(float)
    for t in result.transmissions:
        wire = t.frame_bytes / payload_size
        offered[t.edge_id] += float(t.offered) * rate * weight * wire
        carried[t.edge_id] += float(t.carried) * rate * weight * wire
    return offered, carried


def derive_placement(
    state: NetworkState,
    model: int = UNCONSTRAINED,
    views: Any = None,
    detail: bool = True,
    *,
    fluid_step: _Step | None = None,
) -> NetworkState:
    """PLACEMENT derivation over the immutable root.

    An injected ``fluid_step`` bypasses cached class reuse: arbitrary external
    views have no dependency token. Production SR expansion in the default
    step must record its dependencies before enabling cached SR classes.
    """
    previous: PlacementReport | None = state.placement
    n, edge_links, caps = _edge_tables(state)
    offered = [0.0] * n
    carried = [0.0] * n
    dropped = [0.0] * n
    results: dict[str, DemandResult] = {}
    classes: dict[tuple, ClassResult] = {}
    by_reason: dict[str, float] = defaultdict(float)
    delivered_total = 0.0
    demands = sorted(state.demands.values(), key=lambda d: (-d.priority, d.id))
    if views is None:
        from netsim.model.network import _View

        def _default_views(d: str) -> Any:
            return _View(state, d)

        views = _default_views

    def account(
        demand: Demand,
        edges: dict[int, float],
        carried_edges: dict[int, float],
        delivered: float,
        drops: dict[tuple[str, str], float],
        attributed: bool = True,
        *,
        wire: tuple[dict[int, float], dict[int, float]],
        policy_outcomes: tuple = (),
    ) -> None:
        nonlocal delivered_total
        for e, r in wire[0].items():
            offered[e] += r
        for e, r in wire[1].items():
            carried[e] += r
        for (reason, where), r in drops.items():
            eid = edge_of_location(where)
            if eid is not None:
                dropped[eid] += r
            by_reason[reason] += r
        delivered_total += delivered
        results[demand.id] = DemandResult(
            demand.id,
            delivered,
            tuple(sorted((reason, where, r) for (reason, where), r in drops.items())),
            tuple(sorted(edges.items())) if detail and attributed else (),
            _policy_deliveries(policy_outcomes),
        )

    residual = list(caps) if model == LOSSY else None
    tokens: dict[str, DepToken] = {}
    if model == UNCONSTRAINED:
        groups: dict[tuple, list[Demand]] = defaultdict(list)
        for d in demands:
            if d.mode == FLUID:
                groups[d.class_key].append(d)
        for key in sorted(groups):
            members = groups[key]
            sources: dict[str, Fraction] = defaultdict(Fraction)
            exact_total = sum((Fraction(d.rate) for d in members), Fraction(0))
            total = float(exact_total)
            if exact_total <= 0:
                for d in members:
                    account(d, {}, {}, 0.0, {}, wire=({}, {}))
                continue
            for d in members:
                sources[d.source] += _share(d.rate, exact_total)
            cached = previous.classes.get(key) if previous is not None else None
            src_items = tuple(sorted(sources.items()))
            if (
                cached is not None
                and fluid_step is None
                and cached.cacheable
                and cached.sources == src_items
                and _deps_valid(previous, cached, state, tokens)
            ):
                walk = (
                    cached  # unchanged inputs: reuse (a pure rate change only rescales)
                )
            else:
                representative = members[0]
                walk = walk_class(
                    state,
                    key[0],
                    key[1],
                    key[3],
                    dict(sources),
                    template=representative.template,
                    steer=representative.steer,
                    dscp=representative.dscp,
                    step=fluid_step,
                )
            classes[key] = walk
            for dev in walk.visited:
                if dev not in tokens:
                    tokens[dev] = dep_token(state, dev)
            # Edge loads are linear, so the class walk gives them exactly.
            # Per-demand delivery is exact only per source: a class with
            # several sources that loses traffic is re-walked per source.
            if (
                len(sources) > 1
                and (walk.drops or walk.policy_outcomes)
                and not walk.per_source
            ):
                walk = dataclasses.replace(
                    walk,
                    per_source=tuple(
                        (src, _source_result(state, members[0], src, fluid_step))
                        for src in sorted(sources)
                    ),
                )
                classes[key] = walk
            if cached is not None and walk is not cached and walk == cached:
                # An uncached derivation can still produce identical content.
                # Reuse the record while retaining freshly checked dependencies.
                walk = classes[key] = cached
            per_source = dict(walk.per_source)
            single = len(sources) == 1
            for d in members:
                payload_size = (
                    payload_bytes(d.template) if d.template else d.payload_size
                )
                sr = per_source.get(d.source)
                if sr is not None:
                    account(
                        d,
                        {e: float(fr) * d.rate for e, fr in sr.edges},
                        {e: float(fr) * d.rate for e, fr in sr.carried},
                        float(sr.delivered) * d.rate,
                        {(r, w): float(fr) * d.rate for r, w, fr in sr.drops},
                        wire=_wire_rates(sr, payload_size, d.rate),
                        policy_outcomes=_scaled_outcomes(sr, d.rate),
                    )
                    continue
                f = d.rate / total
                account(
                    d,
                    {e: float(fr) * total * f for e, fr in walk.edges},
                    {e: float(fr) * total * f for e, fr in walk.carried},
                    float(walk.delivered) * total * f,
                    {(r, w): float(fr) * total * f for r, w, fr in walk.drops},
                    attributed=single,
                    wire=_wire_rates(walk, payload_size, total, f),
                    policy_outcomes=_scaled_outcomes(walk, d.rate),
                )
        for d in demands:
            if d.mode == HASH:
                wire = ({}, {})
                outcomes = {} if state.srv6_consumers or d.steer is not None else None
                o, c, dl, dr = walk_hash(
                    state, d, views, wire=wire, policy_outcomes=outcomes
                )
                account(
                    d,
                    o,
                    c,
                    dl,
                    dr,
                    wire=wire,
                    policy_outcomes=tuple(
                        (*k, v) for k, v in sorted((outcomes or {}).items())
                    ),
                )
    else:
        for d in demands:
            if d.mode == HASH:
                wire = ({}, {})
                outcomes = {} if state.srv6_consumers or d.steer is not None else None
                o, c, dl, dr = walk_hash(
                    state,
                    d,
                    views,
                    residual=residual,
                    wire=wire,
                    policy_outcomes=outcomes,
                )
                account(
                    d,
                    o,
                    c,
                    dl,
                    dr,
                    wire=wire,
                    policy_outcomes=tuple(
                        (*k, v) for k, v in sorted((outcomes or {}).items())
                    ),
                )
                continue
            walk = walk_class(
                state,
                d.af,
                d.dst,
                d.payload_size,
                {d.source: Fraction(1)},
                residual,
                d.rate,
                template=d.template,
                steer=d.steer,
                dscp=d.dscp,
                step=fluid_step,
            )
            account(
                d,
                {e: float(fr) * d.rate for e, fr in walk.edges},
                {e: float(fr) * d.rate for e, fr in walk.carried},
                float(walk.delivered) * d.rate,
                {(r, w): float(fr) * d.rate for r, w, fr in walk.drops},
                policy_outcomes=_scaled_outcomes(walk, d.rate),
                wire=_wire_rates(
                    walk,
                    payload_bytes(d.template) if d.template else d.payload_size,
                    d.rate,
                ),
            )
    version = (previous.version + 1) if previous is not None else 1
    report = PlacementReport(
        version,
        model,
        n,
        FloatArray.from_iterable(offered),
        FloatArray.from_iterable(carried),
        FloatArray.from_iterable(dropped),
        FloatArray.from_iterable(caps),
        PMap(sorted(edge_links.items())),
        PMap(sorted(results.items())),
        PMap(sorted(classes.items())),
        PMap(sorted(tokens.items())),
        delivered_total,
        PMap(sorted(by_reason.items())),
    )
    if previous is not None and _same_report(previous, report):
        if _same_cache(previous, report):
            return state
        # Same measurements, refreshed cache: keep version, carry the new
        # classes and dependency tokens so the next run can reuse them.
        return dataclasses.replace(
            state,
            placement=dataclasses.replace(
                previous, classes=report.classes, deps=report.deps
            ),
        )
    return dataclasses.replace(state, placement=report)


def _same_cache(a: PlacementReport, b: PlacementReport) -> bool:
    if len(a.classes) != len(b.classes) or len(a.deps) != len(b.deps):
        return False
    if diff_pmap(a.classes, b.classes, by_identity=True):
        return False
    for k in diff_pmap(a.deps, b.deps, by_identity=True).keys:
        old = a.deps.get(k)
        new = b.deps.get(k)
        if old is None or new is None or not old.same(new):
            return False
    return True


def _source_result(
    state: NetworkState,
    demand: Demand,
    source: str,
    step: _Step | None = None,
) -> SourceResult:
    w = walk_class(
        state,
        demand.af,
        demand.dst,
        demand.payload_size,
        {source: Fraction(1)},
        template=demand.template,
        steer=demand.steer,
        dscp=demand.dscp,
        step=step,
    )
    return SourceResult(
        w.edges,
        w.carried,
        w.delivered,
        w.drops,
        policy_outcomes=w.policy_outcomes,
        transmissions=w.transmissions,
    )


def _deps_valid(
    previous: PlacementReport | None,
    cached: ClassResult,
    state: NetworkState,
    tokens: dict[str, DepToken],
) -> bool:
    if previous is None:
        return False
    for d in cached.visited:
        old = previous.deps.get(d)
        if old is None or d not in state.devices:
            return False
        current = tokens.get(d)
        if current is None:
            current = tokens[d] = dep_token(state, d)
        if not old.same(current):
            return False
    return True


def _same_report(a: PlacementReport, b: PlacementReport) -> bool:
    return (
        a.model == b.model
        and a.edge_count == b.edge_count
        and a.offered == b.offered
        and a.carried == b.carried
        and a.dropped == b.dropped
        and a.capacity == b.capacity
        and a.edge_links == b.edge_links
        and a.demands == b.demands
        and a.delivered_total == b.delivered_total
        and a.dropped_by_reason == b.dropped_by_reason
    )


__all__ = [
    'Demand',
    'DemandResult',
    'PlacementReport',
    'ClassResult',
    'SourceResult',
    'FLUID',
    'HASH',
    'UNCONSTRAINED',
    'LOSSY',
    'derive_placement',
    'make_demand',
    'walk_class',
    'walk_hash',
    'ip_bytes',
    'edge_of_location',
    'BalancerKind',
]


def _policy_deliveries(outcomes: tuple) -> tuple[PolicyDelivery, ...]:
    grouped: dict[tuple[str, int, int], list[tuple[str, str, float]]] = defaultdict(
        list
    )
    for device, color, endpoint, reason, where, rate in outcomes:
        grouped[(device, color, endpoint)].append((reason, where, float(rate)))
    return tuple(
        PolicyDelivery(
            *key,
            sum(rate for reason, _, rate in values if reason == fw.DELIVER),
            tuple(
                (reason, where, rate)
                for reason, where, rate in values
                if reason != fw.DELIVER
            ),
        )
        for key, values in sorted(grouped.items())
    )


def _scaled_outcomes(walk: ClassResult | SourceResult, rate: float) -> tuple:
    return tuple(
        (d, c, e, r, w, float(f) * rate) for d, c, e, r, w, f in walk.policy_outcomes
    )


def _packet_token(packet: Any) -> tuple:
    """Full FLUID header continuation, omitting only TTL/hop limit."""
    payload = packet.payload
    tail = (
        (1, payload.sport, payload.dport)
        if isinstance(payload, L4Header)
        else (2, _packet_token(payload))
        if isinstance(payload, (IPv4Packet, IPv6Packet))
        else (0,)
    )
    if isinstance(packet, IPv4Packet):
        return (
            4,
            packet.src,
            packet.dst,
            packet.protocol,
            packet.dscp,
            packet.ecn,
            packet.payload_size,
            tail,
        )
    srh = packet.srh
    header = (
        (
            srh.entries,
            srh.segments_left,
            srh.last_entry,
            srh.flags,
            srh.tag,
            srh.next_header,
        )
        if srh
        else ()
    )
    return (
        6,
        packet.src,
        packet.dst,
        packet.next_header,
        packet.traffic_class,
        packet.flow_label,
        packet.payload_size,
        header,
        tail,
    )


def _packet_from_token(token: tuple):
    tail = token[-1]
    payload = (
        L4Header(tail[1], tail[2])
        if tail[0] == 1
        else _packet_from_token(tail[1])
        if tail[0] == 2
        else None
    )
    if token[0] == 4:
        return IPv4Packet(
            token[1],
            token[2],
            token[3],
            ttl=255,
            dscp=token[4],
            ecn=token[5],
            payload_size=token[6],
            payload=payload,
        )
    return IPv6Packet(
        token[1],
        token[2],
        token[3],
        hop_limit=255,
        traffic_class=token[4],
        flow_label=token[5],
        payload_size=token[6],
        srh=SRH(*token[7]) if token[7] else None,
        payload=payload,
    )


def _state_from_packet(
    packet: Any, stage: int, policies: tuple[tuple[str, int, int], ...] = ()
) -> _PacketState:
    inner = packet
    while isinstance(inner.payload, (IPv4Packet, IPv6Packet)):
        inner = inner.payload
    af = 4 if isinstance(inner, IPv4Packet) else 6
    outer = inner is not packet
    srh = packet.srh if isinstance(packet, IPv6Packet) else None
    return _PacketState(
        af,
        outer,
        len(srh.entries) if srh else 0,
        packet.dst,
        (),
        stage,
        policies,
        _packet_token(packet),
    )
