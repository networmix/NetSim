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
from typing import Any, Iterator

from netsim.model import derive
from netsim.model import forwarding as fw
from netsim.model.addressing import IPV4, to_int
from netsim.model.hashing import BalancerKind
from netsim.model.interfaces import AdminState, OperState, PortChannelNode, l3_usable
from netsim.model.links import transfer_blocked
from netsim.model.packets import (
    ETHERNET_HEADER,
    ETHERTYPE_IPV4,
    ETHERTYPE_IPV6,
    IPV4_HEADER,
    IPV6_HEADER,
    ORIGINATED,
    EthernetFrame,
    IPv4Packet,
    PacketTemplate,
    ip_bytes,
)
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

    def __post_init__(self) -> None:
        if self.rate < 0 or self.payload_size <= 0 or self.flows < 1:
            raise ValueError('demand needs rate >= 0, payload_size > 0, flows >= 1')

    @property
    def class_key(self) -> tuple:
        return (self.af, self.dst, self.dscp, self.payload_size)


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


@record
class _Edge:
    edge_id: int
    peer: str
    fraction: Fraction
    blocked: str | None
    """Physical or MTU reason, or ``None``."""


def _egress_edges(
    state: NetworkState, device: str, af: int, dst: int, ip_len: int
) -> tuple[str, Any, list[_Edge], list[tuple[str, Fraction]]]:
    """FIB decision at *device* for *dst*: ``(action, entry, edges, node_drops)``."""
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
                _Edge(link.edge_id((device, member)), peer[0], member_share, blocked)
            )
    return fw.TRANSMIT, entry, edges, drops


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


def _tarjan(nodes: list[str], succ: dict[str, list[str]]) -> list[list[str]]:
    """Strongly connected components in reverse topological order (Tarjan),
    with an explicit stack so a forwarding chain of any length works."""
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    on_stack: set[str] = set()
    stack: list[str] = []
    comps: list[list[str]] = []
    counter = 0
    for root in nodes:
        if root in index:
            continue
        index[root] = low[root] = counter
        counter += 1
        stack.append(root)
        on_stack.add(root)
        work: list[tuple[str, Iterator[str]]] = [(root, iter(succ.get(root, ())))]
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
) -> ClassResult:
    """Walk one class from *sources* (device → fraction of a unit inflow).

    With ``residual`` (LOSSY), edge shares are clipped against the residual
    wire capacity scaled by *scale* (the demand's payload rate) and the
    residual is decremented in place.
    """
    ip_len = (IPV4_HEADER if af == IPV4 else IPV6_HEADER) + payload_size
    wire_per_payload = _frame_bytes(af, payload_size) / payload_size
    decisions: dict[str, tuple[str, Any, list[_Edge], list[tuple[str, Fraction]]]] = {}
    succ: dict[str, list[str]] = {}
    order: list[str] = []
    frontier = sorted(sources)
    seen: set[str] = set()
    while frontier:
        d = frontier.pop()
        if d in seen or d not in state.devices:
            continue
        seen.add(d)
        order.append(d)
        dec = _egress_edges(state, d, af, dst, ip_len)
        decisions[d] = dec
        nxt = sorted({e.peer for e in dec[2] if e.blocked is None})
        succ[d] = nxt
        frontier.extend(n for n in nxt if n not in seen)
    comps = _tarjan(sorted(order), succ)
    comp_of: dict[str, int] = {}
    cyclic: set[int] = set()
    for i, comp in enumerate(comps):
        for v in comp:
            comp_of[v] = i
        if len(comp) > 1 or (comp and comp[0] in succ.get(comp[0], ())):
            cyclic.add(i)
    inflow: dict[str, Fraction] = defaultdict(Fraction)
    for s, f in sources.items():
        inflow[s] += f
    edges: dict[int, Fraction] = defaultdict(Fraction)
    carried: dict[int, Fraction] = defaultdict(Fraction)
    delivered = Fraction(0)
    drops: dict[tuple[str, str], Fraction] = defaultdict(Fraction)
    # Tarjan yields components in reverse topological order: process from the last.
    for ci in range(len(comps) - 1, -1, -1):
        for v in comps[ci]:
            q = inflow.get(v, Fraction(0))
            if q == 0:
                continue
            action, _entry, egress, node_drops = decisions[v]
            if action == fw.DELIVER:
                delivered += q
                continue
            for reason, f in node_drops:
                drops[(reason, v)] += q * f
            if action == fw.DROP:
                continue
            for e in egress:
                share = q * e.fraction
                edges[e.edge_id] += share
                if e.blocked is not None:
                    drops[(e.blocked, _edge_loc(e.edge_id))] += share
                    continue
                if comp_of.get(e.peer) == ci and ci in cyclic:
                    drops[(fw.LOOP, _edge_loc(e.edge_id))] += share
                    continue
                if residual is not None:
                    # payload bit/s -> wire bit/s: frame bytes per payload byte
                    wire_cap = residual[e.edge_id]
                    attempted = float(share) * scale * wire_per_payload
                    if attempted > wire_cap:
                        carried_share = (
                            _bounded_fraction(
                                max(wire_cap, 0.0) / (scale * wire_per_payload)
                            )
                            if scale > 0
                            else Fraction(0)
                        )
                        carried_share = min(carried_share, share)
                        drops[(fw.CONGESTION, _edge_loc(e.edge_id))] += (
                            share - carried_share
                        )
                        residual[e.edge_id] = 0.0
                    else:
                        carried_share = share
                        residual[e.edge_id] = wire_cap - attempted
                else:
                    carried_share = share
                carried[e.edge_id] += carried_share
                inflow[e.peer] += carried_share
    return ClassResult(
        tuple(sorted(edges.items())),
        tuple(sorted(carried.items())),
        delivered,
        tuple(sorted((r, w, f) for (r, w), f in drops.items())),
        tuple(sorted(order)),
        tuple(sorted(sources.items())),
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
) -> tuple[dict[int, float], dict[int, float], float, dict[tuple[str, str], float]]:
    """Per microflow forward-step execution; returns edge offered/carried
    payload rates, delivered payload rate and drops. With ``residual``
    (LOSSY) each hop is clipped against the remaining wire capacity."""
    template = demand.template or PacketTemplate(
        demand.af,
        _source_address(state, demand.source, demand.af),
        demand.dst,
        dscp=demand.dscp,
        payload_size=demand.payload_size,
    )
    per_flow = demand.rate / demand.flows
    wire_per_payload = (
        _frame_bytes(demand.af, demand.payload_size) / demand.payload_size
    )
    offered: dict[int, float] = defaultdict(float)
    carried: dict[int, float] = defaultdict(float)
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
        for _ in range(max_hops):
            view = views(current)
            if frame is None:
                res = fw.forward_ip(view, packet, None, ORIGINATED)
            else:
                assert ingress is not None
                res = fw.receive_frame(view, ingress, frame)
            where = current
            if res.edge_id is not None:
                offered[res.edge_id] += rate
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
                    residual[res.edge_id] = 0.0
                    rate = fits
                else:
                    residual[res.edge_id] = wire_cap - attempted
            carried[res.edge_id] += rate
            packet = res.packet
            ethertype = (
                ETHERTYPE_IPV4 if isinstance(packet, IPv4Packet) else ETHERTYPE_IPV6
            )
            frame = EthernetFrame(res.mac_dst or 0, res.mac_src or 0, ethertype, packet)
            current, ingress = res.peer
        if outcome == fw.DELIVER:
            delivered += rate
        else:
            drops[(reason or fw.LOOP, where)] += rate
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


def derive_placement(
    state: NetworkState,
    model: int = UNCONSTRAINED,
    views: Any = None,
    detail: bool = True,
) -> NetworkState:
    """PLACEMENT derivation: recompute the report from FIBs, oper state and demands."""
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
    ) -> None:
        nonlocal delivered_total
        frame = _frame_bytes(demand.af, demand.payload_size)
        wire = 8 * frame / (8 * demand.payload_size)
        for e, r in edges.items():
            offered[e] += r * wire
        for e, r in carried_edges.items():
            carried[e] += r * wire
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
                    account(d, {}, {}, 0.0, {})
                continue
            for d in members:
                sources[d.source] += _share(d.rate, exact_total)
            cached = previous.classes.get(key) if previous is not None else None
            src_items = tuple(sorted(sources.items()))
            if (
                cached is not None
                and cached.sources == src_items
                and _deps_valid(previous, cached, state, tokens)
            ):
                walk = (
                    cached  # unchanged inputs: reuse (a pure rate change only rescales)
                )
            else:
                walk = walk_class(state, key[0], key[1], key[3], dict(sources))
            classes[key] = walk
            for dev in walk.visited:
                if dev not in tokens:
                    tokens[dev] = dep_token(state, dev)
            # Edge loads are linear, so the class walk gives them exactly.
            # Per-demand delivery is exact only per source: a class with
            # several sources that loses traffic is re-walked per source.
            if len(sources) > 1 and walk.drops and not walk.per_source:
                walk = dataclasses.replace(
                    walk,
                    per_source=tuple(
                        (src, _source_result(state, key[0], key[1], key[3], src))
                        for src in sorted(sources)
                    ),
                )
                classes[key] = walk
            per_source = dict(walk.per_source)
            single = len(sources) == 1
            for d in members:
                sr = per_source.get(d.source)
                if sr is not None:
                    account(
                        d,
                        {e: float(fr) * d.rate for e, fr in sr.edges},
                        {e: float(fr) * d.rate for e, fr in sr.carried},
                        float(sr.delivered) * d.rate,
                        {(r, w): float(fr) * d.rate for r, w, fr in sr.drops},
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
                )
        for d in demands:
            if d.mode == HASH:
                o, c, dl, dr = walk_hash(state, d, views)
                account(d, o, c, dl, dr)
    else:
        for d in demands:
            if d.mode == HASH:
                o, c, dl, dr = walk_hash(state, d, views, residual=residual)
                account(d, o, c, dl, dr)
                continue
            walk = walk_class(
                state,
                d.af,
                d.dst,
                d.payload_size,
                {d.source: Fraction(1)},
                residual,
                d.rate,
            )
            account(
                d,
                {e: float(fr) * d.rate for e, fr in walk.edges},
                {e: float(fr) * d.rate for e, fr in walk.carried},
                float(walk.delivered) * d.rate,
                {(r, w): float(fr) * d.rate for r, w, fr in walk.drops},
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
    state: NetworkState, af: int, dst: int, payload_size: int, source: str
) -> SourceResult:
    w = walk_class(state, af, dst, payload_size, {source: Fraction(1)})
    return SourceResult(w.edges, w.carried, w.delivered, w.drops)


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
