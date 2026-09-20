"""RIB rows, resolution policy and the resolver that produces a FIB.

Rows are keyed by ``(prefix, source client, distinguisher)`` and sharded
by prefix length. Selection happens inside the resolver (zebra
``rib_process``): groups of equal (distance, metric, source) are visited
best first; a row whose next-hops all fail to resolve does not shadow a
lower-ranked row. Resolution is recursive (RFC 8430 next-hop chains),
never through the prefix being resolved (gray stack), with on-link
substitution for connected routes and exact weight flattening.
"""

from __future__ import annotations

from dataclasses import field, replace
from fractions import Fraction
from math import lcm
from typing import Any, Iterator, Protocol

from netsim.model.addressing import IPV4, IPV6, MacAddress, is_link_local_v6, mask_for
from netsim.model.contracts import CONNECTED, LOCAL, ClientId, ClientProfile
from netsim.model.forwarding import (
    CROSS_CONNECT,
    DECAP_LOOKUP,
    DROP_ACTIONS,
    DROP_BLACKHOLE,
    DROP_PROHIBIT,
    DROP_UNREACHABLE,
    FORWARD,
    RECEIVE,
    RELOOKUP,
    SRV6_LOCAL,
    Adjacency,
    DependsOn,
    Fib,
    FibEntry,
    NexthopGroup,
    Prefix,
    local_sid,
)
from netsim.model.lpm import FrozenPrefixTable, PrefixTable
from netsim.model.packets import IPv4Packet, IPv6Packet, encapsulate
from netsim.model.srv6 import (
    FALLBACK_DROP,
    LocalSid,
    PolicyRef,
    Srv6Encap,
    policy_programs,
)
from netsim.model.state import PMap, PMapBuilder, empty_pmap, record

# Special next-hops (RFC 8349 special-next-hop plus SRv6 local behaviours).
BLACKHOLE = 1
UNREACHABLE = 2
PROHIBIT = 3
RECEIVE_NH = 4
SRV6_LOCAL_NH = 5

_SPECIAL_ACTION = {
    BLACKHOLE: DROP_BLACKHOLE,
    UNREACHABLE: DROP_UNREACHABLE,
    PROHIBIT: DROP_PROHIBIT,
    RECEIVE_NH: RECEIVE,
    SRV6_LOCAL_NH: SRV6_LOCAL,
}


@record
class Nexthop:
    """Exactly one legal shape: interface only; interface + address;
    address only (recursive); special; (Gate B) srv6 encap; policy."""

    interface: str | None = None
    address: int | None = None
    af: int | None = None
    """Family of ``address``; required with an address."""
    special: int | None = None
    srv6: Any = None
    policy: Any = None
    weight: int = 1
    onlink: bool = False
    behavior: Any = None
    """SRV6_LOCAL behaviour record (Gate B)."""

    def __post_init__(self) -> None:
        if self.weight < 1:
            raise ValueError('next-hop weight must be >= 1')
        shapes = sum(
            (
                self.special is not None,
                self.srv6 is not None,
                self.policy is not None,
                self.interface is not None or self.address is not None,
            )
        )
        if shapes != 1:
            raise ValueError(f'next-hop must have exactly one shape: {self!r}')
        if self.address is not None and self.af not in (IPV4, IPV6):
            raise ValueError('an address next-hop needs af')

    @classmethod
    def via(
        cls,
        interface: str,
        address: int | None = None,
        af: int | None = None,
        **kw: Any,
    ) -> Nexthop:
        return cls(interface=interface, address=address, af=af, **kw)

    @classmethod
    def recursive(cls, address: int, af: int, **kw: Any) -> Nexthop:
        return cls(address=address, af=af, **kw)

    @classmethod
    def blackhole(cls) -> Nexthop:
        return cls(special=BLACKHOLE)

    @classmethod
    def unreachable(cls) -> Nexthop:
        return cls(special=UNREACHABLE)

    @classmethod
    def receive(cls) -> Nexthop:
        return cls(special=RECEIVE_NH)

    @property
    def is_recursive(self) -> bool:
        return self.interface is None and self.address is not None


RowKey = tuple[int, int, ClientId, tuple[Any, ...]]
"""``(network_int, prefix_len, source, distinguisher)``."""


@record
class Route:
    prefix: Prefix
    af: int
    source: ClientId
    distance: int
    nexthops: tuple[Nexthop, ...]
    metric: int = 0
    distinguisher: tuple[Any, ...] = ()
    tag: int | None = None
    attrs: tuple[tuple[str, Any], ...] = ()
    table: int = 0

    @property
    def key(self) -> RowKey:
        return (self.prefix[0], self.prefix[1], self.source, self.distinguisher)

    def __post_init__(self) -> None:
        net, plen = self.prefix
        bits = 32 if self.af == IPV4 else 128
        if not 0 <= plen <= bits or net & ~mask_for(plen, bits):
            raise ValueError(f'invalid prefix {net:#x}/{plen} for af {self.af}')
        if not self.nexthops:
            raise ValueError('a route needs at least one next-hop')


# ---------------------------------------------------------------------------
# RibState
# ---------------------------------------------------------------------------


@record
class RibState:
    """Rows sharded by prefix length, indexed by prefix and source client.

    Prefix tuples are ranked for resolution. Client rows and per-length
    prefix indexes use persistent maps so small edits copy only touched
    map shards. Both indexes share the immutable rows in ``shards``.
    """

    af: int
    version: int = 0
    shards: PMap[int, PMap[RowKey, Route]] = field(default_factory=empty_pmap)
    prefixes: FrozenPrefixTable[tuple[Route, ...]] = field(
        default_factory=lambda: PrefixTable(32).freeze()
    )
    clients: PMap[ClientId, PMap[RowKey, Route]] = field(default_factory=empty_pmap)

    link_state_sources: frozenset[ClientId] = field(default_factory=frozenset)
    """Immutable metric provenance copied from client profiles by rib_apply."""

    @classmethod
    def empty(cls, af: int) -> RibState:
        return cls(af=af, prefixes=PrefixTable(32 if af == IPV4 else 128).freeze())

    def rows(self, prefix: Prefix) -> tuple[Route, ...]:
        """Candidates in rank order, with no scan or sort at read time."""
        return self.prefixes.get(*prefix, ())

    def rows_of(self, client: ClientId) -> tuple[Route, ...]:
        """Materialize one client's rows in key order; mutations need no sort."""
        rows = self.clients.get(client)
        return () if rows is None else tuple(r for _, r in rows.sorted_items())

    def candidates(self, prefix: Prefix) -> tuple[Route, ...]:
        return self.rows(prefix)

    def best_groups(self, prefix: Prefix) -> Iterator[tuple[Route, ...]]:
        """Groups of equal (distance, metric, source), best first."""
        group: list[Route] = []
        for r in self.candidates(prefix):
            if group and (group[0].distance, group[0].metric, group[0].source) != (
                r.distance,
                r.metric,
                r.source,
            ):
                yield tuple(group)
                group = []
            group.append(r)
        if group:
            yield tuple(group)

    def all_prefixes(self) -> list[Prefix]:
        return [(net, plen) for net, plen, _ in self.prefixes.items()]

    def __len__(self) -> int:
        return sum(len(s) for s in self.shards.values())


def _rank(r: Route) -> tuple[int, int, str, int, tuple[Any, ...]]:
    return (r.distance, r.metric, r.source.name, r.source.instance, r.distinguisher)


def rib_apply(
    rib: RibState,
    *,
    add: tuple[Route, ...] = (),
    delete: tuple[RowKey, ...] = (),
    sync: tuple[ClientId, tuple[Route, ...]] | None = None,
    profile: ClientProfile | None = None,
) -> RibState:
    """Pure RIB mutation; returns the same object when nothing changed.

    ``sync`` replaces every row of the client (all distinguishers) with the
    given rows in one step, reusing unchanged row objects. ``delete``
    ignores missing keys. ``add`` upserts by key and is a no-op for an
    identical row.
    """
    shard_builders: dict[int, PMapBuilder[RowKey, Route]] = {}
    prefix_rows: dict[Prefix, dict[RowKey, Route]] = {}
    client_builders: dict[ClientId, PMapBuilder[RowKey, Route]] = {}

    def shard(plen: int) -> PMapBuilder[RowKey, Route]:
        b = shard_builders.get(plen)
        if b is None:
            b = shard_builders[plen] = rib.shards.get(plen, PMap()).builder()
        return b

    def by_prefix(prefix: Prefix) -> dict[RowKey, Route]:
        rows = prefix_rows.get(prefix)
        if rows is None:
            rows = prefix_rows[prefix] = {r.key: r for r in rib.rows(prefix)}
        return rows

    def by_client(client: ClientId) -> PMapBuilder[RowKey, Route]:
        b = client_builders.get(client)
        if b is None:
            b = client_builders[client] = rib.clients.get(client, PMap()).builder()
        return b

    def put(route: Route) -> None:
        if route.af != rib.af:
            raise ValueError('route family does not match the RIB')
        b = shard(route.prefix[1])
        key = route.key
        old = b.get(key)
        if old == route:
            return
        b.set(key, route)
        by_prefix(route.prefix)[key] = route
        by_client(route.source).set(key, route)

    def drop(key: RowKey) -> None:
        b = shard(key[1])
        if key in b:
            b.remove(key)
            del by_prefix((key[0], key[1]))[key]
            by_client(key[2]).remove(key)

    if sync is not None:
        client, rows = sync
        wanted = {r.key: r for r in rows}
        for key in rib.clients.get(client, PMap()):
            if key not in wanted:
                drop(key)
        for r in rows:
            if r.source != client:
                raise ValueError('sync rows must belong to the syncing client')
            put(r)
    for key in delete:
        drop(key)
    for r in add:
        put(r)
    sources = rib.link_state_sources
    if profile is not None:
        sources = (
            sources | {profile.client}
            if profile.link_state
            else sources - {profile.client}
        )
    if not prefix_rows:
        return (
            rib
            if sources == rib.link_state_sources
            else replace(rib, link_state_sources=frozenset(sources))
        )
    sb = rib.shards.builder()
    for plen, b in shard_builders.items():
        new = b.build()
        if len(new) == 0:
            sb.remove(plen)
        else:
            sb.set(plen, new)
    # The outer length table is bounded by the address width. Each inner
    # index is a PMap: edit its touched shards, not the complete length table.
    tables = dict(rib.prefixes._tables)
    prefix_builders: dict[int, PMapBuilder[int, tuple[Route, ...]]] = {}
    for (net, plen), rows in prefix_rows.items():
        b = prefix_builders.get(plen)
        if b is None:
            table = tables.get(plen)
            base = table if isinstance(table, PMap) else PMap(table)
            b = prefix_builders[plen] = base.builder()
        if rows:
            b.set(net, tuple(sorted(rows.values(), key=_rank)))
        else:
            b.remove(net)
    for plen, b in prefix_builders.items():
        table = b.build()
        if table:
            tables[plen] = table
        else:
            tables.pop(plen, None)
    prefixes = FrozenPrefixTable._owned(rib.prefixes.bits, tables, rib.prefixes._masks)
    cb = rib.clients.builder()
    for client, b in client_builders.items():
        rows = b.build()
        if rows:
            cb.set(client, rows)
        else:
            cb.remove(client)
    return RibState(
        rib.af, rib.version + 1, sb.build(), prefixes, cb.build(), frozenset(sources)
    )


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


@record
class ResolutionPolicy:
    resolve_via_default: bool = False
    all_or_nothing: bool = False
    max_recursion_depth: int = 8
    max_ecmp_paths: int = 64
    lpm_fallthrough: bool = True
    resolve_via_drop: bool = False
    validate_all_sids: bool = True
    prefer_installed: bool = False


class ResolutionContext(Protocol):
    """What the resolver may ask about the device (implemented by ``Device``)."""

    def interface_exists(self, name: str) -> bool: ...

    def l3_usable(self, name: str, af: int) -> bool: ...

    def neighbor_mac(self, interface: str, address: int) -> int | None: ...

    def peer_mac(self, interface: str) -> int | None:
        """MAC of the point-to-point peer for interface-only next-hops."""
        ...

    def rib(self, af: int) -> RibState: ...


NOT_INSTALLED = 0
INSTALLED = 1

# Rejection reasons
UNRESOLVED = 'UNRESOLVED'
AMBIGUOUS_ACTION = 'AMBIGUOUS_ACTION'
SHADOWED = 'SHADOWED'
LOOP_DETECTED = 'LOOP_DETECTED'
TOO_DEEP = 'TOO_DEEP'


@record
class RowOutcome:
    key: RowKey
    status: int
    reason: str | None = None
    group_id: int | None = None


@record
class ResolverOutcome:
    af: int
    processed_epoch: int
    rows: PMap[RowKey, RowOutcome] = field(default_factory=empty_pmap)


@record
class _Leg:
    interface: str
    nexthop: int | None
    mac: int | None
    share: Fraction
    encap: Any = None
    af: int | None = None
    """Family of ``nexthop`` (RFC 8950: it may differ from the route's)."""


class _Resolver:
    def __init__(self, ctx: ResolutionContext, policy: ResolutionPolicy) -> None:
        self.ctx = ctx
        self.policy = policy
        dev = getattr(ctx, 'dev', None)
        table = getattr(dev, 'srv6_policies', None)
        self.programs = policy_programs(table)
        self.bsids = table.bsids if table else PMap()
        self.memo: dict[
            tuple[int, Prefix], tuple[FibEntry | None, tuple[_Leg, ...]]
        ] = {}
        self.stack: list[tuple[int, Prefix]] = []
        # One dependency frame per prefix being resolved: a frame collects
        # what *this* resolution consulted (its own lookups plus, merged in,
        # those of the children it recursed through), so an entry's
        # ``depends_on`` is exact and the total stays linear in the table.
        self.frames: list[_Frame] = []
        self.memo_deps: dict[tuple[int, Prefix], DependsOn] = {}
        self.row_outcomes: dict[RowKey, RowOutcome] = {}

    # -- entry points --------------------------------------------------------

    def resolve_prefix(
        self, af: int, prefix: Prefix
    ) -> tuple[FibEntry | None, tuple[_Leg, ...]]:
        key = (af, prefix)
        if key in self.memo:
            if self.frames:
                self.frames[-1].merge(self.memo_deps[key])
            return self.memo[key]
        if key in self.stack:
            return None, ()
        if len(self.stack) >= self.policy.max_recursion_depth:
            return None, ()
        self.stack.append(key)
        frame = _Frame()
        self.frames.append(frame)
        try:
            result = self._resolve_prefix(af, prefix)
        finally:
            self.stack.pop()
            self.frames.pop()
        deps = frame.freeze()
        self.memo[key] = result
        self.memo_deps[key] = deps
        if self.frames:
            self.frames[-1].merge(deps)
        return result

    def _resolve_prefix(
        self, af: int, prefix: Prefix
    ) -> tuple[FibEntry | None, tuple[_Leg, ...]]:
        self.frames[-1].prefixes.add((af, prefix[0], prefix[1]))
        rejected: list[tuple[RowKey, str]] = []
        for group in self._groups(af, prefix):
            entry, legs, reason = self._resolve_group(af, prefix, group)
            if entry is not None:
                for r in group:
                    self.row_outcomes[r.key] = RowOutcome(
                        r.key, INSTALLED, None, entry.group_id
                    )
                for k, why in rejected:
                    self.row_outcomes[k] = RowOutcome(k, NOT_INSTALLED, why)
                return entry, legs
            for r in group:
                rejected.append((r.key, reason or UNRESOLVED))
        for k, why in rejected:
            self.row_outcomes[k] = RowOutcome(k, NOT_INSTALLED, why)
        return None, ()

    def _groups(self, af: int, prefix: Prefix) -> Iterator[tuple[Route, ...]]:
        return self.ctx.rib(af).best_groups(prefix)

    def _lookup(
        self, af: int, address: int
    ) -> Iterator[tuple[int, int, tuple[Route, ...]]]:
        return self.ctx.rib(af).prefixes.lookup_iter(
            address,
            min_len=0 if self.policy.resolve_via_default else 1,
            exclude=lambda net, plen: (af, (net, plen)) in self.stack,
        )

    def _resolve_group(
        self, af: int, prefix: Prefix, group: tuple[Route, ...]
    ) -> tuple[FibEntry | None, tuple[_Leg, ...], str | None]:
        nexthops = sorted({nh for r in group for nh in r.nexthops}, key=_nh_sort_key)
        specials = {nh.special for nh in nexthops if nh.special is not None}
        contributing = tuple(sorted(r.key for r in group))
        if specials:
            if len(specials) > 1:
                return None, (), AMBIGUOUS_ACTION
            action = _SPECIAL_ACTION[next(iter(specials))]
            sid = None
            if action == SRV6_LOCAL:
                sids = {nh.behavior for nh in nexthops if nh.special == SRV6_LOCAL_NH}
                if len(sids) != 1 or not isinstance(next(iter(sids)), LocalSid):
                    return None, (), AMBIGUOUS_ACTION
                sid = next(iter(sids))
            entry = FibEntry(prefix, action, None, contributing, self._deps(), sid=sid)
            return entry, (), None
        # A down DROP policy is a programmed discard, not an unresolved route
        # that falls through to a lower-ranked IGP row (RFC 9256 section 8.2).
        for nh in nexthops:
            key = self._policy_key(nh)
            if key is not None:
                program = self.programs.get(key)
                if (
                    program is None
                    or not program.lists
                    and program.fallback == FALLBACK_DROP
                ):
                    return (
                        FibEntry(
                            prefix,
                            DROP_UNREACHABLE,
                            contributing=contributing,
                            depends_on=self._deps(),
                            program=PolicyRef(*key),
                        ),
                        (),
                        None,
                    )
        legs: list[_Leg] = []
        unresolved = 0
        total_weight = sum(nh.weight for nh in nexthops)
        for nh in nexthops:
            resolved = self._resolve_nexthop(af, nh)
            if not resolved:
                unresolved += 1
                continue
            for leg in resolved:
                legs.append(
                    _Leg(
                        leg.interface,
                        leg.nexthop,
                        leg.mac,
                        leg.share * Fraction(nh.weight, total_weight),
                        leg.encap,
                        leg.af,
                    )
                )
        if not legs or (unresolved and self.policy.all_or_nothing):
            return None, (), UNRESOLVED
        legs_t = _merge(legs)
        return FibEntry(prefix, FORWARD, None, contributing, self._deps()), legs_t, None

    def _policy_key(self, nh: Nexthop) -> tuple[int, int] | None:
        if isinstance(nh.policy, PolicyRef):
            return nh.policy.color, nh.policy.endpoint
        return (
            self.bsids.get(nh.address)
            if nh.af == IPV6 and nh.address is not None
            else None
        )

    def _resolve_nexthop(self, af: int, nh: Nexthop) -> tuple[_Leg, ...]:
        ctx = self.ctx
        key = self._policy_key(nh)
        if key is not None:
            program = self.programs.get(key)
            if program is None or not program.lists:
                return ()  # IGP fallback excludes this policy-bearing next hop.
            total = sum(weight for _, weight in program.lists)
            result = []
            for encap, weight in program.lists:
                for leg in self._resolve_nexthop(af, Nexthop(srv6=encap)):
                    result.append(
                        _Leg(
                            leg.interface,
                            leg.nexthop,
                            leg.mac,
                            leg.share * Fraction(weight, total),
                            encap,
                            leg.af,
                        )
                    )
            return tuple(result)
        if isinstance(nh.srv6, Srv6Encap):
            try:
                packet = encapsulate(
                    # The compiler must retain the inner family through local
                    # decapsulation (RFC 8986 sections 4.8 and 4.16.3).
                    IPv4Packet(0, 0, 17) if af == IPV4 else IPv6Packet(0, 0, 17),
                    nh.srv6.entries,
                    behavior=nh.srv6.behavior,
                    source=0,
                    hop_limit=255,
                    flow_label=0,
                    transit=False,
                )
            except ValueError:
                return ()
            return tuple(
                _Leg(leg.interface, leg.nexthop, leg.mac, leg.share, nh.srv6, leg.af)
                for leg in self._resolve_outer(packet, set())
                if leg.encap is None  # Gate B rejects nested encapsulation.
            )
        if nh.interface is not None:
            self.frames[-1].interfaces.add(nh.interface)
            if not ctx.interface_exists(nh.interface) or not ctx.l3_usable(
                nh.interface, af
            ):
                return ()
            if nh.address is None:
                mac = ctx.peer_mac(nh.interface)
                if mac is None:
                    return ()
                return (_Leg(nh.interface, None, mac, Fraction(1)),)
            mac = ctx.neighbor_mac(nh.interface, nh.address)
            if (
                mac is None
                and af == IPV4
                and nh.af == IPV6
                and is_link_local_v6(nh.address)
            ):
                # RFC 8950 on IPv4-only links: L3 already exposes the scoped
                # physical peer MAC, even without IPv6 forwarding/ND entries.
                # Only that peer's exact EUI-64 identity may resolve this way.
                peer = ctx.peer_mac(nh.interface)
                if peer is not None and MacAddress(peer).link_local_int() == nh.address:
                    mac = peer
            if mac is None:
                return ()
            return (_Leg(nh.interface, nh.address, mac, Fraction(1), af=nh.af or af),)
        if nh.address is not None:
            assert nh.af is not None
            return self._resolve_recursive(nh.af, nh.address)
        return ()

    def _resolve_outer(
        self,
        packet: IPv6Packet,
        visited: set[tuple[int, int]],
    ) -> tuple[_Leg, ...]:
        """Resolve the first wire entry, executing this device's local SIDs.

        The same pure local_sid function runs on received packets. End.X
        resolves its bound adjacency directly; End looks up its updated DA.
        Every lookup, including failed queries, enters the dependency frame.
        """
        address = packet.dst
        sl = packet.srh.segments_left if packet.srh is not None else -1
        if (address, sl) in visited:
            return ()
        visited.add((address, sl))
        self.frames[-1].lookups.add((IPV6, address))
        for net, plen, _ in self._lookup(IPV6, address):
            entry, legs = self.resolve_prefix(IPV6, (net, plen))
            if entry is None:
                if self.policy.lpm_fallthrough:
                    continue
                return ()
            if entry.action == SRV6_LOCAL:
                result = local_sid(packet, entry.sid)
                if result.action == CROSS_CONNECT:
                    assert result.adjacency is not None and result.packet is not None
                    adj = result.adjacency
                    return self._resolve_nexthop(
                        # USD exposes the inner header; egress usability follows
                        # that packet, while the bound SID neighbor stays IPv6.
                        result.packet.af,
                        Nexthop.via(adj.interface, adj.nexthop, IPV6),
                    )
                if result.action == RELOOKUP and isinstance(result.packet, IPv6Packet):
                    return self._resolve_outer(result.packet, visited)
                if result.action == DECAP_LOOKUP and not self.stack:
                    # Layer-2 policy validation permits a headend terminal: an
                    # ingress policy exposes an independent inner lookup. During
                    # route compilation that lookup re-enters the triggering
                    # prefix, so it cannot supply an outer egress (gray stack).
                    return (_Leg('', None, None, Fraction(1)),)
                return ()
            if entry.action != FORWARD:
                return ()
            # This is a lookup of the outer packet's DA, not recursive next-hop
            # resolution. Keep the resolved route's egress exactly as transit
            # forwarding does, including an unnumbered interface-only peer.
            # Substituting the remote SID as an on-link neighbor loses that leg.
            return legs

        return ()

    def _substitute(
        self,
        af: int,
        address: int,
        legs: tuple[_Leg, ...],
    ) -> tuple[_Leg, ...]:
        out = []
        for leg in legs:
            if leg.nexthop is None and leg.encap is None:
                mac = self.ctx.neighbor_mac(leg.interface, address)
                if mac is None:
                    continue
                out.append(_Leg(leg.interface, address, mac, leg.share, af=af))
            else:
                out.append(leg)
        return tuple(out)

    def _resolve_recursive(self, af: int, address: int) -> tuple[_Leg, ...]:
        self.frames[-1].lookups.add((af, address))
        for net, plen, _ in self._lookup(af, address):
            entry, legs = self.resolve_prefix(af, (net, plen))
            if entry is None:
                if self.policy.lpm_fallthrough:
                    continue
                return ()
            if entry.action == RECEIVE:
                return ()
            if entry.action in DROP_ACTIONS:
                return ()
            if entry.action != FORWARD:
                return ()
            return self._substitute(af, address, legs)
        return ()

    def _deps(self) -> DependsOn:
        return self.frames[-1].freeze()


class _Frame:
    """Dependencies consulted while resolving one prefix."""

    __slots__ = ('prefixes', 'lookups', 'interfaces')

    def __init__(self) -> None:
        self.prefixes: set[tuple[int, int, int]] = set()
        self.lookups: set[tuple[int, int]] = set()
        self.interfaces: set[str] = set()

    def merge(self, deps: DependsOn) -> None:
        self.prefixes.update(deps.prefixes)
        self.lookups.update(deps.lookups)
        self.interfaces.update(deps.interfaces)

    def freeze(self) -> DependsOn:
        return DependsOn(
            tuple(sorted(self.prefixes)),
            tuple(sorted(self.lookups)),
            tuple(sorted(self.interfaces)),
        )


def _nh_sort_key(nh: Nexthop) -> tuple:
    return (
        nh.special is None,
        nh.interface or '',
        nh.address if nh.address is not None else -1,
        nh.weight,
        _encap_sort_key(nh.srv6),
        (nh.policy.color, nh.policy.endpoint)
        if isinstance(nh.policy, PolicyRef)
        else (),
    )


def _encap_sort_key(encap: Any) -> tuple:
    if not isinstance(encap, Srv6Encap):
        return ()
    return (
        encap.entries,
        encap.behavior,
        encap.source if encap.source is not None else -1,
        encap.policy if encap.policy is not None else (),
    )


def _merge(legs: list[_Leg]) -> tuple[_Leg, ...]:
    merged: dict[tuple[str, int | None, Any], _Leg] = {}
    for leg in legs:
        k = (leg.interface, leg.nexthop, leg.encap)
        prev = merged.get(k)
        if prev is None:
            merged[k] = leg
        else:
            merged[k] = _Leg(
                leg.interface,
                leg.nexthop,
                leg.mac,
                prev.share + leg.share,
                leg.encap,
                leg.af,
            )
    return tuple(
        sorted(
            merged.values(),
            key=lambda leg: (
                leg.interface,
                leg.nexthop if leg.nexthop is not None else -1,
                _encap_sort_key(leg.encap),
            ),
        )
    )


def _group_from_legs(legs: tuple[_Leg, ...], max_paths: int) -> tuple[Adjacency, ...]:
    legs = legs[:max_paths]
    total = sum(leg.share for leg in legs)
    shares = [leg.share / total for leg in legs]
    denom = 1
    for s in shares:
        denom = lcm(denom, s.denominator)
    return tuple(
        Adjacency(
            leg.interface, leg.nexthop, leg.mac, int(s * denom), leg.encap, leg.af
        )
        for leg, s in zip(legs, shares, strict=True)
    )


def resolve_fib(
    rib: RibState,
    ctx: ResolutionContext,
    policy: ResolutionPolicy,
    version: int,
    processed_epoch: int,
    old: Fib | None = None,
) -> tuple[Fib, ResolverOutcome]:
    """Rebuild the FIB for one address family.

    Groups are interned by their adjacency tuple with ids assigned in
    sorted order, so identical states yield identical ids; entries equal
    to the previous FIB's are reused (canonicalization).
    """
    resolver = _Resolver(ctx, policy)
    bits = 32 if rib.af == IPV4 else 128
    table: PrefixTable[FibEntry] = PrefixTable(bits)
    pending: list[tuple[Prefix, FibEntry, tuple[_Leg, ...]]] = []
    for prefix in rib.all_prefixes():
        entry, legs = resolver.resolve_prefix(rib.af, prefix)
        if entry is not None:
            pending.append((prefix, entry, legs))
    # Intern groups.
    adjacency_tuples: dict[tuple[Adjacency, ...], int] = {}
    finals: list[tuple[Prefix, FibEntry]] = []
    for prefix, entry, legs in pending:
        if entry.action == FORWARD:
            adjs = _group_from_legs(legs, policy.max_ecmp_paths)
            adjacency_tuples.setdefault(adjs, -1)
            finals.append(
                (
                    prefix,
                    FibEntry(
                        entry.prefix,
                        entry.action,
                        None,
                        entry.contributing,
                        entry.depends_on,
                    ),
                )
            )
            finals[-1] = (prefix, finals[-1][1], adjs)  # type: ignore[assignment]
        else:
            finals.append((prefix, entry))
    ordered = sorted(
        adjacency_tuples,
        key=lambda adjs: tuple(
            (a.interface, a.nexthop or -1, a.weight, _encap_sort_key(a.encap))
            for a in adjs
        ),
    )
    group_ids = {adjs: i for i, adjs in enumerate(ordered)}
    groups: dict[int, NexthopGroup] = {
        i: NexthopGroup(i, adjs, sum(a.weight for a in adjs))
        for adjs, i in group_ids.items()
    }
    old_entries = old.entries if old is not None else None
    for item in finals:
        prefix, entry = item[0], item[1]
        if len(item) == 3:
            gid = group_ids[item[2]]  # type: ignore[misc]
            entry = FibEntry(
                entry.prefix, entry.action, gid, entry.contributing, entry.depends_on
            )
            for row_key in entry.contributing:
                o = resolver.row_outcomes.get(row_key)
                if o is not None and o.status == INSTALLED:
                    resolver.row_outcomes[row_key] = RowOutcome(
                        row_key, INSTALLED, None, gid
                    )
        if old_entries is not None:
            prev = old_entries.get(prefix[0], prefix[1])
            if prev == entry:
                entry = prev
        table.insert(prefix[0], prefix[1], entry)
    dev = getattr(ctx, 'dev', None)
    policies = getattr(dev, 'srv6_policies', None)
    fib = Fib(
        rib.af,
        version,
        table.freeze(),
        PMap(groups),
        policy_programs(policies),
        policies.steering if policies else (),
    )
    if (
        old is not None
        and old.entries.items() == fib.entries.items()
        and old.groups == fib.groups
        and old.policy_programs == fib.policy_programs
        and old.steering == fib.steering
    ):
        fib = old
    outcome = ResolverOutcome(
        rib.af, processed_epoch, PMap(sorted(resolver.row_outcomes.items()))
    )
    return fib, outcome


PENDING = 'PENDING'


def row_status(
    input_epoch: int, outcome: ResolverOutcome | None, key: RowKey
) -> tuple[str, str | None]:
    """Derived route status: PENDING while the FIB has not consumed the
    latest inputs, else the last outcome."""
    if outcome is None or input_epoch > outcome.processed_epoch:
        return PENDING, None
    o = outcome.rows.get(key)
    if o is None:
        return 'NOT_INSTALLED', SHADOWED
    return ('INSTALLED' if o.status == INSTALLED else 'NOT_INSTALLED'), o.reason


def resolve_first_entry(
    ctx: ResolutionContext, policy: ResolutionPolicy, encap: Srv6Encap
) -> tuple[bool, DependsOn]:
    """Layer 2 uses the same first-entry local dispatch as FIB compilation."""
    resolver = _Resolver(ctx, policy)
    frame = _Frame()
    resolver.frames.append(frame)
    legs = resolver._resolve_nexthop(IPV6, Nexthop(srv6=encap))
    return bool(legs), frame.freeze()


def resolve_underlay_query(
    ctx: ResolutionContext, policy: ResolutionPolicy, address: int
) -> tuple[FibEntry | None, tuple[Adjacency, ...], DependsOn]:
    """RIB-derived forwarding at a transit node, retaining failed recursion.

    Unlike recursive next-hop resolution, this is the ordinary forwarding LPM
    (including /0), exactly as lookup in the resulting installed FIB would be.
    """
    resolver = _Resolver(ctx, policy)
    frame = _Frame()
    resolver.frames.append(frame)
    frame.lookups.add((IPV6, address))
    for net, plen, _ in ctx.rib(IPV6).prefixes.lookup_iter(address):
        entry, legs = resolver.resolve_prefix(IPV6, (net, plen))
        if entry is not None:
            return entry, _group_from_legs(legs, policy.max_ecmp_paths), frame.freeze()
    return None, (), frame.freeze()


@record
class CandidateResolution:
    """Prospective RIB resolution, independent of installed forwarding."""

    entry: FibEntry | None
    legs: tuple[Adjacency, ...]
    rows: tuple[Route, ...]
    queries: tuple[tuple[int, int, bool], ...]
    interfaces: tuple[str, ...]
    reason: str | None = None
    excluded_prefix: Prefix | None = None


class _CandidateResolver(_Resolver):
    """A per-query exclusion overlay using the normal forwarding resolver.

    Neither the RIB nor its prefix index is copied. Only rows at matching
    prefixes are inspected. No memo is reused across gray-stack contexts.
    """

    def __init__(
        self,
        ctx: ResolutionContext,
        policy: ResolutionPolicy,
        exclude_rows: frozenset[RowKey],
        connected_only: bool,
    ) -> None:
        super().__init__(ctx, policy)
        self.exclude_rows = exclude_rows
        self.connected_only = connected_only
        self.queries: list[tuple[int, int, bool]] = []
        self.selected: list[Route] = []
        self.loop = False
        self.too_deep = False
        self.excluded_prefix: Prefix | None = None

    def allowed(self, row: Route) -> bool:
        return row.key not in self.exclude_rows and (
            not self.connected_only
            or row.source in (CONNECTED, LOCAL)
            or all(nh.interface is not None for nh in row.nexthops)
        )

    def _groups(self, af: int, prefix: Prefix) -> Iterator[tuple[Route, ...]]:
        for group in super()._groups(af, prefix):
            kept = tuple(row for row in group if self.allowed(row))
            if kept:
                yield kept

    def _lookup(
        self, af: int, address: int
    ) -> Iterator[tuple[int, int, tuple[Route, ...]]]:
        index = len(self.queries)
        self.queries.append((af, address, False))
        for net, plen, rows in self.ctx.rib(af).prefixes.lookup_iter(
            address, min_len=0 if self.policy.resolve_via_default else 1
        ):
            if not any(self.allowed(row) for row in rows):
                if index == 0 and any(row.key in self.exclude_rows for row in rows):
                    if self.excluded_prefix is None:
                        self.excluded_prefix = net, plen
                continue
            self.queries[index] = af, address, True
            if (af, (net, plen)) in self.stack:
                self.loop = True
                continue
            yield net, plen, rows

    def resolve_prefix(self, af: int, prefix: Prefix):
        self.memo.clear()
        self.memo_deps.clear()
        if len(self.stack) >= self.policy.max_recursion_depth:
            self.too_deep = True
        return super().resolve_prefix(af, prefix)

    def _resolve_nexthop(self, af: int, nh: Nexthop) -> tuple[_Leg, ...]:
        start = len(self.selected)
        legs = super()._resolve_nexthop(af, nh)
        if not legs:
            del self.selected[start:]
        return legs

    def _resolve_group(self, af: int, prefix: Prefix, group: tuple[Route, ...]):
        start = len(self.selected)
        entry, legs, reason = super()._resolve_group(af, prefix, group)
        if entry is None:
            del self.selected[start:]
        else:
            self.selected.extend(group)
        return entry, legs, reason


def resolve_candidate(
    ctx: ResolutionContext,
    policy: ResolutionPolicy,
    af: int,
    address: int,
    *,
    exclude_rows: frozenset[RowKey] = frozenset(),
    connected_only: bool = False,
    interface: str | None = None,
) -> CandidateResolution:
    """Resolve a prospective next hop through a non-copying RIB overlay.

    The caller excludes the candidate's own row as well as withdrawn rows.
    A self-covering candidate with no alternative reports SELF_COVERED and
    its excluded prefix. Recursive gray-stack hits report LOOP_DETECTED.
    Scoped IPv6 link-local queries use the same adjacency resolver directly.
    """
    resolver = _CandidateResolver(ctx, policy, exclude_rows, connected_only)
    frame = _Frame()
    resolver.frames.append(frame)
    entry = None
    legs: tuple[_Leg, ...] = ()
    if af == IPV6 and address >> 118 == 0x3FA and interface is not None:
        legs = resolver._resolve_nexthop(af, Nexthop.via(interface, address, af))
    else:
        for net, plen, _ in resolver._lookup(af, address):
            candidate, candidate_legs = resolver.resolve_prefix(af, (net, plen))
            if candidate is None:
                if policy.lpm_fallthrough:
                    continue
                break
            if candidate.action == FORWARD:
                candidate_legs = resolver._substitute(af, address, candidate_legs)
                if interface is not None:
                    candidate_legs = tuple(
                        leg for leg in candidate_legs if leg.interface == interface
                    )
                if candidate_legs:
                    entry, legs = candidate, candidate_legs
            elif candidate.action == RECEIVE:
                entry = candidate
            break
    reason = None
    if entry is None and not legs:
        reason = (
            LOOP_DETECTED
            if resolver.loop
            else TOO_DEEP
            if resolver.too_deep
            else 'SELF_COVERED'
            if resolver.excluded_prefix is not None
            else UNRESOLVED
        )
    return CandidateResolution(
        entry,
        _group_from_legs(legs, policy.max_ecmp_paths),
        tuple(resolver.selected),
        tuple(resolver.queries),
        tuple(sorted(frame.interfaces)),
        reason,
        resolver.excluded_prefix,
    )
