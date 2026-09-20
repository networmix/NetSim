"""A single-area link-state teaching protocol (not Open/R compatible).

Register ``net.add_agent(device, ReferenceAgent(ReferenceConfig()))`` on
unique, stable router IDs. The only input is the detached AgentContext.
IPv6 link-control hellos work on either L3 family, even IPv4-only links.
Neighbors are (receiving interface, advertised router ID); seeing ourselves
in a peer's hello completes three-way discovery. Interface names never
leave the device. Both peers initially open; only the connection initiated
by the lower router ID survives, with full LSDB synchronization on every
new connection. Floods batch immutable LSAs and use sessions exclusively.

There are three independently sequenced LSAs per origin. Bodies, not sequence
or timestamp refreshes, drive SPF and learned SR publication. Prefix metrics
are zero (cost to the advertising router, matching the teaching oracle);
interface costs live on directed adjacency edges. Families must be usable
at both ends. IPv4 routes use scoped IPv6 next hops (RFC 8950 style), or
interface-only next hops on unnumbered links. SR owners/peers are decimal
router-ID strings; interface names are local-only, so use literal SID lists
rather than remote interface-name symbols in learned SR policies.

Refreshes occur at refresh_interval; absolute originated_at + max_age expiry
is never extended by forwarding or duplicates. Empty bodies are purges:
higher sequence replaces the previous body, and ages out normally. Expired
records are removed, bounding history to live origins within max_age. Old
in-flight records cannot resurrect them because their absolute age is checked.
Restart seeds sequences from time; receiving a newer self LSA fights back
above it with current local bodies, including after a same-timestamp reset.

Route publication: initial/restart output syncs both families (including
empty rows, to withdraw retained state). Thereafter identical row sets emit
nothing; <= delta_limit changed row operations use delete then add, larger
changes sync. Hello refreshes only rearm runtime timers and return the same
state object. All working dicts/sets are invocation-local; committed state,
configuration and payloads are frozen. This is a trusted teaching protocol,
without authentication, areas, graceful restart or wire interoperability.
"""

from __future__ import annotations

import heapq
import math
from dataclasses import dataclass, field, replace
from typing import Any

from netsim.model import contracts as c
from netsim.model.addressing import is_link_local_v6, mask_for
from netsim.model.routing import Nexthop, Route
from netsim.model.state import PMap, empty_pmap


@dataclass(frozen=True, slots=True)
class ReferenceConfig:
    hello_interval: float = 1.0
    hold_time: float = 3.0
    max_age: float = 30.0
    refresh_interval: float = 10.0
    port: int = 6999
    run_delay: float = 0.001
    processing_delay: float = 0.001
    session_timeout: float = 5.0
    delta_limit: int = 8

    def __post_init__(self) -> None:
        for name in (
            'hello_interval',
            'hold_time',
            'max_age',
            'refresh_interval',
            'processing_delay',
            'session_timeout',
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
                raise ValueError(f'{name} must be finite and positive')
        if self.hold_time <= self.hello_interval:
            raise ValueError('hold_time must exceed hello_interval')
        if self.refresh_interval >= self.max_age:
            raise ValueError('refresh_interval must be less than max_age')
        if (
            isinstance(self.delta_limit, bool)
            or not isinstance(self.delta_limit, int)
            or self.delta_limit < 0
        ):
            raise ValueError('delta_limit must be a non-negative integer')
        # Reuse contract validation for the runtime options.
        c.AgentConfig(run_delay=self.run_delay, listen_ports=(self.port,))


@dataclass(frozen=True, slots=True)
class Hello:
    router_id: int
    seen: tuple[int, ...]
    families: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class Adjacency:
    router_id: int
    interface: str
    index: int
    generation: int
    address: int
    families: tuple[int, ...]
    up: bool


@dataclass(frozen=True, slots=True)
class AdjacencyLink:
    peer: int
    index: int
    metric: int
    nexthop: int
    families: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class Prefix:
    af: int
    prefix: tuple[int, int]
    metric: int = 0


@dataclass(frozen=True, slots=True)
class SidBody:
    locators: tuple[tuple[int, int], ...]
    sids: tuple[c.RemoteSid, ...]
    endpoints: tuple[tuple[int, int], ...] = ()


@dataclass(frozen=True, slots=True)
class Lsa:
    origin: int
    seq: int
    kind: str
    body: tuple[Any, ...]
    originated_at: float

    def __post_init__(self) -> None:
        if self.kind not in ('ADJACENCY', 'PREFIX', 'SID'):
            raise ValueError('unknown LSA kind')
        if self.seq < 0 or not math.isfinite(self.originated_at):
            raise ValueError('invalid LSA version/time')

    @property
    def key(self) -> tuple[int, str]:
        return self.origin, self.kind


@dataclass(frozen=True, slots=True)
class LsUpdate:
    lsas: tuple[Lsa, ...]


@dataclass(frozen=True, slots=True)
class ReferenceState:
    router_id: int
    lsdb: PMap[tuple[int, str], Lsa] = field(default_factory=empty_pmap)
    adjacencies: PMap[tuple[str, int], Adjacency] = field(default_factory=empty_pmap)
    sessions: PMap[tuple[str, int], int] = field(default_factory=empty_pmap)
    listeners: tuple[c.Endpoint, ...] = ()
    sequences: PMap[str, int] = field(default_factory=empty_pmap)
    rows: tuple[Route, ...] = ()


def _families(interface: c.InterfaceView) -> tuple[int, ...]:
    return tuple(
        af
        for af, usable in ((4, interface.l3_usable_v4), (6, interface.l3_usable_v6))
        if usable
    )


def _hold(key: tuple[str, int]) -> str:
    return f'hold:{key[1]}:{key[0]}'


def _expiry(key: tuple[int, str]) -> str:
    return f'lsa:{key[0]}:{key[1]}'


def _semantic(lsdb: PMap[tuple[int, str], Lsa], sid_only: bool = False) -> tuple:
    return tuple(
        (key, lsa.body)
        for key, lsa in lsdb.sorted_items()
        if lsa.body and (not sid_only or lsa.kind == 'SID')
    )


def _local_bodies(ctx: c.AgentContext, adjacencies: PMap) -> dict[str, tuple]:
    links = tuple(
        AdjacencyLink(
            a.router_id,
            a.index,
            ctx.interfaces[a.interface].metric,
            a.address,
            a.families,
        )
        for _, a in adjacencies.sorted_items()
        if a.up
    )
    prefixes: set[tuple[int, tuple[int, int]]] = set()
    endpoints: set[tuple[int, int]] = set()
    for interface in ctx.interfaces.values():
        if interface.oper != 1 or interface.aggregate_id is not None:
            continue
        for af, addresses in ((4, interface.ipv4), (6, interface.ipv6)):
            # Loopbacks have no MAC or channel and originate while UP.
            if interface.mac is not None and af not in _families(interface):
                continue
            for host, length in addresses:
                prefixes.add(
                    (af, (host & mask_for(length, 32 if af == 4 else 128), length))
                )
                if af == 6:
                    endpoints.add((host, 128))
    locators, sids = (), ()
    if ctx.srdb_local is not None and ctx.config.enabled:
        locators = tuple(sorted(loc.prefix for loc in ctx.srdb_local.locators.values()))
        peers = {a.interface: a.router_id for a in adjacencies.values() if a.up}
        sids = tuple(
            c.RemoteSid(
                sid.sid,
                sid.length,
                sid.behavior,
                sid.flavors,
                sid.structure,
                str(ctx.router_id),
                sid.adjacency_up and (sid.interface is None or sid.interface in peers),
                str(peers[sid.interface]) if sid.interface in peers else None,
            )
            for _, sid in ctx.srdb_local.sids.sorted_items()
        )
    return {
        'ADJACENCY': links,
        'PREFIX': tuple(Prefix(af, prefix) for af, prefix in sorted(prefixes)),
        'SID': (SidBody(locators, sids, tuple(sorted(endpoints))),)
        if locators or sids or endpoints
        else (),
    }


def shortest_paths(
    ctx: c.AgentContext, lsdb: PMap, client: c.ClientId
) -> tuple[Route, ...]:
    """Pure ECMP SPF over advertisements; only first-hop names are local."""
    source = ctx.router_id
    links = {
        origin: lsa.body for (origin, kind), lsa in lsdb.items() if kind == 'ADJACENCY'
    }
    interfaces = {interface.index: interface for interface in ctx.interfaces.values()}
    rows = []
    for af in (4, 6):
        graph = {
            origin: tuple(
                edge
                for edge in edges
                if af in edge.families
                and any(
                    back.peer == origin and af in back.families
                    for back in links.get(edge.peer, ())
                )
            )
            for origin, edges in links.items()
        }
        distances = {source: 0}
        heap = [(0, source)]
        while heap:
            cost, origin = heapq.heappop(heap)
            if cost != distances[origin]:
                continue
            for edge in graph.get(origin, ()):
                new_cost = cost + edge.metric
                if new_cost < distances.get(edge.peer, math.inf):
                    distances[edge.peer] = new_cost
                    heapq.heappush(heap, (new_cost, edge.peer))
        # Propagate along tight edges, including zero-cost ties. Never feed a
        # cycle back into the source. This terminates as finite hop sets grow.
        hops: dict[int, set[tuple[str, int | None]]] = {
            node: set() for node in distances
        }
        for edge in graph.get(source, ()):
            iface = interfaces.get(edge.index)
            if (
                iface is not None
                and af in _families(iface)
                and distances.get(edge.peer) == edge.metric
            ):
                hops[edge.peer].add(
                    (iface.name, None if iface.unnumbered else edge.nexthop)
                )
        changed = True
        while changed:
            changed = False
            for origin in sorted(distances, key=lambda node: (distances[node], node)):
                if origin == source:
                    continue
                for edge in graph.get(origin, ()):
                    if (
                        edge.peer != source
                        and distances.get(edge.peer) == distances[origin] + edge.metric
                    ):
                        before = len(hops[edge.peer])
                        hops[edge.peer].update(hops[origin])
                        changed |= before != len(hops[edge.peer])
        destinations: dict[int, list[Prefix]] = {}
        for (origin, kind), lsa in lsdb.sorted_items():
            if kind == 'PREFIX':
                destinations.setdefault(origin, []).extend(
                    p for p in lsa.body if p.af == af
                )
            elif kind == 'SID' and af == 6:
                destinations.setdefault(origin, []).extend(
                    Prefix(6, prefix) for body in lsa.body for prefix in body.locators
                )
        local = {p.prefix for p in destinations.get(source, ())}
        for origin, prefixes in sorted(destinations.items()):
            if origin == source or not hops.get(origin):
                continue
            nexthops = tuple(
                Nexthop.via(iface, addr, 6 if addr is not None else None)
                for iface, addr in sorted(
                    hops[origin], key=lambda h: (h[0], h[1] or -1)
                )
            )
            for prefix in sorted(set(prefixes), key=lambda p: (p.prefix, p.metric)):
                if prefix.prefix not in local:
                    rows.append(
                        Route(
                            prefix.prefix,
                            af,
                            client,
                            115,
                            nexthops,
                            distances[origin] + prefix.metric,
                            (origin,),
                        )
                    )
    return tuple(rows)


def _route_ops(
    old: tuple[Route, ...], new: tuple[Route, ...], limit: int, initial: bool
) -> tuple[c.RouteOp, ...]:
    operations = []
    for af in (4, 6):
        before = {row.key: row for row in old if row.af == af}
        after = {row.key: row for row in new if row.af == af}
        if not initial and before == after:
            continue
        deletes = tuple(key for key, row in before.items() if after.get(key) != row)
        adds = tuple(row for key, row in after.items() if before.get(key) != row)
        if initial or len(deletes) + len(adds) > limit:
            operations.append(c.RouteOp(af, sync=tuple(after.values())))
        else:
            operations.append(c.RouteOp(af, add=adds, delete=deletes))
    return tuple(operations)


def _sr_view(lsdb: PMap, old: c.SrDbView | None) -> c.SrDbView:
    sids, locators = [], []
    for (origin, kind), lsa in lsdb.sorted_items():
        if kind != 'SID':
            continue
        for body in lsa.body:
            sids.extend(body.sids)
            locators.extend(
                (str(origin), prefix)
                for prefix in sorted(set(body.locators + body.endpoints))
            )
    candidate = c.SrDbView(tuple(sids), tuple(locators), old.version if old else 0)
    if old is not None and candidate == old:
        return old
    return replace(candidate, version=candidate.version + 1)


@dataclass(frozen=True, slots=True, init=False)
class ReferenceAgent:
    config: c.AgentConfig
    client: c.ClientId
    profile: c.ClientProfile

    def __init__(self, config: ReferenceConfig | None = None) -> None:
        options = config if config is not None else ReferenceConfig()
        client = c.ClientId('ref', 0)
        object.__setattr__(self, 'client', client)
        object.__setattr__(
            self, 'profile', c.ClientProfile(client, 115, 20, link_state=True)
        )
        object.__setattr__(
            self,
            'config',
            c.AgentConfig(
                run_delay=options.run_delay,
                processing_delay=options.processing_delay,
                listen_ports=(options.port,),
                params=options,
            ),
        )

    def subscriptions(self) -> tuple[c.Path, ...]:
        return (('interfaces',), ('config',), ('oper',), ('srv6_sids',))

    def on_init(self, ctx: c.AgentContext) -> c.AgentOutput:
        return self._run(ctx, initial=True)

    def on_run(self, ctx: c.AgentContext) -> c.AgentOutput:
        return self._run(ctx, initial=False)

    def _run(self, ctx: c.AgentContext, *, initial: bool) -> c.AgentOutput:
        cfg: ReferenceConfig = self.config.params
        old: ReferenceState = (
            ReferenceState(ctx.router_id) if initial else ctx.agent_state
        )
        adj = old.adjacencies
        lsdb = old.lsdb
        sequences = old.sequences
        timers: dict[str, c.TimerOp] = {}
        sessions: list[c.SessionOp] = []
        flood: dict[tuple[int, str], Lsa] = {}
        new_up: set[tuple[str, int]] = set()
        fired = {entry.name for entry in ctx.inbox if isinstance(entry, c.TimerFired)}
        refresh = initial or 'refresh' in fired
        hello = initial or 'hello' in fired
        usable = {
            name: interface
            for name, interface in ctx.interfaces.sorted_items()
            if interface.link_local is not None and _families(interface)
        }
        listeners = tuple(
            c.Endpoint(6, interface.link_local, cfg.port, name)
            for name, interface in usable.items()
            if interface.link_local is not None
        )
        for endpoint in old.listeners:
            if endpoint not in listeners:
                sessions.append(c.SessionOp('unlisten', endpoint))
        for endpoint in listeners:
            if endpoint not in old.listeners:
                sessions.append(c.SessionOp('listen', endpoint))
                hello = True

        # Prune only on local observations or the protocol's own hold timers.
        # Transport reachability/rejections are deliberately not failure oracles.
        for key, a in adj.sorted_items():
            iface = usable.get(a.interface)
            if iface is None or iface.generation != a.generation or _hold(key) in fired:
                adj = adj.remove(key)
                timers[_hold(key)] = c.TimerOp(_hold(key))
        for entry in ctx.inbox:
            if not isinstance(entry, c.Delivery) or not isinstance(
                entry.payload, Hello
            ):
                continue
            msg = entry.payload
            iface = usable.get(entry.interface or '')
            sender = entry.sender
            if (
                iface is None
                or sender is None
                or msg.router_id == ctx.router_id
                or sender.interface != iface.name
                or sender.endpoint.af != 6
                or sender.endpoint.scope != iface.name
                or sender.endpoint.port != cfg.port
                or not is_link_local_v6(sender.endpoint.address)
            ):
                continue
            remaining = entry.time + cfg.hold_time - ctx.now
            if remaining <= 0:
                continue
            key = iface.name, msg.router_id
            families = tuple(af for af in _families(iface) if af in msg.families)
            a = Adjacency(
                msg.router_id,
                iface.name,
                iface.index,
                iface.generation,
                sender.endpoint.address,
                families,
                ctx.router_id in msg.seen and bool(families),
            )
            previous = adj.get(key)
            if a != previous:
                adj = adj.set(key, a)
                if a.up and (previous is None or not previous.up):
                    new_up.add(key)
            timers[_hold(key)] = c.TimerOp(_hold(key), remaining)
        if adj is not old.adjacencies:
            hello = True
        # Local family changes affect costs/eligibility immediately, without
        # waiting for the next hello to echo our updated configuration.
        for key, a in adj.sorted_items():
            families = tuple(
                af for af in a.families if af in _families(usable[a.interface])
            )
            if families != a.families:
                adj = adj.set(
                    key, replace(a, families=families, up=a.up and bool(families))
                )

        bodies = _local_bodies(ctx, adj)
        fightback: dict[str, int] = {}
        for entry in ctx.inbox:
            if not isinstance(entry, c.Delivery) or not isinstance(
                entry.payload, LsUpdate
            ):
                continue
            # Only an established, discovered session can carry LSAs.
            connection = (
                ctx.connections.get(entry.connection)
                if entry.connection is not None
                else None
            )
            if (
                connection is None
                or connection.state != c.ESTABLISHED
                or not any(
                    a.up
                    and connection.local.scope == a.interface
                    and connection.remote is not None
                    and connection.remote.address == a.address
                    for a in adj.values()
                )
            ):
                continue
            for lsa in entry.payload.lsas:
                if (
                    lsa.originated_at > ctx.now
                    or lsa.originated_at + cfg.max_age <= ctx.now
                ):
                    continue
                previous = lsdb.get(lsa.key)
                if previous is not None and lsa.seq <= previous.seq:
                    continue
                if lsa.origin == ctx.router_id:
                    fightback[lsa.kind] = max(fightback.get(lsa.kind, -1), lsa.seq)
                    continue
                lsdb = lsdb.set(lsa.key, lsa)
                flood[lsa.key] = lsa
                timers[_expiry(lsa.key)] = c.TimerOp(
                    _expiry(lsa.key), lsa.originated_at + cfg.max_age - ctx.now
                )
        for key, lsa in lsdb.sorted_items():
            if (
                lsa.origin != ctx.router_id
                and lsa.originated_at + cfg.max_age <= ctx.now
            ):
                lsdb = lsdb.remove(key)
                timers[_expiry(key)] = c.TimerOp(_expiry(key))
        # Router-ID edits explicitly purge the previous identity too.
        if old.router_id != ctx.router_id:
            for kind in ('ADJACENCY', 'PREFIX', 'SID'):
                previous = lsdb.get((old.router_id, kind))
                if previous:
                    purge = Lsa(old.router_id, previous.seq + 1, kind, (), ctx.now)
                    lsdb = lsdb.set(purge.key, purge)
                    flood[purge.key] = purge
                    timers[_expiry(purge.key)] = c.TimerOp(
                        _expiry(purge.key), cfg.max_age
                    )
        for kind, body in bodies.items():
            key = ctx.router_id, kind
            previous = lsdb.get(key)
            if (
                refresh
                or kind in fightback
                or previous is None
                or previous.body != body
            ):
                seq = (
                    max(
                        sequences.get(kind, -1),
                        previous.seq if previous else -1,
                        fightback.get(kind, -1),
                        int(ctx.now * 1_000_000) if initial else -1,
                    )
                    + 1
                )
                sequences = sequences.set(kind, seq)
                lsa = Lsa(ctx.router_id, seq, kind, body, ctx.now)
                lsdb = lsdb.set(key, lsa)
                flood[key] = lsa
        if refresh:
            timers['refresh'] = c.TimerOp('refresh', cfg.refresh_interval)

        chosen: PMap[tuple[str, int], int] = PMap()
        aborted: set[int] = set()
        for key, a in adj.sorted_items():
            if not a.up:
                continue
            candidates = [
                conn
                for conn in ctx.connections.values()
                if conn.state in (c.CONNECTING, c.ESTABLISHED)
                and conn.local.scope == a.interface
                and conn.remote is not None
                and conn.remote.address == a.address
            ]
            keep = [
                conn
                for conn in candidates
                if conn.initiator == (ctx.router_id < a.router_id)
            ]
            for conn in candidates:
                if conn not in keep:
                    aborted.add(conn.id)
            established = sorted(
                (conn.id for conn in keep if conn.state == c.ESTABLISHED)
            )
            if established:
                chosen = chosen.set(key, established[0])
                aborted.update(established[1:])
            elif not keep and (
                key in new_up or (hello and ctx.router_id < a.router_id)
            ):
                sessions.append(
                    c.SessionOp(
                        'open',
                        c.Endpoint(
                            6,
                            usable[a.interface].link_local or 0,
                            cfg.port,
                            a.interface,
                        ),
                        c.Endpoint(6, a.address, cfg.port, a.interface),
                        timeout=cfg.session_timeout,
                    )
                )
        for key, connection in old.sessions.items():
            if key not in chosen:
                aborted.add(connection)
        for entry in ctx.inbox:
            if isinstance(entry, c.Rejection):
                ctx.stats.add('rejections')
                if entry.connection is not None:
                    aborted.add(entry.connection)
        for connection in sorted(aborted):
            view = ctx.connections.get(connection)
            if view is not None and view.state != c.DOWN:
                sessions.append(c.SessionOp('abort', connection=connection))
        messages = []
        for key, connection in chosen.sorted_items():
            if connection in aborted:
                chosen = chosen.remove(key)
                continue
            payload = (
                tuple(lsa for _, lsa in lsdb.sorted_items())
                if old.sessions.get(key) != connection
                else tuple(flood[k] for k in sorted(flood))
            )
            if payload:
                messages.append(
                    c.Message(
                        connection, LsUpdate(payload), size=64 + 128 * len(payload)
                    )
                )
        datagrams = []
        if hello:
            for name in usable:
                seen = tuple(
                    sorted(a.router_id for a in adj.values() if a.interface == name)
                )
                datagrams.append(
                    c.Datagram(
                        name,
                        Hello(ctx.router_id, seen, _families(usable[name])),
                        cfg.port,
                        size=32 + 4 * len(seen),
                        link_local=True,
                    )
                )
            # Event-driven hellos must not postpone the periodic heartbeat.
            if initial or 'hello' in fired:
                timers['hello'] = c.TimerOp('hello', cfg.hello_interval)
        semantic = initial or _semantic(lsdb) != _semantic(old.lsdb)
        rows = shortest_paths(ctx, lsdb, self.client) if semantic else old.rows
        if semantic:
            ctx.stats.add('spf')
        operations = _route_ops(old.rows, rows, cfg.delta_limit, initial)
        view = ctx.srdb_view
        if initial or _semantic(lsdb, True) != _semantic(old.lsdb, True):
            view = _sr_view(lsdb, view)
        candidate = ReferenceState(
            ctx.router_id, lsdb, adj, chosen, listeners, sequences, rows
        )
        state = old if candidate == old else candidate
        if messages:
            ctx.stats.add('messages_sent', len(messages))
        if datagrams:
            ctx.stats.add('hellos_sent', len(datagrams))
        return c.AgentOutput(
            state=state,
            route_ops=operations,
            datagrams=tuple(datagrams),
            messages=tuple(messages),
            sessions=tuple(sessions),
            timers=tuple(timers[name] for name in sorted(timers)),
            srdb_view=view,
        )
