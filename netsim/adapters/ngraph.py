"""NetGraph adapter: run a network built with NetGraph end to end.

Works on duck-typed NetGraph objects (``Network`` with ``nodes`` and
``links`` dicts of ``Node(name, disabled, risk_groups, attrs)`` and
``Link(source, target, capacity, cost, disabled, risk_groups, attrs, id)``;
``Scenario`` with ``network``, ``demand_set.sets``, ``failure_policy_set``
and ``seed``; ``TrafficDemand(source, target, volume, priority, mode)``),
so ``ngraph`` is imported only where its own classes are constructed.
"""

from __future__ import annotations

import copy
import importlib
import ipaddress
import re
from dataclasses import dataclass
from typing import Any, Callable, Iterable

from netsim.model.flows import PlacementReport
from netsim.model.igp import oracle_igp
from netsim.model.network import Network
from netsim.model.state import check_name
from netsim.runtime.failures import Draws, FaultEvent, Schedule, resolve_groups

DEFAULT_LOOPBACK_POOL = '10.255.0.0/16'
DEFAULT_LINK_POOL = '10.0.0.0/8'
DEFAULT_LOOPBACK_POOL_V6 = '2001:db8:ffff::/48'
DEFAULT_LINK_POOL_V6 = '2001:db8::/32'
DEFAULT_ANYCAST_POOL = '10.254.0.0/16'
"""Anycast /32s that translate NetGraph ``combine`` demands (one per demand)."""


@dataclass(frozen=True)
class FailureIteration:
    index: int
    excluded_nodes: tuple[str, ...]
    excluded_links: tuple[str, ...]


@dataclass(frozen=True)
class FailureSchedule:
    """Failure iterations from a NetGraph policy, applied as timed operations."""

    iterations: tuple[FailureIteration, ...]

    def apply(
        self,
        net: Network,
        sim: Any,
        *,
        start: float,
        dwell: float,
        restore: bool = True,
    ) -> list[float]:
        """Schedule each iteration ``dwell`` seconds apart from ``start``;
        returns the times at which each iteration is in effect."""
        if net is not sim.network:
            raise ValueError('schedule network must match the simulation')
        times = [start + i * dwell for i in range(len(self.iterations))]
        sim.failures(
            Schedule(
                FaultEvent(
                    tuple(
                        [('device', n) for n in it.excluded_nodes]
                        + [('link', lid) for lid in it.excluded_links]
                    ),
                    time,
                    dwell / 2 if restore else None,
                )
                for it, time in zip(self.iterations, times, strict=False)
            )
        )
        return times


def _device_name(node_name: str) -> str:
    return node_name


def _link_ids(net: Network) -> dict[str, str]:
    """NetGraph link id → NetSim link id (recorded at import)."""
    return getattr(net, 'ngraph_link_ids', {})


class _Pools:
    def __init__(
        self, loopback: str, link: str, loopback_v6: str, link_v6: str
    ) -> None:
        self.loopbacks = ipaddress.ip_network(loopback).hosts()
        self.links = ipaddress.ip_network(link).subnets(new_prefix=31)
        self.loopbacks_v6 = ipaddress.ip_network(loopback_v6).subnets(new_prefix=128)
        self.links_v6 = ipaddress.ip_network(link_v6).subnets(new_prefix=127)


class _MetadataGuard:
    """Restore the adapter's ``netsim_*`` attributes if an import aborts, so
    exported labels never disagree with the committed tree."""

    def __init__(self, net: Network) -> None:
        self.net = net
        self.saved = {
            k: copy.copy(v) for k, v in vars(net).items() if k.startswith('netsim_')
        }

    def __enter__(self) -> _MetadataGuard:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is not None:
            for k in [k for k in vars(self.net) if k.startswith('netsim_')]:
                delattr(self.net, k)
            for k, v in self.saved.items():
                setattr(self.net, k, v)


def from_network(
    network: Any,
    *,
    addressing: str = 'unnumbered',
    capacity_unit: float = 1e9,
    igp: bool = True,
    ipv6: bool = False,
    pools: dict[str, str] | None = None,
    seed: int = 0,
) -> Network:
    """Build a NetSim ``Network`` from a NetGraph ``Network``."""
    net = Network(seed=seed)
    with _MetadataGuard(net), net.batch():
        _populate_network(
            net,
            network,
            addressing=addressing,
            capacity_unit=capacity_unit,
            igp=igp,
            ipv6=ipv6,
            pools=pools,
        )
    return net


def _populate_network(
    net: Network,
    network: Any,
    *,
    addressing: str = 'unnumbered',
    capacity_unit: float = 1e9,
    igp: bool = True,
    ipv6: bool = False,
    pools: dict[str, str] | None = None,
) -> None:
    if addressing not in ('unnumbered', 'p2p'):
        raise ValueError("addressing must be 'unnumbered' or 'p2p'")
    pools = pools or {}
    p = _Pools(
        pools.get('loopback', DEFAULT_LOOPBACK_POOL),
        pools.get('link', DEFAULT_LINK_POOL),
        pools.get('loopback_v6', DEFAULT_LOOPBACK_POOL_V6),
        pools.get('link_v6', DEFAULT_LINK_POOL_V6),
    )
    net.ngraph_link_ids = {}
    failure_parameters: dict[tuple[str, str], dict[str, Any]] = {}
    # Adapter metadata is outside the immutable forwarding tree. Study copies
    # it before forking, so no model-layer dependency on this adapter is needed.
    net.netsim_capacity_unit = capacity_unit  # type: ignore[attr-defined]
    net.netsim_demand_destinations = {}  # type: ignore[attr-defined]
    net.netsim_demand_priorities = {}  # type: ignore[attr-defined]
    net.netsim_failure_parameters = failure_parameters  # type: ignore[attr-defined]
    for name in sorted(network.nodes):
        node = network.nodes[name]
        check_name(name, allow_slash=True)
        dev = net.add_device(name, enabled=not node.disabled, allow_slash=True)
        attrs = _attrs(node)
        settings = attrs.get('netsim', {})
        dev.configure(
            **{k: settings[k] for k in ('fib_delay', 'fast_failover') if k in settings}
        )
        if 'mtbf' in settings and 'mttr' in settings:
            failure_parameters[('device', name)] = dict(settings)
        v4 = attrs.get('loopback_ipv4') or f'{next(p.loopbacks)}/32'
        v6 = attrs.get('loopback_ipv6') or (
            str(next(p.loopbacks_v6).network_address) + '/128' if ipv6 else None
        )
        dev.add_loopback('lo0', ipv4=[v4], ipv6=[v6] if v6 else ())
    counters: dict[str, int] = {}
    lag_groups: dict[tuple[str, str, str], list[Any]] = {}
    for lid in sorted(network.links):
        link = network.links[lid]
        attrs = _attrs(link)
        lag = attrs.get('lag')
        if lag is not None:
            lag_groups.setdefault((link.source, link.target, str(lag)), []).append(link)
            continue
        _add_link(net, link, counters, p, addressing, capacity_unit, ipv6)
    for (src, dst, lag), links in sorted(lag_groups.items()):
        attrs = _attrs(links[0])
        min_links = int(attrs.get('min_links', 1))
        members_a = [_next_name(counters, src) for _ in links]
        members_b = [_next_name(counters, dst) for _ in links]
        v4 = _pair(p.links) if addressing == 'p2p' else None
        v6 = _pair6(p.links_v6) if (addressing == 'p2p' and ipv6) else None
        speed = float(links[0].capacity) * capacity_unit
        net.add_lag(
            net.device(src),
            lag,
            members_a,
            net.device(dst),
            lag,
            members_b,
            ipv4=v4,
            ipv6=v6,
            min_links=min_links,
            speed=speed,
            metric=int(links[0].cost),
            unnumbered=addressing == 'unnumbered',
        )
        for i, link in enumerate(links):
            nid = f'{src}:{members_a[i]}--{dst}:{members_b[i]}'
            from netsim.model.links import link_id

            net.ngraph_link_ids[link.id] = link_id(
                (src, members_a[i]), (dst, members_b[i])
            )
            if link.disabled:
                net.link(net.ngraph_link_ids[link.id]).fail()
            del nid
        if ipv6 and addressing == 'unnumbered':
            for d, po in ((src, lag), (dst, lag)):
                net.device(d)[po].configure(forwarding_v6=True)
    groups: dict[str, list[tuple[str, str]]] = {}
    for name, node in sorted(network.nodes.items()):
        for group in sorted(getattr(node, 'risk_groups', ())):
            groups.setdefault(group, []).append(('device', name))
    for lid, link in sorted(network.links.items()):
        for group in sorted(getattr(link, 'risk_groups', ())):
            groups.setdefault(group, []).append(('link', lid))
        settings = _attrs(link).get('netsim', {})
        if 'mtbf' in settings and 'mttr' in settings:
            failure_parameters[('link', lid)] = dict(settings)
        imported = net.link(net.ngraph_link_ids[lid])
        imported.configure(risk_groups=tuple(sorted(getattr(link, 'risk_groups', ()))))
        for device, iface in (imported.node.a, imported.node.b):
            delay = {
                k: settings[k]
                for k in ('carrier_delay_down', 'carrier_delay_up')
                if k in settings
            }
            if delay:
                net.device(device)[iface].configure(**delay)

    def group_members(group: Any, visiting: set[str]) -> None:
        if group.name in visiting:
            raise ValueError(f'cyclic risk group: {group.name}')
        visiting = visiting | {group.name}
        groups.setdefault(group.name, [])
        settings = _attrs(group).get('netsim', {})
        if 'mtbf' in settings and 'mttr' in settings:
            failure_parameters[('risk_group', group.name)] = dict(settings)
        for child in sorted(group.children, key=lambda g: g.name):
            groups[group.name].append(('risk_group', child.name))
            group_members(child, visiting)

    for _, group in sorted(getattr(network, 'risk_groups', {}).items()):
        group_members(group, set())
    net.netsim_risk_groups = resolve_groups(groups)  # type: ignore[attr-defined]
    if igp:
        net.add_source(oracle_igp)


def _attrs(entity: Any) -> dict[str, Any]:
    """Legacy top-level attrs plus the namespaced NetSim settings."""
    attrs = dict(getattr(entity, 'attrs', {}) or {})
    attrs.update(attrs.get('netsim', {}))
    return attrs


def _next_name(counters: dict[str, int], device: str) -> str:
    n = counters.get(device, 0)
    counters[device] = n + 1
    return f'eth{n}'


def _pair(subnets) -> tuple[str, str]:
    sn = next(subnets)
    hosts = (
        list(sn.hosts())
        if sn.prefixlen < 31
        else [sn.network_address, sn.broadcast_address]
    )
    return f'{hosts[0]}/{sn.prefixlen}', f'{hosts[1]}/{sn.prefixlen}'


def _pair6(subnets) -> tuple[str, str]:
    sn = next(subnets)
    a = sn.network_address
    return f'{a}/{sn.prefixlen}', f'{a + 1}/{sn.prefixlen}'


def _add_link(
    net: Network,
    link: Any,
    counters: dict[str, int],
    p: _Pools,
    addressing: str,
    capacity_unit: float,
    ipv6: bool,
) -> None:
    attrs = _attrs(link)
    a_name = attrs.get('source_interface') or _next_name(counters, link.source)
    b_name = attrs.get('target_interface') or _next_name(counters, link.target)
    v4 = _pair(p.links) if addressing == 'p2p' else None
    v6 = _pair6(p.links_v6) if (addressing == 'p2p' and ipv6) else None
    speed = float(link.capacity) * capacity_unit
    lk = net.add_p2p(
        net.device(link.source),
        a_name,
        net.device(link.target),
        b_name,
        ipv4=v4,
        ipv6=v6,
        speed=speed,
        metric=int(link.cost),
        unnumbered=addressing == 'unnumbered',
    )
    reverse_cost = attrs.get('reverse_cost')
    if reverse_cost is not None:
        net.device(link.target)[b_name].configure(metric=int(reverse_cost))
    if ipv6 and addressing == 'unnumbered':
        net.device(link.source)[a_name].configure(forwarding_v6=True)
        net.device(link.target)[b_name].configure(forwarding_v6=True)
    net.ngraph_link_ids[link.id] = lk.id
    if link.disabled:
        lk.fail()


def _match(network: Any, selector: Any) -> list[str]:
    if isinstance(selector, str):
        if hasattr(network, 'select_node_groups_by_path'):
            groups = network.select_node_groups_by_path(selector)
            return sorted({n.name for nodes in groups.values() for n in nodes})
        pattern = re.compile(selector)
        return sorted(n for n in network.nodes if pattern.match(n))
    raise ValueError(f'unsupported selector {selector!r}')


def demands_from(
    network: Any,
    net: Network,
    demand_sets: dict[str, list[Any]],
    *,
    capacity_unit: float = 1e9,
    strict: bool = True,
) -> list[str]:
    """Expand NetGraph ``TrafficDemand`` entries into NetSim demands (loopback destinations)."""
    with _MetadataGuard(net), net.batch():
        return _populate_demands(
            network, net, demand_sets, capacity_unit=capacity_unit, strict=strict
        )


SUPPORTED_FLOW_POLICIES = frozenset(
    {'SHORTEST_PATHS_ECMP', 'SHORTEST_PATHS_ECMP_LOSSY', 1, 6}
)
"""NetGraph presets whose forwarding NetSim reproduces (hop-by-hop ECMP over
the oracle IGP; the lossy variant maps to the LOSSY capacity model)."""


def _check_supported(td: Any, where: str) -> None:
    """Reject NetGraph demand options the adapter does not translate."""
    paths = getattr(td, 'static_paths', ()) or ()
    if paths:
        raise ValueError(
            f'{where}: static_paths are not supported (Gate B: SR policies)'
        )
    group_mode = getattr(td, 'group_mode', None)
    if group_mode not in (None, 'flatten'):
        raise ValueError(f'{where}: group_mode {group_mode!r} is not supported')
    policy = getattr(td, 'flow_policy', None)
    if policy is not None:
        key = getattr(policy, 'name', policy)
        value = getattr(policy, 'value', policy)
        if key not in SUPPORTED_FLOW_POLICIES and value not in SUPPORTED_FLOW_POLICIES:
            raise ValueError(
                f'{where}: flow_policy {key!r} is not supported (only shortest-path ECMP)'
            )


def _populate_demands(
    network: Any,
    net: Network,
    demand_sets: dict[str, list[Any]],
    *,
    capacity_unit: float = 1e9,
    strict: bool = True,
) -> list[str]:
    """Expand demands. Semantics kept from NetGraph: ``pairwise`` splits the
    volume over the pairs; ``combine`` sends an even share from every source
    to one anycast prefix announced by all targets (the nearest target wins,
    as with NetGraph's pseudo sink), targets that are also sources are
    excluded. Known approximation: NetGraph re-splits a combined volume over
    the sources that can reach a target in each iteration, NetSim keeps each
    source's share fixed (a cut-off source drops its share). With ``strict``
    the options that are not translated (static paths, group modes, WCMP and
    TE flow policies) raise instead of being silently approximated."""
    ids: list[str] = []
    used = _used_ipv4(net)
    for set_name in sorted(demand_sets):
        for i, td in enumerate(demand_sets[set_name]):
            if strict:
                _check_supported(td, f'{set_name}[{i}]')
            sources = _match(network, td.source)
            targets = _match(network, td.target)
            # NetGraph: lower priority numbers are served first; NetSim
            # serves higher numbers first, so the sign flips here and the
            # original value is kept for exported results.
            ng_priority = int(getattr(td, 'priority', 0))
            mode = str(getattr(td, 'mode', 'combine')).lower()
            destinations = getattr(net, 'netsim_demand_destinations', None)
            priorities = getattr(net, 'netsim_demand_priorities', None)
            if mode == 'pairwise':
                # ``pairwise``: the volume is split evenly over the pairs.
                pairs = [(s, t) for s in sources for t in targets if s != t]
                if not pairs:
                    continue
                per_pair = td.volume * capacity_unit / len(pairs)
                for s, t in pairs:
                    did = f'{set_name}:{i}:{s}>{t}'
                    net.add_demand(
                        did,
                        s,
                        _loopback_v4(net, t),
                        per_pair,
                        priority=-ng_priority,
                        tag=set_name,
                    )
                    if destinations is not None:
                        destinations[did] = t
                    if priorities is not None:
                        priorities[did] = ng_priority
                    ids.append(did)
                continue
            # ``combine``: one aggregate between the source set and the target
            # set. NetGraph attaches every target to a pseudo sink at cost 0
            # and originates an even share at every source, so each source
            # reaches its nearest target(s) by shortest path. The same
            # forwarding is an anycast prefix announced by every target.
            source_set = set(sources)
            targets = [t for t in targets if t not in source_set]
            if not sources or not targets:
                continue  # as NetGraph: overlap can empty the target set
            anycast = _anycast(net, f'{set_name}:{i}', targets, used)
            per_source = td.volume * capacity_unit / len(sources)
            for s in sources:
                did = f'{set_name}:{i}:{s}>*'
                net.add_demand(
                    did, s, anycast, per_source, priority=-ng_priority, tag=set_name
                )
                if destinations is not None:
                    destinations[did] = f'{{{",".join(targets)}}}'
                if priorities is not None:
                    priorities[did] = ng_priority
                ids.append(did)
    return ids


def _used_ipv4(net: Network) -> set[int]:
    """Every IPv4 host address configured on any interface of the tree."""
    out: set[int] = set()
    for dev in net.state.devices.values():
        for node in dev.interfaces.values():
            for host, _plen in getattr(node.config, 'ipv4', ()) or ():
                out.add(host)
    for address, _targets in (getattr(net, 'netsim_anycast', None) or {}).values():
        out.add(int(ipaddress.IPv4Address(address)))
    return out


def _anycast(net: Network, key: str, targets: list[str], used: set[int]) -> str:
    """Allocate one anycast /32 for *key* from the anycast pool, skipping every
    address the tree already uses, and announce it on each target's ``lo0``.
    The allocation cursor lives in the adapter metadata (copied by
    ``Network.fork``), so a fork never re-issues an address of its base."""
    table = getattr(net, 'netsim_anycast', None)
    if table is None:
        table = net.netsim_anycast = {}  # type: ignore[attr-defined]
    pool = ipaddress.ip_network(
        getattr(net, 'netsim_anycast_pool', DEFAULT_ANYCAST_POOL)
    )
    cursor = int(getattr(net, 'netsim_anycast_cursor', 0))
    hosts = pool.num_addresses - 2
    address = None
    while cursor < hosts:
        candidate = int(pool.network_address) + 1 + cursor
        cursor += 1
        if candidate not in used:
            address = candidate
            break
    net.netsim_anycast_cursor = cursor  # type: ignore[attr-defined]
    if address is None:
        raise ValueError(f'anycast pool {pool} exhausted while importing {key}')
    used.add(address)
    text = str(ipaddress.IPv4Address(address))
    table[key] = (text, tuple(targets))
    for t in targets:
        lo = net.device(t)['lo0']
        current = [
            f'{ipaddress.IPv4Address(h)}/{plen}' for h, plen in lo.node.config.ipv4
        ]
        lo.configure(ipv4=[*current, f'{text}/32'])
    return text


def _loopback_v4(net: Network, device: str) -> str:
    node = net.device(device)['lo0'].node
    host, plen = node.config.ipv4[0]
    return str(ipaddress.IPv4Address(host))


def failure_schedule(
    network: Any,
    failure_policy_set: Any,
    *,
    policy: str | None = None,
    iterations: int = 1,
    seed: int | None = None,
) -> FailureSchedule:
    """Failure iterations from NetGraph's ``FailureManager`` (requires ``ngraph``)."""
    draws = Draws.from_policy(
        network, failure_policy_set, policy=policy, iterations=iterations, seed=seed
    )
    return FailureSchedule(
        tuple(
            FailureIteration(i, d.excluded_nodes, d.excluded_links)
            for i, d in enumerate(draws)
        )
    )


def from_scenario(
    scenario: Any,
    *,
    addressing: str = 'unnumbered',
    capacity_unit: float = 1e9,
    failure_policy: str | None = None,
    demand_set: str | None = None,
    iterations: int = 0,
    strict: bool = True,
    **kw: Any,
) -> tuple[Network, list[str], FailureSchedule | None]:
    """NetGraph ``Scenario`` → ``(Network, demand ids, FailureSchedule | None)``."""
    seed = int(getattr(scenario, 'seed', 0) or 0)
    net = Network(seed=seed)
    with _MetadataGuard(net), net.batch():
        _populate_network(
            net,
            scenario.network,
            addressing=addressing,
            capacity_unit=capacity_unit,
            **kw,
        )
        sets = getattr(getattr(scenario, 'demand_set', None), 'sets', {}) or {}
        if demand_set is not None:
            sets = {demand_set: sets[demand_set]}
        ids = _populate_demands(
            scenario.network, net, sets, capacity_unit=capacity_unit, strict=strict
        )
    schedule = None
    if iterations:
        schedule = failure_schedule(
            scenario.network,
            scenario.failure_policy_set,
            policy=failure_policy,
            iterations=iterations,
            seed=seed,
        )
    return net, ids, schedule


def to_network(
    net: Network,
    report: PlacementReport | None = None,
    *,
    capacity_unit: float = 1e9,
    node_cls: Callable[..., Any] | None = None,
    link_cls: Callable[..., Any] | None = None,
    network_cls: Callable[..., Any] | None = None,
) -> Any:
    """Export devices and links as a NetGraph ``Network`` (or duck-typed
    classes), writing utilization into link attrs when a report is given."""
    if network_cls is None:
        ngraph_mod = importlib.import_module('ngraph')
        node_cls, link_cls, network_cls = (
            ngraph_mod.Node,
            ngraph_mod.Link,
            ngraph_mod.Network,
        )
    assert node_cls is not None and link_cls is not None and network_cls is not None
    out: Any = network_cls()
    state = net.state
    for name, dev in state.devices.sorted_items():
        out.add_node(
            node_cls(
                name,
                disabled=not dev.config.enabled,
                attrs={'loopback_ipv4': _loopback_v4(net, name)},
            )
        )
    from netsim.model import derive

    for lid, link in sorted(state.links.items(), key=lambda kv: kv[1].index):
        a, b = link.a, link.b
        cap = derive.link_capacity(state, link) / capacity_unit
        metric = state.devices[a[0]].interfaces[a[1]].config.metric
        attrs: dict[str, Any] = {'netsim_link': lid, 'src_if': a[1], 'dst_if': b[1]}
        if report is not None:
            fwd, rev = link.edge_id(a), link.edge_id(b)
            attrs.update(
                offered_fwd=report.offered[fwd],
                carried_fwd=report.carried[fwd],
                dropped_fwd=report.dropped[fwd],
                offered_rev=report.offered[rev],
                carried_rev=report.carried[rev],
                dropped_rev=report.dropped[rev],
                utilization=max(report.utilization(fwd), report.utilization(rev)),
            )
        out.add_link(
            link_cls(
                a[0],
                b[0],
                capacity=cap,
                cost=metric,
                disabled=link.oper.state == 0,
                attrs=attrs,
            )
        )
    return out


def results_json(
    net: Network,
    report: PlacementReport,
    *,
    exclusions: Iterable[tuple[str, ...]] | None = None,
) -> dict[str, Any]:
    """A results document in the shape NetGraph tooling reads."""
    state = net.state
    edges = []
    for lid, link in sorted(state.links.items(), key=lambda kv: kv[1].index):
        for tx in (link.a, link.b):
            e = link.edge_id(tx)
            edges.append(
                {
                    'link': lid,
                    'from': tx[0],
                    'to': link.other(tx)[0],
                    'offered': report.offered[e],
                    'carried': report.carried[e],
                    'dropped': report.dropped[e],
                    'capacity': report.capacity[e],
                }
            )
    return {
        'delivered': report.delivered_total,
        'dropped_by_reason': dict(report.dropped_by_reason.items()),
        'oversubscribed': [
            {'edge': e, 'utilization': u} for e, u in report.oversubscribed()
        ],
        'edges': edges,
        'demands': {
            d: {'delivered': r.delivered, 'drops': list(r.drops)}
            for d, r in report.demands.items()
        },
        'exclusions': [list(x) for x in (exclusions or ())],
    }


__all__ = [
    'from_network',
    'from_scenario',
    'to_network',
    'results_json',
    'demands_from',
    'failure_schedule',
    'FailureSchedule',
    'FailureIteration',
]


# Registration is optional; importing the zero-dependency core never imports this
# adapter. Importlib keeps NetGraph out of the static typing dependency graph too.
try:
    _workflow = importlib.import_module('ngraph.workflow.base')
except ModuleNotFoundError as exc:
    if exc.name != 'ngraph':
        raise
else:

    @dataclass
    class NetSimStudy(_workflow.WorkflowStep):
        """NetGraph workflow entry point for serial NetSim studies."""

        demand_set: str | None = None
        failure_policy: str | None = None
        iterations: int = 1
        mode: str = 'iterations'
        addressing: str = 'unnumbered'
        capacity_unit: float = 1e9
        horizon: float = 100.0
        rate: float = 1.0
        duration: Any = 1.0
        keep: dict[str, Any] | None = None
        keep_roots: int | None = 0
        keep_deltas: int = 0
        keep_arrays: bool = False
        keep_reports: bool = False
        keep_timeline: bool = False
        t0: float = 1.0
        settle: float = 1.0
        restore: bool = True
        parallelism: int = 1
        results_json: str | dict[str, Any] | None = None
        step: str | None = None
        select: list[str] | None = None
        # Explicit declarations also make construction independent of whether
        # a type checker can inspect the optional base class.
        name: str = ''
        seed: int | None = None

        def run(self, scenario: Any) -> None:
            from netsim.runtime.failures import Process
            from netsim.study import Study

            keep = {
                'roots': self.keep_roots,
                'deltas': self.keep_deltas,
                'arrays': self.keep_arrays,
                'reports': self.keep_reports,
                'timeline': self.keep_timeline,
                **(self.keep or {}),
            }
            study = Study.from_scenario(
                scenario,
                demand_set=self.demand_set,
                addressing=self.addressing,
                capacity_unit=self.capacity_unit,
                keep=keep,
            )
            options = {
                't0': self.t0,
                'settle': self.settle,
                'restore': self.restore,
                'parallelism': self.parallelism,
            }
            if self.mode == 'iterations':
                draws = Draws.from_policy(
                    scenario.network,
                    scenario.failure_policy_set,
                    policy=self.failure_policy,
                    iterations=self.iterations,
                    seed=self.seed,
                )
                result = study.iterations(draws, **options)
            elif self.mode == 'process':
                seed = self.seed if self.seed is not None else int(scenario.seed or 0)
                if self.failure_policy:
                    source = Process.from_policy(
                        scenario.network,
                        scenario.failure_policy_set,
                        policy=self.failure_policy,
                        rate=self.rate,
                        duration=self.duration,
                        seed=seed,
                    )
                else:
                    source = Process(study.failure_parameters, seed=seed)
                result = study.process(source, self.horizon)
            elif self.mode == 'replay':
                document = (
                    self.results_json
                    if self.results_json is not None
                    else scenario.results.to_dict()
                )
                result = study.replay(document, self.step, self.select, **options)
            else:
                raise ValueError('mode must be iterations, process or replay')
            exported = result.to_ngraph()
            scenario.results.put('metadata', exported['metadata'])
            scenario.results.put('data', exported['data'])

    _workflow.register_workflow_step('NetSimStudy')(NetSimStudy)
    __all__.append('NetSimStudy')
