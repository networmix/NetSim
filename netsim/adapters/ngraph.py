"""NetGraph adapter: run a network built with NetGraph end to end.

Works on duck-typed NetGraph objects (``Network`` with ``nodes`` and
``links`` dicts of ``Node(name, disabled, risk_groups, attrs)`` and
``Link(source, target, capacity, cost, disabled, risk_groups, attrs, id)``;
``Scenario`` with ``network``, ``demand_set.sets``, ``failure_policy_set``
and ``seed``; ``TrafficDemand(source, target, volume, priority, mode)``),
so ``ngraph`` is imported only where its own classes are constructed.
"""

from __future__ import annotations

import importlib
import ipaddress
import re
from dataclasses import dataclass
from typing import Any, Callable, Iterable

from netsim.model.flows import PlacementReport
from netsim.model.igp import oracle_igp
from netsim.model.network import Network
from netsim.model.state import check_name

DEFAULT_LOOPBACK_POOL = '10.255.0.0/16'
DEFAULT_LINK_POOL = '10.0.0.0/8'
DEFAULT_LOOPBACK_POOL_V6 = '2001:db8:ffff::/48'
DEFAULT_LINK_POOL_V6 = '2001:db8::/32'


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
        times = []
        for i, it in enumerate(self.iterations):
            t = start + i * dwell
            times.append(t)

            def fail(it=it):
                for n in it.excluded_nodes:
                    net.device(_device_name(n)).configure(enabled=False)
                for lid in it.excluded_links:
                    net.link(_link_ids(net).get(lid, lid)).fail()

            def heal(it=it):
                for n in it.excluded_nodes:
                    net.device(_device_name(n)).configure(enabled=True)
                for lid in it.excluded_links:
                    net.link(_link_ids(net).get(lid, lid)).restore()

            sim.at(t, fail)
            if restore:
                sim.at(t + dwell / 2, heal)
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
    if addressing not in ('unnumbered', 'p2p'):
        raise ValueError("addressing must be 'unnumbered' or 'p2p'")
    pools = pools or {}
    p = _Pools(
        pools.get('loopback', DEFAULT_LOOPBACK_POOL),
        pools.get('link', DEFAULT_LINK_POOL),
        pools.get('loopback_v6', DEFAULT_LOOPBACK_POOL_V6),
        pools.get('link_v6', DEFAULT_LINK_POOL_V6),
    )
    net = Network(seed=seed)
    net.ngraph_link_ids = {}
    for name in sorted(network.nodes):
        node = network.nodes[name]
        check_name(name, allow_slash=True)
        dev = net.add_device(name, enabled=not node.disabled, allow_slash=True)
        attrs = getattr(node, 'attrs', {}) or {}
        v4 = attrs.get('loopback_ipv4') or f'{next(p.loopbacks)}/32'
        v6 = attrs.get('loopback_ipv6') or (
            str(next(p.loopbacks_v6).network_address) + '/128' if ipv6 else None
        )
        dev.add_loopback('lo0', ipv4=[v4], ipv6=[v6] if v6 else ())
    counters: dict[str, int] = {}
    lag_groups: dict[tuple[str, str, str], list[Any]] = {}
    for lid in sorted(network.links):
        link = network.links[lid]
        attrs = getattr(link, 'attrs', {}) or {}
        lag = attrs.get('lag')
        if lag is not None:
            lag_groups.setdefault((link.source, link.target, str(lag)), []).append(link)
            continue
        _add_link(net, link, counters, p, addressing, capacity_unit, ipv6)
    for (src, dst, lag), links in sorted(lag_groups.items()):
        attrs = getattr(links[0], 'attrs', {}) or {}
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
    if igp:
        net.add_source(oracle_igp)
    return net


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
    attrs = getattr(link, 'attrs', {}) or {}
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
) -> list[str]:
    """Expand NetGraph ``TrafficDemand`` entries into NetSim demands (loopback destinations)."""
    ids: list[str] = []
    for set_name in sorted(demand_sets):
        for i, td in enumerate(demand_sets[set_name]):
            sources = _match(network, td.source)
            targets = _match(network, td.target)
            pairs = [(s, t) for s in sources for t in targets if s != t]
            if not pairs:
                continue
            mode = getattr(td, 'mode', 'combine')
            per_pair = (
                td.volume * capacity_unit / len(pairs)
                if mode == 'combine'
                else td.volume * capacity_unit
            )
            for s, t in pairs:
                dst = _loopback_v4(net, t)
                did = f'{set_name}:{i}:{s}>{t}'
                net.add_demand(
                    did,
                    s,
                    dst,
                    per_pair,
                    priority=int(getattr(td, 'priority', 0)),
                    tag=set_name,
                )
                ids.append(did)
    return ids


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
    fm_module = importlib.import_module('ngraph.analysis.failure_manager')
    fm = fm_module.FailureManager(network, failure_policy_set)
    pol = failure_policy_set.get_policy(policy) if policy else None
    out = []
    for i in range(iterations):
        nodes, links = fm.compute_exclusions(pol, seed_offset=(seed or 0) + i)
        out.append(FailureIteration(i, tuple(sorted(nodes)), tuple(sorted(links))))
    return FailureSchedule(tuple(out))


def from_scenario(
    scenario: Any,
    *,
    addressing: str = 'unnumbered',
    capacity_unit: float = 1e9,
    failure_policy: str | None = None,
    iterations: int = 0,
    **kw: Any,
) -> tuple[Network, list[str], FailureSchedule | None]:
    """NetGraph ``Scenario`` → ``(Network, demand ids, FailureSchedule | None)``."""
    seed = int(getattr(scenario, 'seed', 0) or 0)
    net = from_network(
        scenario.network,
        addressing=addressing,
        capacity_unit=capacity_unit,
        seed=seed,
        **kw,
    )
    sets = getattr(getattr(scenario, 'demand_set', None), 'sets', {}) or {}
    ids = demands_from(scenario.network, net, sets, capacity_unit=capacity_unit)
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
