"""Oracle IGP: a ``RouteSource`` that installs shortest-path ECMP routes.

Per device and address family: Dijkstra over interfaces that are L3
usable for that family, with interface metrics as costs, destinations =
every device's loopback and interface prefixes. Next-hops are interface +
neighbor address on numbered links and interface-only on unnumbered
links (RFC 8950 style). Rows carry the ``igp`` client at distance 110 and
are installed with ``sync`` so unchanged rows keep their identity.
"""

from __future__ import annotations

import dataclasses
import heapq

from netsim.model import derive
from netsim.model.addressing import IPV4, IPV6, mask_for
from netsim.model.contracts import IGP, IGP_PROFILE
from netsim.model.interfaces import (
    EthernetNode,
    LoopbackNode,
    PortChannelNode,
    l3_usable,
)
from netsim.model.routing import Nexthop, RibState, Route, rib_apply
from netsim.model.state import NetworkState, PMap


def _egresses(
    state: NetworkState, device: str, af: int
) -> list[tuple[str, str, int, int | None, int | None]]:
    """``(interface, peer device, metric, peer address, peer af)`` for every usable egress."""
    dev = state.devices[device]
    out = []
    for name, node in dev.interfaces.sorted_items():
        if isinstance(node, LoopbackNode) or not l3_usable(node, af):
            continue
        if isinstance(node, EthernetNode):
            peer = derive.peer_endpoint(state, device, name)
            if peer is None:
                continue
            peer_dev = peer[0]
            peer_node = derive.l3_owner(state, peer[0], peer[1])
        else:
            peer_dev = None
            peer_node = None
            for m in derive.bundle_members(dev, name):
                pk = derive.peer_bundle_key(state, device, m.name)
                if pk is not None:
                    peer_dev = pk[0]
                    peer_node = state.devices[pk[0]].interfaces.get(pk[1])
                    break
            if peer_dev is None:
                continue
        if peer_node is None or not l3_usable(peer_node, af):
            continue
        peer_addr = None
        bits = 32 if af == IPV4 else 128
        local_addrs = node.config.ipv4 if af == IPV4 else node.config.ipv6
        peer_addrs = peer_node.config.ipv4 if af == IPV4 else peer_node.config.ipv6
        for host, plen in local_addrs:
            mask = mask_for(plen, bits)
            for phost, _ in peer_addrs:
                if (phost & mask) == (host & mask):
                    peer_addr = phost
                    break
            if peer_addr is not None:
                break
        out.append(
            (
                name,
                peer_dev,
                node.config.metric,
                peer_addr,
                af if peer_addr is not None else None,
            )
        )
    return out


def _destinations(state: NetworkState, device: str, af: int) -> list[tuple[int, int]]:
    """Prefixes a device originates: loopbacks and interface subnets."""
    dev = state.devices[device]
    bits = 32 if af == IPV4 else 128
    out: set[tuple[int, int]] = set()
    for node in dev.interfaces.values():
        if isinstance(node, EthernetNode) and node.config.aggregate_id is not None:
            continue
        if not (
            isinstance(node, LoopbackNode) and node.oper.oper == 1
        ) and not l3_usable(node, af):
            continue
        for host, plen in node.config.ipv4 if af == IPV4 else node.config.ipv6:
            out.add((host & mask_for(plen, bits), plen))
    return sorted(out)


def shortest_path_routes(
    state: NetworkState, device: str, af: int
) -> tuple[Route, ...]:
    """ECMP shortest-path rows from *device* to every originated prefix."""
    if not state.devices[device].config.enabled:
        return ()
    graph: dict[str, list[tuple[str, str, int, int | None, int | None]]] = {}
    for name, dev in state.devices.items():
        if dev.config.enabled:
            graph[name] = _egresses(state, name, af)
    dist: dict[str, int] = {device: 0}
    first_hops: dict[str, set[tuple[str, int | None, int | None]]] = {device: set()}
    heap = [(0, device)]
    done: set[str] = set()
    while heap:
        d, u = heapq.heappop(heap)
        if u in done:
            continue
        done.add(u)
        for iface, v, metric, peer_addr, peer_af in graph.get(u, ()):
            nd = d + metric
            hops = {(iface, peer_addr, peer_af)} if u == device else first_hops[u]
            if v not in dist or nd < dist[v]:
                dist[v] = nd
                first_hops[v] = set(hops)
                heapq.heappush(heap, (nd, v))
            elif nd == dist[v]:
                first_hops[v] |= hops
    rows: list[Route] = []
    local = set(_destinations(state, device, af))
    for target in sorted(dist):
        if target == device:
            continue
        hops = sorted(first_hops[target], key=lambda h: (h[0], h[1] or -1))
        if not hops:
            continue
        nexthops = tuple(Nexthop.via(iface, addr, paf) for iface, addr, paf in hops)
        for prefix in _destinations(state, target, af):
            if prefix in local:
                continue
            rows.append(
                Route(
                    prefix,
                    af,
                    IGP,
                    IGP_PROFILE.distance,
                    nexthops,
                    metric=dist[target],
                    distinguisher=(target,),
                )
            )
    # Several targets may originate the same prefix (anycast): keep distinct rows by distinguisher.
    return tuple(rows)


def oracle_igp(state: NetworkState, now: float) -> NetworkState:
    """RouteSource: sync ``igp`` rows on every device for both families."""
    devices = state.devices.builder()
    for name, dev in state.devices.sorted_items():
        new_dev = dev
        ribs = dev.ribs.builder()
        for af in (IPV4, IPV6):
            rib = dev.ribs.get(af) or RibState.empty(af)
            rows = shortest_path_routes(state, name, af)
            new_rib = rib_apply(rib, sync=(IGP, rows))
            if new_rib is not rib:
                ribs.set(af, new_rib)
        built = ribs.build()
        if built is not dev.ribs:
            new_dev = dataclasses.replace(dev, ribs=built)
            devices.set(name, new_dev)
    new_devices = devices.build()
    return (
        state
        if new_devices is state.devices
        else dataclasses.replace(state, devices=new_devices)
    )


__all__ = ['oracle_igp', 'shortest_path_routes', 'PMap', 'PortChannelNode']
