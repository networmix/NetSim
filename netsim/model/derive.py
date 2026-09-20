"""Every derivation of the state tree, and the kind order.

Each derivation is a pure function ``(NetworkState, now, ...) -> NetworkState``
that returns the same root when nothing changed. ``converge`` runs them in
kind order until the tree is stable; the runtime schedules the same
functions per kind with delays.
"""

from __future__ import annotations

import dataclasses
from typing import Callable, Iterable

from netsim.model.addressing import IPV4, IPV6, MacAddress
from netsim.model.contracts import CONNECTED, CONNECTED_PROFILE, LOCAL, LOCAL_PROFILE
from netsim.model.forwarding import Fib, NeighborTable
from netsim.model.interfaces import (
    AdminState,
    BundleInput,
    EthernetNode,
    LoopbackNode,
    MemberInput,
    OperState,
    PortChannelNode,
    derive_ethernet,
    derive_lag_component,
    derive_loopback,
    forwarding_enabled,
    has_global_address,
    l3_usable,
)
from netsim.model.links import LinkNode
from netsim.model.routing import (
    Nexthop,
    ResolutionPolicy,
    RibState,
    Route,
    resolve_fib,
    rib_apply,
)
from netsim.model.state import (
    DeviceOper,
    DeviceState,
    NetworkState,
    PMap,
    canon,
    diff_pmap,
    record,
)

# Kind order (priority offsets above core.DEFERRED).
CARRIER = 0
LAG = 1
L3 = 2
IGP = 3
AGENT = 4
FIB = 5
TRANSPORT = 6
PLACEMENT = 7

AFS = (IPV4, IPV6)


# ---------------------------------------------------------------------------
# Lookups
# ---------------------------------------------------------------------------


def peer_endpoint(
    state: NetworkState, device: str, iface: str
) -> tuple[str, str] | None:
    node = state.devices[device].interfaces.get(iface)
    if not isinstance(node, EthernetNode) or node.link is None:
        return None
    link = state.links.get(node.link)
    if link is None:
        return None
    return link.other((device, iface))


def link_of(state: NetworkState, device: str, iface: str) -> LinkNode | None:
    node = state.devices[device].interfaces.get(iface)
    if not isinstance(node, EthernetNode) or node.link is None:
        return None
    return state.links.get(node.link)


@record
class InterfaceIndex:
    interfaces: PMap[str, object]
    bundles: PMap[str, tuple[str, ...]]


def _interface_index(dev: DeviceState) -> InterfaceIndex:
    """Update membership from changed interface shards, without a runtime cache.

    The input map is an identity certificate. Pure callers with an uncommitted
    candidate get a fresh correct index too; committed devices retain it.
    """
    old: InterfaceIndex | None = dev.interface_index
    if old is not None and old.interfaces is dev.interfaces:
        return old
    previous = old.interfaces if old is not None else None
    bundles = old.bundles if old is not None else PMap()
    changed: dict[str, set[str]] = {}
    for name in diff_pmap(previous, dev.interfaces, by_identity=True).keys:
        before = previous.get(name) if previous is not None else None
        after = dev.interfaces.get(name)
        old_po = (
            before.config.aggregate_id if isinstance(before, EthernetNode) else None
        )
        new_po = after.config.aggregate_id if isinstance(after, EthernetNode) else None
        if old_po == new_po:
            continue
        if old_po is not None:
            if old_po not in changed:
                changed[old_po] = set(bundles.get(old_po, ()))
            changed[old_po].discard(name)
        if new_po is not None:
            if new_po not in changed:
                changed[new_po] = set(bundles.get(new_po, ()))
            changed[new_po].add(name)
    builder = bundles.builder()
    for po, names in sorted(changed.items()):
        if names:
            builder.set(po, canon(bundles.get(po), tuple(sorted(names))))
        else:
            builder.remove(po)
    return InterfaceIndex(dev.interfaces, builder.build())


def bundle_members(dev: DeviceState, po: str) -> list[EthernetNode]:
    names = _interface_index(dev).bundles.get(po, ())
    return sorted((dev.interfaces[name] for name in names), key=lambda n: n.index)


def l3_owner(state: NetworkState, device: str, iface: str):
    """The L3 entity for a port: its bundle if it is a member, else itself."""
    node = state.devices[device].interfaces[iface]
    if isinstance(node, EthernetNode) and node.config.aggregate_id is not None:
        return state.devices[device].interfaces.get(node.config.aggregate_id)
    return node


def peer_bundle_key(
    state: NetworkState, device: str, member: str
) -> tuple[str, str] | None:
    peer = peer_endpoint(state, device, member)
    if peer is None:
        return None
    pnode = state.devices[peer[0]].interfaces.get(peer[1])
    if isinstance(pnode, EthernetNode) and pnode.config.aggregate_id is not None:
        return (peer[0], pnode.config.aggregate_id)
    return None


def link_capacity(state: NetworkState, link: LinkNode) -> float:
    if link.config.capacity is not None:
        return link.config.capacity
    a = state.devices[link.a[0]].interfaces[link.a[1]]
    b = state.devices[link.b[0]].interfaces[link.b[1]]
    return float(min(a.config.speed, b.config.speed))


# ---------------------------------------------------------------------------
# CARRIER
# ---------------------------------------------------------------------------


def derive_carrier(
    state: NetworkState,
    now: float,
    targets: Iterable[tuple[str, str]] | None = None,
    effective: Callable[[str, str, bool], bool | None] | None = None,
) -> NetworkState:
    """Ethernet oper state. ``effective(device, iface, raw)`` returns the
    debounced carrier or ``None`` to use the raw value (clock-free)."""
    if targets is None:
        targets = [
            (d, n)
            for d, dev in state.devices.sorted_items()
            for n, node in dev.interfaces.sorted_items()
            if isinstance(node, EthernetNode)
        ]
    devices = state.devices.builder()
    for device, iface in targets:
        dev = devices[device]
        node = dev.interfaces.get(iface)
        if not isinstance(node, EthernetNode):
            continue
        link = link_of(state, device, iface)
        peer = link.other((device, iface)) if link is not None else None
        peer_cfg = None
        peer_enabled = False
        if peer is not None:
            pnode = state.devices[peer[0]].interfaces.get(peer[1])
            if isinstance(pnode, EthernetNode):
                peer_cfg = pnode.config
                peer_enabled = state.devices[peer[0]].config.enabled
        raw_now, _ = _raw(link, peer_cfg, peer_enabled)
        eff = effective(device, iface, raw_now) if effective is not None else None
        oper = derive_ethernet(
            node,
            dev.config.enabled,
            link.oper if link else None,
            peer_cfg,
            peer_enabled,
            eff,
            now,
        )
        if oper is not node.oper:
            devices.set(
                device,
                dataclasses.replace(
                    dev,
                    interfaces=dev.interfaces.set(
                        iface, dataclasses.replace(node, oper=oper)
                    ),
                ),
            )
    new_devices = devices.build()
    return (
        state
        if new_devices is state.devices
        else dataclasses.replace(state, devices=new_devices)
    )


def _raw(link, peer_cfg, peer_enabled):
    from netsim.model.interfaces import raw_carrier

    return raw_carrier(link.oper if link else None, peer_cfg, peer_enabled)


# ---------------------------------------------------------------------------
# LAG
# ---------------------------------------------------------------------------


def _components(
    state: NetworkState, keys: Iterable[tuple[str, str]]
) -> list[set[tuple[str, str]]]:
    seen: set[tuple[str, str]] = set()
    comps: list[set[tuple[str, str]]] = []
    for key in keys:
        if key in seen:
            continue
        comp: set[tuple[str, str]] = set()
        stack = [key]
        while stack:
            k = stack.pop()
            if k in comp:
                continue
            comp.add(k)
            dev = state.devices.get(k[0])
            if dev is None or not isinstance(dev.interfaces.get(k[1]), PortChannelNode):
                continue
            for m in bundle_members(dev, k[1]):
                pk = peer_bundle_key(state, k[0], m.name)
                if pk is not None and pk not in comp:
                    stack.append(pk)
        seen |= comp
        comps.append(comp)
    return comps


def derive_lag(
    state: NetworkState,
    now: float,
    targets: Iterable[tuple[str, str]] | None = None,
    delay_elapsed: Callable[[tuple[str, str], str, float], bool] | None = None,
) -> NetworkState:
    if targets is None:
        targets = [
            (d, n)
            for d, dev in state.devices.sorted_items()
            for n, node in dev.interfaces.sorted_items()
            if isinstance(node, PortChannelNode)
        ]
    devices = state.devices.builder()
    for comp in _components(state, targets):
        bundles: dict[tuple[str, str], BundleInput] = {}
        for key in sorted(comp):
            dev = state.devices.get(key[0])
            if dev is None:
                continue
            node = dev.interfaces.get(key[1])
            if not isinstance(node, PortChannelNode):
                continue
            members = []
            for m in bundle_members(dev, key[1]):
                link = link_of(state, key[0], m.name)
                members.append(
                    MemberInput(
                        m.name,
                        m.index,
                        m.oper.oper == OperState.UP,
                        peer_bundle_key(state, key[0], m.name),
                        link_capacity(state, link) if link is not None else 0.0,
                    )
                )
            bundles[key] = BundleInput(
                key, node.config, dev.config.enabled, tuple(members), node.oper
            )
        for key, oper in derive_lag_component(bundles, now, delay_elapsed).items():
            dev = devices[key[0]]
            node = dev.interfaces[key[1]]
            if oper is not node.oper:
                devices.set(
                    key[0],
                    dataclasses.replace(
                        dev,
                        interfaces=dev.interfaces.set(
                            key[1], dataclasses.replace(node, oper=oper)
                        ),
                    ),
                )
    new_devices = devices.build()
    return (
        state
        if new_devices is state.devices
        else dataclasses.replace(state, devices=new_devices)
    )


# ---------------------------------------------------------------------------
# L3: loopbacks, connected and local rows, neighbors, router id
# ---------------------------------------------------------------------------


def _bits(af: int) -> int:
    return 32 if af == IPV4 else 128


def _connected_rows(
    dev: DeviceState, af: int, names: Iterable[str]
) -> tuple[Route, ...]:
    from netsim.model.addressing import mask_for

    rows: list[Route] = []
    bits = _bits(af)
    for name in names:
        node = dev.interfaces.get(name)
        if node is None:
            continue
        if not l3_usable(node, af) and not (
            isinstance(node, LoopbackNode) and node.oper.oper == OperState.UP
        ):
            continue
        addrs = node.config.ipv4 if af == IPV4 else node.config.ipv6
        for host, plen in addrs:
            if plen < bits and not isinstance(node, LoopbackNode):
                net = host & mask_for(plen, bits)
                rows.append(
                    Route(
                        (net, plen),
                        af,
                        CONNECTED,
                        CONNECTED_PROFILE.distance,
                        (Nexthop.via(name),),
                        distinguisher=(name,),
                    )
                )
            rows.append(
                Route(
                    (host, bits),
                    af,
                    LOCAL,
                    LOCAL_PROFILE.distance,
                    (Nexthop.receive(),),
                    distinguisher=(name,),
                )
            )
    return tuple(rows)


@record
class L3Interface:
    """Immutable ownership of derived rows and neighbors for one interface.

    CONNECTED/LOCAL rows belong to the interface in their distinguisher,
    including RECEIVE rows whose next hop deliberately has no interface.
    Keeping the previous contribution permits withdrawals after address or
    interface removal without searching the entire RIB or neighbor table.
    """

    rows: tuple[Route, ...]
    neighbors: tuple[tuple[int, int], ...]
    peer: int | None
    loopback: bool


def _interface_neighbors(
    state: NetworkState, device: str, name: str
) -> tuple[tuple[tuple[int, int], ...], int | None]:
    from netsim.model.addressing import mask_for

    dev = state.devices[device]
    node = dev.interfaces[name]
    if isinstance(node, LoopbackNode):
        return (), None
    if isinstance(node, EthernetNode):
        if node.config.aggregate_id is not None:
            return (), None
        peer = peer_endpoint(state, device, name)
        if peer is None:
            return (), None
        peer_l3 = l3_owner(state, peer[0], peer[1])
    else:  # PortChannel: peer bundle via any member
        peer_l3 = None
        for m in bundle_members(dev, name):
            pk = peer_bundle_key(state, device, m.name)
            if pk is not None:
                peer_l3 = state.devices[pk[0]].interfaces.get(pk[1])
                break
    if peer_l3 is None or isinstance(peer_l3, LoopbackNode):
        return (), None
    entries: dict[int, int] = {}
    for af in AFS:
        if not forwarding_enabled(node.config, af):
            continue
        bits = _bits(af)
        local_addrs = node.config.ipv4 if af == IPV4 else node.config.ipv6
        peer_addrs = peer_l3.config.ipv4 if af == IPV4 else peer_l3.config.ipv6
        for host, plen in local_addrs:
            mask = mask_for(plen, bits)
            for phost, _ in peer_addrs:
                if (phost & mask) == (host & mask):
                    entries[phost] = peer_l3.mac
        if af == IPV6:
            entries[MacAddress(peer_l3.mac).link_local_int()] = peer_l3.mac
    return tuple(sorted(entries.items())), peer_l3.mac


def _replace_neighbors(
    old: NeighborTable | None,
    previous: PMap[str, L3Interface],
    current: PMap[str, L3Interface],
    names: list[str],
    full: bool,
) -> NeighborTable:
    if full or old is None:
        entries = {}
        peers = {}
        for name in names:
            contribution = current.get(name)
            if contribution is not None:
                for address, mac in contribution.neighbors:
                    entries[(name, address)] = mac
                if contribution.peer is not None:
                    peers[name] = contribution.peer
        result = NeighborTable(PMap(entries), PMap(peers))
        return old if old is not None and old == result else result
    entries = old.entries.builder()
    peers = old.peers.builder()
    for name in names:
        before = previous.get(name)
        after = current.get(name)
        if before is after:
            continue
        wanted = dict(after.neighbors) if after is not None else {}
        if before is not None:
            for address, _ in before.neighbors:
                if address not in wanted:
                    entries.remove((name, address))
        for address, mac in wanted.items():
            if entries.get((name, address)) != mac:
                entries.set((name, address), mac)
        peer = after.peer if after is not None else None
        if peer is None:
            peers.remove(name)
        elif peers.get(name) != peer:
            peers.set(name, peer)
    new_entries, new_peers = entries.build(), peers.build()
    return (
        old
        if new_entries is old.entries and new_peers is old.peers
        else NeighborTable(new_entries, new_peers)
    )


def _router_id(dev: DeviceState) -> int:
    if dev.config.router_id is not None:
        return dev.config.router_id
    best = 0
    for node in dev.interfaces.values():
        if isinstance(node, LoopbackNode):
            for host, _ in node.config.ipv4:
                best = max(best, host)
    return best


def derive_l3(
    state: NetworkState,
    now: float,
    targets: Iterable[str | tuple[str, frozenset[str]]] | None = None,
) -> NetworkState:
    """Derive full devices or selected interfaces; full targets dominate scopes.

    The contribution index lives in the immutable tree, so forks and pure
    calls need no runtime cache. An uninitialized device takes the full path.
    """
    scopes: dict[str, set[str] | None] = {}
    for target in state.devices if targets is None else targets:
        if isinstance(target, str):
            scopes[target] = None
        else:
            device, interfaces = target
            if device not in scopes:
                scopes[device] = set(interfaces)
            elif (scope := scopes[device]) is not None:
                scope.update(interfaces)
    devices = state.devices.builder()
    for device in sorted(scopes):
        dev = devices.get(device)
        if dev is None:
            continue
        scope = scopes[device]
        full = scope is None or dev.l3_interfaces is None
        previous: PMap[str, L3Interface] = (
            dev.l3_interfaces if dev.l3_interfaces is not None else PMap()
        )
        if scope is None or dev.l3_interfaces is None:
            names = sorted(set(dev.interfaces) | set(previous))
        else:
            names = sorted(scope)
        new_dev = dev
        ifaces = dev.interfaces.builder()
        loopback_changed = full
        for name in names:
            node = dev.interfaces.get(name)
            before = previous.get(name)
            if before is not None and before.loopback:
                loopback_changed = True  # includes a removed/replaced loopback
            if isinstance(node, LoopbackNode):
                loopback_changed = True
                oper = derive_loopback(node.config, dev.config.enabled, node.oper, now)
                if oper is not node.oper:
                    ifaces.set(name, dataclasses.replace(node, oper=oper))
        new_ifaces = ifaces.build()
        if new_ifaces is not dev.interfaces:
            new_dev = dataclasses.replace(new_dev, interfaces=new_ifaces)
        contributions = previous.builder()
        local_state = _with_device(state, device, new_dev)
        for name in names:
            node = new_dev.interfaces.get(name)
            if node is None:
                contributions.remove(name)
                continue
            rows = tuple(r for af in AFS for r in _connected_rows(new_dev, af, (name,)))
            neighbors, peer = _interface_neighbors(local_state, device, name)
            contribution = L3Interface(
                rows, neighbors, peer, isinstance(node, LoopbackNode)
            )
            contributions.set(name, canon(previous.get(name), contribution))
        current = contributions.build()
        ribs = new_dev.ribs.builder()
        for af in AFS:
            rib = new_dev.ribs.get(af) or RibState.empty(af)
            rows = tuple(
                r
                for name in names
                if (c := current.get(name)) is not None
                for r in c.rows
                if r.af == af
            )
            if full:
                rib2 = rib_apply(
                    rib,
                    sync=(CONNECTED, tuple(r for r in rows if r.source == CONNECTED)),
                )
                rib2 = rib_apply(
                    rib2, sync=(LOCAL, tuple(r for r in rows if r.source == LOCAL))
                )
            else:
                wanted = {r.key for r in rows}
                delete = tuple(
                    r.key
                    for name in names
                    if (c := previous.get(name)) is not None
                    for r in c.rows
                    if r.af == af and r.key not in wanted
                )
                rib2 = rib_apply(rib, add=rows, delete=delete)
            if rib2 is not rib or af not in new_dev.ribs:
                ribs.set(af, rib2)
        new_ribs = ribs.build()
        if new_ribs is not new_dev.ribs:
            new_dev = dataclasses.replace(new_dev, ribs=new_ribs)
        neighbors = _replace_neighbors(dev.neighbors, previous, current, names, full)
        if neighbors is not new_dev.neighbors:
            new_dev = dataclasses.replace(new_dev, neighbors=neighbors)
        if loopback_changed:
            rid = _router_id(new_dev)
            if rid != new_dev.oper.router_id:
                new_dev = dataclasses.replace(new_dev, oper=DeviceOper(rid))
        if current is not dev.l3_interfaces:
            new_dev = dataclasses.replace(new_dev, l3_interfaces=current)
        if new_dev is not dev:
            devices.set(device, new_dev)
    new_devices = devices.build()
    return (
        state
        if new_devices is state.devices
        else dataclasses.replace(state, devices=new_devices)
    )


def _with_device(state: NetworkState, name: str, dev: DeviceState) -> NetworkState:
    return (
        state
        if state.devices.get(name) is dev
        else dataclasses.replace(state, devices=state.devices.set(name, dev))
    )


# ---------------------------------------------------------------------------
# FIB
# ---------------------------------------------------------------------------


class DeviceContext:
    """``ResolutionContext`` over one device of a state."""

    __slots__ = ('state', 'dev')

    def __init__(self, state: NetworkState, device: str) -> None:
        self.state = state
        self.dev = state.devices[device]

    def interface_exists(self, name: str) -> bool:
        return name in self.dev.interfaces

    def l3_usable(self, name: str, af: int) -> bool:
        node = self.dev.interfaces.get(name)
        return node is not None and l3_usable(node, af)

    def neighbor_mac(self, interface: str, address: int) -> int | None:
        nt = self.dev.neighbors
        return None if nt is None else nt.mac(interface, address)

    def peer_mac(self, interface: str) -> int | None:
        nt = self.dev.neighbors
        return None if nt is None else nt.peer_mac(interface)

    def rib(self, af: int) -> RibState:
        return self.dev.ribs.get(af) or RibState.empty(af)


def derive_fib(
    state: NetworkState, targets: Iterable[tuple[str, int]] | None = None
) -> NetworkState:
    if targets is None:
        targets = [(d, af) for d, _ in state.devices.sorted_items() for af in AFS]
    devices = state.devices.builder()
    for device, af in targets:
        dev = devices.get(device)
        if dev is None:
            continue
        ctx = DeviceContext(_with_device(state, device, dev), device)
        policy = dev.config.resolution_policy or ResolutionPolicy()
        old = dev.fibs.get(af)
        epoch = dev.resolver_input_epoch.get(af, 0)
        prev_outcome = dev.resolver_outcomes.get(af)
        if (
            old is not None
            and prev_outcome is not None
            and prev_outcome.processed_epoch == epoch
        ):
            continue  # inputs unchanged since the last resolution: nothing to do
        version = (old.version + 1) if old is not None else 1
        fib, outcome = resolve_fib(ctx.rib(af), ctx, policy, version, epoch, old)
        new_dev = dev
        if fib is not old:
            new_dev = dataclasses.replace(new_dev, fibs=new_dev.fibs.set(af, fib))
        if prev_outcome != outcome:
            new_dev = dataclasses.replace(
                new_dev, resolver_outcomes=new_dev.resolver_outcomes.set(af, outcome)
            )
        if new_dev is not dev:
            devices.set(device, new_dev)
    new_devices = devices.build()
    return (
        state
        if new_devices is state.devices
        else dataclasses.replace(state, devices=new_devices)
    )


# ---------------------------------------------------------------------------
# Resolver input epochs
# ---------------------------------------------------------------------------


def bump_epochs(old: NetworkState, new: NetworkState) -> NetworkState:
    """Advance the resolver input epoch of every (device, AF) whose FIB
    inputs (rows, interfaces, neighbors, config, load balancers) differ
    between *old* and *new*, unless the epoch already advanced in *new*
    (idempotent across a transaction that bumped it itself). Also maintain
    the interface membership index at this common commit boundary."""
    if old.devices is new.devices:
        return new
    devices = new.devices.builder()
    changes = diff_pmap(old.devices, new.devices, by_identity=True)
    for name in changes.added + changes.changed:
        dev = new.devices[name]
        index = _interface_index(dev)
        if index is not dev.interface_index:
            dev = dataclasses.replace(dev, interface_index=index)
            devices.set(name, dev)
        odev = old.devices.get(name)
        all_changed = (
            odev is None
            or (
                odev.interfaces is not dev.interfaces
                and odev.interfaces != dev.interfaces
            )
            or odev.neighbors != dev.neighbors
            or odev.config != dev.config
            or odev.load_balancers != dev.load_balancers
        )
        epochs = dev.resolver_input_epoch
        # A row of one family may resolve recursively through the other
        # family's RIB, so any RIB change is an input of both resolvers.
        any_rib_changed = odev is None or any(
            (odev.ribs.get(af) is not dev.ribs.get(af))
            and odev.ribs.get(af) != dev.ribs.get(af)
            for af in AFS
        )
        for af in AFS:
            old_epoch = odev.resolver_input_epoch.get(af, 0) if odev is not None else 0
            if epochs.get(af, 0) != old_epoch:
                continue  # already advanced in this transaction
            if all_changed or any_rib_changed:
                epochs = epochs.set(af, old_epoch + 1)
        if epochs is not dev.resolver_input_epoch:
            devices.set(name, dataclasses.replace(dev, resolver_input_epoch=epochs))
    built = devices.build()
    return new if built is new.devices else dataclasses.replace(new, devices=built)


# ---------------------------------------------------------------------------
# converge
# ---------------------------------------------------------------------------

Sources = Iterable[Callable[[NetworkState, float], NetworkState]]


def converge(
    state: NetworkState,
    now: float,
    sources: Sources = (),
    placement: Callable[[NetworkState], NetworkState] | None = None,
    max_iterations: int = 20,
) -> NetworkState:
    """Clock-free convergence: run every kind in order, repeat until stable.

    ``sources`` are RouteSources (pure functions of the tree, e.g. the
    oracle IGP) run in the IGP slot; ``placement`` is the PLACEMENT kind.
    """
    for _ in range(max_iterations):
        before = state
        state = derive_carrier(state, now)
        state = derive_lag(state, now)
        state = derive_l3(state, now)
        for source in sources:
            state = source(state, now)
        state = bump_epochs(before, state)
        state = derive_fib(state)
        if placement is not None:
            state = placement(state)
        if state is before:
            return state
    raise RuntimeError('converge() did not reach a fixed point')


def usable_mac_of(node) -> int | None:
    return getattr(node, 'mac', None)


__all__ = [
    'AFS',
    'AGENT',
    'CARRIER',
    'FIB',
    'IGP',
    'L3',
    'LAG',
    'PLACEMENT',
    'TRANSPORT',
    'DeviceContext',
    'converge',
    'derive_carrier',
    'derive_fib',
    'derive_l3',
    'derive_lag',
    'link_capacity',
    'link_of',
    'peer_endpoint',
    'peer_bundle_key',
    'bundle_members',
    'l3_owner',
    'has_global_address',
    'AdminState',
    'Fib',
    'usable_mac_of',
]
