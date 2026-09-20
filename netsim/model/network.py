"""``Network``: owner of the state tree and the transactional ``update``.

``update(fn, origin)`` applies a pure function to the current root,
validates and canonicalizes the candidate, computes the delta *before*
commit, returns ``None`` for an empty content delta, commits once and
then dispatches the delta to ``on_delta`` hooks. The commit machinery
owns the per-(device, AF) resolver input epochs.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Callable, Iterable

from netsim.model import derive
from netsim.model.addressing import LOCAL_ADMIN_BASE, mac_from_index
from netsim.model.contracts import (
    CONNECTED_PROFILE,
    IGP_PROFILE,
    LOCAL_PROFILE,
    STATIC_PROFILE,
    ClientId,
    ClientProfile,
)
from netsim.model.entities import Device, Interface, Link, StaleHandleError, _Handle
from netsim.model.interfaces import (
    EthernetConfig,
    EthernetNode,
    EthernetOper,
    LoopbackNode,
    LoopbackOper,
    PortChannelConfig,
    PortChannelNode,
    PortChannelOper,
)
from netsim.model.links import LinkConfig, LinkNode, LinkOper, link_id
from netsim.model.state import (
    DEBUG_VALIDATE,
    DeviceConfig,
    DeviceState,
    NetworkState,
    StateDelta,
    check_name,
    validate_immutable,
)

DeltaHook = Callable[[float, Any, StateDelta], None]
RouteSource = Callable[[NetworkState, float], NetworkState]


class Network:
    def __init__(self, *, seed: int = 0) -> None:
        self._state = NetworkState()
        self.seed = seed
        self.clock: Callable[[], float] = lambda: 0.0
        self.on_delta: list[DeltaHook] = []
        self.profiles: dict[ClientId, ClientProfile] = {
            STATIC_PROFILE.client: STATIC_PROFILE,
            IGP_PROFILE.client: IGP_PROFILE,
            CONNECTED_PROFILE.client: CONNECTED_PROFILE,
            LOCAL_PROFILE.client: LOCAL_PROFILE,
        }
        self.sources: list[RouteSource] = []
        self.capacity_model: int = 1  # flows.UNCONSTRAINED
        self.debug_validate = DEBUG_VALIDATE
        self._dispatching = False
        self._handles: dict[tuple, _Handle] = {}
        self.ngraph_link_ids: dict[str, str] = {}
        """NetGraph link id → NetSim link id, filled by the NetGraph adapter."""
        self._mac_base = LOCAL_ADMIN_BASE | ((seed & 0xFF) << 32)

    # -- state and update -----------------------------------------------------

    @property
    def state(self) -> NetworkState:
        return self._state

    def fork(self) -> Network:
        """A new ``Network`` starting from this one's current root.

        The tree is immutable, so nothing is copied: the fork shares the
        root and diverges on its first update. Seed, client profiles,
        route sources, capacity model and NetGraph ids carry over; hooks,
        handles and any ``Simulation`` binding do not. This is how a study
        runs many failure iterations from one converged baseline.
        """
        net = Network(seed=self.seed)
        net._state = self._state
        net.profiles = dict(self.profiles)
        net.sources = list(self.sources)
        net.capacity_model = self.capacity_model
        net.debug_validate = self.debug_validate
        net.ngraph_link_ids = dict(self.ngraph_link_ids)
        return net

    def update(
        self, fn: Callable[[NetworkState], NetworkState], origin: Any = 'op'
    ) -> StateDelta | None:
        if self._dispatching:
            raise RuntimeError('nested Network.update() from a derivation or observer')
        old = self._state
        candidate = fn(old)
        if candidate is old:
            return None
        candidate = derive.bump_epochs(old, candidate)
        candidate = dataclasses.replace(candidate, version=old.version + 1)
        if self.debug_validate:
            validate_immutable(candidate)
        delta = StateDelta(old, candidate)
        if delta.is_empty():
            return None
        self._state = candidate
        self._dispatching = True
        try:
            for hook in list(self.on_delta):
                hook(self.clock(), origin, delta)
        finally:
            self._dispatching = False
        return delta

    def _handle(self, cls: type, key: tuple, generation: int) -> Any:
        cache_key = (cls.__name__, key, generation)
        h = self._handles.get(cache_key)
        if h is None:
            h = self._handles[cache_key] = cls(self, key, generation)
        return h

    # -- clients --------------------------------------------------------------

    def register_client(self, profile: ClientProfile) -> None:
        if profile.client in self.profiles:
            raise ValueError(f'client {profile.client} already registered')
        self.profiles[profile.client] = profile

    def add_source(self, source: RouteSource) -> None:
        self.sources.append(source)

    # -- devices ----------------------------------------------------------------

    def add_device(
        self,
        name: str,
        *,
        enabled: bool = True,
        seed: int | None = None,
        fib_delay: float = 0.0,
        fast_failover: bool = False,
        resolution_policy: Any = None,
        allow_slash: bool = False,
    ) -> Device:
        check_name(name, allow_slash=allow_slash)
        now = self.clock()

        def fn(state: NetworkState) -> NetworkState:
            if name in state.devices:
                raise ValueError(f'device {name!r} exists')
            allocators, gen = state.allocators.take_generation()
            cfg = DeviceConfig(
                enabled=enabled,
                enabled_since=now,
                seed=_fnv1a(name) if seed is None else seed,
                fib_delay=fib_delay,
                fast_failover=fast_failover,
                resolution_policy=resolution_policy,
            )
            dev = DeviceState(name, gen, config=cfg)
            return dataclasses.replace(
                state, devices=state.devices.set(name, dev), allocators=allocators
            )

        self.update(fn, ('add_device', name))
        return self.device(name)

    def device(self, name: str) -> Device:
        dev = self._state.devices.get(name)
        if dev is None:
            raise KeyError(name)
        return self._handle(Device, (name,), dev.generation)

    @property
    def devices(self) -> dict[str, Device]:
        return {n: self.device(n) for n, _ in self._state.devices.sorted_items()}

    def __getitem__(self, name: str) -> Device:
        return self.device(name)

    # -- interfaces ---------------------------------------------------------------

    def _add_interface(
        self,
        device: Device,
        name: str,
        kind: str,
        config: Any,
        *,
        mac: int | None = None,
    ) -> Interface:
        check_name(name)

        def fn(state: NetworkState) -> NetworkState:
            dev = state.devices.get(device.name)
            if dev is None or dev.generation != device.generation:
                raise StaleHandleError(device.name)
            if name in dev.interfaces:
                raise ValueError(f'{device.name} already has interface {name!r}')
            allocators, gen = state.allocators.take_generation()
            allocators, index = allocators.take_ifindex(device.name)
            if kind == 'loopback':
                node: Any = LoopbackNode(name, index, gen, config, LoopbackOper())
            else:
                if mac is None:
                    allocators, mi = allocators.take_mac_index()
                    mac_value = int(mac_from_index(mi, self._mac_base))
                else:
                    mac_value = int(mac)
                if kind == 'ethernet':
                    node = EthernetNode(
                        name, index, gen, mac_value, config, EthernetOper()
                    )
                else:
                    node = PortChannelNode(
                        name, index, gen, mac_value, config, PortChannelOper()
                    )
            self._validate_interface_config(state, device.name, name, node, config)
            new_dev = dataclasses.replace(
                dev, interfaces=dev.interfaces.set(name, node)
            )
            return dataclasses.replace(
                state,
                devices=state.devices.set(device.name, new_dev),
                allocators=allocators,
            )

        self.update(fn, ('add_interface', device.name, name))
        return device.interface(name)

    def _validate_interface_config(
        self, state: NetworkState, device: str, name: str, node: Any, cfg: Any
    ) -> None:
        dev = state.devices[device]
        if isinstance(cfg, EthernetConfig):
            if cfg.speed <= 0 or cfg.metric <= 0:
                raise ValueError('speed and metric must be positive')
            if cfg.aggregate_id is not None:
                if cfg.ipv4 or cfg.ipv6:
                    raise ValueError(
                        f'{device}:{name} is a bundle member and may not carry addresses'
                    )
                po = dev.interfaces.get(cfg.aggregate_id)
                if not isinstance(po, PortChannelNode):
                    raise ValueError(
                        f'{device} has no PortChannel {cfg.aggregate_id!r}'
                    )
                self._check_single_partner(state, device, cfg.aggregate_id, node)
            if cfg.carrier_delay_up < 0 or cfg.carrier_delay_down < 0:
                raise ValueError('carrier delays must be non-negative')
        if isinstance(cfg, PortChannelConfig):
            if cfg.min_links < 1 or cfg.metric <= 0:
                raise ValueError('min_links must be >= 1 and metric positive')
        mtu = getattr(cfg, 'mtu', None)
        if mtu is not None and cfg.ipv6 and mtu < 1280:
            raise ValueError('IPv6 requires MTU >= 1280')
        # No duplicate host addresses on the device.
        seen: set[tuple[int, int]] = set()
        for other_name, other in dev.interfaces.items():
            c = cfg if other_name == name else other.config
            for af_addrs in (c.ipv4, c.ipv6):
                for host, plen in af_addrs:
                    if (host, plen) in seen:
                        raise ValueError(f'duplicate address on {device}')
                    seen.add((host, plen))

    def _check_single_partner(
        self, state: NetworkState, device: str, po: str, extra: Any = None
    ) -> None:
        """Reject a bundle whose members terminate on different peer bundles.

        ``extra`` is a member node not (yet) in the tree; a member without a
        link has no partner and cannot conflict.
        """
        dev = state.devices[device]
        partners = set()
        members = list(derive.bundle_members(dev, po))
        if extra is not None and extra.name not in dev.interfaces:
            members.append(extra)
        for m in members:
            if m.link is None or m.name not in dev.interfaces:
                continue
            pk = derive.peer_bundle_key(state, device, m.name)
            if pk is not None:
                partners.add(pk)
        if len(partners) > 1:
            raise ValueError(
                f'{device}:{po} members terminate on different peer bundles: {sorted(partners)}'
            )

    # -- links --------------------------------------------------------------------

    def add_link(
        self,
        a: Interface | tuple[str, str],
        b: Interface | tuple[str, str],
        *,
        capacity: float | None = None,
        delay: float = 0.0,
        risk_groups: Iterable[str] = (),
    ) -> Link:
        ea = a.endpoint if isinstance(a, Interface) else a
        eb = b.endpoint if isinstance(b, Interface) else b
        if delay < 0:
            raise ValueError('link delay must be non-negative')
        lid = link_id(ea, eb)
        now = self.clock()

        def fn(state: NetworkState) -> NetworkState:
            if ea[0] == eb[0]:
                raise ValueError('a link needs two different devices')
            if lid in state.links:
                raise ValueError(f'link {lid} exists')
            nodes = {}
            for dev_name, if_name in (ea, eb):
                dev = state.devices.get(dev_name)
                node = dev.interfaces.get(if_name) if dev else None
                if not isinstance(node, EthernetNode):
                    raise ValueError(
                        f'{dev_name}:{if_name} is not an Ethernet interface'
                    )
                if node.link is not None:
                    raise ValueError(f'{dev_name}:{if_name} is already linked')
                nodes[(dev_name, if_name)] = node
            allocators, gen = state.allocators.take_generation()
            allocators, index = allocators.take_link_index()
            from netsim.model.links import canonical_endpoints

            ca, cb = canonical_endpoints(ea, eb)
            link = LinkNode(
                lid,
                index,
                gen,
                ca,
                cb,
                LinkConfig(capacity, delay, tuple(sorted(risk_groups))),
                LinkOper(1, now),
            )
            devices = state.devices.builder()
            for (dev_name, if_name), node in nodes.items():
                dev = devices[dev_name]
                devices.set(
                    dev_name,
                    dataclasses.replace(
                        dev,
                        interfaces=dev.interfaces.set(
                            if_name, dataclasses.replace(node, link=lid)
                        ),
                    ),
                )
            new_state = dataclasses.replace(
                state,
                devices=devices.build(),
                links=state.links.set(lid, link),
                allocators=allocators,
            )
            for dev_name, if_name in (ea, eb):
                node = new_state.devices[dev_name].interfaces[if_name]
                if node.config.aggregate_id is not None:
                    self._check_single_partner(
                        new_state, dev_name, node.config.aggregate_id
                    )
            return new_state

        self.update(fn, ('add_link', lid))
        return self.link(lid)

    def link(self, lid: str) -> Link:
        link = self._state.links.get(lid)
        if link is None:
            raise KeyError(lid)
        return self._handle(Link, (lid,), link.generation)

    @property
    def links(self) -> dict[str, Link]:
        return {i: self.link(i) for i, _ in self._state.links.sorted_items()}

    def add_p2p(
        self,
        dev_a: Device,
        if_a: str,
        dev_b: Device,
        if_b: str,
        *,
        ipv4: tuple[str, str] | None = None,
        ipv6: tuple[str, str] | None = None,
        speed: float = 10e9,
        metric: int = 1,
        mtu: int = 1500,
        delay: float = 0.0,
        unnumbered: bool = False,
    ) -> Link:
        """Create both Ethernets (with the address pair) and the link."""
        a = dev_a.add_ethernet(
            if_a,
            speed=speed,
            metric=metric,
            mtu=mtu,
            unnumbered=unnumbered,
            ipv4=[ipv4[0]] if ipv4 else (),
            ipv6=[ipv6[0]] if ipv6 else (),
        )
        b = dev_b.add_ethernet(
            if_b,
            speed=speed,
            metric=metric,
            mtu=mtu,
            unnumbered=unnumbered,
            ipv4=[ipv4[1]] if ipv4 else (),
            ipv6=[ipv6[1]] if ipv6 else (),
        )
        return self.add_link(a, b, delay=delay)

    def add_lag(
        self,
        dev_a: Device,
        po_a: str,
        members_a: Iterable[str],
        dev_b: Device,
        po_b: str,
        members_b: Iterable[str],
        *,
        ipv4: tuple[str, str] | None = None,
        ipv6: tuple[str, str] | None = None,
        min_links: int = 1,
        speed: float = 10e9,
        metric: int = 1,
        mtu: int = 1500,
        delay: float = 0.0,
        unnumbered: bool = False,
    ) -> tuple[Interface, Interface, list[Link]]:
        """Create member Ethernets, pairwise links and both PortChannels."""
        ma, mb = list(members_a), list(members_b)
        if len(ma) != len(mb) or not ma:
            raise ValueError(
                'add_lag needs the same non-empty number of members on both sides'
            )
        pa = dev_a.add_portchannel(
            po_a,
            min_links=min_links,
            metric=metric,
            mtu=mtu,
            unnumbered=unnumbered,
            ipv4=[ipv4[0]] if ipv4 else (),
            ipv6=[ipv6[0]] if ipv6 else (),
        )
        pb = dev_b.add_portchannel(
            po_b,
            min_links=min_links,
            metric=metric,
            mtu=mtu,
            unnumbered=unnumbered,
            ipv4=[ipv4[1]] if ipv4 else (),
            ipv6=[ipv6[1]] if ipv6 else (),
        )
        links = []
        for x, y in zip(ma, mb, strict=True):
            ea = dev_a.add_ethernet(x, speed=speed, aggregate_id=po_a)
            eb = dev_b.add_ethernet(y, speed=speed, aggregate_id=po_b)
            links.append(self.add_link(ea, eb, delay=delay))
        return pa, pb, links

    # -- derivations ------------------------------------------------------------------

    def converge(self, now: float | None = None) -> None:
        t = self.clock() if now is None else now

        def fn(state: NetworkState) -> NetworkState:
            return derive.converge(
                state,
                t,
                self.sources,
                self._placement
                if state.demands or state.placement is not None
                else None,
            )

        self.update(fn, 'converge')

    # -- demands and placement ------------------------------------------------------

    def add_demand(self, id: str, source: str, dst: str, rate: float, **kw: Any) -> Any:
        from netsim.model.flows import make_demand

        demand = make_demand(id, source, dst, rate, **kw)

        def fn(state: NetworkState) -> NetworkState:
            if demand.source not in state.devices:
                raise ValueError(f'unknown source device {demand.source!r}')
            if state.demands.get(id) == demand:
                return state
            return dataclasses.replace(state, demands=state.demands.set(id, demand))

        self.update(fn, ('add_demand', id))
        return demand

    def remove_demand(self, id: str) -> None:
        self.update(
            lambda state: dataclasses.replace(state, demands=state.demands.remove(id)),
            ('remove_demand', id),
        )

    def set_capacity_model(self, model: int) -> None:
        self.capacity_model = model

    def _placement(self, state: NetworkState) -> NetworkState:
        from netsim.model.flows import derive_placement

        return derive_placement(state, self.capacity_model)

    def place(self) -> Any:
        """Clock-free placement over the current FIBs; returns the report."""
        self.update(self._placement, 'place')
        return self._state.placement

    @property
    def placement(self) -> Any:
        return self._state.placement

    # -- forwarding -------------------------------------------------------------------

    def view(self, device: str) -> _View:
        return _View(self._state, device)

    def trace(self, device: Device | str, packet: Any, max_hops: int = 64):
        from netsim.model.forwarding import trace as _trace

        name = device.name if isinstance(device, Device) else device
        state = self._state
        return _trace(lambda d: _View(state, d), name, packet, max_hops)

    def validate(self) -> list[str]:
        problems: list[str] = []
        state = self._state
        for dname, dev in state.devices.sorted_items():
            for name, node in dev.interfaces.sorted_items():
                if isinstance(node, PortChannelNode):
                    members = derive.bundle_members(dev, name)
                    partners = {
                        derive.peer_bundle_key(state, dname, m.name) for m in members
                    }
                    partners.discard(None)
                    if len(partners) > 1:
                        problems.append(
                            f'{dname}:{name} members terminate on different peer bundles'
                        )
                    for m in members:
                        if (
                            derive.peer_bundle_key(state, dname, m.name) is None
                            and m.link is not None
                        ):
                            problems.append(
                                f'{dname}:{m.name} peer port is not bundled'
                            )
        return problems


class _View:
    """``DeviceView`` over one device of a state (used by probes and HASH placement)."""

    __slots__ = ('state', 'dev', 'name')

    def __init__(self, state: NetworkState, device: str) -> None:
        self.state = state
        self.dev = state.devices[device]
        self.name = device

    def enabled(self) -> bool:
        return self.dev.config.enabled

    def fast_failover(self) -> bool:
        return self.dev.config.fast_failover

    def interface(self, name: str) -> Any:
        return self.dev.interfaces.get(name)

    def fib(self, af: int) -> Any:
        return self.dev.fibs.get(af)

    def neighbor_mac(self, interface: str, address: int) -> int | None:
        nt = self.dev.neighbors
        return None if nt is None else nt.mac(interface, address)

    def load_balancer(self, kind: int) -> Any:
        from netsim.model.hashing import LoadBalancer

        lbs = self.dev.load_balancers
        if lbs is not None and kind in lbs:
            return lbs[kind]
        return LoadBalancer(kind=kind, seed=self.dev.config.seed)

    def active_members(self, port_channel: str) -> tuple[str, ...]:
        node = self.dev.interfaces.get(port_channel)
        if not isinstance(node, PortChannelNode):
            return ()
        return tuple(sorted(n for n, m in node.oper.members.items() if m.active))

    def link_for(self, interface: str) -> tuple[Any, tuple[str, str]] | None:
        link = derive.link_of(self.state, self.name, interface)
        if link is None:
            return None
        return link, link.other((self.name, interface))

    def endpoint_usable(self, endpoint: tuple[str, str]) -> tuple[bool, bool]:
        dev = self.state.devices.get(endpoint[0])
        node = dev.interfaces.get(endpoint[1]) if dev is not None else None
        if dev is None or node is None:
            return False, False
        from netsim.model.interfaces import AdminState

        return node.config.admin == AdminState.UP, dev.config.enabled


def _fnv1a(text: str) -> int:
    h = 0xCBF29CE484222325
    for b in text.encode():
        h ^= b
        h = (h * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return h


__all__ = ['Network', 'RouteSource', 'DeltaHook']
