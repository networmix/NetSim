"""Handles: thin, generation-checked views over the tree with the
builder and operations API. Handles carry ``__dict__`` so users can
attach attributes; every access validates the entity's generation."""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any, Iterable

from netsim.model import srv6
from netsim.model.addressing import (
    IPV4,
    IPV6,
    AddressFamily,
    interface_to_int,
    prefix_to_int,
    to_int,
)
from netsim.model.contracts import STATIC, STATIC_PROFILE, ClientId
from netsim.model.forwarding import Fib
from netsim.model.interfaces import (
    AdminState,
    EthernetConfig,
    EthernetNode,
    LoopbackConfig,
    LoopbackNode,
    PortChannelConfig,
    PortChannelNode,
    l3_usable,
)
from netsim.model.links import LINK_FAILED, LINK_UP, LinkNode, LinkOper
from netsim.model.routing import (
    BLACKHOLE,
    PROHIBIT,
    RECEIVE_NH,
    UNREACHABLE,
    Nexthop,
    RibState,
    Route,
    RowKey,
    rib_apply,
    row_status,
)
from netsim.model.state import DeviceState, NetworkState

if TYPE_CHECKING:
    from netsim.model.network import Network, _Edits


class StaleHandleError(LookupError):
    """The entity behind a handle was removed (or replaced)."""


class _Handle:
    __slots__ = ('network', 'key', 'generation', '_valid', '_incarnation', '__dict__')

    def __init__(self, network: Network, key: tuple, generation: int) -> None:
        self.network = network
        self.key = key
        self.generation = generation
        self._valid = True
        self._incarnation = network._handle_incarnation

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, _Handle)
            and other.network is self.network
            and other.key == self.key
            and other.generation == self.generation
            and other._incarnation == self._incarnation
        )

    def __hash__(self) -> int:
        return hash((id(self.network), self.key, self.generation, self._incarnation))

    def _check_valid(self) -> None:
        if not self._valid:
            raise StaleHandleError(self.key)

    def __repr__(self) -> str:
        return f'{type(self).__name__}{self.key}'


def _addresses(
    values: Iterable[str | tuple[int, int]] | None, af: AddressFamily
) -> tuple[tuple[int, int], ...]:
    out: list[tuple[int, int]] = []
    for v in values or ():
        if isinstance(v, tuple):
            out.append(v)
            continue
        host, plen, fam = interface_to_int(v)
        if fam != af:
            raise ValueError(f'{v} is not an {af.name} address')
        out.append((host, plen))
    return tuple(out)


class Device(_Handle):
    @property
    def name(self) -> str:
        return self.key[0]

    @property
    def node(self) -> DeviceState:
        self._check_valid()
        dev = self.network._device_node(self.name)
        if dev is None or dev.generation != self.generation:
            raise StaleHandleError(self.name)
        edits = self.network._edits
        return edits.device_snapshot(self.name) if edits is not None else dev

    @property
    def exists(self) -> bool:
        dev = self.network._device_node(self.name)
        return self._valid and dev is not None and dev.generation == self.generation

    @property
    def enabled(self) -> bool:
        return self.node.config.enabled

    def configure(self, **fields: Any) -> None:
        now = self.network.clock()

        def fn(state: _Edits) -> None:
            dev = self._node_in(state)
            cfg = dev.config
            if 'enabled' in fields and fields['enabled'] != cfg.enabled:
                fields['enabled_since'] = now
            new_cfg = dataclasses.replace(cfg, **fields)
            if not 1 <= new_cfg.srv6_hop_limit <= 255:
                raise ValueError('invalid SRv6 hop limit')
            if new_cfg.srv6_source is not None:
                srv6.ipv6(new_cfg.srv6_source)
            state.devices.set(self.name, dataclasses.replace(dev, config=new_cfg))

        self.network._edit(fn, ('configure', self.name))

    def _node_in(self, state: NetworkState | _Edits) -> DeviceState:
        self._check_valid()
        dev = state.devices.get(self.name)
        if dev is None or dev.generation != self.generation:
            raise StaleHandleError(self.name)
        return dev

    # -- interfaces -------------------------------------------------------

    def add_loopback(
        self,
        name: str,
        *,
        ipv4: Iterable[str] = (),
        ipv6: Iterable[str] = (),
        admin: int = AdminState.UP,
    ) -> Interface:
        cfg = LoopbackConfig(
            admin=admin,
            admin_since=self.network.clock(),
            ipv4=_addresses(ipv4, IPV4),
            ipv6=_addresses(ipv6, IPV6),
        )
        return self.network._add_interface(self, name, 'loopback', cfg)

    def add_ethernet(
        self,
        name: str,
        *,
        speed: float = 10e9,
        mtu: int = 1500,
        metric: int = 1,
        ipv4: Iterable[str] = (),
        ipv6: Iterable[str] = (),
        unnumbered: bool = False,
        forwarding_v4: bool | None = None,
        forwarding_v6: bool | None = None,
        aggregate_id: str | None = None,
        carrier_delay_up: float = 0.0,
        carrier_delay_down: float = 0.0,
        mac: int | None = None,
        admin: int = AdminState.UP,
    ) -> Interface:
        cfg = EthernetConfig(
            admin=admin,
            admin_since=self.network.clock(),
            mtu=mtu,
            speed=float(speed),
            metric=metric,
            ipv4=_addresses(ipv4, IPV4),
            ipv6=_addresses(ipv6, IPV6),
            unnumbered=unnumbered,
            forwarding_v4=forwarding_v4,
            forwarding_v6=forwarding_v6,
            aggregate_id=aggregate_id,
            carrier_delay_up=carrier_delay_up,
            carrier_delay_down=carrier_delay_down,
        )
        return self.network._add_interface(self, name, 'ethernet', cfg, mac=mac)

    def add_portchannel(
        self,
        name: str,
        *,
        members: Iterable[str] = (),
        min_links: int = 1,
        mtu: int = 1500,
        metric: int = 1,
        ipv4: Iterable[str] = (),
        ipv6: Iterable[str] = (),
        unnumbered: bool = False,
        forwarding_v4: bool | None = None,
        forwarding_v6: bool | None = None,
        member_delay_up: float = 0.0,
        member_delay_down: float = 0.0,
        admin: int = AdminState.UP,
    ) -> Interface:
        cfg = PortChannelConfig(
            admin=admin,
            admin_since=self.network.clock(),
            mtu=mtu,
            metric=metric,
            ipv4=_addresses(ipv4, IPV4),
            ipv6=_addresses(ipv6, IPV6),
            unnumbered=unnumbered,
            forwarding_v4=forwarding_v4,
            forwarding_v6=forwarding_v6,
            min_links=min_links,
            member_delay_up=member_delay_up,
            member_delay_down=member_delay_down,
        )
        po = self.network._add_interface(self, name, 'portchannel', cfg)
        for m in members:
            self.interface(m).configure(aggregate_id=name)
        return po

    def interface(self, name: str) -> Interface:
        self._check_valid()
        dev = self.network._device_node(self.name)
        if dev is None or dev.generation != self.generation:
            raise StaleHandleError(self.name)
        node = self.network._interface_node(self.name, name)
        if node is None:
            raise KeyError(f'{self.name} has no interface {name!r}')
        return self.network._handle(Interface, (self.name, name), node.generation)

    def __getitem__(self, name: str) -> Interface:
        return self.interface(name)

    @property
    def interfaces(self) -> list[Interface]:
        return [self.interface(n) for n, _ in self.node.interfaces.sorted_items()]

    # -- SRv6 --------------------------------------------------------------

    def _sr_update(self, fn: Any, operation: str) -> Any:
        result = None

        def apply(state: NetworkState) -> NetworkState:
            nonlocal result
            self._node_in(state)
            candidate, result = fn(state)
            return candidate

        self.network.update(apply, (operation, self.name))
        return result

    def add_locator(
        self,
        name: str,
        prefix: str | tuple[int, int] | None = None,
        *,
        structure: srv6.SidStructure = srv6.UNCOMPRESSED,
        block: str | tuple[int, int] = srv6.DEFAULT_BLOCK,
        node_id: int | None = None,
        ranges: srv6.SidRanges | None = None,
        **options: Any,
    ) -> srv6.Locator:
        srv6.require_gate_b(srv6.END, **options)
        return self._sr_update(
            lambda state: srv6.add_locator(
                state,
                self.name,
                name,
                prefix,
                structure,
                block,
                node_id,
                ranges if ranges is not None else srv6.SidRanges(),
            ),
            'add_locator',
        )

    def add_local_sid(
        self,
        behavior: int,
        *,
        structure: srv6.SidStructure,
        flavors: int = 0,
        sid: int | str | None = None,
        interface: str | Interface | None = None,
        nexthop: int | str | None = None,
        owner: ClientId = STATIC,
        locator: str | None = None,
        **options: Any,
    ) -> srv6.LocalSid:
        self._sr_client(owner)
        if isinstance(interface, Interface):
            if interface.network is not self.network or interface.device != self.name:
                raise ValueError('SID interface must belong to its device')
            _ = interface.node  # generation check
            interface = interface.name
        return self._sr_update(
            lambda state: srv6.add_local_sid(
                state,
                self.name,
                behavior,
                structure=structure,
                flavors=flavors,
                sid=sid,
                interface=interface,
                nexthop=nexthop,
                owner=owner,
                locator=locator,
                **options,
            ),
            'add_local_sid',
        )

    def remove_local_sid(self, sid: int | str) -> None:
        self._sr_update(
            lambda state: (srv6.remove_local_sid(state, self.name, sid), None),
            'remove_local_sid',
        )

    def _sr_client(self, client: ClientId) -> None:
        _ = self.node
        if client not in self.network.profiles:
            raise ValueError(f'unregistered client {client}')

    def sid_client(self, client: ClientId = STATIC) -> SidClient:
        self._sr_client(client)
        return SidClient(self, client)

    def policy_client(self, client: ClientId = STATIC) -> PolicyClient:
        self._sr_client(client)
        return PolicyClient(self, client)

    def remove_interface(self, name: str) -> None:
        _ = self.interface(name).node

        def apply(state: NetworkState) -> NetworkState:
            self._node_in(state)
            return _remove_interfaces(state, {(self.name, name)})

        self.network.update(apply, ('remove_interface', self.name, name))

    # -- routing ------------------------------------------------------------

    def rib(self, af: int) -> RibState:
        return self.node.ribs.get(af) or RibState.empty(af)

    def fib(self, af: int) -> Fib | None:
        return self.node.fibs.get(af)

    def rib_client(
        self, client: ClientId = STATIC, af: int = IPV4, distance: int | None = None
    ) -> RibClient:
        self._check_valid()
        d = STATIC_PROFILE.distance if distance is None else distance
        if client != STATIC:
            profile = self.network.profiles.get(client)
            if profile is None:
                raise ValueError(f'unregistered client {client}')
            d = profile.distance if distance is None else distance
        return RibClient(self.network, self.name, client, af, d)

    def add_route(
        self,
        prefix: str,
        nexthops: Iterable[Any],
        *,
        distance: int = 1,
        metric: int = 0,
        distinguisher: tuple[Any, ...] = (),
    ) -> Route:
        net, plen, af = prefix_to_int(prefix)
        nhs = tuple(parse_nexthop(spec) for spec in nexthops)
        route = Route(
            (net, plen),
            af,
            STATIC,
            distance,
            nhs,
            metric=metric,
            distinguisher=distinguisher,
        )
        self.rib_client(STATIC, af).add_routes((route,))
        return route

    def route_status(self, af: int, key: RowKey) -> tuple[str, str | None]:
        dev = self.node
        return row_status(
            dev.resolver_input_epoch.get(af, 0), dev.resolver_outcomes.get(af), key
        )


class SidClient:
    """Owner-scoped SID requests; request ids are local to a client."""

    def __init__(self, device: Device, client: ClientId) -> None:
        self.device, self.client = device, client

    def request_sid(
        self,
        request_id: str,
        behavior: int,
        args: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> srv6.LocalSid:
        options = dict(args or {})
        if options.keys() & kwargs.keys():
            raise ValueError('duplicate SID request arguments')
        options.update(kwargs)
        return self.device._sr_update(
            lambda state: srv6.request_sid(
                state, self.device.name, self.client, request_id, behavior, options
            ),
            'request_sid',
        )

    def remove_sid(self, sid: int | str) -> None:
        self.device._sr_update(
            lambda state: (
                srv6.remove_local_sid(state, self.device.name, sid, self.client),
                None,
            ),
            'remove_sid',
        )


class PolicyClient:
    """One owner per policy key; client-scoped steering replacement."""

    def __init__(self, device: Device, client: ClientId) -> None:
        self.device, self.client = device, client

    def _put(
        self, policy: srv6.SrPolicy, replace_existing: bool, locator: str | None
    ) -> srv6.SrPolicy:
        def apply(state: NetworkState) -> tuple[NetworkState, srv6.SrPolicy]:
            new = srv6.put_policy(
                state,
                self.device.name,
                self.client,
                policy,
                replace_existing=replace_existing,
                locator=locator,
            )
            table = new.devices[self.device.name].srv6_policies
            assert table is not None
            return new, table.policies[policy.key]

        return self.device._sr_update(
            apply, 'replace_policy' if replace_existing else 'add_policy'
        )

    def add(
        self, policy: srv6.SrPolicy, *, locator: str | None = None, **options: Any
    ) -> srv6.SrPolicy:
        srv6.require_gate_b(srv6.END, **options)
        return self._put(policy, False, locator)

    def replace(
        self, policy: srv6.SrPolicy, *, locator: str | None = None, **options: Any
    ) -> srv6.SrPolicy:
        srv6.require_gate_b(srv6.END, **options)
        return self._put(policy, True, locator)

    def delete(self, key: tuple[int, int]) -> None:
        self.device._sr_update(
            lambda state: (
                srv6.delete_policy(state, self.device.name, self.client, key),
                None,
            ),
            'delete_policy',
        )

    def set_steering(self, rules: Iterable[srv6.SteeringRule]) -> None:
        rows = tuple(rules)
        self.device._sr_update(
            lambda state: (
                srv6.set_steering(state, self.device.name, self.client, rows),
                None,
            ),
            'set_steering',
        )

    add_policy = add
    replace_policy = replace
    delete_policy = delete


def _remove_interfaces(state: NetworkState, keys: set[tuple[str, str]]) -> NetworkState:
    """Detach physical links and delete bound SIDs, retaining allocator cursors."""
    devices = state.devices
    links = state.links
    for lid, link in state.links.sorted_items():
        if any(endpoint in keys for endpoint in (link.a, link.b)):
            links = links.remove(lid)
            for dname, iface in (link.a, link.b):
                dev = devices[dname]
                node = dev.interfaces[iface]
                devices = devices.set(
                    dname,
                    dataclasses.replace(
                        dev,
                        interfaces=dev.interfaces.set(
                            iface, dataclasses.replace(node, link=None)
                        ),
                    ),
                )
    candidate = dataclasses.replace(state, devices=devices, links=links)
    for dname, name in sorted(keys):
        dev = candidate.devices[dname]
        if dev.srv6_sids:
            for row in dev.srv6_sids.sids.values():
                if row.interface == name:
                    candidate = srv6.remove_local_sid(
                        candidate, dname, row.sid, row.owner
                    )
        dev = candidate.devices[dname]
        interfaces = dev.interfaces.remove(name)
        for iname, node in interfaces.sorted_items():
            if isinstance(node, EthernetNode) and node.config.aggregate_id == name:
                interfaces = interfaces.set(
                    iname,
                    dataclasses.replace(
                        node, config=dataclasses.replace(node.config, aggregate_id=None)
                    ),
                )
        candidate = dataclasses.replace(
            candidate,
            devices=candidate.devices.set(
                dname, dataclasses.replace(dev, interfaces=interfaces)
            ),
        )
    return candidate


def parse_nexthop(spec: Any) -> Nexthop:
    """``Nexthop`` | address str (recursive) | interface str | ``(interface, address)``
    | ``'blackhole'`` | ``'unreachable'`` | ``'prohibit'`` | ``'receive'``."""
    if isinstance(spec, Nexthop):
        return spec
    if isinstance(spec, tuple):
        iface, addr = spec
        host, af = to_int(addr)
        return Nexthop.via(iface, host, af)
    if isinstance(spec, str):
        specials = {
            'blackhole': BLACKHOLE,
            'unreachable': UNREACHABLE,
            'prohibit': PROHIBIT,
            'receive': RECEIVE_NH,
        }
        if spec in specials:
            return Nexthop(special=specials[spec])
        try:
            host, af = to_int(spec)
        except ValueError:
            return Nexthop.via(spec)
        return Nexthop.recursive(host, af)
    raise TypeError(f'cannot parse next-hop {spec!r}')


class RibClient:
    """The one door for a client's rows in one device and address family."""

    def __init__(
        self, network: Network, device: str, client: ClientId, af: int, distance: int
    ) -> None:
        self.network = network
        self.device = device
        self.client = client
        self.af = af
        self.distance = distance
        self._owner = network.device(device)

    def _check(self, rows: Iterable[Route]) -> tuple[Route, ...]:
        rows = tuple(rows)
        for r in rows:
            if r.source != self.client:
                raise ValueError(f'row {r.key} does not belong to {self.client}')
            if r.af != self.af:
                raise ValueError('row family does not match the client')
        return rows

    def _apply(self, origin: str, **kw: Any) -> Any:
        self._owner._check_valid()
        if self.network._edits is not None:
            edits = self.network._edits
            self._owner._node_in(edits)
            edits.rib_ops.setdefault(self.device, {}).setdefault(self.af, []).append(kw)
            self.network._batch_ops += 1
            return None

        def fn(state: NetworkState) -> NetworkState:
            dev = self._owner._node_in(state)
            rib = dev.ribs.get(self.af) or RibState.empty(self.af)
            new = rib_apply(rib, **kw)
            if new is rib:
                return state
            return _set_device(
                state,
                self.device,
                dataclasses.replace(dev, ribs=dev.ribs.set(self.af, new)),
            )

        return self.network.update(fn, (origin, self.device, self.client.name))

    def add_routes(self, rows: Iterable[Route]) -> Any:
        return self._apply('add_routes', add=self._check(rows))

    def delete_routes(self, keys: Iterable[RowKey]) -> Any:
        keys = tuple(keys)
        for key in keys:
            if key[2] != self.client:
                raise ValueError(f'row {key} does not belong to {self.client}')
        return self._apply('delete_routes', delete=keys)

    def sync(self, rows: Iterable[Route]) -> Any:
        return self._apply('sync', sync=(self.client, self._check(rows)))

    def get_routes(self) -> tuple[tuple[Route, tuple[str, str | None]], ...]:
        dev = self._owner.node
        rib = dev.ribs.get(self.af) or RibState.empty(self.af)
        return tuple(
            (
                r,
                row_status(
                    dev.resolver_input_epoch.get(self.af, 0),
                    dev.resolver_outcomes.get(self.af),
                    r.key,
                ),
            )
            for r in rib.rows_of(self.client)
        )


class Interface(_Handle):
    @property
    def device(self) -> str:
        return self.key[0]

    @property
    def name(self) -> str:
        return self.key[1]

    @property
    def node(self):
        self._check_valid()
        node = self.network._interface_node(self.device, self.name)
        if node is None or node.generation != self.generation:
            raise StaleHandleError(self.key)
        return node

    @property
    def exists(self) -> bool:
        node = self.network._interface_node(self.device, self.name)
        return self._valid and node is not None and node.generation == self.generation

    @property
    def config(self):
        return self.node.config

    @property
    def oper(self):
        return self.node.oper

    @property
    def mac(self) -> int | None:
        return getattr(self.node, 'mac', None)

    @property
    def index(self) -> int:
        return self.node.index

    def l3_usable(self, af: int) -> bool:
        return l3_usable(self.node, af)

    def configure(self, **fields: Any) -> None:
        now = self.network.clock()
        if 'ipv4' in fields:
            fields['ipv4'] = _addresses(fields['ipv4'], IPV4)
        if 'ipv6' in fields:
            fields['ipv6'] = _addresses(fields['ipv6'], IPV6)

        def fn(state: _Edits) -> None:
            self._check_valid()
            interfaces = state.interfaces(self.device)
            node = interfaces.get(self.name)
            if node is None or node.generation != self.generation:
                raise StaleHandleError(self.key)
            cfg = node.config
            if 'admin' in fields and fields['admin'] != cfg.admin:
                fields['admin_since'] = now
            new_cfg = dataclasses.replace(cfg, **fields)
            self.network._validate_interface_config(
                state, self.device, self.name, node, new_cfg
            )
            if new_cfg == cfg:
                return
            interfaces.set(self.name, dataclasses.replace(node, config=new_cfg))

        self.network._edit(fn, ('configure', self.device, self.name))

    def admin_down(self) -> None:
        self.configure(admin=AdminState.DOWN)

    def admin_up(self) -> None:
        self.configure(admin=AdminState.UP)

    @property
    def endpoint(self) -> tuple[str, str]:
        return (self.device, self.name)


class Link(_Handle):
    @property
    def id(self) -> str:
        return self.key[0]

    @property
    def node(self) -> LinkNode:
        self._check_valid()
        link = self.network._link_node(self.id)
        if link is None or link.generation != self.generation:
            raise StaleHandleError(self.id)
        return link

    @property
    def exists(self) -> bool:
        link = self.network._link_node(self.id)
        return self._valid and link is not None and link.generation == self.generation

    @property
    def state(self) -> int:
        return self.node.oper.state

    def _set_state(self, new_state: int) -> None:
        now = self.network.clock()

        def fn(state: _Edits) -> None:
            self._check_valid()
            link = state.links.get(self.id)
            if link is None or link.generation != self.generation:
                raise StaleHandleError(self.id)
            if link.oper.state == new_state:
                return
            state.links.set(
                self.id, dataclasses.replace(link, oper=LinkOper(new_state, now))
            )

        self.network._edit(
            fn, ('link', self.id, 'fail' if new_state == LINK_FAILED else 'restore')
        )

    def fail(self) -> None:
        self._set_state(LINK_FAILED)

    def restore(self) -> None:
        self._set_state(LINK_UP)

    def configure(self, **fields: Any) -> None:
        """Change link config fields (``capacity`` override, ``delay``,
        ``risk_groups``); validated like the rest of the tree."""

        def fn(state: _Edits) -> None:
            self._check_valid()
            link = state.links.get(self.id)
            if link is None or link.generation != self.generation:
                raise StaleHandleError(self.id)
            cfg = dataclasses.replace(link.config, **fields)
            if cfg == link.config:
                return
            state.links.set(self.id, dataclasses.replace(link, config=cfg))

        self.network._edit(fn, ('link', self.id, 'configure'))

    def edge(self, device: str) -> int:
        link = self.node
        tx = link.a if link.a[0] == device else link.b
        if tx[0] != device:
            raise ValueError(f'{device} is not an endpoint of {self.id}')
        return link.edge_id(tx)

    @property
    def endpoints(self) -> tuple[tuple[str, str], tuple[str, str]]:
        link = self.node
        return link.a, link.b


def _set_device(state: NetworkState, name: str, dev: DeviceState) -> NetworkState:
    return dataclasses.replace(state, devices=state.devices.set(name, dev))


__all__ = [
    'Device',
    'Interface',
    'Link',
    'RibClient',
    'StaleHandleError',
    'parse_nexthop',
    'EthernetNode',
    'LoopbackNode',
    'PortChannelNode',
]
