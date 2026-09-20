"""``Network``: owner of the state tree and the transactional ``update``.

``update(fn, origin)`` applies a pure function to the current root,
validates and canonicalizes the candidate, computes the delta *before*
commit, returns ``None`` for an empty content delta, commits once and
then dispatches the delta to every ``on_delta`` hook in registration order.
Observer failures never roll back the commit or starve other observers; the
first exception is re-raised after dispatch. The commit machinery owns the
per-(device, AF) resolver input epochs.
"""

from __future__ import annotations

import copy
import dataclasses
from contextlib import contextmanager
from typing import Any, Callable, Iterable, Iterator

from netsim.model import derive, routing, srv6
from netsim.model.addressing import IPV4, IPV6, LOCAL_ADMIN_BASE, mac_from_index
from netsim.model.contracts import (
    CONNECTED_PROFILE,
    IGP_PROFILE,
    LOCAL_PROFILE,
    SRV6_LOCAL_PROFILE,
    STATIC_PROFILE,
    AgentNode,
    ClientId,
    ClientProfile,
    DeviceAgent,
    check_agent,
)
from netsim.model.entities import (
    Device,
    Interface,
    Link,
    StaleHandleError,
    _Handle,
    _remove_interfaces,
)
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
    PMap,
    StateDelta,
    canon,
    check_name,
    validate_immutable,
)

DeltaHook = Callable[[float, Any, StateDelta], None]
RouteSource = Callable[[NetworkState, float], NetworkState]


class _MapEdit:
    """An owned PMapBuilder plus a write journal; never part of a state tree."""

    def __init__(self, base: PMap, edits: _Edits) -> None:
        self.base = base
        self.builder = base.builder()
        self.journal = edits.undo
        self.touched: dict[Any, None] = {}

    def get(self, key: Any, default: Any = None) -> Any:
        return self.builder.get(key, default)

    def __getitem__(self, key: Any) -> Any:
        return self.builder[key]

    def __contains__(self, key: Any) -> bool:
        return key in self.builder

    def set(self, key: Any, value: Any) -> None:
        self.journal.append((self, key, key in self.builder, self.get(key)))
        self.touched[key] = None
        self.builder.set(key, value)

    def remove(self, key: Any) -> None:
        self.journal.append((self, key, key in self.builder, self.get(key)))
        self.touched[key] = None
        self.builder.remove(key)

    def items(self) -> Iterator[tuple[Any, Any]]:
        for key in self.base:
            if key in self.builder:
                yield key, self.builder[key]
        for key in self.touched:
            if key not in self.base and key in self.builder:
                yield key, self.builder[key]

    def build(self) -> PMap:
        return canon(self.base, self.builder.build())


def _fold_rib_ops(
    rib: routing.RibState, operations: list[dict[str, Any]]
) -> routing.RibState:
    """Fold pending operations into only changed rows, without copying the RIB.

    A sync discards earlier edits for its client. Only synced clients need a
    scan of existing rows, via the client index; add/delete use keyed lookups.
    """
    changes: dict[ClientId, dict[routing.RowKey, routing.Route | None]] = {}
    synced: set[ClientId] = set()
    for op in operations:
        sync = op.get('sync')
        if sync is not None:
            client, rows = sync
            changes[client] = {row.key: row for row in rows}
            synced.add(client)
        for key in op.get('delete', ()):
            changes.setdefault(key[2], {})[key] = None
        for row in op.get('add', ()):
            changes.setdefault(row.source, {})[row.key] = row
    added: list[routing.Route] = []
    deleted: dict[routing.RowKey, None] = {}
    for client, wanted in changes.items():
        if client in synced:
            for row in rib.rows_of(client):
                if wanted.get(row.key) is None:
                    deleted[row.key] = None
        for key, row in wanted.items():
            shard = rib.shards.get(key[1])
            previous = shard.get(key) if shard is not None else None
            if row == previous:
                continue
            if row is None:
                deleted[key] = None
            else:
                added.append(row)
    if not added and not deleted:
        return rib
    return routing.rib_apply(rib, add=tuple(added), delete=tuple(deleted))


class _Edits:
    """Private staging area shared by builder operations within a transaction.

    Only snapshot() publishes immutable records. Explicit reads and pure
    update callbacks are snapshot boundaries; ordinary operations never freeze
    top-level or per-device maps. Failed operations undo only their writes.
    """

    def __init__(self, base: NetworkState) -> None:
        self.base = base
        self.undo: list[tuple[_MapEdit, Any, bool, Any]] = []
        self.devices = _MapEdit(base.devices, self)
        self.links = _MapEdit(base.links, self)
        self.demands = _MapEdit(base.demands, self)
        self.ifindices = _MapEdit(base.allocators.next_ifindex, self)
        self.allocators = base.allocators
        self.interface_maps: dict[str, _MapEdit] = {}
        self.rib_ops: dict[str, dict[int, list[dict[str, Any]]]] = {}

    def interfaces(self, name: str) -> _MapEdit:
        result = self.interface_maps.get(name)
        if result is None:
            result = self.interface_maps[name] = _MapEdit(
                self.devices[name].interfaces, self
            )
        return result

    def take_ifindex(self, name: str) -> int:
        index = self.ifindices.get(name, 1)
        self.ifindices.set(name, index + 1)
        return index

    def apply(self, fn: Callable[[_Edits], None]) -> None:
        allocators = self.allocators
        try:
            fn(self)
        except BaseException:
            for mapping, key, existed, value in reversed(self.undo):
                if existed:
                    mapping.builder.set(key, value)
                else:
                    mapping.builder.remove(key)
            self.allocators = allocators
            raise
        finally:
            self.undo.clear()

    def device_snapshot(self, name: str) -> DeviceState:
        dev = self.devices[name]
        fields: dict[str, Any] = {}
        if name in self.interface_maps:
            fields['interfaces'] = self.interface_maps[name].build()
        operations = self.rib_ops.get(name)
        if operations:
            ribs = dev.ribs.builder()
            base_dev = self.base.devices.get(name)
            for af, pending in operations.items():
                rib = dev.ribs.get(af)
                if rib is None:
                    rib = routing.RibState.empty(af)
                new = _fold_rib_ops(rib, pending)
                # Reads are freeze boundaries, not commits. Undoing a change
                # after a read must still recover the original RIB identity
                # and version when its rows return to the baseline contents.
                base_rib = base_dev.ribs.get(af) if base_dev is not None else None
                if base_rib is not None and new.shards == base_rib.shards:
                    new = base_rib
                if base_rib is None and not new.shards:
                    ribs.remove(af)
                elif new is not rib:
                    ribs.set(af, new)
            fields['ribs'] = ribs.build()
        new_dev = canon(dev, dataclasses.replace(dev, **fields))
        if new_dev is not dev:
            # Publish the frozen device into staging, without recording a
            # builder-operation undo entry. The immutable base remains the
            # last update boundary used for epoch invalidation.
            self.devices.builder.set(name, new_dev)
            self.devices.touched[name] = None
        self.rib_ops.pop(name, None)
        return new_dev

    def snapshot(self) -> NetworkState:
        touched = dict.fromkeys(self.interface_maps)
        touched.update(dict.fromkeys(self.rib_ops))
        for name in touched:
            self.device_snapshot(name)
        candidate = dataclasses.replace(
            self.base,
            devices=self.devices.build(),
            links=self.links.build(),
            demands=self.demands.build(),
            allocators=canon(
                self.base.allocators,
                dataclasses.replace(
                    self.allocators, next_ifindex=self.ifindices.build()
                ),
            ),
        )
        return canon(self.base, candidate)


def mark_published(error: BaseException) -> None:
    """Mark *error* as raised by an observer after the commit was published:
    the new root stands and the work that produced it is consumed."""
    try:
        error.netsim_published = True  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover - exotic exception types
        pass


def published_failure(error: BaseException) -> bool:
    """Whether *error* came from an observer after a successful publication."""
    return bool(getattr(error, 'netsim_published', False))


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
            SRV6_LOCAL_PROFILE.client: SRV6_LOCAL_PROFILE,
        }
        self.sources: list[RouteSource] = []
        self.agents: dict[tuple[str, str], DeviceAgent] = {}
        """Registered plugin objects by (device, agent name); the tree keeps
        the matching ``AgentNode`` (config, state, generation)."""
        self.capacity_model: int = 1  # flows.UNCONSTRAINED
        self.debug_validate = DEBUG_VALIDATE
        self._dispatching = False
        self._edits: _Edits | None = None
        self._batch_ops = 0
        self._handle_incarnation = 0
        self._batch_handles: list[_Handle] = []
        self._handles: dict[tuple, _Handle] = {}
        self.ngraph_link_ids: dict[str, str] = {}
        """NetGraph link id → NetSim link id, filled by the NetGraph adapter."""
        self._mac_base = LOCAL_ADMIN_BASE | ((seed & 0xFF) << 32)

    # -- state and update -----------------------------------------------------

    @property
    def state(self) -> NetworkState:
        return self._edits.snapshot() if self._edits is not None else self._state

    @contextmanager
    def batch(self) -> Iterator[Network]:
        """Stage builder operations and commit once, with origin ('batch', n_ops).

        Yields this network. Nested batches are rejected. n_ops counts primitive
        builder/update calls (including no-ops); add_p2p contributes three.
        Hooks run only at exit, at the commit clock time. Explicit state/node
        reads and pure update callbacks freeze a snapshot of staged work.
        converge() and place() are rejected: derivations must run on committed
        roots, after exiting the batch.

        An exception aborts the tree and allocators. Handles for entities born
        in the batch are permanently invalidated on abort, even if a later
        entity reuses the same generation. Exceptions from post-commit hooks
        retain the committed tree and do not prevent other hooks from running,
        as for update().
        """
        if self._edits is not None or self._dispatching:
            raise RuntimeError('nested Network.batch()')
        old = self._state
        self._edits = _Edits(old)
        self._batch_ops = 0
        self._batch_handles = []
        try:
            yield self
            # Account for builder edits since the last pure update callback
            # before comparing against the batch-entry root at commit.
            candidate = self._edits.snapshot()
            if self._edits.base is not old:
                candidate = derive.bump_epochs(self._edits.base, candidate)
            count = self._batch_ops
            self._edits = None
            self.update(lambda _: candidate, ('batch', count))
        finally:
            self._edits = None
            if self._state is old:
                # Only escaped provisional handles need invalidation. An
                # existing entity first looked up in the block stays valid.
                invalidated = False
                for handle in self._batch_handles:
                    if handle.generation >= old.allocators.next_generation:
                        handle._valid = False
                        key = (type(handle).__name__, handle.key, handle.generation)
                        del self._handles[key]
                        invalidated = True
                if invalidated:
                    self._handle_incarnation += 1
            self._batch_handles = []

    def _edit(self, fn: Callable[[_Edits], None], origin: Any) -> None:
        if self._dispatching:
            raise RuntimeError('nested Network.update() from a derivation or observer')
        edits = self._edits
        if edits is not None:
            edits.apply(fn)
            self._batch_ops += 1
        else:
            edits = _Edits(self._state)
            edits.apply(fn)
            self.update(lambda _: edits.snapshot(), origin)

    def _device_node(self, name: str) -> DeviceState | None:
        if self._edits is not None:
            return self._edits.devices.get(name)
        return self._state.devices.get(name)

    def _interface_node(self, device: str, name: str) -> Any:
        if self._edits is not None:
            if device not in self._edits.devices:
                return None
            return self._edits.interfaces(device).get(name)
        dev = self._state.devices.get(device)
        return dev.interfaces.get(name) if dev is not None else None

    def _link_node(self, lid: str) -> LinkNode | None:
        if self._edits is not None:
            return self._edits.links.get(lid)
        return self._state.links.get(lid)

    def fork(self) -> Network:
        """A new ``Network`` starting from this one's current root.

        The tree is immutable, so nothing is copied: the fork shares the
        root and diverges on its first update. Seed, client profiles,
        route sources, capacity model and NetGraph ids carry over; hooks,
        handles and any ``Simulation`` binding do not. This is how a study
        runs many failure iterations from one converged baseline.
        """
        net = Network(seed=self.seed)
        net._state = self.state
        net.profiles = dict(self.profiles)
        net.sources = list(self.sources)
        net.agents = dict(self.agents)
        net.capacity_model = self.capacity_model
        net.debug_validate = self.debug_validate
        net.ngraph_link_ids = dict(self.ngraph_link_ids)
        # Adapter metadata (units, demand labels, allocators) lives beside the
        # tree; a fork keeps its own copy so imports into the fork stay
        # consistent with what the shared root already contains.
        for name, value in vars(self).items():
            if name.startswith('netsim_'):
                setattr(net, name, copy.deepcopy(value))
        return net

    def update(
        self, fn: Callable[[NetworkState], NetworkState], origin: Any = 'op'
    ) -> StateDelta | None:
        """Apply a pure edit; batches stage it until their single outer commit.

        After publication, every observer runs even if earlier observers fail.
        Failures never roll back the root; the first is re-raised after dispatch.
        """
        if self._dispatching:
            raise RuntimeError('nested Network.update() from a derivation or observer')
        if self._edits is not None:
            base = self._edits.base
            snapshot = self._edits.snapshot()
            candidate = fn(derive.bump_epochs(base, snapshot))
            # The callback may change inputs too. Invalidate its result before
            # adopting it as the baseline for subsequent staged operations.
            candidate = derive.bump_epochs(base, candidate)
            self._edits = _Edits(candidate)
            self._batch_ops += 1
            return None
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
        errors: list[BaseException] = []
        try:
            for hook in list(self.on_delta):
                try:
                    hook(self.clock(), origin, delta)
                except BaseException as error:
                    errors.append(error)
        finally:
            self._dispatching = False
        if errors:
            # The root is published: observers ran after the commit and may
            # not roll it back. The original exception propagates, marked so
            # callers can tell it apart from a failure before publication.
            mark_published(errors[0])
            raise errors[0]
        return delta

    def _handle(self, cls: type, key: tuple, generation: int) -> Any:
        cache_key = (cls.__name__, key, generation)
        h = self._handles.get(cache_key)
        if h is None:
            h = self._handles[cache_key] = cls(self, key, generation)
            if self._edits is not None:
                self._batch_handles.append(h)
        return h

    # -- clients --------------------------------------------------------------

    def register_client(self, profile: ClientProfile) -> None:
        if profile.client in self.profiles:
            raise ValueError(f'client {profile.client} already registered')
        self.profiles[profile.client] = profile

    def add_source(self, source: RouteSource) -> None:
        self.sources.append(source)

    # -- agents ------------------------------------------------------------------

    def add_agent(
        self, device: Device | str, agent: DeviceAgent, name: str | None = None
    ) -> AgentNode:
        """Register a protocol plugin on a device (Gate C contract).

        Validates the plugin, registers its client profile (or checks it
        against an existing registration of the same client), and commits an
        ``AgentNode`` with a fresh generation. The runtime schedules
        ``on_init`` from the resulting delta; ``converge()`` never runs agents.
        One agent per name and one agent per client on a device.
        """
        check_agent(agent)
        owner = self.device(device) if isinstance(device, str) else device
        agent_name = check_name(agent.client.name if name is None else name)
        existing = self.profiles.get(agent.client)
        if existing is None:
            self.profiles[agent.client] = agent.profile
        elif existing != agent.profile:
            raise ValueError(
                f'client {agent.client} is registered with a different profile'
            )
        client = agent.client
        config = agent.config

        def apply(state: NetworkState) -> NetworkState:
            dev = owner._node_in(state)
            if agent_name in dev.agents:
                raise ValueError(f'agent {agent_name!r} exists on {owner.name!r}')
            for other in dev.agents.values():
                if other.client == client:
                    raise ValueError(
                        f'client {client} is owned by agent {other.name!r} on {owner.name!r}'
                    )
            allocators, generation = state.allocators.take_generation()
            node = AgentNode(agent_name, generation, client, config)
            dev = dataclasses.replace(dev, agents=dev.agents.set(agent_name, node))
            return dataclasses.replace(
                state,
                devices=state.devices.set(owner.name, dev),
                allocators=allocators,
            )

        self.update(apply, ('add_agent', owner.name, agent_name))
        self.agents[(owner.name, agent_name)] = agent
        node = self.state.devices[owner.name].agents[agent_name]
        assert isinstance(node, AgentNode)
        return node

    def remove_agent(self, device: Device | str, name: str) -> None:
        """Remove an agent's node and plugin; the runtime cancels its timers,
        sessions and inbox from the delta (``StateDelta.agents``). Its routes,
        policies, SIDs and registrations stay until removed explicitly."""
        owner = self.device(device) if isinstance(device, str) else device

        def apply(state: NetworkState) -> NetworkState:
            dev = owner._node_in(state)
            if name not in dev.agents:
                raise KeyError((owner.name, name))
            dev = dataclasses.replace(dev, agents=dev.agents.remove(name))
            return dataclasses.replace(
                state, devices=state.devices.set(owner.name, dev)
            )

        self.update(apply, ('remove_agent', owner.name, name))
        self.agents.pop((owner.name, name), None)

    def agent(self, device: str, name: str) -> DeviceAgent:
        return self.agents[(device, name)]

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

        def fn(state: _Edits) -> None:
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
            state.devices.set(name, dev)
            state.allocators = allocators

        self._edit(fn, ('add_device', name))
        return self.device(name)

    def remove_device(self, name: str) -> None:
        """Reject deletion while dependent demands remain in the staged tree.

        A demand depends on its source device and on the owner of its effective
        destination (the packet template overrides demand.dst). Ownership is
        a configured IPv4/IPv6 interface or loopback host, local SID prefix,
        or BSID, regardless of oper state; shared addresses still count.
        Connected subnets and locator summaries alone do not imply ownership.

        Remove dependent demands first, then the device, inside batch() to
        publish both deletions atomically. Rejection changes neither topology
        nor demands; an uncaught rejection rolls the whole batch back.
        """
        owner = self.device(name)

        def apply(state: NetworkState) -> NetworkState:
            dev = owner._node_in(state)
            addresses = {
                (af, address)
                for interface in dev.interfaces.values()
                for af, configured in (
                    (IPV4, interface.config.ipv4),
                    (IPV6, interface.config.ipv6),
                )
                for address, _ in configured
            }
            sids = (
                tuple((sid.sid, sid.length) for sid in dev.srv6_sids.sids.values())
                if dev.srv6_sids
                else ()
            )
            bsids = dev.srv6_policies.bsids if dev.srv6_policies else ()
            dependent = []
            for demand_id, demand in state.demands.sorted_items():
                packet = demand.template if demand.template is not None else demand
                if (
                    demand.source == name
                    or (packet.af, packet.dst) in addresses
                    or (
                        packet.af == IPV6
                        and (
                            packet.dst in bsids
                            or any(srv6.contains(prefix, packet.dst) for prefix in sids)
                        )
                    )
                ):
                    dependent.append(demand_id)
            if dependent:
                raise ValueError(
                    f'cannot remove device {name!r}: dependent demands {dependent!r}; '
                    'remove them first (in the same batch for atomic deletion)'
                )
            candidate = _remove_interfaces(
                state, {(name, iface) for iface in dev.interfaces}
            )
            return dataclasses.replace(
                candidate, devices=candidate.devices.remove(name)
            )

        self.update(apply, ('remove_device', name))
        for key in [k for k in self.agents if k[0] == name]:
            del self.agents[key]

    def device(self, name: str) -> Device:
        dev = self._device_node(name)
        if dev is None:
            raise KeyError(name)
        return self._handle(Device, (name,), dev.generation)

    @property
    def devices(self) -> dict[str, Device]:
        return {n: self.device(n) for n, _ in self.state.devices.sorted_items()}

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

        def fn(state: _Edits) -> None:
            device._check_valid()
            dev = state.devices.get(device.name)
            if dev is None or dev.generation != device.generation:
                raise StaleHandleError(device.name)
            interfaces = state.interfaces(device.name)
            if name in interfaces:
                raise ValueError(f'{device.name} already has interface {name!r}')
            allocators, gen = state.allocators.take_generation()
            index = state.take_ifindex(device.name)
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
            interfaces.set(name, node)
            state.allocators = allocators

        self._edit(fn, ('add_interface', device.name, name))
        return device.interface(name)

    def _validate_interface_config(
        self, state: _Edits, device: str, name: str, node: Any, cfg: Any
    ) -> None:
        interfaces = state.interfaces(device)
        for dname, dev in state.devices.items():
            if dev.srv6_sids is not None:
                for loc in dev.srv6_sids.locators.values():
                    if any(srv6.contains(loc.block, addr) for addr, _ in cfg.ipv6):
                        raise ValueError(
                            f'locator block on {dname} covers interface address'
                        )
        if isinstance(cfg, EthernetConfig):
            if cfg.speed <= 0 or cfg.metric <= 0:
                raise ValueError('speed and metric must be positive')
            if cfg.aggregate_id is not None:
                if cfg.ipv4 or cfg.ipv6:
                    raise ValueError(
                        f'{device}:{name} is a bundle member and may not carry addresses'
                    )
                po = interfaces.get(cfg.aggregate_id)
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
        for other_name, other in interfaces.items():
            c = cfg if other_name == name else other.config
            for af_addrs in (c.ipv4, c.ipv6):
                for host, plen in af_addrs:
                    if (host, plen) in seen:
                        raise ValueError(f'duplicate address on {device}')
                    seen.add((host, plen))

    def _check_single_partner(
        self, state: _Edits, device: str, po: str, extra: Any = None
    ) -> None:
        """Reject a bundle whose members terminate on different peer bundles.

        ``extra`` is a member node not (yet) in the tree; a member without a
        link has no partner and cannot conflict.
        """
        interfaces = state.interfaces(device)
        partners = set()
        members = [
            n
            for _, n in interfaces.items()
            if isinstance(n, EthernetNode) and n.config.aggregate_id == po
        ]
        if extra is not None and extra.name not in interfaces:
            members.append(extra)
        for m in members:
            if m.link is None or m.name not in interfaces:
                continue
            peer = state.links[m.link].other((device, m.name))
            pnode = state.interfaces(peer[0]).get(peer[1])
            if (
                isinstance(pnode, EthernetNode)
                and pnode.config.aggregate_id is not None
            ):
                partners.add((peer[0], pnode.config.aggregate_id))
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
        if isinstance(a, Interface):
            a._check_valid()
        if isinstance(b, Interface):
            b._check_valid()
        ea = a.endpoint if isinstance(a, Interface) else a
        eb = b.endpoint if isinstance(b, Interface) else b
        if delay < 0:
            raise ValueError('link delay must be non-negative')
        lid = link_id(ea, eb)
        now = self.clock()

        def fn(state: _Edits) -> None:
            if ea[0] == eb[0]:
                raise ValueError('a link needs two different devices')
            if lid in state.links:
                raise ValueError(f'link {lid} exists')
            nodes = {}
            for dev_name, if_name in (ea, eb):
                dev = state.devices.get(dev_name)
                node = state.interfaces(dev_name).get(if_name) if dev else None
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
            for (dev_name, if_name), node in nodes.items():
                state.interfaces(dev_name).set(
                    if_name, dataclasses.replace(node, link=lid)
                )
            state.links.set(lid, link)
            state.allocators = allocators
            for dev_name, if_name in (ea, eb):
                node = state.interfaces(dev_name)[if_name]
                if node.config.aggregate_id is not None:
                    self._check_single_partner(
                        state, dev_name, node.config.aggregate_id
                    )

        self._edit(fn, ('add_link', lid))
        return self.link(lid)

    def link(self, lid: str) -> Link:
        link = self._link_node(lid)
        if link is None:
            raise KeyError(lid)
        return self._handle(Link, (lid,), link.generation)

    @property
    def links(self) -> dict[str, Link]:
        return {i: self.link(i) for i, _ in self.state.links.sorted_items()}

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
        if self._edits is not None:
            raise RuntimeError('converge() inside batch(): commit the batch first')
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

        def fn(state: _Edits) -> None:
            if demand.source not in state.devices:
                raise ValueError(f'unknown source device {demand.source!r}')
            if state.demands.get(id) == demand:
                return
            state.demands.set(id, demand)

        self._edit(fn, ('add_demand', id))
        return demand

    def remove_demand(self, id: str) -> None:
        self._edit(
            lambda state: state.demands.remove(id),
            ('remove_demand', id),
        )

    def set_capacity_model(self, model: int) -> None:
        self.capacity_model = model

    def _placement(self, state: NetworkState) -> NetworkState:
        from netsim.model.flows import derive_placement

        return derive_placement(state, self.capacity_model)

    def place(self) -> Any:
        """Clock-free placement over the current FIBs; returns the report."""
        if self._edits is not None:
            raise RuntimeError('place() inside batch(): commit the batch first')
        self.update(self._placement, 'place')
        return self.state.placement

    @property
    def placement(self) -> Any:
        return self.state.placement

    # -- forwarding -------------------------------------------------------------------

    def view(self, device: str) -> _View:
        return _View(self.state, device)

    def trace(self, device: Device | str, packet: Any, max_hops: int = 64):
        from netsim.model.forwarding import trace as _trace

        name = device.name if isinstance(device, Device) else device
        state = self.state
        return _trace(lambda d: _View(state, d), name, packet, max_hops)

    def validate(self) -> list[str]:
        state = self.state
        problems: list[str] = srv6.validate(state)
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


__all__ = ['Network', 'RouteSource', 'DeltaHook', 'mark_published', 'published_failure']
