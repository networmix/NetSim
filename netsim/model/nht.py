"""Local next-hop tracking over immutable RIB inputs.

``resolve`` is a prospective query, ``installed`` is the separate programmed
answer. Registration never advances a resolver epoch. ``refresh`` and FIB
resolution canonicalize each NhtResult: identity is C1's notification contract
(including cost, failed lookups and scope), independent of Fib identity.
Epoch-only refreshes reuse the result; NhtTable.input_epochs records the latest
check, while the result retains the epoch that produced its semantic answer. C1 applies register/unregister to its staging root and may call
refresh before publication; remove_client implements reset(purge=True).
"""

from __future__ import annotations

from dataclasses import replace
from typing import overload

from netsim.model.addressing import IPV4, IPV6, mask_for
from netsim.model.contracts import (
    IGP,
    ClientId,
    LookupView,
    NhtKey,
    NhtResult,
    NhtTable,
)
from netsim.model.forwarding import Fib, NeighborTable
from netsim.model.interfaces import l3_usable
from netsim.model.routing import (
    ResolutionContext,
    ResolutionPolicy,
    ResolverOutcome,
    RibState,
    RowKey,
    resolve_candidate,
)
from netsim.model.srv6 import Srv6Policies
from netsim.model.state import (
    DeviceConfig,
    DeviceState,
    NetworkState,
    PMap,
    StateDelta,
    diff_pmap,
    record,
)


@record
class LocalInterface:
    """Only effective resolution inputs; no raw carrier or link endpoints."""

    generation: int
    ipv4_usable: bool
    ipv6_usable: bool


@record
class LocalContext:
    """Detached immutable ResolutionContext and installed-forwarding snapshot.

    Contains selected local inputs, never a Device, DeviceState, NetworkState,
    model view or live callback. Local compiled policies are retained so a
    prospective query uses the same policy-bearing next hops as the resolver.
    """

    interfaces: PMap[str, LocalInterface]
    ribs: PMap[int, RibState]
    neighbors: NeighborTable | None
    config: DeviceConfig
    fibs: PMap[int, Fib]
    resolver_outcomes: PMap[int, ResolverOutcome]
    resolver_input_epoch: PMap[int, int]
    srv6_policies: Srv6Policies | None

    def interface_exists(self, name: str) -> bool:
        return name in self.interfaces

    def interface_generation(self, name: str) -> int | None:
        node = self.interfaces.get(name)
        return node.generation if node is not None else None

    def l3_usable(self, name: str, af: int) -> bool:
        node = self.interfaces.get(name)
        return node is not None and (
            node.ipv4_usable if af == IPV4 else node.ipv6_usable
        )

    def neighbor_mac(self, interface: str, address: int) -> int | None:
        return self.neighbors.mac(interface, address) if self.neighbors else None

    def peer_mac(self, interface: str) -> int | None:
        return self.neighbors.peer_mac(interface) if self.neighbors else None

    def rib(self, af: int) -> RibState:
        return self.ribs.get(af) or RibState.empty(af)


def local_context(dev: DeviceState) -> LocalContext:
    """Build a ResolutionContext using only this device's immutable subtree.

    A runtime may capture it before a callback, then expose prospective
    ``resolve(ctx, policy, key, exclude_rows=..., input_epoch=...)`` and separate
    ``installed(ctx, key)`` answers without keeping a root or device handle.
    """
    return LocalContext(
        PMap(
            (
                name,
                LocalInterface(
                    node.generation, l3_usable(node, IPV4), l3_usable(node, IPV6)
                ),
            )
            for name, node in dev.interfaces.items()
        ),
        dev.ribs,
        dev.neighbors,
        dev.config,
        dev.fibs,
        dev.resolver_outcomes,
        dev.resolver_input_epoch,
        dev.srv6_policies,
    )


def _validate_key(key: NhtKey) -> None:
    if key.af not in (IPV4, IPV6) or not 0 <= key.address < 1 << (
        32 if key.af == IPV4 else 128
    ):
        raise ValueError('invalid NHT address family or address')
    if key.af == IPV6 and key.address >> 118 == 0x3FA and not key.interface:
        raise ValueError('link-local NHT requires an interface scope')
    if key.interface_generation is not None and key.interface is None:
        raise ValueError('NHT interface generation requires an interface scope')


def resolve(
    ctx: ResolutionContext,
    policy: ResolutionPolicy,
    key: NhtKey,
    *,
    exclude_rows: frozenset[RowKey] = frozenset(),
    input_epoch: int,
) -> NhtResult:
    """RFC 4271 prospective resolution, excluding the candidate's own RowKey.

    A self-covering candidate (only its own excluded prefix covers the queried
    address) returns eligible=False, via_prefix=candidate.prefix, SELF_COVERED.
    Default-route permission comes exclusively from key. Metrics are never
    added across recursion levels or different clients. Unlike registration,
    a one-shot query with no interface_generation uses the current scope.
    """
    _validate_key(key)
    if key.interface_generation is not None:
        generation = getattr(ctx, 'interface_generation', lambda _: None)(key.interface)
        if generation != key.interface_generation:
            return NhtResult(
                False,
                input_epoch,
                reason='SCOPE_STALE',
                interfaces=(key.interface,) if key.interface else (),
            )
    answer = resolve_candidate(
        ctx,
        replace(policy, resolve_via_default=key.resolve_via_default),
        key.af,
        key.address,
        exclude_rows=exclude_rows,
        connected_only=key.connected_only,
        interface=key.interface,
    )
    entry = answer.entry
    selected = next(
        (
            row
            for row in reversed(answer.rows)
            if entry is not None and row.key in entry.contributing
        ),
        None,
    )
    eligible = entry is not None or bool(answer.legs)
    cost = None
    cost_source = None
    reason = answer.reason
    if eligible:
        sources = {row.source for row in answer.rows}
        if (
            selected is not None
            and len(sources) == 1
            and (
                selected.source == IGP
                or selected.source in ctx.rib(selected.af).link_state_sources
            )
        ):
            cost, cost_source = selected.metric, selected.source
        else:
            reason = 'COST_UNAVAILABLE'
    return NhtResult(
        eligible,
        input_epoch,
        entry.prefix if entry else answer.excluded_prefix,
        selected.source if selected else None,
        cost,
        cost_source,
        answer.legs,
        answer.queries,
        reason,
        answer.interfaces,
    )


def registration_key(dev: DeviceState, key: NhtKey) -> NhtKey:
    """Validate and bind a registration request to the current scope incarnation.

    Callers may retain this canonical key; register uses it as the table key
    and NhtClient.register returns it. Unscoped keys are returned unchanged.
    """
    _validate_key(key)
    if (
        key.interface_generation is not None
        and dev.nht is not None
        and key in dev.nht.registrations
    ):
        return key  # An existing explicit registration remains an identity no-op.
    if key.interface is not None:
        interface = dev.interfaces.get(key.interface)
        if interface is None or (
            key.interface_generation is not None
            and interface.generation != key.interface_generation
        ):
            raise ValueError('NHT interface scope is missing or stale')
        if key.interface_generation is None:
            return replace(key, interface_generation=interface.generation)
    return key


def registered_key(dev: DeviceState, key: NhtKey) -> NhtKey:
    """Find a retained registration without rebinding to the current interface.

    For compatibility, result/unregister accept the original unbound request
    when exactly one incarnation matches. With multiple matching generations,
    callers must use the canonical key returned by register (or in ctx.nht).
    Explicit keys use an O(1) lookup and work after interface removal.
    """
    table = dev.nht
    if (
        table is None
        or key in table.registrations
        or key.interface is None
        or key.interface_generation is not None
    ):
        return key
    matches = [
        registered
        for registered in table.registrations
        if replace(registered, interface_generation=None) == key
    ]
    if len(matches) > 1:
        raise ValueError('ambiguous NHT scope; use a generation-bound key')
    return matches[0] if matches else key


def register(state: NetworkState, device: str, key: NhtKey) -> NetworkState:
    """Store a generation-bound key; preserve the state-returning mutation API.

    Recreating an interface never retargets a stored registration. Explicitly
    unregister the old key and register again to replace its scope incarnation.
    """
    _validate_key(key)
    dev = state.devices[device]
    table = dev.nht or NhtTable()
    if key in table.registrations:
        return state
    key = registration_key(dev, key)
    if key in table.registrations:
        return state
    return _put(
        state,
        device,
        replace(
            table,
            registrations=table.registrations.set(key, None),
            version=table.version + 1,
        ),
    )


def unregister(state: NetworkState, device: str, key: NhtKey) -> NetworkState:
    dev = state.devices[device]
    key = registered_key(dev, key)
    table = dev.nht
    if table is None or key not in table.registrations:
        return state
    return _put(
        state,
        device,
        replace(
            table,
            registrations=table.registrations.remove(key),
            version=table.version + 1,
        ),
    )


def remove_client(state: NetworkState, device: str, client: ClientId) -> NetworkState:
    """Purge exactly one owner's registrations; absent owners are a no-op."""
    dev = state.devices.get(device)
    if dev is None or dev.nht is None:
        return state
    table = dev.nht
    registrations = table.registrations.builder()
    for key in table.registrations:
        if key.owner == client:
            registrations.remove(key)
    built = registrations.build()
    if built is table.registrations:
        return state
    return _put(
        state, device, replace(table, registrations=built, version=table.version + 1)
    )


def _put(state: NetworkState, device: str, table: NhtTable) -> NetworkState:
    dev = state.devices[device]
    return replace(state, devices=state.devices.set(device, replace(dev, nht=table)))


def refresh(state: NetworkState, device: str, af: int | None = None) -> NetworkState:
    """Recompute all registrations in the selected device/family at current inputs.

    This can run before delayed programming: it does not consume the FIB's
    processed epoch. Calls on a device without registrations are identity no-ops.
    """
    dev = state.devices[device]
    table = dev.nht
    if table is None or not table.registrations:
        return state
    ctx = local_context(dev)
    policy = dev.config.resolution_policy or ResolutionPolicy()
    registrations = table.registrations.builder()
    epochs = table.input_epochs
    for key, old in table.registrations.sorted_items():
        if af is None or key.af == af:
            result = resolve(
                ctx, policy, key, input_epoch=dev.resolver_input_epoch.get(key.af, 0)
            )
            # Provenance epochs do not turn an unchanged answer into a wakeup.
            if old is not None and replace(result, input_epoch=old.input_epoch) == old:
                result = old
            registrations.set(key, result)
            epochs = epochs.set(key.af, dev.resolver_input_epoch.get(key.af, 0))
    built = registrations.build()
    return (
        state
        if built is table.registrations and epochs is table.input_epochs
        else _put(
            state,
            device,
            replace(
                table,
                registrations=built,
                version=table.version + 1,
                input_epochs=epochs,
            ),
        )
    )


@overload
def installed(state: LocalContext, device: NhtKey) -> LookupView: ...


@overload
def installed(state: NetworkState, device: str, key: NhtKey) -> LookupView: ...


def installed(
    state: LocalContext | NetworkState, device: NhtKey | str, key: NhtKey | None = None
) -> LookupView:
    """Installed forwarding and its processed epoch; never a prospective proof.

    Prefer ``installed(local_context(dev), key)`` for detached queries. The
    original ``installed(state, device, key)`` form remains supported.
    """
    if isinstance(state, NetworkState):
        assert isinstance(device, str) and key is not None
        dev = local_context(state.devices[device])
    else:
        assert isinstance(device, NhtKey) and key is None
        dev, key = state, device
    _validate_key(key)
    fib = dev.fibs.get(key.af)
    outcome = dev.resolver_outcomes.get(key.af)
    processed = outcome.processed_epoch if outcome is not None else 0
    entry = fib.lookup(key.address) if fib is not None else None
    group = fib.group(entry) if fib is not None and entry is not None else None
    legs = group.adjacencies if group is not None else ()
    if key.interface is not None:
        node = dev.interfaces.get(key.interface)
        if node is None or (
            key.interface_generation is not None
            and key.interface_generation != node.generation
        ):
            entry, legs = None, ()
        else:
            legs = tuple(leg for leg in legs if leg.interface == key.interface)
    return LookupView(
        key.af,
        key.address,
        entry.prefix if entry is not None else None,
        legs,
        entry.action if entry is not None else None,
        fib.version if fib else 0,
        processed,
        'PENDING'
        if outcome is None or processed < dev.resolver_input_epoch.get(key.af, 0)
        else 'INSTALLED',
    )


def affected_by(delta: StateDelta, state: NetworkState, device: str, af: int) -> bool:
    """Whether recorded lookup ranges or consulted interfaces changed.

    Checks both successful and failed addresses against inserted, removed and
    changed prefixes in *either* AF (cross-family recursion). Coarse delta
    sections are inspected at field level, including neighbor changes.
    The FIB kind still conservatively refreshes all dirty registrations.
    """
    dev = state.devices.get(device)
    if dev is None or dev.nht is None:
        return False
    results = tuple(
        result
        for key, result in dev.nht.registrations.items()
        if key.af == af and result is not None
    )
    if not results:
        return False
    before, after = delta.old.devices.get(device), delta.new.devices.get(device)
    if before is None or after is None:
        return True
    interfaces = {name for result in results for name in result.interfaces}
    interfaces.update(leg.interface for result in results for leg in result.legs)
    changes = delta.interfaces(device)
    if interfaces.intersection(changes.added + changes.removed + changes.changed):
        return True
    if before.neighbors != after.neighbors:
        a = before.neighbors.entries if before.neighbors else None
        b = after.neighbors.entries if after.neighbors else None
        changed = diff_pmap(a, b)
        if any(
            key[0] in interfaces
            for key in changed.added + changed.removed + changed.changed
        ):
            return True
        a = before.neighbors.peers if before.neighbors else None
        b = after.neighbors.peers if after.neighbors else None
        changed = diff_pmap(a, b)
        if interfaces.intersection(changed.added + changed.removed + changed.changed):
            return True
    for family, address, _ in {q for result in results for q in result.queries}:
        old, new = before.ribs.get(family), after.ribs.get(family)
        if old is new:
            continue
        # Bounded prefix-width probes, not a scan over the RIB's row count.
        lengths = set(old.shards if old else ()) | set(new.shards if new else ())
        for length in lengths:
            prefix = address & mask_for(length, 32 if family == IPV4 else 128), length
            if (old.rows(prefix) if old else ()) != (new.rows(prefix) if new else ()):
                return True
    return False
