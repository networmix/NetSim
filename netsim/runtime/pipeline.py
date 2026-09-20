"""The coordinator: kinds, rounds and stage events.

The engine heap orders events by ``(time, priority, eid)``; it knows
nothing about dependencies. The coordinator turns kind priorities into
**rounds**: a round at time ``t`` is the set of kind runs scheduled at
``t`` before its ``ROUND_END`` sentinel. Work for a kind that has not run
yet in the active round joins it; work for a kind that already ran joins
the successor round at the same time; work due later belongs to that
later time. Every round counts against a convergence limit.
"""

from __future__ import annotations

import dataclasses
from heapq import heapify, heappop, heappush
from typing import Any, Callable

from netsim import core
from netsim.model import derive
from netsim.model.interfaces import EthernetNode, PortChannelNode
from netsim.model.state import NetworkState, StateDelta

ROUND_END = core.DEFERRED + 8
SETTLED = core.DEFERRED + 9

COALESCE = 1
DEBOUNCE = 2

KIND_NAMES = {
    derive.CARRIER: 'carrier',
    derive.LAG: 'lag',
    derive.L3: 'l3',
    derive.IGP: 'igp',
    derive.AGENT: 'agent',
    derive.FIB: 'fib',
    derive.TRANSPORT: 'transport',
    derive.PLACEMENT: 'placement',
}


class ConvergenceError(RuntimeError):
    def __init__(self, time: float, rounds: int, last_origins: tuple[Any, ...]) -> None:
        super().__init__(f'{rounds} rounds at t={time}; last origins {last_origins}')
        self.time = time
        self.rounds = rounds
        self.last_origins = last_origins


class StageEvent(core.Event):
    """A pre-triggered event scheduled at a band priority (the pattern
    ``run(until)`` uses); ``callbacks`` run the coordinator."""

    __slots__ = ('kind', 'target', 'generation')

    def __init__(
        self,
        env: core.Environment,
        priority: int,
        delay: float,
        callback: Callable[[core.Event], None],
        kind: int,
        target: float,
        generation: int,
    ) -> None:
        self.env = env
        self.callbacks: list[core.EventCallback] | None = [callback]
        self._triggered = True
        self._value: Any = None
        self._ok = True
        self._defused = False
        self.kind = kind
        self.target = target
        self.generation = generation
        env.schedule(self, priority, delay)


def _check_delay(delay: float) -> float:
    if not (delay >= 0) or delay == float('inf'):
        raise ValueError(f'delay must be finite and non-negative, got {delay!r}')
    return delay


class Kind:
    """One band: pending work per entity plus the derivation to run."""

    def __init__(
        self,
        offset: int,
        mode: int,
        run: Callable[[NetworkState, float, list[Any]], NetworkState],
        affected: Callable[[StateDelta, NetworkState], set[Any]],
        delay: Callable[[NetworkState, Any], float] | None = None,
    ) -> None:
        self.offset = offset
        self.priority = core.DEFERRED + offset
        self.name = KIND_NAMES[offset]
        self.mode = mode
        self.run = run
        self.affected = affected
        self.delay = delay
        self.pending: dict[Any, tuple[float, int]] = {}
        """entity → (deadline, ticket), authoritative over the lazy heap."""
        self._heap: list[tuple[float, int, Any]] = []
        self._ticket = 0  # per-kind, independent of round generations
        self.retryable: dict[float, dict[Any, int]] = {}

    def _enqueue(self, entity: Any, deadline: float) -> None:
        self._ticket += 1
        self.pending[entity] = (deadline, self._ticket)
        # Unique tickets also prevent comparisons between unlike entity types.
        heappush(self._heap, (deadline, self._ticket, entity))
        self._compact()

    def _compact(self) -> None:
        # Each live pending entry has exactly one matching heap entry. Rebuild
        # only when stale entries outnumber live ones, amortizing the scan.
        if len(self._heap) > 2 * len(self.pending):
            self._heap = [(t, ticket, e) for e, (t, ticket) in self.pending.items()]
            heapify(self._heap)
        # This bounds only this kind's heap. Stale StageEvents stay in the
        # engine queue until consumed; generation and pending-deadline checks
        # make them harmless. Do not remove their scheduled/ROUND_END keys here.

    def _claim_due(self, now: float) -> dict[Any, tuple[float, int]]:
        claimed: dict[Any, tuple[float, int]] = {}
        while self._heap and self._heap[0][0] <= now:
            deadline, ticket, entity = heappop(self._heap)
            pending = self.pending.get(entity)
            if pending is None or pending[1] != ticket:
                continue
            claimed[entity] = self.pending.pop(entity)
        self._compact()
        return claimed


class Pipeline:
    def __init__(
        self,
        env: core.Environment,
        network: Any,
        kinds: list[Kind],
        *,
        max_rounds_per_timestamp: int = 10_000,
        on_settled: Callable[[float], None] | None = None,
    ) -> None:
        self.env = env
        self.network = network
        self.kinds = sorted(kinds, key=lambda k: k.priority)
        self.by_offset = {k.offset: k for k in self.kinds}
        self.max_rounds = max_rounds_per_timestamp
        self.on_settled = on_settled
        # Per-time round bookkeeping.
        self.round_time: float | None = None
        self.round_gen: int = 0
        self.rounds_at_time: int = 0
        self.ran_this_round: set[int] = set()
        self.successor: dict[int, set[Any]] = {}
        self.scheduled: set[tuple[int, float, int]] = set()
        self.round_end_scheduled: dict[float, int] = {}
        self.last_origins: list[Any] = []
        self.suspended = False

    # -- dispatch from deltas -----------------------------------------------

    def on_delta(self, time: float, origin: Any, delta: StateDelta) -> None:
        if self.suspended:
            return
        state = self.network.state
        for kind in self.kinds:
            entities = kind.affected(delta, state)
            if entities:
                self.mark(kind, entities, time, state)

    def mark(
        self,
        kind: Kind,
        entities: set[Any],
        now: float,
        state: NetworkState | None = None,
    ) -> None:
        st: NetworkState = self.network.state if state is None else state
        for entity in sorted(entities, key=repr):
            delay = (
                _check_delay(kind.delay(st, entity)) if kind.delay is not None else 0.0
            )
            self.schedule_entity(kind, entity, now + delay, now)

    def schedule_entity(
        self, kind: Kind, entity: Any, target: float, now: float
    ) -> None:
        target = max(target, now)
        existing = kind.pending.get(entity)
        if kind.mode == COALESCE:
            if existing is not None and existing[0] <= target:
                return  # joins the pending run
        else:
            if existing is not None and existing[0] == target:
                return
        kind._enqueue(entity, target)
        if target > now:
            self._ensure_event(kind, target, generation=0)
            return
        # Work due now: current round if the kind has not run yet, else the successor.
        if self.round_time == now and kind.offset in self.ran_this_round:
            self.successor.setdefault(kind.offset, set()).add(entity)
            return
        self._ensure_event(kind, now, self._generation_for(now))

    def round_open(self, time: float) -> bool:
        """Whether a round at *time* is still going to end (``ROUND_END`` pending)."""
        return (
            self.round_time == time
            and self.round_end_scheduled.get(time) == self.round_gen
        )

    def round_of(self, time: float) -> int:
        """Round index of a delta committed at *time* (0 for the first round)."""
        return self.round_gen if self.round_time == time else 0

    def _generation_for(self, time: float) -> int:
        if self.round_time != time:
            self.round_time = time
            self.round_gen = 0
            self.rounds_at_time = 0
            self.ran_this_round = set()
            self.successor = {}
        return self.round_gen

    def _ensure_event(self, kind: Kind, target: float, generation: int) -> None:
        key = (kind.offset, target, generation)
        if key in self.scheduled:
            return
        self.scheduled.add(key)
        StageEvent(
            self.env,
            kind.priority,
            target - self.env.now,
            self._run_kind,
            kind.offset,
            target,
            generation,
        )
        if (
            target not in self.round_end_scheduled
            or self.round_end_scheduled[target] < generation
        ):
            self.round_end_scheduled[target] = generation
            StageEvent(
                self.env,
                ROUND_END,
                target - self.env.now,
                self._round_end,
                -1,
                target,
                generation,
            )

    # -- running -------------------------------------------------------------

    def _run_kind(self, event: core.Event) -> None:
        assert isinstance(event, StageEvent)
        now = self.env.now
        gen = self._generation_for(now)
        self.scheduled.discard((event.kind, event.target, event.generation))
        if event.generation != gen:
            return  # stale generation
        kind = self.by_offset[event.kind]
        # Claim before running: a cause raised by this very commit (the kind
        # re-dirtying itself) must create fresh work for the successor round.
        claimed = kind._claim_due(now)
        due = sorted(claimed, key=repr)
        if not due:
            self.ran_this_round.add(kind.offset)
            return
        self.ran_this_round.add(kind.offset)
        self.last_origins.append((kind.name, now, gen))
        self.last_origins = self.last_origins[-8:]
        try:
            self.network.update(
                lambda state: kind.run(state, now, due), ('kind', kind.name, gen)
            )
        except Exception:
            retryable = kind.retryable.setdefault(now, {})
            for e in due:
                deadline, ticket = claimed[e]
                if e not in kind.pending:
                    kind.pending[e] = (deadline, ticket)
                    heappush(kind._heap, (deadline, ticket, e))
                retryable[e] = ticket
            raise

    def _round_end(self, event: core.Event) -> None:
        assert isinstance(event, StageEvent)
        now = self.env.now
        if event.generation != self.round_gen or self.round_time != now:
            return
        self.rounds_at_time += 1
        if self.rounds_at_time > self.max_rounds:
            raise ConvergenceError(now, self.rounds_at_time, tuple(self.last_origins))
        successor = self.successor
        if successor:
            self.round_gen += 1
            self.ran_this_round = set()
            self.successor = {}
            gen = self.round_gen
            for offset in sorted(successor):
                kind = self.by_offset[offset]
                self._ensure_event(kind, now, gen)
            return
        # The round is closed: later work at this time starts a fresh round.
        self.round_gen += 1
        self.ran_this_round = set()
        self.round_end_scheduled.pop(now, None)  # bookkeeping must not grow with time
        if self.on_settled is not None:
            self.on_settled(now)

    def retry(self) -> None:
        now = self.env.now
        for kind in self.kinds:
            if kind.retryable:
                for _t, entities in kind.retryable.items():
                    for e, ticket in entities.items():
                        pending = kind.pending.get(e)
                        # Newer requests (even completed/cancelled ones) win.
                        if pending is not None and pending[1] == ticket:
                            if pending[0] < now:
                                kind._enqueue(e, now)
                kind.retryable = {}
                self._ensure_event(kind, now, self._generation_for(now))

    def has_pending_now(self) -> bool:
        return self.env.peek() == self.env.now


# ---------------------------------------------------------------------------
# Kind construction for a network
# ---------------------------------------------------------------------------


def _ethernets_of(state: NetworkState, device: str) -> set[tuple[str, str]]:
    dev = state.devices.get(device)
    if dev is None:
        return set()
    return {
        (device, n)
        for n, node in dev.interfaces.items()
        if isinstance(node, EthernetNode)
    }


def _bundles_of(state: NetworkState, device: str) -> set[tuple[str, str]]:
    dev = state.devices.get(device)
    if dev is None:
        return set()
    out = {
        (device, n)
        for n, node in dev.interfaces.items()
        if isinstance(node, PortChannelNode)
    }
    for _d, n in list(out):
        for m in derive.bundle_members(dev, n):
            pk = derive.peer_bundle_key(state, device, m.name)
            if pk is not None:
                out.add(pk)
    return out


def _link_endpoints(delta: StateDelta, state: NetworkState) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    for lid in delta.links().keys:
        link = state.links.get(lid) or delta.old.links.get(lid)
        if link is not None:
            out.add(link.a)
            out.add(link.b)
    return out


def _interface(state: NetworkState, key: tuple[str, str]):
    dev = state.devices.get(key[0])
    return dev.interfaces.get(key[1]) if dev is not None else None


def carrier_affected(delta: StateDelta, state: NetworkState) -> set[Any]:
    out: set[tuple[str, str]] = set(_link_endpoints(delta, state))
    d = delta.devices()
    for name in d.added + d.changed:
        if delta.config_changed(name):
            for e in _ethernets_of(state, name):
                out.add(e)
                peer = derive.peer_endpoint(state, e[0], e[1])
                if peer is not None:
                    out.add(peer)
        for iface, cfg_changed, _ in delta.interface_changes(name):
            if cfg_changed:
                out.add((name, iface))
                for root in (delta.old, state):
                    if isinstance(_interface(root, (name, iface)), EthernetNode):
                        peer = derive.peer_endpoint(root, name, iface)
                        if peer is not None:
                            out.add(peer)
    return {e for e in out if isinstance(_interface(state, e), EthernetNode)}


def _bundle_of(state: NetworkState, key: tuple[str, str]) -> tuple[str, str] | None:
    node = _interface(state, key)
    if isinstance(node, PortChannelNode):
        return key
    if isinstance(node, EthernetNode) and node.config.aggregate_id is not None:
        return key[0], node.config.aggregate_id
    return None


def lag_affected(delta: StateDelta, state: NetworkState) -> set[Any]:
    out: set[tuple[str, str]] = set()
    d = delta.devices()
    for name in d.added + d.changed:
        if delta.config_changed(name):
            out |= _bundles_of(state, name)
        for iface, cfg, oper in delta.interface_changes(name):
            if cfg or (
                oper and isinstance(_interface(state, (name, iface)), EthernetNode)
            ):
                for root in (delta.old, state):
                    bundle = _bundle_of(root, (name, iface))
                    if bundle is not None:
                        out.add(bundle)
                        node = _interface(root, (name, iface))
                        if isinstance(node, EthernetNode):
                            peer = derive.peer_bundle_key(root, name, iface)
                            if peer is not None:
                                out.add(peer)
    for endpoint in _link_endpoints(delta, state):
        for root in (delta.old, state):
            bundle = _bundle_of(root, endpoint)
            if bundle is not None:
                out.add(bundle)
    return out


def _l3_related(state: NetworkState, key: tuple[str, str]) -> set[tuple[str, str]]:
    """Local/peer L3 owners, including bundle owners for member config changes."""
    out = {key}
    node = _interface(state, key)
    if isinstance(node, EthernetNode):
        owner = _bundle_of(state, key)
        if owner is not None:
            out.add(owner)
        peer = derive.peer_endpoint(state, *key)
        if peer is not None:
            out.add(peer)
            owner = _bundle_of(state, peer)
            if owner is not None:
                out.add(owner)
    elif isinstance(node, PortChannelNode):
        for member in derive.bundle_members(state.devices[key[0]], key[1]):
            peer = derive.peer_endpoint(state, key[0], member.name)
            if peer is not None:
                out.add(_bundle_of(state, peer) or peer)
    return out


def l3_affected(delta: StateDelta, state: NetworkState) -> set[Any]:
    # Per-interface entities coalesce independently; l3_run unions them per
    # device. Plain device names are the full-recompute causes.
    out: set[Any] = set()
    d = delta.devices()
    for name in d.added + d.changed:
        if delta.config_changed(name):
            out.add(name)
        for iface, cfg, oper in delta.interface_changes(name):
            if cfg:
                out |= _l3_related(delta.old, (name, iface))
                out |= _l3_related(state, (name, iface))
            elif oper:
                out.add((name, iface))
    for endpoint in _link_endpoints(delta, state):
        out |= _l3_related(delta.old, endpoint)
        out |= _l3_related(state, endpoint)
    return {
        e
        for e in out
        if (e if isinstance(e, str) else e[0]) in state.devices
        and (isinstance(e, str) or e[0] not in out)
    }


def igp_affected(delta: StateDelta, state: NetworkState) -> set[Any]:
    d = delta.devices()
    if delta.links().keys:
        return {'*'}
    for name in d.added + d.removed + d.changed:
        if delta.config_changed(name) or delta.interface_changes(name):
            return {'*'}
    return set()


def fib_affected(delta: StateDelta, state: NetworkState) -> set[Any]:
    out: set[tuple[str, int]] = set()
    d = delta.devices()
    for name in d.added + d.changed:
        dev = state.devices.get(name)
        if dev is None:
            continue
        if (
            delta.config_changed(name)
            or delta.interface_changes(name)
            or delta.device_field_changed(name, 'neighbors')
            or delta.device_field_changed(name, 'load_balancers')
            or delta.ribs(name).keys  # either family: recursion crosses families
        ):
            for af in derive.AFS:
                out.add((name, af))
    return out


def placement_affected(delta: StateDelta, state: NetworkState) -> set[Any]:
    if not state.demands:
        # The last demand went away: one more run empties the report.
        return {'*'} if delta.demands().keys and state.placement is not None else set()
    if delta.links().keys or delta.demands().keys or delta.traffic_classes().keys:
        return {'*'}
    d = delta.devices()
    for name in d.added + d.removed + d.changed:
        if (
            delta.config_changed(name)
            or delta.interface_changes(name)
            or delta.fibs(name).keys
            or delta.load_balancers_changed(name)
        ):
            return {'*'}
    return set()


def build_kinds(
    network: Any, pipeline_ref: dict[str, Any], *, settle_delay: float = 0.0
) -> list[Kind]:
    """Kinds bound to *network*; ``pipeline_ref['pipeline']`` is filled after construction."""

    def carrier_run(
        state: NetworkState, now: float, entities: list[Any]
    ) -> NetworkState:
        pipeline: Pipeline = pipeline_ref['pipeline']
        kind = pipeline.by_offset[derive.CARRIER]

        def effective(device: str, iface: str, raw: bool) -> bool | None:
            node = state.devices[device].interfaces[iface]
            oper = node.oper
            if raw == oper.carrier_effective:
                return raw
            delay = (
                node.config.carrier_delay_up if raw else node.config.carrier_delay_down
            )
            raw_since = oper.carrier_raw_since if raw == oper.carrier_raw else now
            target = max(now, raw_since + delay)
            if now >= target:
                return raw
            pipeline.schedule_entity(kind, (device, iface), target, now)
            return oper.carrier_effective

        return derive.derive_carrier(state, now, entities, effective)

    def lag_run(state: NetworkState, now: float, entities: list[Any]) -> NetworkState:
        pipeline: Pipeline = pipeline_ref['pipeline']
        kind = pipeline.by_offset[derive.LAG]

        def delay_elapsed(key: tuple[str, str], member: str, since: float) -> bool:
            cfg = state.devices[key[0]].interfaces[key[1]].config
            target = since + cfg.member_delay_up
            if now >= target:
                return True
            pipeline.schedule_entity(kind, key, target, now)
            return False

        return derive.derive_lag(state, now, entities, delay_elapsed)

    def l3_run(state: NetworkState, now: float, entities: list[Any]) -> NetworkState:
        return derive.derive_l3(
            state,
            now,
            [e if isinstance(e, str) else (e[0], frozenset((e[1],))) for e in entities],
        )

    def igp_run(state: NetworkState, now: float, entities: list[Any]) -> NetworkState:
        for source in network.sources:
            state = source(state, now)
        return state

    def fib_run(state: NetworkState, now: float, entities: list[Any]) -> NetworkState:
        state = derive.bump_epochs(state, state)
        return derive.derive_fib(state, entities)

    def placement_run(
        state: NetworkState, now: float, entities: list[Any]
    ) -> NetworkState:
        return network._placement(state)

    def fib_delay(state: NetworkState, entity: Any) -> float:
        dev = state.devices.get(entity[0])
        return dev.config.fib_delay if dev is not None else 0.0

    return [
        Kind(derive.CARRIER, DEBOUNCE, carrier_run, carrier_affected),
        Kind(derive.LAG, COALESCE, lag_run, lag_affected),
        Kind(derive.L3, COALESCE, l3_run, l3_affected),
        Kind(derive.IGP, COALESCE, igp_run, igp_affected),
        Kind(derive.FIB, COALESCE, fib_run, fib_affected, fib_delay),
        Kind(
            derive.PLACEMENT,
            COALESCE,
            placement_run,
            placement_affected,
            lambda s, e: settle_delay,
        ),
    ]


def dirty_everything(pipeline: Pipeline, state: NetworkState, now: float) -> None:
    for kind in pipeline.kinds:
        if kind.offset == derive.CARRIER:
            entities = {e for d in state.devices for e in _ethernets_of(state, d)}
        elif kind.offset == derive.LAG:
            entities = {b for d in state.devices for b in _bundles_of(state, d)}
        elif kind.offset == derive.L3:
            entities = set(state.devices)
        elif kind.offset == derive.FIB:
            entities = {(d, af) for d in state.devices for af in derive.AFS}
        else:
            entities = {'*'}
        pipeline.mark(kind, entities, now, state)


__all__ = [
    'Pipeline',
    'Kind',
    'StageEvent',
    'ConvergenceError',
    'build_kinds',
    'dirty_everything',
    'ROUND_END',
    'SETTLED',
    'COALESCE',
    'DEBOUNCE',
    'dataclasses',
]
