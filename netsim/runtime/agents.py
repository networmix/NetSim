"""Agent scheduler: the AGENT kind, subscriptions, inboxes, timers and
publication (Gate C slice C1).

This module owns every runtime object of an agent that is not in the tree:
the inbox deque and its captured prefix, armed timers, the per-run RNG
handoff and stat buffers, the subscription index and the receipt journal.
The tree keeps ``contracts.AgentNode``; the transport keeps queues.

Contract between this runtime and ``netsim.runtime.transport`` (both sides
are implemented by their own slice; the method names below are fixed):

- ``AgentRuntime.deliver(device, agent, entry)`` appends an inbox entry for
  the agent's *current* generation and schedules a run (``run_delay``);
  it returns ``False`` when the agent is gone, the generation is stale or
  the inbox is full (the caller reports the outcome, never drops silently).
  Scheduled deliveries must stamp ``entry.generation`` with the destination
  generation. ``None`` remains compatible with immediate current-generation
  callers. A stale generation is rejected before touching the inbox.
- ``AgentRuntime.generation(device, agent)`` is the live generation or
  ``None``.
- ``TransportRuntime.send_datagram / send_message / session_op`` are called
  by publication with the agent's generation; ``TransportRuntime.
  cancel_agent(device, agent, generation)`` aborts everything of a
  generation on reset or removal. Cancellation is invoked by commit dispatch
  (it must not synchronously call Network.update from that hook).
- Optional connection counters are read from ``transport.budget()['connections']``:
  a mapping of connection id to ``queued_messages`` and ``queued_bytes``;
  absent counters default to zero. No transport runtime object enters Context.
"""

from __future__ import annotations

import math
import random
from collections import deque
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from netsim.model import contracts as c
from netsim.model import derive, interfaces, routing, srv6
from netsim.model.addressing import MacAddress
from netsim.model.network import mark_published
from netsim.model.state import (
    NetworkState,
    PMap,
    StateDelta,
    validate_immutable,
)
from netsim.runtime.pipeline import COALESCE, Kind
from netsim.runtime.subscriptions import SubscriptionIndex

if TYPE_CHECKING:
    from netsim.runtime.simulation import Simulation


def _future(now: float, delay: float) -> float:
    target = now + delay
    if not math.isfinite(target) or target <= now:
        raise ValueError('positive delay must advance the finite float clock')
    return target


@dataclass(frozen=True, slots=True)
class Context:
    """A detached local snapshot: no runtime, model root, handle or closure."""

    now: float
    device: str
    agent: str
    client: c.ClientId
    generation: int
    config: Any
    agent_config: c.AgentConfig
    router_id: int
    interfaces: PMap[str, c.InterfaceView]
    neighbors: tuple[c.NeighborView, ...]
    nht: PMap[c.NhtKey, c.NhtResult | None]
    sid_results: tuple[c.SidResultView, ...]
    srdb_local: Any
    policy_states: PMap[Any, Any]
    agent_state: Any
    srdb_view: Any
    inbox: tuple[c.InboxEntry, ...]
    connections: PMap[int, c.ConnectionView]
    timers: PMap[str, float]
    causes: tuple[c.Cause, ...]
    rng: random.Random
    stats: c.AgentStats
    _rows: PMap[int, tuple[c.RouteView, ...]]
    _fibs: PMap[int, Any]
    _epochs: PMap[int, int]
    _processed: PMap[int, int]

    def rib_view(self, af: int) -> tuple[c.RouteView, ...]:
        return self._rows.get(af, ())

    def lookup(self, af: int, address: int, scope: str | None = None) -> c.LookupView:
        fib = self._fibs.get(af)
        entry = fib.lookup(address) if fib else None
        group = fib.group(entry) if fib and entry else None
        legs = group.adjacencies if group else ()
        if scope is not None:
            legs = tuple(leg for leg in legs if leg.interface == scope)
        processed = self._processed.get(af, 0)
        return c.LookupView(
            af,
            address,
            entry.prefix if entry else None,
            legs,
            entry.action if entry else None,
            fib.version if fib else 0,
            processed,
            'PENDING' if self._epochs.get(af, 0) != processed else 'INSTALLED',
        )


@dataclass(slots=True)
class _Capture:
    inbox: tuple[c.InboxEntry, ...]
    causes: tuple[c.Cause, ...]
    run_id: int


@dataclass(frozen=True, slots=True)
class AgentRejection:
    device: str
    agent: str
    error: Exception
    receipt: c.RunReceipt


class AgentBatchError(RuntimeError):
    def __init__(self, rejections: tuple[AgentRejection, ...]) -> None:
        self.rejections = rejections
        super().__init__(
            '; '.join(f'{r.device}/{r.agent}: {r.error}' for r in rejections)
        )


class _AgentKind(Kind):
    def _claim_due(self, now, successor=None):
        # A fresh arrival cannot implicitly retry a rejected captured run.
        blocked = set(successor or ())
        for entries in self.retryable.values():
            blocked.update(entries)
        return super()._claim_due(now, blocked)


class AgentRuntime:
    def __init__(self, sim: Simulation) -> None:
        self.sim = sim
        self.subscriptions = SubscriptionIndex()
        self._live: dict[tuple[str, str], int] = {}
        self._inboxes: dict[tuple[str, str, int], deque[c.InboxEntry]] = {}
        self._causes: dict[tuple[str, str, int], set[c.Cause]] = {}
        self._captures: dict[tuple[str, str, int], _Capture] = {}
        self._timers: dict[tuple[str, str, int], dict[str, tuple[float, int]]] = {}
        self._timer_ticket = 0
        self._armed_count = 0
        self._timer_events: dict[int, Any] = {}
        self._published: list[tuple[tuple[str, str, int], c.AgentOutput, tuple]] = []
        self._rejections: list[AgentRejection] = []
        self._kind = _AgentKind(
            derive.AGENT,
            COALESCE,
            self._run,
            self.affected,
            self._delay,
            after_run=self._after_run,
        )

    def kind(self) -> Kind:
        return self._kind

    def _delay(self, state: NetworkState, entity: tuple[str, str]) -> float:
        node = state.devices[entity[0]].agents[entity[1]]
        # Registration/reset supplies a snapshot at now, without batching delay.
        if not node.initialized and (*entity, node.generation) not in self._captures:
            return 0.0
        delay = node.config.run_delay
        if delay:
            _future(self.sim.env.now, delay)
        return delay

    def bind(self) -> None:
        for device, name in sorted(self.sim.network.agents):
            node = self.sim.state.devices[device].agents.get(name)
            if node is not None:
                self._register(device, name, node)
                if not node.initialized:
                    self._cause(device, name, c.Cause(c.CAUSE_INIT))
                    self.sim.pipeline.mark(
                        self._kind, {(device, name)}, self.sim.env.now
                    )

    def _register(self, device: str, name: str, node: c.AgentNode) -> None:
        self._live[device, name] = node.generation
        plugin = self.sim.network.agents[device, name]
        self.subscriptions.add(device, name, plugin.subscriptions())

    def generation(self, device: str, agent: str) -> int | None:
        dev = self.sim.state.devices.get(device)
        node = dev.agents.get(agent) if dev else None
        return node.generation if node else None

    def _cause(self, device: str, name: str, cause: c.Cause) -> None:
        gen = self.generation(device, name)
        if gen is not None:
            self._causes.setdefault((device, name, gen), set()).add(cause)

    def affected(self, delta: StateDelta, state: NetworkState) -> set[Any]:
        out: set[Any] = set()
        if not self._live and not self.sim.network.agents:
            return out
        for device in delta.devices().keys:
            old, new = delta.old.devices.get(device), state.devices.get(device)
            changes = delta.agents(device)
            for name in changes.keys:
                before = old.agents.get(name) if old else None
                after = new.agents.get(name) if new else None
                if before is not None and (
                    after is None or before.generation != after.generation
                ):
                    self._cancel(device, name, before.generation)
                if after is not None and (
                    before is None or before.generation != after.generation
                ):
                    self._register(device, name, after)
                    self._cause(
                        device, name, c.Cause(c.CAUSE_RESET if before else c.CAUSE_INIT)
                    )
                    out.add((device, name))
            if new is None or not new.agents:
                continue
            for name, paths in self.subscriptions.affected(device, old, new).items():
                for path in paths:
                    self._cause(device, name, c.Cause(c.CAUSE_SUBSCRIPTION, path))
                out.add((device, name))
            # Only agents on this changed device can own changed results.
            if old is None:
                continue
            routes_changed = old.resolver_outcomes is not new.resolver_outcomes
            nht_changed = old.nht is not new.nht
            sids_changed = old.srv6_sids is not new.srv6_sids
            if not (routes_changed or nht_changed or sids_changed):
                continue
            for name, node in new.agents.items():
                if routes_changed and self._outcomes(
                    old, node.client
                ) != self._outcomes(new, node.client):
                    self._cause(device, name, c.Cause(c.CAUSE_ROUTES))
                    out.add((device, name))
                if nht_changed and self._nht(old, node.client) != self._nht(
                    new, node.client
                ):
                    self._cause(device, name, c.Cause(c.CAUSE_NHT))
                    out.add((device, name))
                if sids_changed and self._sids(old, node.client) != self._sids(
                    new, node.client
                ):
                    self._cause(device, name, c.Cause(c.CAUSE_SIDS))
                    out.add((device, name))
        return out

    @staticmethod
    def _outcomes(dev: Any, client: c.ClientId) -> tuple:
        return tuple(
            (af, key, value)
            for af, result in dev.resolver_outcomes.sorted_items()
            for key, value in result.rows.sorted_items()
            if key[2] == client
        )

    @staticmethod
    def _nht(dev: Any, client: c.ClientId) -> PMap:
        return (
            PMap(
                {
                    key: value
                    for key, value in dev.nht.registrations.items()
                    if key.owner == client
                }
            )
            if dev.nht
            else PMap()
        )

    @staticmethod
    def _sids(dev: Any, client: c.ClientId) -> tuple[c.SidResultView, ...]:
        if dev.srv6_sids is None:
            return ()
        result = []
        for _, req in dev.srv6_sids.requests.sorted_items():
            if req.owner == client:
                sid = dev.srv6_sids.sids.get(req.result.sid)
                result.append(
                    c.SidResultView(
                        req.request_id,
                        req.behavior,
                        sid.sid if sid else None,
                        sid.length if sid else None,
                        sid.adjacency_up if sid else False,
                    )
                )
        return tuple(result)

    def deliver(self, device: str, agent: str, entry: c.InboxEntry) -> bool:
        generation = self.generation(device, agent)
        if generation is None or (
            entry.generation is not None and entry.generation != generation
        ):
            return False
        validate_immutable(entry, 'inbox')
        queue = self._inboxes.setdefault((device, agent, generation), deque())
        node = self.sim.state.devices[device].agents[agent]
        if len(queue) >= node.config.inbox_limit:
            return False
        # Validate before admission, so an unrepresentable deadline changes nothing.
        self._delay(self.sim.state, (device, agent))
        queue.append(entry)
        kind = (
            c.CAUSE_TIMER
            if isinstance(entry, c.TimerFired)
            else (
                c.CAUSE_SESSION if isinstance(entry, c.SessionEvent) else c.CAUSE_INBOX
            )
        )
        self._cause(
            device,
            agent,
            c.Cause(kind, entry.name if isinstance(entry, c.TimerFired) else None),
        )
        self.sim.pipeline.mark(self._kind, {(device, agent)}, self.sim.env.now)
        return True

    def _context(
        self,
        state: NetworkState,
        device: str,
        name: str,
        node: c.AgentNode,
        capture: _Capture,
        now: float,
    ) -> Context:
        dev = state.devices[device]
        views = {}
        for key, interface in dev.interfaces.sorted_items():
            cfg, oper = interface.config, interface.oper
            mac = getattr(interface, 'mac', None)
            link = state.links.get(getattr(interface, 'link', '') or '')
            views[key] = c.InterfaceView(
                key,
                interface.index,
                interface.kind,
                cfg.admin,
                oper.oper,
                oper.reason,
                oper.since,
                getattr(cfg, 'mtu', 1500),
                getattr(cfg, 'metric', 1),
                cfg.ipv4,
                cfg.ipv6,
                getattr(cfg, 'unnumbered', False),
                MacAddress(mac).link_local_int() if mac is not None else None,
                mac,
                interfaces.l3_usable(interface, 4),
                interfaces.l3_usable(interface, 6),
                getattr(cfg, 'aggregate_id', None),
                tuple(sorted(k for k, m in oper.members.items() if m.active))
                if isinstance(interface, interfaces.PortChannelNode)
                else (),
                getattr(oper, 'bandwidth', None),
                c.LinkView(
                    link.index,
                    link.config.delay,
                    link.config.capacity
                    if link.config.capacity is not None
                    else getattr(cfg, 'speed', 0.0),
                )
                if link
                else None,
                generation=interface.generation,
                config=cfg,
            )
        neighbors = (
            tuple(
                c.NeighborView(
                    iface,
                    4 if address < 1 << 32 else 6,
                    address,
                    mac,
                    iface if address >> 118 == 0b1111111010 else None,
                )
                for (iface, address), mac in dev.neighbors.entries.sorted_items()
            )
            if dev.neighbors
            else ()
        )
        rows = {}
        for af, rib in dev.ribs.sorted_items():
            fib = dev.fibs.get(af)
            outcome = dev.resolver_outcomes.get(af)
            rows[af] = tuple(
                c.RouteView(
                    row,
                    *routing.row_status(
                        dev.resolver_input_epoch.get(af, 0), outcome, row.key
                    ),
                    selected=bool(
                        fib
                        and (entry := fib.entries.get(*row.prefix))
                        and row.key in entry.contributing
                    ),
                )
                for client in sorted(rib.clients)
                for row in rib.rows_of(client)
            )
        connections = {}
        transport = state.transport
        # C3 may expose per-connection admission counters in its budget snapshot.
        budgets = self.sim.transport.budget().get('connections', {})
        if transport:
            for cid, conn in transport.connections.sorted_items():
                a = (conn.a_device, conn.a_agent, conn.a_generation) == (
                    device,
                    name,
                    node.generation,
                )
                b = (conn.b_device, conn.b_agent, conn.b_generation) == (
                    device,
                    name,
                    node.generation,
                )
                if not (a or b):
                    continue
                local, remote = (
                    (conn.a_local, conn.b_local) if a else (conn.b_local, conn.a_local)
                )
                if local is None:
                    continue
                counters = budgets.get(cid, {}) if isinstance(budgets, dict) else {}
                connections[cid] = c.ConnectionView(
                    cid,
                    conn.state,
                    local,
                    remote,
                    conn.initiator_a if a else not conn.initiator_a,
                    conn.reason,
                    node.generation,
                    counters.get('queued_messages', 0),
                    counters.get('queued_bytes', 0),
                    conn.a_to_b_reachable if a else conn.b_to_a_reachable,
                )
        rng = random.Random()
        if node.rng is None:
            rng.seed(
                f'{self.sim.network.seed}:{dev.config.seed}:{name}:{node.generation}'
            )
        else:
            rng.setstate(node.rng)
        return Context(
            now,
            device,
            name,
            node.client,
            node.generation,
            dev.config,
            node.config,
            dev.oper.router_id,
            PMap(views),
            neighbors,
            self._nht(dev, node.client),
            self._sids(dev, node.client),
            dev.srv6_sids,
            dev.srv6_policies.states if dev.srv6_policies else PMap(),
            node.state,
            node.srdb_view,
            capture.inbox,
            PMap(connections),
            PMap(
                {
                    k: v[0]
                    for k, v in self._timers.get(
                        (device, name, node.generation), {}
                    ).items()
                }
            ),
            capture.causes,
            rng,
            c.AgentStats(),
            PMap(rows),
            dev.fibs,
            dev.resolver_input_epoch,
            PMap(
                {
                    af: outcome.processed_epoch
                    for af, outcome in dev.resolver_outcomes.items()
                }
            ),
        )

    def _apply(
        self,
        state: NetworkState,
        device: str,
        client: c.ClientId,
        output: c.AgentOutput,
    ) -> NetworkState:
        dev = state.devices[device]
        ribs = dev.ribs
        for op in output.route_ops:
            if op.af not in (4, 6):
                raise ValueError('invalid route family')
            if any(row.source != client for row in op.add + (op.sync or ())):
                raise ValueError('route belongs to another client')
            if any(key[2] != client for key in op.delete):
                raise ValueError('route delete belongs to another client')
            old = ribs.get(op.af)
            rib = old if old is not None else routing.RibState.empty(op.af)
            changed = routing.rib_apply(
                rib,
                add=op.add,
                delete=op.delete,
                sync=(client, op.sync) if op.sync is not None else None,
            )
            if changed is not rib:
                ribs = ribs.set(op.af, changed)
        if ribs is not dev.ribs:
            state = replace(
                state, devices=state.devices.set(device, replace(dev, ribs=ribs))
            )
        for op in output.policy_ops:
            if op.kind == c.SET_STEERING:
                state = srv6.set_steering(state, device, client, op.rules)
            elif op.kind == c.DELETE_POLICY:
                assert op.key is not None
                state = srv6.delete_policy(state, device, client, op.key)
            else:
                state = srv6.put_policy(
                    state,
                    device,
                    client,
                    op.policy,
                    replace_existing=op.kind == c.REPLACE_POLICY,
                    locator=op.locator,
                )
        for sid_op in output.sid_ops:
            if sid_op.kind == c.REQUEST_SID:
                state, _ = srv6.request_sid(
                    state,
                    device,
                    client,
                    sid_op.request_id,
                    sid_op.behavior,
                    dict(sid_op.args),
                )
            else:
                db = state.devices[device].srv6_sids
                req = (
                    db.requests.get(srv6.request_key(client, sid_op.request_id))
                    if db
                    else None
                )
                if req:
                    state = srv6.remove_local_sid(state, device, req.result.sid, client)
        dev = state.devices[device]
        table = dev.nht or c.NhtTable()
        registrations = table.registrations
        for nht_op in output.nht_ops:
            key = nht_op.key
            if key.owner != client:
                raise ValueError('NHT registration belongs to another client')
            if key.af not in (4, 6) or not 0 <= key.address < (
                1 << (32 if key.af == 4 else 128)
            ):
                raise ValueError('invalid NHT address family/address')
            if nht_op.kind == c.REGISTER_NHT:
                if key not in registrations:
                    registrations = registrations.set(key, None)
            else:
                registrations = registrations.remove(key)
        if registrations is not table.registrations:
            table = replace(
                table, registrations=registrations, version=table.version + 1
            )
            state = replace(
                state, devices=state.devices.set(device, replace(dev, nht=table))
            )
        return state

    def _run(self, state: NetworkState, now: float, due: list[Any]) -> NetworkState:
        self._published = []
        self._rejections = []
        staging = state
        claimed = []
        # Capture the entire batch before any plugin runs. Deliveries during
        # preparation belong to the next run, even for a later-sorted owner.
        for device, name in sorted(due):
            dev = state.devices.get(device)
            node = dev.agents.get(name) if dev else None
            if node is None:
                continue
            key = (device, name, node.generation)
            capture = self._captures.get(key)
            if capture is None:
                capture = _Capture(
                    tuple(self._inboxes.get(key, ())),
                    tuple(
                        sorted(
                            self._causes.pop(key, set()),
                            key=lambda cause: (cause.kind, repr(cause.key)),
                        )
                    ),
                    node.runs + 1,
                )
                self._captures[key] = capture
            claimed.append((device, name, dev, node, key, capture))
        for device, name, dev, node, key, capture in claimed:
            receipt = c.RunReceipt(
                dev.generation,
                node.generation,
                capture.run_id,
                now,
                inbox_consumed=len(capture.inbox),
                causes_count=len(capture.causes),
            )
            journal = staging
            try:
                ctx = self._context(state, device, name, node, capture, now)
                plugin = self.sim.network.agents[device, name]
                output = c.check_output(
                    plugin.on_run(ctx) if node.initialized else plugin.on_init(ctx)
                )
                validate_immutable(output, 'AgentOutput')
                for timer in output.timers:
                    if timer.delay is not None:
                        _future(now, timer.delay)
                staging = self._apply(staging, device, node.client, output)
                count = sum(
                    len(getattr(output, field))
                    for field in (
                        'route_ops',
                        'policy_ops',
                        'sid_ops',
                        'nht_ops',
                        'datagrams',
                        'messages',
                        'sessions',
                        'timers',
                    )
                )
                receipt = replace(receipt, ops_count=count)
                updated = replace(
                    node,
                    state=output.state,
                    srdb_view=output.srdb_view,
                    rng=ctx.rng.getstate(),
                    receipt=receipt,
                    runs=node.runs + 1,
                    initialized=True,
                )
                self._published.append((key, output, ctx.stats.drain()))
            except Exception as error:
                staging = journal
                receipt = replace(
                    receipt,
                    status=c.RECEIPT_REJECTED,
                    reason=str(error),
                    inbox_consumed=0,
                )
                updated = replace(node, receipt=receipt)
                self._rejections.append(AgentRejection(device, name, error, receipt))
            changed = staging.devices[device]
            staging = replace(
                staging,
                devices=staging.devices.set(
                    device, replace(changed, agents=changed.agents.set(name, updated))
                ),
            )
        return staging

    def _after_run(self, now: float, due: list[Any]) -> None:
        # Consume journals before calling external publication code; even a
        # post-commit exception cannot cause duplicate outbox publication.
        published, self._published = self._published, []
        rejections, self._rejections = tuple(self._rejections), []
        for rejection in rejections:
            entity = (rejection.device, rejection.agent)
            self._kind._enqueue(entity, now)
            ticket = self._kind.pending[entity][1]
            self._kind.retryable.setdefault(now, {})[entity] = ticket
            self.sim.pipeline.successor.get(derive.AGENT, set()).discard(entity)
        errors = []
        for key, output, stats in published:
            capture = self._captures.pop(key)
            queue = self._inboxes.get(key)
            if queue is not None:
                for _ in capture.inbox:
                    queue.popleft()
                if not queue:
                    self._inboxes.pop(key, None)
            for stat_name, value in stats + output.stats:
                self.sim.stats.add(stat_name, now, value)
            try:
                self._publish_outbox(key, output, now)
            except Exception as error:
                errors.append(error)
            # Later arrivals survive a retry's captured prefix and get a run.
            if self._inboxes.get(key):
                self.sim.pipeline.mark(self._kind, {key[:2]}, now)
        if rejections:
            error = AgentBatchError(rejections)
            mark_published(error)
            raise error
        if errors:
            mark_published(errors[0])
            raise errors[0]

    def _transport_shim(
        self, method: str, key: tuple[str, str, int], item: Any = None
    ) -> None:
        """Temporary C0/C3 seam: ONLY NotImplementedError becomes Rejection.

        Cancellation has no surviving recipient on removal. Other transport
        errors remain post-publication errors and never replay the receipt.
        """
        try:
            fn = getattr(self.sim.transport, method)
            if method == 'cancel_agent':
                fn(*key)
            else:
                fn(*key, item)
        except NotImplementedError:
            if method != 'cancel_agent':
                entry = c.Rejection(
                    self.sim.env.now,
                    'TRANSPORT_UNAVAILABLE',
                    detail=method,
                    generation=key[2],
                )
                if not self.deliver(key[0], key[1], entry):
                    raise RuntimeError(
                        'transport shim rejection inbox overflow'
                    ) from None

    def _publish_outbox(
        self, key: tuple[str, str, int], output: c.AgentOutput, now: float
    ) -> None:
        if self.generation(*key[:2]) != key[2]:
            return
        for timer in output.timers:
            armed = self._timers.setdefault(key, {})
            if timer.delay is None:
                if armed.pop(timer.name, None) is not None:
                    self._armed_count -= 1
            else:
                target = _future(now, timer.delay)
                self._timer_ticket += 1
                ticket = self._timer_ticket
                if timer.name not in armed:
                    self._armed_count += 1
                armed[timer.name] = (target, ticket)
                event = self.sim.env.timeout(target - now)
                self._timer_events[ticket] = event
                assert event.callbacks is not None
                event.callbacks.append(
                    lambda _, k=key, n=timer.name, t=ticket: self._fire(k, n, t)
                )
        self._compact_timers()
        errors = []
        for method, entries in (
            ('send_datagram', output.datagrams),
            ('send_message', output.messages),
            ('session_op', output.sessions),
        ):
            for entry in entries:
                try:
                    self._transport_shim(method, key, entry)
                except Exception as error:
                    errors.append(error)
        if errors:
            raise errors[0]

    def _fire(self, key: tuple[str, str, int], name: str, ticket: int) -> None:
        self._timer_events.pop(ticket, None)
        armed = self._timers.get(key, {})
        entry = armed.get(name)
        if entry is None or entry[1] != ticket or self.generation(*key[:2]) != key[2]:
            return
        del armed[name]
        self._armed_count -= 1
        if not armed:
            self._timers.pop(key, None)
        if not self.deliver(
            key[0], key[1], c.TimerFired(self.sim.env.now, name, key[2])
        ):
            raise RuntimeError(f'timer inbox overflow: {key[:2]}/{name}')

    def _compact_timers(self) -> None:
        if len(self._timer_events) <= max(64, 2 * self._armed_count):
            return
        live = {
            ticket for armed in self._timers.values() for _, ticket in armed.values()
        }
        # Remove only our lazy timer events, preserving the engine's ordering.
        from heapq import heapify

        stale = {
            id(event)
            for ticket, event in self._timer_events.items()
            if ticket not in live
        }
        self.sim.env._queue[:] = [
            entry for entry in self.sim.env._queue if id(entry[3]) not in stale
        ]
        heapify(self.sim.env._queue)
        self._timer_events = {
            ticket: event
            for ticket, event in self._timer_events.items()
            if ticket in live
        }

    def _cancel(self, device: str, name: str, generation: int) -> None:
        entity = (device, name)
        key = (*entity, generation)
        self._live.pop(entity, None)
        self.subscriptions.remove(*entity)
        self._inboxes.pop(key, None)
        self._causes.pop(key, None)
        self._captures.pop(key, None)
        self._armed_count -= len(self._timers.pop(key, {}))
        self._kind.pending.pop(entity, None)
        for entries in self._kind.retryable.values():
            entries.pop(entity, None)
        self._kind._compact()
        self.sim.pipeline.successor.get(derive.AGENT, set()).discard(entity)
        self._compact_timers()
        self._transport_shim('cancel_agent', key)

    def reset_agent(self, device: str, name: str, *, purge: bool = False) -> None:
        def apply(state: NetworkState) -> NetworkState:
            dev = state.devices[device]
            node = dev.agents[name]
            if purge:
                ops = c.AgentOutput(
                    route_ops=(c.RouteOp(4, sync=()), c.RouteOp(6, sync=()))
                )
                state = self._apply(state, device, node.client, ops)
                table = dev.srv6_policies
                if table:
                    for policy_key, policy in table.policies.sorted_items():
                        if policy.owner == node.client:
                            state = srv6.delete_policy(
                                state, device, node.client, policy_key
                            )
                    state = srv6.set_steering(state, device, node.client, ())
                if dev.srv6_sids:
                    for sid in dev.srv6_sids.sids.values():
                        if sid.owner == node.client:
                            state = srv6.remove_local_sid(
                                state, device, sid.sid, node.client
                            )
                state = self._apply(
                    state,
                    device,
                    node.client,
                    c.AgentOutput(
                        nht_ops=tuple(
                            c.NhtOp(c.UNREGISTER_NHT, key)
                            for key in self._nht(dev, node.client)
                        )
                    ),
                )
            allocators, generation = state.allocators.take_generation()
            dev = state.devices[device]
            fresh = c.AgentNode(name, generation, node.client, node.config)
            return replace(
                state,
                allocators=allocators,
                devices=state.devices.set(
                    device, replace(dev, agents=dev.agents.set(name, fresh))
                ),
            )

        self.sim.network.update(apply, ('reset_agent', device, name))

    def budget(self) -> dict[str, Any]:
        armed = self._armed_count
        deadlines = {deadline for deadline, _ in self._kind.pending.values()}
        scheduled = [
            (time, gen)
            for offset, time, gen in self.sim.pipeline.scheduled
            if offset == derive.AGENT
        ]
        live_runs = sum(
            time in deadlines
            and (time > self.sim.env.now or gen == self.sim.pipeline.round_gen)
            for time, gen in scheduled
        )
        return {
            'agents': len(self._live),
            'pending_runs': len(self._kind.pending),
            'inbox_entries': sum(map(len, self._inboxes.values())),
            'armed_timers': armed,
            'scheduled_timer_events': len(self._timer_events),
            'stale_timer_events': len(self._timer_events) - armed,
            'scheduled_runs': len(scheduled),
            'live_run_events': live_runs,
            'stale_run_events': len(scheduled) - live_runs,
        }


__all__ = ['AgentRuntime', 'AgentBatchError', 'AgentRejection', 'Context']
