"""Transport: link channels and sessions (Gate C slice C3).

``LinkChannel(device, interface)`` datagrams and ``Session`` connections
(a reliable-message abstraction, not TCP) as specified in the design's
"Transport" subsection: physical transfer and receive availability checked
at send and delivery, ordered release per channel incarnation, distinct
connection ids, abort versus drain-then-close, no-progress timeouts and
bounded admission with explicit rejections. The TRANSPORT kind derives
path reachability in ``NetworkState.transport``.

The runtime interface used by publication (``netsim.runtime.agents``):
``send_datagram``, ``send_message``, ``session_op`` and ``cancel_agent``;
deliveries call back ``AgentRuntime.deliver``.

Integration decisions:
- Accepted sends return None; rejected sends return and enqueue Rejection.
  Open returns its new integer connection id. Other lifecycle successes
  return None. Session operations publish through Network.update and must be
  invoked outside another update's observer dispatch.
- An open's scoped remote endpoint uses the initiating device's interface
  name. Received SessionEvents rebase that scope to each observer's local
  interface. Datagram Sender likewise exposes only a receiving-local scope.
- Datagram ports cover both supported AFs. IPv6 without a configured address
  uses the interface's EUI-64 link-local address; IPv4 without a source address
  rejects with NO_SOURCE. Bundle selection is documented in channels.py.
- Sequence numbers start at zero independently in each direction. Admission
  accounts Message.size (modeled bytes), including in-flight messages. Inbox
  backpressure retains accepted messages until delivery or explicit timeout.
- Derivation entity 0 is reserved for listener cleanup; real connection ids
  start at 1. Closed connections remain observable in the immutable tree;
  their runtime queues and timers are retired. Budget stale_events counts
  inert scheduled shells; periodic heap compaction preserves engine ordering.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field, replace
from heapq import heapify
from typing import TYPE_CHECKING, Any, Callable

from netsim import core
from netsim.model import contracts as c
from netsim.model import derive
from netsim.model import forwarding as fw
from netsim.model.network import _View
from netsim.model.packets import IPv4Packet, IPv6Packet, L4Header
from netsim.model.state import NetworkState, StateDelta, diff_pmap, validate_immutable
from netsim.runtime.channels import (
    Wire,
    configured,
    eligible,
    future,
    interface,
    scoped,
    source_address,
    wires,
)
from netsim.runtime.pipeline import COALESCE, Kind

if TYPE_CHECKING:
    from netsim.runtime.simulation import Simulation


@dataclass(slots=True)
class _Pending:
    action: Callable[[], None] | None
    event: core.Event | None = None


@dataclass(slots=True)
class _Datagram:
    wire: Wire
    datagram: c.Datagram
    sender: c.Sender
    receivers: tuple[tuple[str, int], ...]
    release: float
    event: _Pending | None = None


@dataclass(slots=True)
class _Queued:
    message: c.Message
    seq: int
    release: float


@dataclass(slots=True)
class _Direction:
    queue: deque[_Queued] = field(default_factory=deque)
    size: int = 0
    seq: int = 0
    event: _Pending | None = None
    timer: _Pending | None = None
    stalled: bool = False


@dataclass(slots=True)
class _Session:
    directions: tuple[_Direction, _Direction] = field(
        default_factory=lambda: (_Direction(), _Direction())
    )
    handshake: _Pending | None = None
    timer: _Pending | None = None


@dataclass(frozen=True, slots=True)
class _Path:
    reachable: bool = False
    delay: float = 0.0
    deps: tuple[str, ...] = ()
    device: str | None = None
    endpoint: c.Endpoint | None = None


def _agent(
    state: NetworkState, device: str | None, name: str | None
) -> c.AgentNode | None:
    dev = state.devices.get(device) if device is not None else None
    return dev.agents.get(name) if dev is not None and name is not None else None


def _key(device: str, ep: c.Endpoint) -> tuple[str, int, int, str | None, int]:
    return device, ep.af, ep.address, ep.scope, ep.port


def _tree(state: NetworkState, transport: c.TransportState) -> NetworkState:
    if state.transport == transport:
        return state
    old = state.transport
    return replace(
        state, transport=replace(transport, version=(old.version if old else 0) + 1)
    )


def _path(
    state: NetworkState,
    device: str,
    local: c.Endpoint,
    remote: c.Endpoint,
    connection: int,
) -> _Path:
    """Clock-free forward steps over one immutable committed snapshot.

    The probe's L4 key is stable for the entire connection, including retries.
    Negative lookups retain every visited device. Scoped peers are resolved
    only across the specified local wire, never by a global address search.
    """
    deps = {device}
    if device not in state.devices:
        return _Path(deps=(device,))
    if scoped(remote):
        if remote.scope is None:
            return _Path(deps=(device,))
        candidates = wires(state, device, remote.scope, physical_only=True)
        fallback = _Path(deps=(device,))
        for wire in candidates:
            peer = wire.rx[0]
            deps.add(peer)
            ep = replace(remote, scope=wire.remote)
            if configured(state, peer, ep) is None:
                continue
            result = _Path(
                wire.blocked(state, remote.af, effective=False) is None,
                wire.delay,
                tuple(sorted(deps)),
                peer,
                ep,
            )
            if result.reachable:
                return result
            fallback = result
        return replace(fallback, deps=tuple(sorted(deps)))
    packet_type = IPv4Packet if local.af == 4 else IPv6Packet
    packet = packet_type(
        local.address,
        remote.address,
        6,
        payload=L4Header((local.port + connection) % 65536, remote.port),
    )
    trace = fw.trace(lambda name: _View(state, name), device, packet)
    deps.update(hop.device for hop in trace.hops)
    delay = sum(
        state.links[hop.link_id].config.delay
        for hop in trace.hops
        if hop.link_id is not None and hop.outcome == fw.TRANSMIT
    )
    if trace.outcome != fw.DELIVER:
        return _Path(False, delay, tuple(sorted(deps)))
    peer = trace.hops[-1].device
    return _Path(True, delay, tuple(sorted(deps)), peer, remote)


class TransportRuntime:
    """Per-simulation channels, queues and timers, with pure path derivation.

    ``listen_ports`` applies to both IPv4 and IPv6 (C0 has no AF selector).
    Queued byte accounting uses the caller's modeled ``Message.size``. One
    timeout per nonempty direction measures lack of successful inbox delivery;
    a CONNECTING request is itself pending work and has the same timeout.
    """

    def __init__(self, sim: Simulation) -> None:
        self.sim = sim
        self._kind = Kind(derive.TRANSPORT, COALESCE, self._run, self.affected)
        self._channels: dict[tuple[str, str, int, str, int], deque[_Datagram]] = {}
        self._owners: dict[
            tuple[str, str, int], set[tuple[str, str, int, str, int]]
        ] = {}
        self._ports: dict[str, tuple[Any, dict[int, tuple[tuple[str, int], ...]]]] = {}
        self._cancelled: dict[tuple[str, str], int] = {}
        self._sessions: dict[int, _Session] = {}
        self._indexed_transport: c.TransportState | None = None
        self._connection_deps: dict[int, tuple[str, ...]] = {}
        self._by_device: dict[str, set[int]] = {}
        self._listener_devices: dict[str, set[Any]] = {}
        self._inflight = 0
        self._dropped = 0
        self._rejected = 0
        self._inbox_rejected = 0
        self._stale = 0
        self._scheduled = 0
        self._retired: set[core.Event] = set()
        self._compacted = 0
        # Publication hooks run even when another observer raises. No update
        # is nested here: this hook only delivers inbox entries/schedules work.
        sim.network.on_delta.append(self._committed)

    def kind(self) -> Kind:
        return self._kind

    def _live(self, device: str, agent: str, generation: int) -> bool:
        return (
            self._cancelled.get((device, agent), -1) < generation
            and self.sim.agents.generation(device, agent) == generation
        )

    def _later(self, target: float, action: Callable[[], None]) -> _Pending:
        future(self.sim.env.now, target - self.sim.env.now)
        pending = _Pending(action)
        self._scheduled += 1
        event = self.sim.env.timeout(target - self.sim.env.now)
        pending.event = event
        assert event.callbacks is not None

        def fire(_: core.Event) -> None:
            self._scheduled -= 1
            pending.event = None
            self._retired.discard(event)
            callback, pending.action = pending.action, None
            if callback is None:
                self._stale -= 1
            else:
                callback()

        event.callbacks.append(fire)
        return pending

    def _cancel(self, pending: _Pending | None) -> None:
        if pending is not None and pending.action is not None:
            pending.action = None  # release payloads immediately; heap shell is inert
            self._stale += 1
            assert pending.event is not None
            self._retired.add(pending.event)
            if self._stale >= 64 and self._stale > self._scheduled - self._stale:
                # The engine has no cancel API. Remove only our inert events,
                # preserving the (time, priority, eid) ordering of every live
                # event. Mutate the heap in place: Environment.run aliases it.
                queue = self.sim.env._queue
                queue[:] = [item for item in queue if item[3] not in self._retired]
                heapify(queue)
                self._scheduled -= self._stale
                self._compacted += self._stale
                self._stale = 0
                self._retired.clear()

    def _reject(
        self,
        device: str,
        agent: str,
        generation: int,
        reason: str,
        *,
        connection: int | None = None,
        interface: str | None = None,
    ) -> c.Rejection:
        rejection = c.Rejection(
            self.sim.env.now,
            reason,
            connection=connection,
            interface=interface,
            generation=generation,
        )
        self._rejected += 1
        if self._live(device, agent, generation) and not self.sim.agents.deliver(
            device, agent, rejection
        ):
            self._inbox_rejected += 1
        return rejection

    def _listeners(self, device: str, port: int) -> tuple[tuple[str, int], ...]:
        agents = self.sim.network.state.devices[device].agents
        cached = self._ports.get(device)
        if cached is None or cached[0] is not agents:
            ports: dict[int, list[tuple[str, int]]] = {}
            for name, node in agents.sorted_items():
                for p in sorted(set(node.config.listen_ports)):
                    ports.setdefault(p, []).append((name, node.generation))
            cached = agents, {p: tuple(names) for p, names in ports.items()}
            self._ports[device] = cached
        return cached[1].get(port, ())

    def send_datagram(
        self, device: str, agent: str, generation: int, datagram: c.Datagram
    ) -> c.Rejection | None:
        validate_immutable(datagram)
        state = self.sim.network.state
        node = interface(state, device, datagram.interface)
        if not self._live(device, agent, generation):
            return self._reject(
                device, agent, generation, c.RESET, interface=datagram.interface
            )
        if node is None or not eligible(node, datagram.af):
            return self._reject(
                device,
                agent,
                generation,
                'INTERFACE_DOWN',
                interface=datagram.interface,
            )
        candidates = wires(state, device, datagram.interface)
        wire = None
        reason = 'LINK_DOWN'
        for candidate in candidates:
            reason = candidate.blocked(state, datagram.af, effective=True)
            if reason is None:
                wire = candidate
                break
        if wire is None:
            return self._reject(
                device,
                agent,
                generation,
                reason or 'LINK_DOWN',
                interface=datagram.interface,
            )
        address = source_address(node, datagram.af)
        if address is None:
            return self._reject(
                device, agent, generation, 'NO_SOURCE', interface=datagram.interface
            )
        config = state.devices[device].agents[agent].config
        target = future(self.sim.env.now, wire.delay + config.processing_delay)
        key = device, agent, generation, datagram.interface, node.generation
        queue = self._channels.setdefault(key, deque())
        if queue:
            target = max(target, queue[-1].release)
        ep = c.Endpoint(datagram.af, address, datagram.port)
        if scoped(ep):
            ep = replace(ep, scope=wire.remote)
        entry = _Datagram(
            wire,
            datagram,
            c.Sender(wire.remote, ep),
            self._listeners(wire.rx[0], datagram.port),
            target,
        )
        queue.append(entry)
        self._owners.setdefault((device, agent, generation), set()).add(key)
        self._inflight += 1
        entry.event = self._later(target, lambda: self._datagram_arrives(key))
        return None

    def _datagram_arrives(self, key: tuple[str, str, int, str, int]) -> None:
        queue = self._channels[key]
        entry = queue.popleft()  # O(1), never scan other channels/datagrams
        if not queue:
            del self._channels[key]
            owner = key[:3]
            self._owners[owner].remove(key)
            if not self._owners[owner]:
                del self._owners[owner]
        self._inflight -= 1
        state = self.sim.network.state
        if (
            not self._live(*key[:3])
            or entry.wire.blocked(state, entry.datagram.af, effective=True) is not None
        ):
            self._dropped += 1
            return
        delivery = c.Delivery(
            self.sim.env.now,
            entry.datagram.payload,
            interface=entry.wire.remote,
            sender=entry.sender,
            port=entry.datagram.port,
        )
        delivered = False
        overflow = False
        for name, generation in entry.receivers:
            if self._live(entry.wire.rx[0], name, generation):
                if self.sim.agents.deliver(
                    entry.wire.rx[0], name, replace(delivery, generation=generation)
                ):
                    delivered = True
                else:
                    self._inbox_rejected += 1
                    overflow = True
        if overflow:
            self._reject(*key[:3], c.OVERFLOW, interface=key[3])
        if not delivered:
            self._dropped += 1

    def _cancel_datagrams(self, device: str, agent: str, generation: int) -> None:
        self._cancelled[device, agent] = max(
            generation, self._cancelled.get((device, agent), -1)
        )
        for key in sorted(self._owners.pop((device, agent, generation), ())):
            queue = self._channels.pop(key)
            self._inflight -= len(queue)
            self._dropped += len(queue)
            for entry in queue:
                self._cancel(entry.event)

    def _index(self, transport: c.TransportState | None) -> None:
        """Index committed dependency queries, including misses and endpoints.

        An unrelated device delta touches only its reverse-index buckets. The
        immutable transport identity makes the common lookup O(1); transport
        edits refresh only changed persistent-map shards. Bootstrap once for
        pre-existing transport trees, and tolerate read-only snapshot queries.
        """
        previous = self._indexed_transport
        if transport is previous:
            return
        before = previous.connections if previous is not None else None
        after = transport.connections if transport is not None else None
        for cid in diff_pmap(before, after).keys:
            for device in self._connection_deps.pop(cid, ()):
                bucket = self._by_device[device]
                bucket.remove(cid)
                if not bucket:
                    del self._by_device[device]
            conn = after.get(cid) if after is not None else None
            if conn is None or conn.state == c.DOWN:
                continue
            deps = set(conn.deps)
            deps.add(conn.a_device)
            if conn.b_device is not None:
                deps.add(conn.b_device)
            self._connection_deps[cid] = tuple(sorted(deps))
            for device in deps:
                self._by_device.setdefault(device, set()).add(cid)
        old_listeners = previous.listeners if previous is not None else None
        new_listeners = transport.listeners if transport is not None else None
        for key in diff_pmap(old_listeners, new_listeners).keys:
            old = old_listeners.get(key) if old_listeners is not None else None
            new = new_listeners.get(key) if new_listeners is not None else None
            if old is not None:
                bucket = self._listener_devices[old.device]
                bucket.remove(key)
                if not bucket:
                    del self._listener_devices[old.device]
            if new is not None:
                self._listener_devices.setdefault(new.device, set()).add(key)
        self._indexed_transport = transport

    def affected(self, delta: StateDelta, state: NetworkState) -> set[Any]:
        transport = state.transport
        self._index(transport)
        if transport is None:
            return set()
        if delta.transport_changed():
            return set(transport.connections)
        changed = set()
        devices = delta.devices()
        for name in devices.keys:
            if delta.agents(name).keys:
                old = delta.old.devices.get(name)
                new = state.devices.get(name)
                for agent in delta.agents(name).keys:
                    before = old.agents.get(agent) if old else None
                    after = new.agents.get(agent) if new else None
                    if before is not None and (
                        after is None or before.generation != after.generation
                    ):
                        changed.add(name)
            if (
                delta.config_changed(name)
                or delta.interface_changes(name)
                or any(
                    delta.device_field_changed(name, f)
                    for f in (
                        'fibs',
                        'neighbors',
                        'nexthop_groups',
                        'load_balancers',
                        'srv6_sids',
                        'srv6_policies',
                    )
                )
            ):
                changed.add(name)
        for lid in delta.links().keys:
            for root in (delta.old, state):
                link = root.links.get(lid)
                if link is not None:
                    changed.update((link.a[0], link.b[0]))
        out: set[Any] = set()
        for device in changed:
            out.update(self._by_device.get(device, ()))
        if any(device in self._listener_devices for device in changed):
            out.add(0)  # reserved maintenance entity; connection ids start at 1
        return out

    def _valid_side(
        self, state: NetworkState, conn: c.ConnectionState, side: str
    ) -> bool:
        device, name, gen = (
            getattr(conn, f'{side}_device'),
            getattr(conn, f'{side}_agent'),
            getattr(conn, f'{side}_generation'),
        )
        node = _agent(state, device, name)
        if node is None or node.generation != gen:
            return False
        iface = interface(state, device, getattr(conn, f'{side}_interface'))
        return (
            iface is not None
            and iface.generation == getattr(conn, f'{side}_interface_generation')
            and configured(state, device, getattr(conn, f'{side}_local')) is iface
        )

    def _derive(
        self, state: NetworkState, conn: c.ConnectionState
    ) -> c.ConnectionState:
        if conn.state == c.DOWN:
            return conn
        if not self._valid_side(state, conn, 'a') or (
            conn.b_agent is not None and not self._valid_side(state, conn, 'b')
        ):
            return replace(
                conn,
                state=c.DOWN,
                reason=c.RESET,
                a_to_b_reachable=False,
                b_to_a_reachable=False,
            )
        assert conn.b_local is not None
        remote = conn.b_local
        if scoped(remote):
            remote = replace(remote, scope=conn.a_local.scope)
        forward = _path(state, conn.a_device, conn.a_local, remote, conn.id)
        backward = _Path()
        changes: dict[str, Any] = {}
        if forward.device is not None and forward.endpoint is not None:
            if conn.b_device is not None and forward.device != conn.b_device:
                forward = replace(forward, reachable=False)
            else:
                bnode = configured(state, forward.device, forward.endpoint)
                if bnode is not None:
                    changes.update(
                        b_device=forward.device,
                        b_local=forward.endpoint,
                        b_interface=bnode.name,
                        b_interface_generation=bnode.generation,
                    )
                    if (
                        conn.b_interface_generation
                        and conn.b_interface_generation != bnode.generation
                    ):
                        return replace(
                            conn,
                            state=c.DOWN,
                            reason=c.RESET,
                            a_to_b_reachable=False,
                            b_to_a_reachable=False,
                        )
        bdevice = changes.get('b_device', conn.b_device)
        blocal = changes.get('b_local', conn.b_local)
        if bdevice is not None:
            target = conn.a_local
            if scoped(target):
                target = replace(target, scope=blocal.scope)
            backward = _path(state, bdevice, blocal, target, conn.id)
            if backward.device != conn.a_device:
                backward = replace(backward, reachable=False)
        deps = set(forward.deps + backward.deps)
        deps.add(conn.a_device)
        if bdevice is not None:
            deps.add(bdevice)
        changes.update(
            a_to_b_reachable=forward.reachable,
            b_to_a_reachable=backward.reachable,
            a_to_b_delay=forward.delay,
            b_to_a_delay=backward.delay,
            deps=tuple(sorted(deps)),
        )
        new = replace(conn, **changes)
        return conn if new == conn else new

    def _run(
        self, state: NetworkState, now: float, entities: list[Any]
    ) -> NetworkState:
        transport: c.TransportState | None = state.transport
        if transport is None:
            return state
        connections = transport.connections
        ids = (
            sorted(connections)
            if '*' in entities
            else sorted(e for e in entities if isinstance(e, int) and e != 0)
        )
        for cid in ids:
            old = connections.get(cid)
            if old is not None:
                new = self._derive(state, old)
                if new is not old:
                    connections = connections.set(cid, new)
        listeners = transport.listeners
        for key, listener in transport.listeners.items():
            owner = _agent(state, listener.device, listener.agent)
            node = configured(state, listener.device, listener.endpoint)
            if (
                owner is None
                or owner.generation != listener.generation
                or node is None
                or node.generation != listener.interface_generation
            ):
                listeners = listeners.remove(key)
        return _tree(
            state, replace(transport, connections=connections, listeners=listeners)
        )

    def session_op(
        self, device: str, agent: str, generation: int, op: c.SessionOp
    ) -> int | c.Rejection | None:
        validate_immutable(op)
        state = self.sim.network.state
        if not self._live(device, agent, generation):
            return self._reject(
                device, agent, generation, c.RESET, connection=op.connection
            )
        transport: c.TransportState = state.transport or c.TransportState()
        if op.kind in (c.LISTEN_OP, c.UNLISTEN_OP, c.OPEN_OP):
            assert op.local is not None
            node = configured(state, device, op.local)
            if node is None:
                return self._reject(device, agent, generation, c.UNREACHABLE_ENDPOINT)
            key = _key(device, op.local)
            listener = transport.listeners.get(key)
            if op.kind == c.LISTEN_OP:
                if listener is not None and (listener.agent, listener.generation) != (
                    agent,
                    generation,
                ):
                    # A restarted agent's on_init runs before the TRANSPORT
                    # kind removes its previous generation's listener in the
                    # same round: an obsolete listener (owner not live or its
                    # interface re-created) is replaced; only a live owner
                    # of another generation or agent is a collision.
                    owner = interface(state, device, listener.interface)
                    obsolete = (
                        not self._live(device, listener.agent, listener.generation)
                        or owner is None
                        or owner.generation != listener.interface_generation
                    )
                    if not obsolete:
                        return self._reject(device, agent, generation, 'ADDRESS_IN_USE')
                new = c.Listener(
                    device, agent, op.local, generation, node.name, node.generation
                )
                self._publish(
                    replace(transport, listeners=transport.listeners.set(key, new)),
                    'listen',
                )
                return None
            if op.kind == c.UNLISTEN_OP:
                if listener is not None and (listener.agent, listener.generation) == (
                    agent,
                    generation,
                ):
                    self._publish(
                        replace(transport, listeners=transport.listeners.remove(key)),
                        'unlisten',
                    )
                return None
            assert op.remote is not None
            if op.local.af != op.remote.af or (
                scoped(op.remote)
                and (not scoped(op.local) or op.remote.scope != op.local.scope)
            ):
                return self._reject(device, agent, generation, c.UNREACHABLE_ENDPOINT)
            future(self.sim.env.now, op.timeout)
            cid = transport.next_connection
            conn = c.ConnectionState(
                cid,
                device,
                agent,
                op.local,
                generation,
                b_local=op.remote,
                timeout=op.timeout,
                a_interface=node.name,
                a_interface_generation=node.generation,
            )
            conn = self._derive(state, conn)
            if conn.a_to_b_reachable:
                future(
                    self.sim.env.now,
                    conn.a_to_b_delay
                    + state.devices[device].agents[agent].config.processing_delay,
                )
            self._publish(
                replace(
                    transport,
                    connections=transport.connections.set(cid, conn),
                    next_connection=cid + 1,
                ),
                'open',
            )
            return cid
        assert op.connection is not None
        conn = transport.connections.get(op.connection)
        direction = self._direction(conn, device, agent, generation)
        if conn is None or direction is None or conn.state == c.DOWN:
            return self._reject(
                device, agent, generation, 'NOT_ESTABLISHED', connection=op.connection
            )
        if op.kind == c.ABORT_OP:
            self._down(conn, c.ABORTED)
        else:
            runtime = self._sessions.get(conn.id)
            if (
                conn.state == c.CONNECTING
                or runtime is None
                or not any(d.queue for d in runtime.directions)
            ):
                self._down(conn, c.CLOSED)
            else:
                self._put(replace(conn, draining=True), 'close')
        return None

    def _publish(self, transport: c.TransportState, origin: str) -> None:
        self.sim.network.update(
            lambda state: _tree(state, transport), ('transport', origin)
        )

    def _put(self, conn: c.ConnectionState, origin: str) -> None:
        transport = self.sim.network.state.transport
        self._publish(
            replace(transport, connections=transport.connections.set(conn.id, conn)),
            origin,
        )

    def _down(self, conn: c.ConnectionState, reason: str) -> None:
        self._put(
            replace(
                conn,
                state=c.DOWN,
                reason=reason,
                a_to_b_reachable=False,
                b_to_a_reachable=False,
            ),
            reason,
        )

    def _direction(
        self, conn: c.ConnectionState | None, device: str, agent: str, generation: int
    ) -> int | None:
        if conn is not None:
            if (conn.a_device, conn.a_agent, conn.a_generation) == (
                device,
                agent,
                generation,
            ):
                return 0
            if (conn.b_device, conn.b_agent, conn.b_generation) == (
                device,
                agent,
                generation,
            ):
                return 1
        return None

    def _session_event(self, conn: c.ConnectionState) -> None:
        for device, agent, generation, local, remote, initiator in (
            (
                conn.a_device,
                conn.a_agent,
                conn.a_generation,
                conn.a_local,
                conn.b_local,
                True,
            ),
            (
                conn.b_device,
                conn.b_agent,
                conn.b_generation,
                conn.b_local,
                conn.a_local,
                False,
            ),
        ):
            if (
                device is not None
                and agent is not None
                and self._live(device, agent, generation)
            ):
                if local is not None and remote is not None and scoped(remote):
                    remote = replace(remote, scope=local.scope)
                if not self.sim.agents.deliver(
                    device,
                    agent,
                    c.SessionEvent(
                        self.sim.env.now,
                        conn.id,
                        conn.state,
                        conn.reason,
                        local,
                        remote,
                        initiator,
                        generation=generation,
                    ),
                ):
                    self._inbox_rejected += 1

    def _retire_session(self, cid: int) -> None:
        runtime = self._sessions.pop(cid, None)
        if runtime is not None:
            self._cancel(runtime.handshake)
            self._cancel(runtime.timer)
            for direction in runtime.directions:
                self._cancel(direction.event)
                self._cancel(direction.timer)

    def _committed(self, time: float, origin: Any, delta: StateDelta) -> None:
        if (
            not self._channels
            and not self._ports
            and delta.old.transport is None
            and delta.new.transport is None
        ):
            return  # No agents/channels/sessions: no model scans or new work.
        self._index(delta.new.transport)
        for device in delta.devices().keys:
            old = delta.old.devices.get(device)
            new = delta.new.devices.get(device)
            for name in delta.agents(device).keys:
                before = old.agents.get(name) if old else None
                after = new.agents.get(name) if new else None
                if before is not None and (
                    after is None or before.generation != after.generation
                ):
                    self._cancel_datagrams(device, name, before.generation)
            if new is None:
                self._ports.pop(device, None)
        if not delta.transport_changed():
            return
        old_transport = delta.old.transport
        for cid, conn in delta.new.transport.connections.sorted_items():
            before = old_transport.connections.get(cid) if old_transport else None
            if before is conn:
                continue
            if conn.state == c.DOWN:
                self._retire_session(cid)
                if before is None or before.state != c.DOWN:
                    self._session_event(conn)
                continue
            runtime = self._sessions.setdefault(cid, _Session())
            if conn.state == c.CONNECTING:
                if runtime.timer is None:
                    runtime.timer = self._later(
                        future(time, conn.timeout), lambda cid=cid: self._expire(cid)
                    )
                if conn.a_to_b_reachable:
                    if runtime.handshake is None:
                        owner = _agent(delta.new, conn.a_device, conn.a_agent)
                        assert owner is not None
                        target = future(
                            time, conn.a_to_b_delay + owner.config.processing_delay
                        )
                        listener = delta.new.transport.listeners.get(
                            _key(conn.b_device, conn.b_local)
                        )
                        if listener is not None and not self._live(
                            listener.device, listener.agent, listener.generation
                        ):
                            listener = None
                        runtime.handshake = self._later(
                            target,
                            lambda cid=cid, listener=listener: self._handshake(
                                cid, listener
                            ),
                        )
                else:
                    self._cancel(runtime.handshake)
                    runtime.handshake = None
            else:
                self._cancel(runtime.timer)
                runtime.timer = None
                if before is None or before.state != c.ESTABLISHED:
                    self._session_event(conn)
                for side in (0, 1):
                    reachable = (
                        conn.a_to_b_reachable if side == 0 else conn.b_to_a_reachable
                    )
                    direction = runtime.directions[side]
                    if not reachable:
                        self._cancel(direction.event)
                        direction.event = None
                        direction.stalled = True
                    else:
                        self._kick(conn, side)

    def _handshake(self, cid: int, expected: c.Listener | None) -> None:
        conn = self.sim.network.state.transport.connections[cid]
        runtime = self._sessions[cid]
        runtime.handshake = None
        derived = self._derive(self.sim.network.state, conn)
        if derived.state == c.DOWN or not derived.a_to_b_reachable:
            self._put(derived, 'probe')
            return
        assert derived.b_device is not None and derived.b_local is not None
        listener = self.sim.network.state.transport.listeners.get(
            _key(derived.b_device, derived.b_local)
        )
        if listener is None or (expected is not None and listener != expected):
            self._down(derived, c.REFUSED if expected is None else c.RESET)
            return
        if not self._live(listener.device, listener.agent, listener.generation):
            self._down(derived, c.REFUSED)
            return
        self._put(
            replace(
                derived,
                b_agent=listener.agent,
                b_generation=listener.generation,
                state=c.ESTABLISHED,
            ),
            'established',
        )

    def _expire(self, cid: int, side: int | None = None) -> None:
        runtime = self._sessions.get(cid)
        if runtime is None:
            return
        if side is not None and not runtime.directions[side].queue:
            return
        conn = self.sim.network.state.transport.connections[cid]
        self._down(conn, c.TIMEOUT)

    def send_message(
        self, device: str, agent: str, generation: int, message: c.Message
    ) -> c.Rejection | None:
        validate_immutable(message)
        state = self.sim.network.state
        transport = state.transport
        conn = transport.connections.get(message.connection) if transport else None
        side = self._direction(conn, device, agent, generation)
        if (
            not self._live(device, agent, generation)
            or conn is None
            or conn.state != c.ESTABLISHED
            or conn.draining
            or side is None
        ):
            return self._reject(
                device,
                agent,
                generation,
                'NOT_ESTABLISHED',
                connection=message.connection,
            )
        direction = self._sessions[conn.id].directions[side]
        config = state.devices[device].agents[agent].config
        if (
            len(direction.queue) >= config.queue_limit
            or direction.size + message.size > config.byte_limit
        ):
            return self._reject(
                device, agent, generation, c.OVERFLOW, connection=conn.id
            )
        # Recheck current committed forwarding: a NORMAL send may precede the
        # TRANSPORT derivation at this timestamp.
        derived = self._derive(state, conn)
        delay = derived.a_to_b_delay if side == 0 else derived.b_to_a_delay
        reachable = derived.a_to_b_reachable if side == 0 else derived.b_to_a_reachable
        target = (
            future(self.sim.env.now, delay + config.processing_delay)
            if reachable
            else 0.0
        )
        future(self.sim.env.now, conn.timeout)
        if derived is not conn:
            self.sim.pipeline.mark(self._kind, {conn.id}, self.sim.env.now)
        if derived.state != c.ESTABLISHED:
            return self._reject(
                device, agent, generation, 'NOT_ESTABLISHED', connection=conn.id
            )
        if direction.queue:
            target = max(target, direction.queue[-1].release)
        direction.queue.append(_Queued(message, direction.seq, target))
        direction.seq += 1
        direction.size += message.size
        published_reachable = (
            conn.a_to_b_reachable if side == 0 else conn.b_to_a_reachable
        )
        if not reachable or not published_reachable:
            direction.stalled = True
        if direction.timer is None:
            direction.timer = self._later(
                future(self.sim.env.now, conn.timeout),
                lambda: self._expire(conn.id, side),
            )
        if reachable and published_reachable:
            self._kick(conn, side)
        return None

    def _kick(self, conn: c.ConnectionState, side: int) -> None:
        direction = self._sessions[conn.id].directions[side]
        reachable = conn.a_to_b_reachable if side == 0 else conn.b_to_a_reachable
        if not direction.queue or direction.event is not None or not reachable:
            return
        if direction.stalled:
            owner = _agent(
                self.sim.network.state,
                conn.a_device if side == 0 else conn.b_device,
                conn.a_agent if side == 0 else conn.b_agent,
            )
            assert owner is not None
            delay = conn.a_to_b_delay if side == 0 else conn.b_to_a_delay
            target = future(self.sim.env.now, delay + owner.config.processing_delay)
            for entry in direction.queue:
                entry.release = target
            direction.stalled = False
        direction.event = self._later(
            direction.queue[0].release, lambda: self._message_arrives(conn.id, side)
        )

    def _message_arrives(self, cid: int, side: int) -> None:
        runtime = self._sessions[cid]
        direction = runtime.directions[side]
        direction.event = None
        conn = self.sim.network.state.transport.connections[cid]
        derived = self._derive(self.sim.network.state, conn)
        reachable = derived.a_to_b_reachable if side == 0 else derived.b_to_a_reachable
        if derived.state == c.DOWN or not reachable:
            direction.stalled = True
            self.sim.pipeline.mark(self._kind, {cid}, self.sim.env.now)
            return
        device, agent = (
            (conn.b_device, conn.b_agent)
            if side == 0
            else (conn.a_device, conn.a_agent)
        )
        assert device is not None and agent is not None
        progressed = False
        while direction.queue and direction.queue[0].release <= self.sim.env.now:
            queued = direction.queue[0]
            local = conn.b_local if side == 0 else conn.a_local
            assert local is not None
            entry = c.Delivery(
                self.sim.env.now,
                queued.message.payload,
                connection=cid,
                port=local.port,
                seq=queued.seq,
                generation=conn.b_generation if side == 0 else conn.a_generation,
            )
            if not self.sim.agents.deliver(device, agent, entry):
                self._inbox_rejected += 1
                direction.stalled = True
                break  # accepted data stays admitted; retry until timeout
            direction.queue.popleft()
            direction.size -= queued.message.size
            progressed = True
        if progressed or not direction.queue:
            self._cancel(direction.timer)
            direction.timer = None
            if direction.queue:
                direction.timer = self._later(
                    future(self.sim.env.now, conn.timeout),
                    lambda: self._expire(cid, side),
                )
        if conn.draining and not any(d.queue for d in runtime.directions):
            self._down(conn, c.CLOSED)
        else:
            self._kick(conn, side)

    def cancel_agent(self, device: str, agent: str, generation: int) -> None:
        self._cancel_datagrams(device, agent, generation)
        transport = self.sim.network.state.transport
        if transport is None:
            return
        if self.sim.network._dispatching:
            # Called from commit dispatch (agent removal or reset observed by
            # the agent runtime): a nested update is illegal there, and the
            # TRANSPORT kind derives the same DOWN/RESET states and listener
            # removals from that very delta (``affected`` sees the lifecycle
            # change), so the tree edit is left to its run. Retire runtime
            # events now: a same-time NORMAL timeout may precede that band.
            self._index(transport)
            for cid in sorted(self._by_device.get(device, ())):
                conn = transport.connections[cid]
                if self._direction(conn, device, agent, generation) is not None:
                    self._retire_session(cid)
            return
        listeners = transport.listeners
        for key, listener in transport.listeners.items():
            if (listener.device, listener.agent, listener.generation) == (
                device,
                agent,
                generation,
            ):
                listeners = listeners.remove(key)
        connections = transport.connections
        for cid, conn in connections.items():
            if (
                conn.state != c.DOWN
                and self._direction(conn, device, agent, generation) is not None
            ):
                connections = connections.set(
                    cid,
                    replace(
                        conn,
                        state=c.DOWN,
                        reason=c.RESET,
                        a_to_b_reachable=False,
                        b_to_a_reachable=False,
                    ),
                )
        self._publish(
            replace(transport, listeners=listeners, connections=connections), 'reset'
        )

    def budget(self) -> dict[str, Any]:
        """Live counters; canceled heap entries retain no payload or queue."""
        per_connection = {}
        messages = size = 0
        for cid, runtime in sorted(self._sessions.items()):
            directions = tuple(
                {'messages': len(d.queue), 'bytes': d.size} for d in runtime.directions
            )
            per_connection[cid] = directions
            messages += sum(d['messages'] for d in directions)
            size += sum(d['bytes'] for d in directions)
        return {
            'inflight_datagrams': self._inflight,
            'datagrams_dropped': self._dropped,
            'rejections': self._rejected,
            'inbox_rejections': self._inbox_rejected,
            'queued_messages': messages,
            'queued_bytes': size,
            'connections': per_connection,
            'scheduled_events': self._scheduled,
            'stale_events': self._stale,
            'compacted_events': self._compacted,
        }


__all__ = ['TransportRuntime']
