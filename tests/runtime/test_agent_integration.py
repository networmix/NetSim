"""C1F: immutable plugins exercising the real AGENT, NHT and transport runtimes.

The plugins retain their observations in AgentNode.state and communicate only
through context projections and AgentOutput. No runtime seam is replaced.
"""

from dataclasses import dataclass, field, replace

import pytest

from netsim import Environment
from netsim.model import contracts as c
from netsim.model import nht
from netsim.model.addressing import MacAddress
from netsim.model.network import published_failure
from netsim.model.routing import Nexthop, ResolutionPolicy, Route
from netsim.model.state import PMap, validate_immutable
from netsim.runtime.agents import AgentBatchError, AgentRuntime
from netsim.runtime.simulation import Simulation
from netsim.runtime.transport import TransportRuntime
from tests.model.test_network import A, build_diamond


@dataclass(frozen=True, slots=True)
class Observation:
    now: float
    initial: bool
    inbox: tuple[c.InboxEntry, ...]
    causes: tuple[c.Cause, ...]
    connections: PMap[int, c.ConnectionView]
    nht: PMap[c.NhtKey, c.NhtResult | None]
    timers: PMap[str, float]


@dataclass(frozen=True, slots=True)
class ProbeState:
    observations: tuple[Observation, ...] = ()
    opened: bool = False
    connection: int | None = None
    metric: int = 10


@dataclass(frozen=True, slots=True)
class Probe:
    """A finite discovery/session script; all evolving data lives in the tree."""

    side: str
    config: c.AgentConfig = field(
        default_factory=lambda: c.AgentConfig(
            run_delay=1 / 32, processing_delay=0, listen_ports=(4300,)
        )
    )
    hellos: int = 1
    sessions: bool = False
    initiator: bool = False
    close: bool = True
    messages: int = 2
    initial_timers: tuple[c.TimerOp, ...] = ()
    watch: int | None = None
    watch_key: c.NhtKey | None = None

    @property
    def client(self):
        return c.ClientId('integration')

    @property
    def profile(self):
        return c.ClientProfile(self.client, 115, link_state=True)

    def subscriptions(self):
        return ()

    def on_init(self, ctx):
        return self._run(ctx, initial=True)

    def on_run(self, ctx):
        return self._run(ctx, initial=False)

    def _run(self, ctx, *, initial):
        state = ProbeState() if initial else ctx.agent_state
        observation = Observation(
            ctx.now,
            initial,
            ctx.inbox,
            ctx.causes,
            ctx.connections,
            ctx.nht,
            ctx.timers,
        )
        state = replace(state, observations=state.observations + (observation,))
        interface = ctx.interfaces['Po1']
        local = c.Endpoint(6, interface.link_local, 4300, 'Po1')
        session_ops, messages, timers = [], [], []
        if initial and self.sessions:
            session_ops.append(c.SessionOp(c.LISTEN_OP, local=local))
        if initial:
            timers.extend(self.initial_timers)
        route_ops = ()
        for entry in ctx.inbox:
            if (
                isinstance(entry, c.Delivery)
                and entry.interface is not None
                and self.sessions
                and self.initiator
                and not state.opened
            ):
                # Discover the endpoint solely from received scoped sender identity.
                session_ops.append(
                    c.SessionOp(c.OPEN_OP, local, entry.sender.endpoint, timeout=2)
                )
                state = replace(state, opened=True)
            if isinstance(entry, c.SessionEvent) and entry.state == c.ESTABLISHED:
                state = replace(state, connection=entry.connection)
                messages.extend(
                    c.Message(entry.connection, (self.side, index), size=index + 1)
                    for index in range(self.messages)
                )
                timers.append(c.TimerOp('queue-sample', 1 / 16))
                if self.initiator and self.close:
                    timers.append(c.TimerOp('close', 1 / 4))
            if isinstance(entry, c.TimerFired):
                if entry.name == 'close':
                    messages.append(
                        c.Message(state.connection, (self.side, 'final'), size=5)
                    )
                    session_ops.append(
                        c.SessionOp(c.CLOSE_OP, connection=state.connection)
                    )
                elif entry.name == 'metric':
                    state = replace(state, metric=20)
        if self.watch is not None and (
            initial or state.metric != ctx.agent_state.metric
        ):
            route = Route(
                (self.watch, 32),
                4,
                ctx.client,
                self.profile.distance,
                (Nexthop(interface='Po1', address=A('10.1.12.1'), af=4),),
                metric=state.metric,
            )
            route_ops = (c.RouteOp(4, add=(route,)),)
        key = self.watch_key or (
            c.NhtKey(ctx.client, 4, self.watch) if self.watch is not None else None
        )
        ctx.stats.add(f'integration.{self.side}.runs')
        return c.AgentOutput(
            state=state,
            route_ops=route_ops,
            nht_ops=(c.NhtOp(c.REGISTER_NHT, key),) if initial and key else (),
            datagrams=tuple(
                c.Datagram('Po1', (self.side, 'hello', i), port=4300)
                for i in range(self.hellos)
            )
            if initial
            else (),
            messages=tuple(messages),
            sessions=tuple(session_ops),
            timers=tuple(timers),
        )


def diamond(left=None, right=None):
    net, routers = build_diamond()
    with net.batch():
        for link in net.links.values():
            link.configure(delay=1 / 8)
        for device in ('R1', 'R2'):
            routers[device]['Po1'].configure(forwarding_v6=True)
        net.add_agent('R1', left or Probe('left'))
        net.add_agent('R2', right or Probe('right'))
    sim = Simulation(Environment(), net, event_budget=1000)
    assert type(sim.agents) is AgentRuntime and type(sim.transport) is TransportRuntime
    return sim, routers


def node(sim, device):
    return sim.state.devices[device].agents['integration']


def observations(sim, device):
    return node(sim, device).state.observations


def entries(sim, device, kind):
    return tuple(
        entry
        for obs in observations(sim, device)
        for entry in obs.inbox
        if isinstance(entry, kind)
    )


def nht_wakes(sim, device='R1'):
    return tuple(
        obs for obs in observations(sim, device) if c.Cause(c.CAUSE_NHT) in obs.causes
    )


def session_diamond():
    return diamond(
        Probe('left', sessions=True, initiator=True),
        Probe('right', sessions=True, messages=3),
    )


def test_datagrams_have_scoped_sender_identity_and_timers_are_real():
    sim, _ = diamond(
        Probe('left', initial_timers=(c.TimerOp('tick', 1 / 2),)),
        Probe('right', initial_timers=(c.TimerOp('tick', 3 / 4),)),
    )
    sim.settle()
    assert sim.agents.budget()['armed_timers'] == 2
    assert sim.transport.budget()['inflight_datagrams'] == 2
    sim.run_until(1)
    for device, peer, side, fire_time in (
        ('R1', 'R2', 'right', 1 / 2),
        ('R2', 'R1', 'left', 3 / 4),
    ):
        (delivery,) = entries(sim, device, c.Delivery)
        address = MacAddress(
            sim.state.devices[peer].interfaces['Po1'].mac
        ).link_local_int()
        assert delivery.payload == (side, 'hello', 0)
        assert delivery.time == 1 / 8 and delivery.interface == 'Po1'
        assert delivery.sender == c.Sender('Po1', c.Endpoint(6, address, 4300, 'Po1'))
        assert not hasattr(delivery.sender, 'device')
        (timer,) = entries(sim, device, c.TimerFired)
        assert timer == c.TimerFired(fire_time, 'tick', node(sim, device).generation)
        assert observations(sim, device)[1].timers['tick'] == fire_time
        assert node(sim, device).runs == 3
    assert sim.agents.budget()['armed_timers'] == 0
    assert sim.transport.budget()['inflight_datagrams'] == 0
    validate_immutable(sim.state)


def test_discovered_session_sequence_directional_counters_and_drain_close():
    sim, _ = session_diamond()
    sim.settle()
    assert len(sim.state.transport.listeners) == 2
    sim.run_until(1 / 2)
    (conn,) = sim.state.transport.connections.values()
    assert (
        conn.state == c.ESTABLISHED and conn.a_to_b_reachable and conn.b_to_a_reachable
    )
    for device, peer, expected_count, own_count, own_bytes, initiator in (
        ('R1', 'right', 3, 2, 3, True),
        ('R2', 'left', 2, 3, 6, False),
    ):
        (established,) = entries(sim, device, c.SessionEvent)
        assert established.state == c.ESTABLISHED and established.initiator == initiator
        assert established.local.scope == established.remote.scope == 'Po1'
        received = tuple(
            entry for entry in entries(sim, device, c.Delivery) if entry.connection
        )
        assert [(entry.payload, entry.seq) for entry in received] == [
            ((peer, i), i) for i in range(expected_count)
        ]
        (sample,) = (
            obs
            for obs in observations(sim, device)
            if any(
                isinstance(entry, c.TimerFired) and entry.name == 'queue-sample'
                for entry in obs.inbox
            )
        )
        view = sample.connections[conn.id]
        assert (view.queued_messages, view.queued_bytes) == (own_count, own_bytes)
    sim.run_until(1)
    final = tuple(entry for entry in entries(sim, 'R2', c.Delivery) if entry.connection)
    assert [(entry.payload, entry.seq) for entry in final] == [
        (('left', 0), 0),
        (('left', 1), 1),
        (('left', 'final'), 2),
    ]
    for device in ('R1', 'R2'):
        (down,) = (
            entry
            for entry in entries(sim, device, c.SessionEvent)
            if entry.state == c.DOWN
        )
        assert down.reason == c.CLOSED
    received = tuple(entry for obs in observations(sim, 'R2') for entry in obs.inbox)
    assert received.index(final[-1]) < next(
        i
        for i, e in enumerate(received)
        if isinstance(e, c.SessionEvent) and e.state == c.DOWN
    )
    assert (
        sim.transport.budget()['queued_messages']
        == sim.transport.budget()['queued_bytes']
        == 0
    )
    validate_immutable(sim.state)


def test_nht_metric_only_wakeup_without_fib_change_and_no_epoch_only_wakeup():
    target = A('192.0.2.1')
    sim, routers = diamond(
        Probe(
            'left', hellos=0, watch=target, initial_timers=(c.TimerOp('metric', 1 / 2),)
        ),
        Probe('right', hellos=0),
    )
    sim.run_until(1 / 4)
    key = c.NhtKey(node(sim, 'R1').client, 4, target)
    before = sim.state.devices['R1']
    result = before.nht.registrations[key]
    fib = before.fibs[4]
    assert result.eligible and result.cost == 10 and result.cost_source == key.owner
    assert key.owner in before.ribs[4].link_state_sources
    initial_wakes = nht_wakes(sim)
    assert len(initial_wakes) == 1
    sim.run_until(3 / 4)
    after = sim.state.devices['R1']
    assert after.fibs[4] is fib
    assert after.nht.registrations[key] is not result
    assert after.nht.registrations[key].cost == 20
    wakes = nht_wakes(sim)
    assert len(wakes) == len(initial_wakes) + 1
    assert wakes[-1].nht[key] is after.nht.registrations[key]
    assert node(sim, 'R2').runs == 1
    runs = node(sim, 'R1').runs
    result = after.nht.registrations[key]
    epochs = after.resolver_input_epoch
    # Pure callbacks changing then restoring resolution policy publish only
    # advanced resolver bookkeeping, with no semantic NHT change.
    original = routers['R1'].node.config.resolution_policy

    def policy_change(policy):
        def apply(state):
            dev = state.devices['R1']
            return replace(
                state,
                devices=state.devices.set(
                    'R1',
                    replace(dev, config=replace(dev.config, resolution_policy=policy)),
                ),
            )

        return apply

    with sim.network.batch():
        sim.network.update(policy_change(ResolutionPolicy(max_ecmp_paths=1)), 'narrow')
        sim.network.update(policy_change(original), 'restore')
    sim.run_until(1)
    fresh = sim.state.devices['R1']
    assert fresh.resolver_input_epoch[4] > epochs[4]
    assert fresh.nht.input_epochs[4] == fresh.resolver_input_epoch[4]
    assert fresh.nht.registrations[key] is result and fresh.fibs[4] is fib
    assert node(sim, 'R1').runs == runs and nht_wakes(sim) == wakes
    assert nht.refresh(sim.state, 'R1') is sim.state


def test_transport_inbox_overflow_is_explicit_and_counted():
    slow = c.AgentConfig(
        run_delay=1 / 2, processing_delay=0, inbox_limit=1, listen_ports=(4300,)
    )
    sim, _ = diamond(Probe('left', hellos=3), Probe('right', config=slow, hellos=0))
    sim.run_until(1 / 4)
    budget = sim.transport.budget()
    assert (
        budget['inbox_rejections']
        == budget['rejections']
        == budget['datagrams_dropped']
        == 2
    )
    rejections = entries(sim, 'R1', c.Rejection)
    assert len(rejections) == 2
    assert all(
        entry.reason == c.OVERFLOW and entry.interface == 'Po1' for entry in rejections
    )
    assert sim.agents.budget()['inbox_entries'] == 1
    assert node(sim, 'R2').runs == 1
    sim.run_until(1)
    (accepted,) = entries(sim, 'R2', c.Delivery)
    assert accepted.payload == ('left', 'hello', 0)
    assert sim.agents.budget()['inbox_entries'] == 0


def test_observer_failure_does_not_replay_real_published_outboxes():
    sim, _ = diamond(Probe('left', sessions=True), Probe('right', sessions=True))

    def observer(_time, origin, _delta):
        if origin[:2] == ('kind', 'agent'):
            raise ValueError('after publication')

    sim.network.on_delta.insert(0, observer)
    with pytest.raises(ValueError, match='after publication') as raised:
        sim.settle()
    assert published_failure(raised.value)
    assert node(sim, 'R1').runs == node(sim, 'R2').runs == 1
    assert len(sim.state.transport.listeners) == 2
    assert sim.transport.budget()['inflight_datagrams'] == 2
    sim.network.on_delta.remove(observer)
    sim.retry()
    sim.settle()
    assert node(sim, 'R1').runs == node(sim, 'R2').runs == 1
    assert sim.transport.budget()['inflight_datagrams'] == 2
    sim.run_until(1)
    assert (
        len(entries(sim, 'R1', c.Delivery)) == len(entries(sim, 'R2', c.Delivery)) == 1
    )
    assert not entries(sim, 'R1', c.Rejection) and not entries(sim, 'R2', c.Rejection)
    assert sim.stats.counters['integration.left.runs'] == [(0, 1), (5 / 32, 1)]


@pytest.mark.parametrize('device', ['R1', 'R2'])
@pytest.mark.parametrize('operation', ['reset', 'remove'])
def test_agent_lifecycle_mid_session_cleans_old_incarnation(device, operation):
    sim, _ = diamond(
        Probe('left', sessions=True, initiator=True, close=False),
        Probe('right', sessions=True, messages=3),
    )
    sim.run_until(3 / 8)
    (conn,) = sim.state.transport.connections.values()
    assert conn.state == c.ESTABLISHED
    assert sim.transport.budget()['queued_messages'] == 5
    previous = node(sim, device)
    peer = 'R2' if device == 'R1' else 'R1'
    if operation == 'reset':
        sim.reset_agent(device, 'integration')
        assert node(sim, device).generation != previous.generation
        assert (
            node(sim, device).receipt
            is node(sim, device).rng
            is node(sim, device).state
            is None
        )
    else:
        sim.network.remove_agent(device, 'integration')
        assert sim.agents.generation(device, 'integration') is None
    sim.settle()
    assert sim.state.transport.connections[conn.id].state == c.DOWN
    assert sim.state.transport.connections[conn.id].reason == c.RESET
    assert not any(
        listener.device == device and listener.generation == previous.generation
        for listener in sim.state.transport.listeners.values()
    )
    assert sim.transport.budget()['queued_messages'] == 0
    assert not sim.agents.deliver(
        device, 'integration', c.TimerFired(sim.env.now, 'stale', previous.generation)
    )
    # Old message deliveries, queue probes and no-progress timers all reach
    # their deadlines; none may revive the connection or deliver old data.
    sim.run_until(3)
    (down,) = (
        entry for entry in entries(sim, peer, c.SessionEvent) if entry.state == c.DOWN
    )
    assert down.connection == conn.id and down.reason == c.RESET
    assert not any(
        entry.connection == conn.id for entry in entries(sim, peer, c.Delivery)
    )
    if operation == 'reset':
        assert not any(
            entry.connection == conn.id for entry in entries(sim, device, c.Delivery)
        )
        assert not entries(sim, device, c.TimerFired)
        assert observations(sim, device)[0].initial
    assert sim.transport.budget()['queued_messages'] == 0
    assert sim.transport.budget()['stale_events'] == 0
    assert sim.agents.budget()['stale_timer_events'] == 0
    validate_immutable(sim.state)


def test_reset_on_init_can_rebind_its_listener():
    # AGENT initialization precedes TRANSPORT cleanup in the reset round.
    # LISTEN must allow replacement of the old, no-longer-live generation.
    sim, _ = diamond(
        Probe('left', sessions=True, initiator=True, close=False),
        Probe('right', sessions=True),
    )
    sim.run_until(3 / 8)
    old_generation = node(sim, 'R2').generation
    sim.reset_agent('R2', 'integration')
    sim.run_until(1)
    new_generation = node(sim, 'R2').generation
    assert new_generation != old_generation
    assert not entries(sim, 'R2', c.Rejection)
    (listener,) = (
        entry
        for entry in sim.state.transport.listeners.values()
        if entry.device == 'R2'
    )
    assert listener.generation == new_generation


@pytest.mark.parametrize('scope', ['missing', 'stale'])
def test_agent_nht_registration_uses_c2_scope_validation(scope):
    # An agent and a direct NhtClient must enforce the same scope contract.
    key = c.NhtKey(
        c.ClientId('integration'),
        6,
        A('fe80::1'),
        interface='missing' if scope == 'missing' else 'Po1',
        interface_generation=0 if scope == 'stale' else None,
    )
    sim, _ = diamond(Probe('left', hellos=0, watch_key=key), Probe('right', hellos=0))
    with pytest.raises(ValueError, match='scope is missing or stale'):
        nht.register(sim.state, 'R1', key)
    with pytest.raises(AgentBatchError, match='scope is missing or stale'):
        sim.settle()
    assert node(sim, 'R2').runs == 1
    assert node(sim, 'R1').runs == 0
    assert node(sim, 'R1').rng is node(sim, 'R1').state is None
    assert sim.state.devices['R1'].nht is None
