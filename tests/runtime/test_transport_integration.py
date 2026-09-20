"""C3F transport through real plugin publication, inboxes and agent rounds."""

from dataclasses import dataclass, field, replace

import pytest

from netsim import Environment
from netsim.model import contracts as c
from netsim.model.addressing import MacAddress
from netsim.model.interfaces import OperState
from netsim.model.network import Network
from netsim.model.state import StateDelta, validate_immutable
from netsim.runtime.agents import AgentRuntime
from netsim.runtime.simulation import Simulation
from tests.model.test_network import A, build_diamond


@dataclass(frozen=True)
class Observations:
    entries: tuple = ()
    runs: tuple = ()


@dataclass(frozen=True)
class Plugin:
    client: c.ClientId = field(default_factory=lambda: c.ClientId('wire'))
    config: c.AgentConfig = field(
        default_factory=lambda: c.AgentConfig(
            run_delay=0, processing_delay=0, listen_ports=(179,)
        )
    )
    initial: c.AgentOutput = field(default_factory=c.AgentOutput)
    actions: tuple = ()

    @property
    def profile(self):
        return c.ClientProfile(self.client, 115)

    def subscriptions(self):
        return ()

    def on_init(self, ctx):
        return replace(
            self.initial, state=Observations(runs=((ctx.now, ctx.connections),))
        )

    def on_run(self, ctx):
        old = ctx.agent_state
        state = Observations(
            old.entries + ctx.inbox, old.runs + ((ctx.now, ctx.connections),)
        )
        actions = dict(self.actions)
        # Every command is an inbox event. Responses and all transport sends
        # pass through the actual AgentOutput receipt/publication path.
        outputs = [
            actions[e.name]
            for e in ctx.inbox
            if isinstance(e, c.TimerFired) and e.name in actions
        ]
        return c.AgentOutput(
            state=state,
            datagrams=tuple(x for out in outputs for x in out.datagrams),
            messages=tuple(x for out in outputs for x in out.messages),
            sessions=tuple(x for out in outputs for x in out.sessions),
        )


def local(net, device, iface):
    node = net.state.devices[device].interfaces[iface]
    return c.Endpoint(6, MacAddress(node.mac).link_local_int(), 179, iface)


def wire_network(delay=0.125):
    net = Network(seed=17)
    a, b = net.add_device('R1'), net.add_device('R2')
    net.add_p2p(a, 'e1', b, 'e2', unnumbered=True, delay=delay)
    return net


def bind(net, a=None, b=None):
    a, b = a or Plugin(), b or Plugin()
    net.add_agent('R1', a)
    net.add_agent('R2', b)
    sim = Simulation(Environment(), net)
    assert isinstance(sim.agents, AgentRuntime)
    sim.settle()
    return sim


def command(sim, name, device='R1'):
    gen = sim.agents.generation(device, 'wire')
    assert sim.agents.deliver(device, 'wire', c.TimerFired(sim.env.now, name, gen))
    sim.settle()


def observed(sim, device='R1', kind=None):
    entries = sim.state.devices[device].agents['wire'].state.entries
    if kind is not None:
        entries = tuple(e for e in entries if isinstance(e, kind))
    return entries


def down(sim, device='R1'):
    return tuple(e for e in observed(sim, device, c.SessionEvent) if e.state == c.DOWN)


def session(
    *,
    timeout=1,
    listen=True,
    simultaneous=False,
    config=None,
    actions=(),
    b_actions=(),
    net=None,
    count=1,
):
    net = net or wire_network()
    if 'e1' in net.state.devices['R1'].interfaces:
        a, b = local(net, 'R1', 'e1'), local(net, 'R2', 'e2')
        remote_b, remote_a = replace(b, scope=a.scope), replace(a, scope=b.scope)
    else:
        a, b = c.Endpoint(4, A('10.0.0.1'), 179), c.Endpoint(4, A('10.0.0.2'), 179)
        remote_b, remote_a = b, a
    config = config or c.AgentConfig(
        run_delay=0, processing_delay=0, listen_ports=(179,)
    )
    a_initial = c.AgentOutput(
        sessions=((c.SessionOp('listen', a),) if simultaneous else ())
        + (c.SessionOp('open', a, remote_b, timeout=timeout),) * count
    )
    b_initial = c.AgentOutput(
        sessions=((c.SessionOp('listen', b),) if listen else ())
        + ((c.SessionOp('open', b, remote_a, timeout=timeout),) if simultaneous else ())
    )
    return bind(
        net,
        Plugin(config=config, initial=a_initial, actions=actions),
        Plugin(config=config, initial=b_initial, actions=b_actions),
    )


def send_output(*payloads, cid=1, size=0):
    return c.AgentOutput(messages=tuple(c.Message(cid, p, size=size) for p in payloads))


def test_cold_start_link_local_exchange_and_generation_stamps():
    net = wire_network()
    sim = bind(
        net,
        Plugin(initial=c.AgentOutput(datagrams=(c.Datagram('e1', 'hello', port=179),))),
        Plugin(initial=c.AgentOutput(datagrams=(c.Datagram('e2', 'reply', port=179),))),
    )
    assert not sim.state.devices['R1'].fibs[6].entries
    assert not observed(sim, kind=c.Delivery)
    sim.run_until(0.125)
    for device, payload, scope in [('R1', 'reply', 'e1'), ('R2', 'hello', 'e2')]:
        (entry,) = observed(sim, device, c.Delivery)
        assert (entry.time, entry.payload, entry.interface) == (0.125, payload, scope)
        assert entry.sender.interface == entry.sender.endpoint.scope == scope
        assert entry.generation == sim.agents.generation(device, 'wire')
        validate_immutable(sim.state.devices[device].agents['wire'].state)


def test_physical_failure_during_carrier_delay_rejects_real_outbox():
    net = wire_network(delay=1)
    for device, iface in [('R1', 'e1'), ('R2', 'e2')]:
        net.devices[device][iface].configure(carrier_delay_down=1)
    send = c.AgentOutput(datagrams=(c.Datagram('e1', 'hello', port=179),))
    sim = bind(net, Plugin(actions=(('send', send),)))
    sim.at(9.5, lambda: command(sim, 'send'))
    sim.at(10, next(iter(net.links.values())).fail)
    sim.run_until(10.25)
    assert sim.state.devices['R1'].interfaces['e1'].oper.oper == OperState.UP
    command(sim, 'send')
    (rejection,) = observed(sim, kind=c.Rejection)
    assert rejection.reason == 'LINK_DOWN'
    assert rejection.generation == sim.agents.generation('R1', 'wire')
    sim.run_until(11)
    assert not observed(sim, 'R2', c.Delivery)
    assert sim.transport.budget()['datagrams_dropped'] == 1


def test_real_datagram_fifo_under_delay_change():
    net = wire_network(delay=1)
    sim = bind(
        net,
        Plugin(
            initial=c.AgentOutput(datagrams=(c.Datagram('e1', 'U1', port=179),)),
            actions=(
                (
                    'second',
                    c.AgentOutput(datagrams=(c.Datagram('e1', 'U2', port=179),)),
                ),
            ),
        ),
    )
    sim.run_until(0.125)
    next(iter(net.links.values())).configure(delay=0.125)
    command(sim, 'second')
    sim.run_until(1)
    assert [(e.time, e.payload) for e in observed(sim, 'R2', c.Delivery)] == [
        (1, 'U1'),
        (1, 'U2'),
    ]


def routed_network():
    net, routers = build_diamond(min_links=1)
    for link in net.links.values():
        link.configure(delay=0.125)
    routers['R2'].add_route('10.0.0.1/32', [('eth3', '10.1.24.1')])
    routers['R4'].add_route('10.0.0.1/32', [('eth2', '10.1.34.0')])
    routers['R3'].add_route('10.0.0.1/32', [('eth1', '10.1.13.0')])
    return net


def test_real_routed_session_one_way_outage_and_ordered_release():
    sim = session(
        net=routed_network(),
        actions=(('send', send_output('U1', 'U2')),),
        b_actions=(('send', send_output('reverse')),),
    )
    sim.run_until(0.125)
    assert sim.state.transport.connections[1].state == c.ESTABLISHED
    assert sim.state.transport.connections[1].deps == ('R1', 'R2', 'R3', 'R4')
    sim.network.devices['R1']['Po1'].admin_down()
    sim.settle()
    conn = sim.state.transport.connections[1]
    assert not conn.a_to_b_reachable and conn.b_to_a_reachable
    command(sim, 'send')
    command(sim, 'send', 'R2')
    sim.run_until(0.625)
    assert [e.payload for e in observed(sim, 'R1', c.Delivery)] == ['reverse']
    assert not observed(sim, 'R2', c.Delivery)
    sim.network.devices['R1']['Po1'].admin_up()
    sim.settle()
    sim.run_until(0.75)
    got = observed(sim, 'R2', c.Delivery)
    assert [(e.time, e.payload, e.seq) for e in got] == [
        (0.75, 'U1', 0),
        (0.75, 'U2', 1),
    ]
    assert all(e.generation == sim.agents.generation('R2', 'wire') for e in got)


def test_refusal_through_real_agent_inbox():
    sim = session(listen=False)
    assert not down(sim)
    sim.run_until(0.125)
    (event,) = down(sim)
    assert event.reason == c.REFUSED
    assert event.generation == sim.agents.generation('R1', 'wire')


def test_no_progress_timeout_reaches_both_real_agents():
    sim = session(timeout=0.5, actions=(('send', send_output('pending')),))
    sim.run_until(0.125)
    command(sim, 'send')
    next(iter(sim.network.links.values())).fail()
    sim.settle()
    sim.run_until(0.625)
    for device in ('R1', 'R2'):
        (event,) = down(sim, device)
        assert event.reason == c.TIMEOUT
        assert event.generation == sim.agents.generation(device, 'wire')
    next(iter(sim.network.links.values())).restore()
    sim.run_until(2)
    assert not observed(sim, 'R2', c.Delivery)


@pytest.mark.parametrize('operation,delivered', [('close', True), ('abort', False)])
def test_close_vs_abort_from_one_published_output(operation, delivered):
    output = replace(
        send_output('final'), sessions=(c.SessionOp(operation, connection=1),)
    )
    sim = session(actions=(('finish', output),))
    sim.run_until(0.125)
    command(sim, 'finish')
    sim.run_until(1)
    entries = observed(sim, 'R2')
    assert bool(observed(sim, 'R2', c.Delivery)) is delivered
    assert down(sim, 'R2')[0].reason == (c.CLOSED if delivered else c.ABORTED)
    if delivered:
        assert entries.index(observed(sim, 'R2', c.Delivery)[0]) < entries.index(
            down(sim, 'R2')[0]
        )


def test_reset_dispatch_removes_listener_and_invalidates_queued_and_late_entries():
    # Listen only on a command, so on_init after reset does not re-register it.
    net = wire_network()
    a, b = local(net, 'R1', 'e1'), local(net, 'R2', 'e2')
    sim = bind(
        net,
        Plugin(
            actions=(
                (
                    'open',
                    c.AgentOutput(
                        sessions=(c.SessionOp('open', a, replace(b, scope='e1')),)
                    ),
                ),
                ('send', send_output('late')),
            )
        ),
        Plugin(
            actions=(
                ('listen', c.AgentOutput(sessions=(c.SessionOp('listen', b),))),
                (
                    'send',
                    replace(
                        send_output('old outbound'),
                        datagrams=(c.Datagram('e2', 'old hello', port=179),),
                    ),
                ),
            )
        ),
    )
    command(sim, 'listen', 'R2')
    command(sim, 'open')
    sim.run_until(0.125)
    command(sim, 'send')
    command(sim, 'send', 'R2')
    old = sim.agents.generation('R2', 'wire')
    sim.reset_agent('R2', 'wire')
    # cancel_agent is called during commit dispatch: publication is deferred.
    assert sim.state.transport.connections[1].state == c.ESTABLISHED
    sim.settle()
    assert sim.agents.generation('R2', 'wire') != old
    assert not sim.state.devices['R2'].agents['wire'].state.runs[0][1]
    assert sim.state.transport.connections[1].reason == c.RESET
    assert not sim.state.transport.listeners
    (event,) = down(sim)
    assert event.reason == c.RESET
    sim.run_until(2)
    assert not observed(sim, 'R1', c.Delivery)
    assert not observed(sim, 'R2', c.Delivery)
    assert sim.transport.budget()['queued_messages'] == 0


def test_backpressure_rejections_and_connection_view_direction_counters():
    config = c.AgentConfig(
        run_delay=0,
        processing_delay=0,
        listen_ports=(179,),
        queue_limit=2,
        byte_limit=5,
    )
    output = c.AgentOutput(
        messages=(
            c.Message(1, 'a', size=3),
            c.Message(1, 'byte overflow', size=3),
            c.Message(1, 'b', size=2),
            c.Message(1, 'count overflow'),
        )
    )
    sim = session(
        config=config,
        actions=(('send', output), ('observe', c.AgentOutput())),
        b_actions=(('observe', c.AgentOutput()),),
    )
    sim.run_until(0.125)
    command(sim, 'send')
    rejects = observed(sim, kind=c.Rejection)
    assert [e.reason for e in rejects] == [c.OVERFLOW, c.OVERFLOW]
    assert all(e.generation == sim.agents.generation('R1', 'wire') for e in rejects)
    command(sim, 'observe')
    command(sim, 'observe', 'R2')
    views = [
        sim.state.devices[d].agents['wire'].state.runs[-1][1][1] for d in ('R1', 'R2')
    ]
    assert [(v.queued_messages, v.queued_bytes) for v in views] == [(2, 5), (0, 0)]
    sim.run_until(0.25)
    assert [(e.payload, e.seq) for e in observed(sim, 'R2', c.Delivery)] == [
        ('a', 0),
        ('b', 1),
    ]


def test_simultaneous_opens_are_distinct_with_real_agents():
    sim = session(simultaneous=True)
    sim.run_until(0.125)
    assert tuple(sim.state.transport.connections) == (1, 2)
    for device in ('R1', 'R2'):
        events = observed(sim, device, c.SessionEvent)
        assert len(events) == 2
        assert all(e.state == c.ESTABLISHED for e in events)
        assert sorted(e.initiator for e in events) == [False, True]


def test_stale_generation_is_rejected_before_inbox_validation_or_mutation():
    config = c.AgentConfig(run_delay=0.125, processing_delay=0, listen_ports=(179,))
    sim = bind(
        wire_network(),
        Plugin(initial=c.AgentOutput(datagrams=(c.Datagram('e1', 'first', port=179),))),
        Plugin(config=config),
    )
    sim.run_until(0.125)
    old = sim.agents.generation('R2', 'wire')
    queued = tuple(sim.agents._inboxes['R2', 'wire', old])
    assert len(queued) == 1 and queued[0].generation == old
    sim.reset_agent('R2', 'wire')
    sim.settle()
    before = dict(sim.agents._inboxes)
    pending = dict(sim.agents.kind().pending)
    # Mutable payload would fail validation if the stale guard ran too late.
    stale = replace(queued[0], payload=[], generation=old)
    assert sim.agents.deliver('R2', 'wire', stale) is False
    assert sim.agents._inboxes == before
    assert sim.agents.kind().pending == pending
    sim.run_until(0.5)
    assert not observed(sim, 'R2', c.Delivery)


@pytest.mark.parametrize('run_delay', [0, 0.03125])
def test_failed_route_query_is_a_dependency_and_insertion_releases_open(
    run_delay, monkeypatch
):
    net = routed_network()
    net.devices['R1'].rib_client().sync([])
    sim = session(
        net=net,
        config=c.AgentConfig(run_delay=run_delay, processing_delay=0),
        actions=(('send', send_output('after route')),),
    )
    runs = []
    for kind in sim.pipeline.kinds:
        original = kind.run

        def run(state, now, entities, original=original, kind=kind):
            runs.append((now, sim.pipeline.round_gen, kind.name))
            return original(state, now, entities)

        monkeypatch.setattr(kind, 'run', run)
    assert sim.state.transport.connections[1].deps == ('R1',)
    assert not observed(sim, kind=c.SessionEvent)
    sim.run_until(0.25)
    net.devices['R1'].add_route('10.0.0.2/32', [('Po1', '10.1.12.1')])
    sim.settle()
    assert sim.state.transport.connections[1].state == c.CONNECTING
    assert sim.state.transport.connections[1].a_to_b_reachable
    assert not observed(sim, kind=c.SessionEvent)
    at_insertion = [(round_, name) for time, round_, name in runs if time == 0.25]
    assert at_insertion == [(0, 'fib'), (0, 'transport'), (1, 'transport')]
    sim.run_until(0.375)
    if run_delay:
        assert not observed(sim, kind=c.SessionEvent)
        key = ('R1', 'wire', sim.agents.generation('R1', 'wire'))
        assert sim.agents._inboxes[key][0].time == 0.375
    sim.run_until(0.375 + run_delay)
    assert observed(sim, kind=c.SessionEvent)[0].time == 0.375
    command(sim, 'send')
    arrival = 0.5 + 2 * run_delay
    sim.run_until(arrival + run_delay)
    assert observed(sim, 'R2', c.Delivery)[0].time == arrival
    # NORMAL arrival at .375 opens a new AGENT round; batching adds real
    # time, never an extra transport message at the route insertion time.
    agent_runs = [time for time, _, name in runs if name == 'agent']
    assert min(agent_runs) == 0.375 + run_delay


@pytest.mark.timeout(60)
def test_unrelated_device_change_does_not_scan_idle_connections(monkeypatch):
    net = wire_network()
    net.add_device('unrelated')
    sim = session(net=net, count=1000)
    sim.run_until(0.125)
    assert len(sim.state.transport.connections) == 1000
    old = sim.state
    net.devices['unrelated'].configure(router_id=A('192.0.2.1'))
    delta = StateDelta(old, sim.state)
    connections = sim.state.transport.connections
    visits = []
    original = type(connections).items

    def counted(mapping):
        for key, value in original(mapping):
            if mapping is connections:
                visits.append(key)
            yield key, value

    monkeypatch.setattr(type(connections), 'items', counted)
    affected = sim.transport.affected(delta, sim.state)
    assert visits == []
    assert affected == set()


def test_fresh_runtime_reestablishes_sessions_with_initial_listeners():
    sim = session()
    sim.run_until(0.125)
    old_generations = {d: sim.agents.generation(d, 'wire') for d in ('R1', 'R2')}
    fresh = Simulation(Environment(), sim.network.fork())
    fresh.run_until(0.125)
    assert fresh.state.transport.connections[1].reason == c.RESET
    assert fresh.state.transport.connections[2].state == c.ESTABLISHED
    assert len(fresh.state.transport.listeners) == 1
    for device in ('R1', 'R2'):
        assert fresh.agents.generation(device, 'wire') != old_generations[device]
        assert not observed(fresh, device, c.Rejection)
    assert sim.state.transport.connections[1].state == c.ESTABLISHED


@pytest.mark.xfail(
    strict=True,
    reason='C1: AgentRuntime._context must rebase a scoped remote endpoint to local.scope',
)
def test_connection_view_remote_scope_is_local_to_the_observing_agent():
    sim = session()
    sim.run_until(0.125)
    for device, interface in [('R1', 'e1'), ('R2', 'e2')]:
        view = sim.state.devices[device].agents['wire'].state.runs[-1][1][1]
        assert view.local.scope == view.remote.scope == interface


def test_dependency_index_tracks_path_replacement_and_closed_connections():
    sim = session(
        net=routed_network(),
        actions=(
            ('abort', c.AgentOutput(sessions=(c.SessionOp('abort', connection=1),))),
        ),
    )
    sim.run_until(0.125)
    sim.network.devices['R2'].add_route('10.0.0.1/32', [('Po1', '10.1.12.0')])
    sim.settle()
    assert sim.state.transport.connections[1].deps == ('R1', 'R2')
    old = sim.state
    sim.network.devices['R3'].configure(router_id=A('192.0.2.3'))
    assert sim.transport.affected(StateDelta(old, sim.state), sim.state) == set()
    sim.network.devices['R2'].add_route('10.0.0.1/32', [('eth3', '10.1.24.1')])
    sim.settle()
    assert sim.state.transport.connections[1].deps == ('R1', 'R2', 'R3', 'R4')
    old = sim.state
    sim.network.devices['R3'].configure(router_id=A('192.0.2.4'))
    assert sim.transport.affected(StateDelta(old, sim.state), sim.state) == {1}
    command(sim, 'abort')
    assert not sim.transport._connection_deps
    old = sim.state
    sim.network.devices['R3'].configure(router_id=A('192.0.2.5'))
    assert sim.transport.affected(StateDelta(old, sim.state), sim.state) == set()


def test_datagram_fanout_stamps_each_receivers_distinct_generation():
    net = wire_network()
    net.add_agent('R2', Plugin(client=c.ClientId('other')))
    sim = bind(
        net,
        Plugin(initial=c.AgentOutput(datagrams=(c.Datagram('e1', 'hello', port=179),))),
    )
    sim.run_until(0.125)
    generations = []
    for name in ('wire', 'other'):
        node = sim.state.devices['R2'].agents[name]
        (entry,) = node.state.entries
        assert entry.generation == node.generation
        generations.append(entry.generation)
    assert generations[0] != generations[1]


def test_default_processing_delay_makes_zero_delay_wire_delivery_future():
    # Exercise main's new default separately from the dyadic timing fixtures.
    config = c.AgentConfig(run_delay=0, listen_ports=(179,))
    sim = bind(
        wire_network(delay=0),
        Plugin(
            config=config,
            initial=c.AgentOutput(datagrams=(c.Datagram('e1', 'hello', port=179),)),
        ),
        Plugin(config=config),
    )
    assert sim.transport.budget()['inflight_datagrams'] == 1
    assert not observed(sim, 'R2', c.Delivery)
    sim.run_until(0.001)
    (entry,) = observed(sim, 'R2', c.Delivery)
    assert entry.time == 0.001


def measure_affected(count=1000, calls=2000, repeats=7, baseline=None):
    """Manual same-fixture comparison; exclude setup and count actual visits.

    Pass the unbound TransportRuntime.affected from the base commit to compare
    before/after in one process. The fixture uses real opens and inboxes.
    """
    from statistics import median
    from time import perf_counter

    net = wire_network()
    net.add_device('unrelated')
    sim = session(net=net, count=count)
    sim.run_until(0.125)
    assert all(
        conn.state == c.ESTABLISHED for conn in sim.state.transport.connections.values()
    )
    assert sim.transport.budget()['queued_messages'] == 0
    before = sim.state
    net.devices['unrelated'].configure(router_id=A('192.0.2.1'))
    delta = StateDelta(before, sim.state)
    functions = [('new', type(sim.transport).affected)]
    if baseline is not None:
        functions.insert(0, ('old', baseline))
    results = []
    for label, affected in functions:
        result = affected(sim.transport, delta, sim.state)
        connections = sim.state.transport.connections
        map_type = type(connections)  # promoted maps override PMap.items
        items = map_type.items
        visits = 0

        def counted(mapping, items=items, connections=connections):
            nonlocal visits
            for key, value in items(mapping):
                if mapping is connections:
                    visits += 1
                yield key, value

        map_type.items = counted
        try:
            affected(sim.transport, delta, sim.state)
        finally:
            map_type.items = items
        samples = []
        for _ in range(repeats):
            start = perf_counter()
            for _ in range(calls):
                affected(sim.transport, delta, sim.state)
            samples.append((perf_counter() - start) / calls * 1e6)
        results.append(
            {
                'version': label,
                'median_us': median(samples),
                'samples_us': samples,
                'connections_examined': visits,
                'affected': sorted(result),
            }
        )
    return {
        'connections': count,
        'calls': calls,
        'repeats': repeats,
        'results': results,
    }


def test_reset_at_old_timeout_deadline_wins_over_late_incarnation_event():
    sim = session(timeout=0.5, actions=(('send', send_output('pending')),))
    sim.run_until(0.125)
    # Register reset first so both NORMAL events are due at .625, reset then
    # timeout. TRANSPORT runs after both: cancellation must retire the timer
    # during dispatch, even though DOWN/RESET publication waits for the kind.
    sim.at(0.625, lambda: sim.reset_agent('R1', 'wire'))
    next(iter(sim.network.links.values())).fail()
    sim.settle()
    command(sim, 'send')
    sim.run_until(0.625)
    assert sim.state.transport.connections[1].reason == c.RESET
    assert [(e.connection, e.reason) for e in down(sim, 'R2')] == [(1, c.RESET)]


@pytest.mark.parametrize('operation', ['send', 'close', 'abort'])
def test_reset_dispatch_rejects_peer_operations_before_transport_cleanup(operation):
    output = (
        send_output('same round')
        if operation == 'send'
        else c.AgentOutput(sessions=(c.SessionOp(operation, connection=1),))
    )
    sim = session(b_actions=(('operate', output),))
    sim.run_until(0.125)
    sim.reset_agent('R1', 'wire')
    assert sim.state.transport.connections[1].state == c.ESTABLISHED
    # Peer AGENT publication precedes TRANSPORT's deferred DOWN/RESET. Its
    # old connection can no longer accept operations or replace RESET.
    command(sim, 'operate', 'R2')
    assert [(e.connection, e.reason) for e in down(sim, 'R2')] == [(1, c.RESET)]
    (rejection,) = observed(sim, 'R2', c.Rejection)
    assert rejection.reason == 'NOT_ESTABLISHED'
    assert rejection.generation == sim.agents.generation('R2', 'wire')
    sim.run_until(0.5)
    assert not observed(sim, 'R1', c.Delivery)
