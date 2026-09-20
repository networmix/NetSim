"""C3R adversarial regressions: locality, explicit outcomes and indexed work."""

from dataclasses import replace

import pytest

from netsim.model import contracts as c
from netsim.model.interfaces import OperState
from netsim.model.state import PMap, StateDelta
from tests.runtime.test_sessions import message, open_pair
from tests.runtime.test_transport import pair, send
from tests.runtime.test_transport_integration import (
    Plugin,
    bind,
    command,
    observed,
    send_output,
    session,
    wire_network,
)


def test_dead_wire_is_not_an_agent_observation_before_detection():
    net = wire_network()
    for device, iface in [('R1', 'e1'), ('R2', 'e2')]:
        net.devices[device][iface].configure(carrier_delay_down=10)
    sim = session(
        net=net,
        actions=(
            ('hello', c.AgentOutput(datagrams=(c.Datagram('e1', 'hello', port=179),))),
        ),
    )
    sim.run_until(0.25)
    next(iter(net.links.values())).fail()
    sim.settle()
    command(sim, 'hello')
    assert net.state.devices['R1'].interfaces['e1'].oper.oper == OperState.UP
    view = net.state.devices['R1'].agents['wire'].state.runs[-1][1][1]
    assert view.state == c.ESTABLISHED
    assert not hasattr(view, 'reachable')
    assert not observed(sim, kind=c.Rejection)
    assert sim.transport.budget()['datagrams_lost_physical'] == 1
    sim.run_until(0.5)
    assert not observed(sim, 'R2', c.Delivery)


def test_rejected_control_delivery_raises():
    sim, ga, _ = pair()
    sim.agents.full = True
    with pytest.raises(RuntimeError, match='control'):
        send(sim, ga, 'bad interface', interface='missing')
    assert sim.transport.budget()['inbox_rejections'] == 1


def test_session_control_failure_does_not_skip_the_peer():
    sim, _, _, _ = open_pair()
    sim.agents.blocked.add(('R1', 'ref'))
    with pytest.raises(RuntimeError, match='control'):
        sim.run_until(0.125)
    assert any(
        d == 'R2' and isinstance(e, c.SessionEvent) for d, _, e in sim.agents.entries
    )


def test_data_inbox_refusal_is_reported_to_sender_once_and_retried():
    sim, ga, _, cid = open_pair()
    sim.run_until(0.125)
    sim.agents.blocked.add(('R2', 'ref'))
    message(sim, ga, cid, 'accepted')
    sim.run_until(0.375)
    rejects = [
        (d, e.reason) for d, _, e in sim.agents.entries if isinstance(e, c.Rejection)
    ]
    assert rejects == [('R1', 'INBOX_FULL')]
    assert sim.transport.budget()['queued_messages'] == 1
    sim.agents.blocked.clear()
    sim.run_until(0.5)
    assert [
        (e.payload, e.seq)
        for _, _, e in sim.agents.entries
        if isinstance(e, c.Delivery)
    ] == [('accepted', 0)]


def test_real_one_slot_inboxes_keep_session_events_and_all_overflows():
    config = c.AgentConfig(
        run_delay=1, processing_delay=0, inbox_limit=1, queue_limit=1
    )
    sim = session(config=config, actions=(('burst', send_output(0, 1, 2)),))
    for device in ('R1', 'R2'):
        assert sim.agents.deliver(device, 'wire', c.TimerFired(0, 'occupied'))
    sim.run_until(1.25)
    for device in ('R1', 'R2'):
        assert [e.state for e in observed(sim, device, c.SessionEvent)] == [
            c.ESTABLISHED
        ]
    command(sim, 'burst')
    sim.run_until(3.5)
    assert [e.reason for e in observed(sim, kind=c.Rejection)] == [
        c.OVERFLOW,
        c.OVERFLOW,
    ]


@pytest.mark.parametrize('count', [10, 1000])
def test_unrelated_agent_state_with_unindexed_down_records_examines_none(
    count, monkeypatch
):
    sim = bind(wire_network(), Plugin())
    old = sim.state
    ep = c.Endpoint(4, 1, 179)
    old = replace(
        old,
        transport=c.TransportState(
            connections=PMap(
                (i, c.ConnectionState(i, 'elsewhere', 'x', ep, 1, state=c.DOWN))
                for i in range(1, count + 1)
            )
        ),
    )
    dev = old.devices['R1']
    new = replace(
        old,
        devices=old.devices.set(
            'R1',
            replace(
                dev,
                agents=dev.agents.set('wire', replace(dev.agents['wire'], state=42)),
            ),
        ),
    )
    connections = old.transport.connections
    items = type(connections).items
    visits = []

    def counted(mapping):
        for key, value in items(mapping):
            if mapping is connections:
                visits.append(key)
            yield key, value

    monkeypatch.setattr(type(connections), 'items', counted)
    assert sim.transport.affected(StateDelta(old, new), new) == set()
    assert visits == []


def test_transport_edit_schedules_only_changed_connections_and_no_global_commit_scan(
    monkeypatch,
):
    sim = session(count=1000)
    sim.run_until(0.125)
    old = sim.state
    table = old.transport.connections
    changed = replace(table[1], draining=True)
    new = replace(
        old, transport=replace(old.transport, connections=table.set(1, changed))
    )
    delta = StateDelta(old, new)
    assert sim.transport.affected(delta, new) == {1}

    def forbid(_):
        raise AssertionError('global connection scan')

    monkeypatch.setattr(type(table), 'sorted_items', forbid)
    sim.transport._committed(sim.env.now, 'probe', delta)


def test_owner_index_and_direction_counters_are_detached():
    sim = session(actions=(('send', send_output('queued', size=7)),))
    sim.run_until(0.125)
    owner = ('R1', 'wire', sim.agents.generation('R1', 'wire'))
    assert sim.transport.connections_of(*owner) == (1,)
    assert sim.transport.connections_of('R1', 'wire', -1) == ()
    command(sim, 'send')
    counters = sim.transport.connection_counters(1)
    assert counters == ({'messages': 1, 'bytes': 7}, {'messages': 0, 'bytes': 0})
    counters[0]['messages'] = 999
    assert sim.transport.connection_counters(1)[0]['messages'] == 1
    assert sim.transport.connection_counters(-1) == (
        {'messages': 0, 'bytes': 0},
        {'messages': 0, 'bytes': 0},
    )


def test_down_compaction_waits_for_both_consumed_events_and_honors_retention():
    sim, ga, gb, cid = open_pair()
    sim.run_until(0.125)
    sim.transport.set_down_retention(0)
    sim.transport.session_op('R1', 'ref', ga, c.SessionOp('abort', connection=cid))
    events = [
        (d, e)
        for d, _, e in sim.agents.entries
        if isinstance(e, c.SessionEvent) and e.state == c.DOWN
    ]
    sim.settle()
    assert cid in sim.state.transport.connections
    # Merely delivering DOWN is not enough, and an equal forged value is
    # not acknowledgement of the captured event object.
    sim.transport.consumed('R1', 'ref', ga, (replace(events[0][1]),))
    sim.settle()
    assert cid in sim.state.transport.connections
    sim.transport.consumed('R1', 'ref', ga, (events[0][1],))
    sim.settle()
    assert cid in sim.state.transport.connections
    sim.transport.consumed('R2', 'ref', gb, (events[1][1],))
    sim.settle()
    assert cid not in sim.state.transport.connections
    assert sim.transport.connections_of('R1', 'ref', ga) == ()
    assert sim.transport.connection_counters(cid)[0]['messages'] == 0
    assert not sim.transport._down_waiting
    # A duplicate consumption is inert, including after compaction.
    sim.transport.consumed('R2', 'ref', gb, (events[1][1],))
    sim.run_until(2)


def test_closed_history_is_bounded_after_churn_and_survives_owner_removal():
    sim, ga, gb, _ = open_pair()
    sim.run_until(0.125)
    sim.transport.set_down_retention(2)
    from tests.runtime.test_sessions import endpoint

    a, b = endpoint(sim, 'R1', 'e1'), endpoint(sim, 'R2', 'e2')
    for cid in range(1, 8):
        if cid != 1:
            sim.transport.session_op(
                'R1', 'ref', ga, c.SessionOp('open', a, replace(b, scope='e1'))
            )
            sim.run_until(sim.env.now + 0.125)
        sim.transport.session_op('R1', 'ref', ga, c.SessionOp('abort', connection=cid))
        events = [
            (d, e)
            for d, _, e in sim.agents.entries
            if isinstance(e, c.SessionEvent)
            and e.state == c.DOWN
            and e.connection == cid
        ]
        sim.transport.consumed('R1', 'ref', ga, (events[0][1],))
        sim.transport.consumed('R2', 'ref', gb, (events[1][1],))
        sim.settle()
        assert len(sim.state.transport.connections) <= 2
    assert tuple(sim.state.transport.connections) == (6, 7)
    sim.transport.set_down_retention(0)
    sim.settle()
    assert not sim.state.transport.connections
    for invalid in (-1, True, 1.5):
        with pytest.raises(ValueError, match='retention'):
            sim.transport.set_down_retention(invalid)


def test_one_device_delta_runs_only_the_connection_depending_on_it(monkeypatch):
    net = wire_network()
    net.add_device('transit')
    sim = session(net=net, count=1000)
    sim.run_until(0.125)
    conn = sim.state.transport.connections[1]
    sim.transport._put(
        replace(conn, deps=conn.deps + ('transit',)), 'dependency fixture'
    )
    old = sim.state
    net.devices['transit'].configure(router_id=123)
    due = sim.transport.affected(StateDelta(old, sim.state), sim.state)
    assert due == {1}
    visited = []

    def derive(state, conn):
        visited.append(conn.id)
        return conn

    monkeypatch.setattr(sim.transport, '_derive', derive)
    sim.transport.kind().run(sim.state, sim.env.now, list(due))
    assert visited == [1]


def test_removed_owner_retires_pending_down_acknowledgement():
    sim, ga, gb, cid = open_pair()
    sim.run_until(0.125)
    sim.transport.set_down_retention(0)
    sim.transport.session_op('R1', 'ref', ga, c.SessionOp('abort', connection=cid))
    event = next(
        e
        for d, _, e in sim.agents.entries
        if d == 'R1' and isinstance(e, c.SessionEvent) and e.state == c.DOWN
    )
    sim.transport.consumed('R1', 'ref', ga, (event,))
    sim.network.remove_agent('R2', 'ref')
    sim.settle()
    assert cid not in sim.state.transport.connections
    assert sim.transport.connections_of('R2', 'ref', gb) == ()


def test_real_down_consumption_compacts_only_after_published_inbox():
    config = c.AgentConfig(run_delay=1, processing_delay=0)
    sim = session(config=config)
    sim.run_until(1.25)
    sim.transport.set_down_retention(0)
    ga = sim.agents.generation('R1', 'wire')
    sim.transport.session_op('R1', 'wire', ga, c.SessionOp('abort', connection=1))
    sim.settle()
    assert 1 in sim.state.transport.connections
    sim.run_until(2.25)
    assert all(
        e.state == c.DOWN
        for device in ('R1', 'R2')
        for e in observed(sim, device, c.SessionEvent)[1:]
    )
    assert 1 not in sim.state.transport.connections


def measure_registry(count, baseline, calls=2000, repeats=7):
    """Same-machine transport method comparison, excluding fixture setup.

    ``baseline`` is a TransportRuntime class loaded from the reviewed commit.
    DOWN records model retained history; one edit changes only connection 1.
    An unrelated agent-state delta is measured separately, with warmed indexes.
    """
    from statistics import median
    from time import perf_counter

    sim = bind(wire_network())
    old = sim.state
    ep = c.Endpoint(4, 1, 179)
    table = PMap(
        (i, c.ConnectionState(i, 'elsewhere', 'x', ep, 1, state=c.DOWN))
        for i in range(1, count + 1)
    )
    old = replace(old, transport=c.TransportState(connections=table))
    new = replace(
        old,
        transport=replace(
            old.transport, connections=table.set(1, replace(table[1], reason=c.CLOSED))
        ),
    )
    edit = StateDelta(old, new)
    dev = old.devices['R1']
    unrelated = replace(
        old,
        devices=old.devices.set(
            'R1',
            replace(
                dev,
                agents=dev.agents.set('wire', replace(dev.agents['wire'], state=42)),
            ),
        ),
    )
    idle = StateDelta(old, unrelated)
    results = []
    for label, cls in [('old', baseline), ('new', type(sim.transport))]:
        operations = (
            (
                'agent_state_affected',
                lambda cls=cls: cls.affected(sim.transport, idle, unrelated),
            ),
            (
                'agent_state_commit',
                lambda cls=cls: cls._committed(sim.transport, 0, 'probe', idle),
            ),
            (
                'one_connection_affected',
                lambda cls=cls: cls.affected(sim.transport, edit, new),
            ),
            (
                'one_connection_commit',
                lambda cls=cls: cls._committed(sim.transport, 0, 'probe', edit),
            ),
        )
        for name, operation in operations:
            operation()  # warm identity/shard/delta caches
            samples = []
            for _ in range(repeats):
                start = perf_counter()
                for _ in range(calls):
                    operation()
                samples.append((perf_counter() - start) / calls * 1e6)
            results.append(
                {
                    'version': label,
                    'operation': name,
                    'median_us': median(samples),
                    'samples_us': samples,
                }
            )
    return {
        'connections': count,
        'calls': calls,
        'repeats': repeats,
        'results': results,
    }


def test_data_drop_is_counted_even_when_sender_control_allowance_is_exhausted():
    sim, ga, _ = pair()
    send(sim, ga, 'full')
    sim.agents.full = True
    with pytest.raises(RuntimeError, match='control'):
        sim.run_until(0.125)
    assert sim.transport.budget()['inflight_datagrams'] == 0
    assert sim.transport.budget()['datagrams_dropped'] == 1
    assert sim.transport.budget()['inbox_rejections'] == 2


def test_listener_cleanup_uses_the_supplied_snapshot():
    from netsim.runtime.transport import _key
    from tests.runtime.test_transport_integration import local

    sim = session()
    original = sim.state
    ep = local(sim.network, 'R1', 'e1')
    node = original.devices['R1'].interfaces['e1']
    obsolete = c.Listener('R1', 'wire', ep, -1, 'e1', node.generation)
    snapshot = replace(
        original,
        transport=replace(
            original.transport,
            listeners=original.transport.listeners.set(_key('R1', ep), obsolete),
        ),
    )
    derived = sim.transport.kind().run(snapshot, 0, [('listeners', 'R1')])
    assert _key('R1', ep) not in derived.transport.listeners
    assert sim.state is original
