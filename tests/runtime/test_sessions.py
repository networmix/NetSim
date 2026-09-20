"""Reliable-message sessions: cold start, forwarding, lifecycle and bounds."""

from dataclasses import replace

import pytest

from netsim.model import contracts as c
from netsim.model.addressing import MacAddress
from netsim.model.state import validate_immutable
from tests.model.test_network import A, build_diamond
from tests.runtime.test_transport import bind, deliveries, pair, register


def endpoint(sim, device, interface, port=179):
    node = sim.state.devices[device].interfaces[interface]
    return c.Endpoint(6, MacAddress(node.mac).link_local_int(), port, interface)


def open_pair(*, timeout=4, listen=True, **kw):
    sim, ga, gb = pair(**kw)
    local = endpoint(sim, 'R1', 'e1')
    remote = endpoint(sim, 'R2', 'e2')
    if listen:
        sim.transport.session_op('R2', 'ref', gb, c.SessionOp('listen', local=remote))
    cid = sim.transport.session_op(
        'R1',
        'ref',
        ga,
        c.SessionOp('open', local, replace(remote, scope='e1'), timeout=timeout),
    )
    return sim, ga, gb, cid


def events(sim, state=None):
    return [
        (d, e)
        for d, _, e in sim.agents.entries
        if isinstance(e, c.SessionEvent) and (state is None or e.state == state)
    ]


def message(sim, gen, cid, payload, *, device='R1', size=0):
    return sim.transport.send_message(device, 'ref', gen, c.Message(cid, payload, size))


def test_cold_start_passive_listener_and_normal_timed_handshake():
    sim, ga, gb, cid = open_pair()
    assert cid == 1
    assert sim.state.transport.connections[cid].state == c.CONNECTING
    assert not events(sim)
    sim.run_until(0.125)
    conn = sim.state.transport.connections[cid]
    assert conn.state == c.ESTABLISHED
    assert conn.a_to_b_reachable and conn.b_to_a_reachable
    assert conn.deps == ('R1', 'R2')
    assert [d for d, _ in events(sim)] == ['R1', 'R2']
    assert events(sim)[0][1].remote.scope == 'e1'
    assert events(sim)[1][1].remote.scope == 'e2'
    assert message(sim, ga, cid, 'one') is None
    assert message(sim, gb, cid, 'two', device='R2') is None
    sim.run_until(0.25)
    assert [(d, e.payload, e.seq) for d, _, e in deliveries(sim)] == [
        ('R2', 'one', 0),
        ('R1', 'two', 0),
    ]
    validate_immutable(sim.state)


def test_refused_after_request_reaches_destination():
    sim, _, _, cid = open_pair(listen=False)
    assert not events(sim)
    sim.run_until(0.125)
    assert [(d, e.reason) for d, e in events(sim, c.DOWN)] == [('R1', c.REFUSED)]
    assert sim.state.transport.connections[cid].state == c.DOWN


def test_two_simultaneous_opens_remain_distinct():
    sim, ga, gb, first = open_pair()
    local = endpoint(sim, 'R1', 'e1')
    remote = endpoint(sim, 'R2', 'e2')
    sim.transport.session_op('R1', 'ref', ga, c.SessionOp('listen', local=local))
    second = sim.transport.session_op(
        'R2', 'ref', gb, c.SessionOp('open', remote, replace(local, scope='e2'))
    )
    assert (first, second) == (1, 2)
    sim.run_until(0.125)
    assert len(events(sim, c.ESTABLISHED)) == 4
    assert all(
        conn.state == c.ESTABLISHED for conn in sim.state.transport.connections.values()
    )


def test_listener_address_validation_and_unlisten():
    sim, ga, _ = pair()
    rejection = sim.transport.session_op(
        'R1', 'ref', ga, c.SessionOp('listen', c.Endpoint(4, A('192.0.2.9'), 179))
    )
    assert rejection.reason == c.UNREACHABLE_ENDPOINT
    local = endpoint(sim, 'R1', 'e1')
    op = c.SessionOp('listen', local)
    sim.transport.session_op('R1', 'ref', ga, op)
    root = sim.state
    sim.transport.session_op('R1', 'ref', ga, op)
    assert sim.state is root
    sim.transport.session_op('R1', 'ref', ga, c.SessionOp('unlisten', local))
    assert not sim.state.transport.listeners


@pytest.mark.parametrize(
    'operation,delivered,reason',
    [('close', True, c.CLOSED), ('abort', False, c.ABORTED)],
)
def test_drain_close_versus_abort_inflight(operation, delivered, reason):
    sim, ga, _, cid = open_pair()
    sim.run_until(0.125)
    message(sim, ga, cid, 'final')
    sim.transport.session_op('R1', 'ref', ga, c.SessionOp(operation, connection=cid))
    sim.run_until(1)
    assert bool(deliveries(sim)) is delivered
    assert [e.reason for _, e in events(sim, c.DOWN)] == [reason, reason]
    if delivered:
        entries = [e for _, _, e in sim.agents.entries]
        assert entries.index(deliveries(sim)[0][2]) < entries.index(
            events(sim, c.DOWN)[0][1]
        )
    assert sim.transport.budget()['queued_messages'] == 0


def test_backpressure_is_explicit_and_accepted_data_is_not_lost():
    sim, ga, _, cid = open_pair(queue_limit=2, byte_limit=5)
    sim.run_until(0.125)
    assert message(sim, ga, cid, 'a', size=3) is None
    assert message(sim, ga, cid, 'bytes', size=3).reason == c.OVERFLOW
    assert message(sim, ga, cid, 'b', size=2) is None
    assert message(sim, ga, cid, 'count').reason == c.OVERFLOW
    assert sim.transport.budget()['queued_bytes'] == 5
    sim.run_until(0.25)
    assert [(e.payload, e.seq) for _, _, e in deliveries(sim)] == [('a', 0), ('b', 1)]
    assert sim.transport.budget()['queued_bytes'] == 0


def test_reset_invalidates_incarnation_and_notifies_peer():
    sim, ga, _, cid = open_pair()
    sim.run_until(0.125)
    message(sim, ga, cid, 'old')
    sim.transport.cancel_agent('R1', 'ref', ga)
    assert [(d, e.reason) for d, e in events(sim, c.DOWN)] == [('R2', c.RESET)]
    sim.run_until(6)
    assert not deliveries(sim)
    assert sim.transport.budget()['stale_events'] == 0
    assert message(sim, ga, cid, 'late').reason == 'NOT_ESTABLISHED'


def test_removal_cleans_listener_and_sessions_without_c1_callback():
    sim, ga, _, cid = open_pair()
    sim.run_until(0.125)
    message(sim, ga, cid, 'old')
    sim.network.remove_agent('R2', 'ref')
    sim.settle()
    assert not sim.state.transport.listeners
    assert [(d, e.reason) for d, e in events(sim, c.DOWN)] == [('R1', c.RESET)]
    sim.run_until(1)
    assert not deliveries(sim)


def routed():
    net, routers = build_diamond(min_links=1)
    for link in net.links.values():
        link.configure(delay=0.125)
    # Deliberately asymmetric: R1->R2, R2->R4->R3->R1.
    routers['R2'].add_route('10.0.0.1/32', [('eth3', '10.1.24.1')])
    routers['R4'].add_route('10.0.0.1/32', [('eth2', '10.1.34.0')])
    routers['R3'].add_route('10.0.0.1/32', [('eth1', '10.1.13.0')])
    ga = register(net, 'R1')
    gb = register(net, 'R2')
    sim = bind(net)
    a = c.Endpoint(4, A('10.0.0.1'), 179)
    b = c.Endpoint(4, A('10.0.0.2'), 179)
    sim.transport.session_op('R2', 'ref', gb, c.SessionOp('listen', b))
    cid = sim.transport.session_op(
        'R1', 'ref', ga, c.SessionOp('open', a, b, timeout=1)
    )
    sim.run_until(0.125)
    assert sim.state.transport.connections[cid].state == c.ESTABLISHED
    return sim, ga, gb, cid, routers


def test_one_way_outage_stalls_in_order_and_recovers_with_fresh_times():
    sim, ga, gb, cid, routers = routed()
    assert sim.state.transport.connections[cid].deps == ('R1', 'R2', 'R3', 'R4')
    routers['R1']['Po1'].admin_down()
    sim.settle()
    conn = sim.state.transport.connections[cid]
    assert not conn.a_to_b_reachable and conn.b_to_a_reachable
    assert conn.state == c.ESTABLISHED
    message(sim, ga, cid, 'U1')
    message(sim, ga, cid, 'U2')
    message(sim, gb, cid, 'reverse', device='R2')
    sim.run_until(0.625)
    assert [e.payload for _, _, e in deliveries(sim)] == ['reverse']
    routers['R1']['Po1'].admin_up()
    sim.settle()
    assert sim.state.transport.connections[cid].a_to_b_reachable
    sim.run_until(0.75)
    assert [(e.payload, e.seq, e.time) for _, _, e in deliveries(sim)] == [
        ('reverse', 0, 0.5),
        ('U1', 0, 0.75),
        ('U2', 1, 0.75),
    ]


def test_timeout_closes_both_sides_and_old_events_do_nothing():
    sim, ga, _, cid = open_pair(timeout=0.5)
    sim.run_until(0.125)
    message(sim, ga, cid, 'inflight')
    next(iter(sim.network.links.values())).fail()
    sim.settle()
    sim.run_until(0.625)
    assert [e.reason for _, e in events(sim, c.DOWN)] == [c.TIMEOUT, c.TIMEOUT]
    next(iter(sim.network.links.values())).restore()
    sim.run_until(5)
    assert not deliveries(sim)
    assert sim.transport.budget()['queued_messages'] == 0


def test_inbox_full_retries_admitted_messages_without_reordering():
    sim, ga, _, cid = open_pair()
    sim.run_until(0.125)
    message(sim, ga, cid, 'first')
    message(sim, ga, cid, 'second')
    sim.agents.full = True
    sim.run_until(0.25)
    assert sim.transport.budget()['queued_messages'] == 2
    sim.agents.full = False
    sim.run_until(0.375)
    assert [(e.payload, e.seq) for _, _, e in deliveries(sim)] == [
        ('first', 0),
        ('second', 1),
    ]


def test_no_agent_network_has_no_transport_work_and_derivation_is_canonical():
    net, _ = build_diamond()
    sim = bind(net)
    sim.dirty_all()
    sim.settle()
    assert sim.state.transport is None
    assert 'transport' not in sim.timeline.stage_names(0)
    sim, _, _, cid = open_pair()
    sim.run_until(0.125)
    old = sim.state
    assert sim.transport.kind().run(old, sim.env.now, [cid]) is old


def test_seed_determinism():
    def run():
        sim, ga, gb, cid = open_pair()
        sim.run_until(0.125)
        for i in range(10):
            message(sim, ga, cid, ('a', i))
            message(sim, gb, cid, ('b', i), device='R2')
        sim.run_until(1)
        return sim.agents.entries

    assert run() == run()


def test_endpoint_address_moved_to_another_interface_invalidates_session():
    sim, ga, _, cid, routers = routed()
    with sim.network.batch():
        routers['R1']['lo0'].configure(ipv4=[])
        routers['R1'].add_loopback('replacement', ipv4=['10.0.0.1/32'])
    sim.settle()
    assert sim.state.transport.connections[cid].reason == c.RESET
    assert message(sim, ga, cid, 'old').reason == 'NOT_ESTABLISHED'


def test_canceled_timeout_shells_are_periodically_compacted():
    sim, ga, _, cid = open_pair(timeout=1000)
    sim.run_until(0.125)
    # The no-progress timer cancels on each delivery; idle connections must
    # not retain one heap entry per historical successful message forever.
    for i in range(150):
        message(sim, ga, cid, i)
        sim.run_until(sim.env.now + 0.125)
    budget = sim.transport.budget()
    assert budget['stale_events'] < 64
    assert len(sim.env._queue) < 65


@pytest.mark.parametrize('af', [4, 6])
@pytest.mark.parametrize('compressed', [False, True])
def test_gate_b_forward_step_paths_and_control_traffic_not_charged(af, compressed):
    from tests.model.test_forwarding_srv6 import path_network

    net, routers, _ = path_network(compressed=compressed)
    for link in net.links.values():
        link.configure(delay=0.125)
    src = '10.0.0.1' if af == 4 else '2001:db8::1'
    dst = '10.0.0.4' if af == 4 else '2001:db8::4'
    prefix = src + ('/32' if af == 4 else '/128')
    routers['R4'].add_route(prefix, [('toR2', '10.1.24.0')])
    routers['R2'].add_route(prefix, [('toR1', '10.1.12.0')])
    ga, gb = register(net, 'R1'), register(net, 'R4')
    sim = bind(net)
    before = sim.state.placement
    a, b = c.Endpoint(af, A(src), 179), c.Endpoint(af, A(dst), 179)
    sim.transport.session_op('R4', 'ref', gb, c.SessionOp('listen', b))
    cid = sim.transport.session_op('R1', 'ref', ga, c.SessionOp('open', a, b))
    sim.run_until(0.25)
    conn = sim.state.transport.connections[cid]
    assert conn.state == c.ESTABLISHED
    assert conn.a_to_b_delay == conn.b_to_a_delay == 0.25
    assert conn.a_to_b_reachable and conn.b_to_a_reachable
    assert conn.deps == ('R1', 'R2', 'R4')
    message(sim, ga, cid, 'over SR')
    sim.run_until(0.5)
    assert deliveries(sim)[0][2].payload == 'over SR'
    assert sim.state.placement is before


def test_failed_route_query_is_a_dependency_and_insertion_releases_open():
    net, routers = build_diamond()
    routers['R1'].rib_client().sync([])
    for link in net.links.values():
        link.configure(delay=0.125)
    ga, gb = register(net, 'R1'), register(net, 'R2')
    sim = bind(net)
    a, b = c.Endpoint(4, A('10.0.0.1'), 179), c.Endpoint(4, A('10.0.0.2'), 179)
    sim.transport.session_op('R2', 'ref', gb, c.SessionOp('listen', b))
    cid = sim.transport.session_op(
        'R1', 'ref', ga, c.SessionOp('open', a, b, timeout=1)
    )
    sim.settle()
    assert sim.state.transport.connections[cid].deps == ('R1',)
    assert not events(sim)
    sim.run_until(0.25)
    routers['R1'].add_route('10.0.0.2/32', [('Po1', '10.1.12.1')])
    sim.settle()
    sim.run_until(0.375)
    assert sim.state.transport.connections[cid].state == c.ESTABLISHED


def test_timeout_resets_on_progress_not_admission():
    sim, ga, _, cid = open_pair(timeout=0.5)
    sim.run_until(0.125)
    message(sim, ga, cid, 'delivered')
    sim.run_until(0.25)
    message(sim, ga, cid, 'stalled')
    next(iter(sim.network.links.values())).fail()
    sim.settle()
    sim.run_until(0.625)
    message(sim, ga, cid, 'later')
    assert not events(sim, c.DOWN)
    sim.run_until(0.75)
    assert [e.reason for _, e in events(sim, c.DOWN)] == [c.TIMEOUT, c.TIMEOUT]


@pytest.mark.parametrize('operation', ['close', 'abort'])
def test_queued_outage_close_and_abort(operation):
    sim, ga, _, cid = open_pair()
    sim.run_until(0.125)
    link = next(iter(sim.network.links.values()))
    link.fail()
    sim.settle()
    message(sim, ga, cid, 'last')
    sim.transport.session_op('R1', 'ref', ga, c.SessionOp(operation, connection=cid))
    assert message(sim, ga, cid, 'after close').reason == 'NOT_ESTABLISHED'
    link.restore()
    sim.settle()
    sim.run_until(1)
    assert [e.payload for _, _, e in deliveries(sim)] == (
        ['last'] if operation == 'close' else []
    )


def test_listener_reset_during_handshake_does_not_accept_old_request():
    sim, _, gb, _ = open_pair()
    sim.transport.cancel_agent('R2', 'ref', gb)
    sim.network.remove_agent('R2', 'ref')
    gb = register(sim.network, 'R2')
    b = endpoint(sim, 'R2', 'e2')
    sim.transport.session_op('R2', 'ref', gb, c.SessionOp('listen', b))
    sim.run_until(1)
    assert not events(sim, c.ESTABLISHED)
    assert events(sim, c.DOWN)[0][1].reason == c.RESET


def test_transport_publication_survives_observer_failure_exactly_once():
    from netsim.model.network import published_failure

    sim, ga, _, cid = open_pair()

    def fail(t, origin, delta):
        if origin == ('transport', 'established'):
            raise RuntimeError('observer')

    sim.network.on_delta.insert(0, fail)
    with pytest.raises(RuntimeError, match='observer') as error:
        sim.run_until(0.125)
    assert published_failure(error.value)
    sim.network.on_delta.remove(fail)
    sim.retry()
    sim.settle()
    assert len(events(sim, c.ESTABLISHED)) == 2
    assert message(sim, ga, cid, 'one') is None
    sim.run_until(0.25)
    assert len(deliveries(sim)) == 1


def test_message_fifo_when_delay_shortens():
    sim, ga, _, cid = open_pair()
    sim.run_until(0.125)
    next(iter(sim.network.links.values())).configure(delay=1)
    message(sim, ga, cid, 'first')
    sim.run_until(0.25)
    next(iter(sim.network.links.values())).configure(delay=0.125)
    message(sim, ga, cid, 'second')
    sim.run_until(1.125)
    assert [(e.time, e.payload, e.seq) for _, _, e in deliveries(sim)] == [
        (1.125, 'first', 0),
        (1.125, 'second', 1),
    ]


def test_message_admission_leaves_path_publication_to_transport_kind():
    sim, ga, _, cid = open_pair()
    sim.run_until(0.125)
    link = next(iter(sim.network.links.values()))
    link.fail()
    sim.settle()
    message(sim, ga, cid, 'stalled')
    link.restore()
    before = sim.state.transport
    message(sim, ga, cid, 'during recovery')
    assert sim.state.transport is before
    assert not sim.state.transport.connections[cid].a_to_b_reachable
    sim.settle()
    assert sim.state.transport.connections[cid].a_to_b_reachable
    sim.run_until(0.25)
    assert [e.payload for _, _, e in deliveries(sim)] == ['stalled', 'during recovery']
