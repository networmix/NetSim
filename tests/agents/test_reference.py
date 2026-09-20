"""Oracle-free reference protocol acceptance and cross-slice regressions."""

import pytest

from netsim.model import contracts as c
from netsim.model.addressing import MacAddress
from netsim.model.network import Network
from tests.runtime.test_transport import bind, deliveries, register


@pytest.mark.parametrize('numbered_v6', [False, True])
def test_link_local_control_datagram_on_numbered_interfaces(numbered_v6):
    net = Network()
    a, b = net.add_device('a'), net.add_device('b')
    net.add_p2p(
        a,
        'a0',
        b,
        'b0',
        delay=0.125,
        ipv4=('192.0.2.0/31', '192.0.2.1/31'),
        ipv6=('2001:db8::/127', '2001:db8::1/127') if numbered_v6 else None,
    )
    ga = register(net, 'a')
    register(net, 'b')
    sim = bind(net)
    datagram = c.Datagram('a0', 'hello', port=179, link_local=True)
    assert sim.transport.send_datagram('a', 'ref', ga, datagram) is None
    sim.run_until(0.125)
    entry = deliveries(sim)[0][2]
    assert entry.sender == c.Sender(
        'b0', c.Endpoint(6, MacAddress(a['a0'].mac).link_local_int(), 179, 'b0')
    )
    # The opt-in still honors both effective eligibility and the physical wire.
    next(iter(net.links.values())).fail()
    sim.settle()
    assert sim.transport.send_datagram('a', 'ref', ga, datagram) is not None


@pytest.mark.parametrize('numbered', [False, True])
@pytest.mark.parametrize('families', [(4,), (6,), (4, 6)])
def test_cold_start_oracle_twin(numbered, families):
    from netsim.agents.reference import Hello, LsUpdate
    from netsim.model.state import validate_immutable
    from tests.agents.fixtures import assert_equivalent, state, twins

    sim, oracle = twins(numbered=numbered, families=families)
    sent = []
    datagram, message = sim.transport.send_datagram, sim.transport.send_message
    sim.transport.send_datagram = lambda *a: (sent.append(a[-1]), datagram(*a))[1]
    sim.transport.send_message = lambda *a: (sent.append(a[-1]), message(*a))[1]
    sim.run_until(2)
    assert_equivalent(sim, oracle)
    for name in sim.state.devices:
        s = state(sim, name)
        assert len(s.lsdb) == 12 and all(a.up for a in s.adjacencies.values())
        validate_immutable(s)
    assert any(isinstance(item.payload, LsUpdate) for item in sent)
    assert all(
        isinstance(item.payload, Hello) for item in sent if isinstance(item, c.Datagram)
    )
    established = [
        conn
        for conn in sim.state.transport.connections.values()
        if conn.state == c.ESTABLISHED
    ]
    assert len(established) == 4
    assert all(int(conn.a_device[1:]) < int(conn.b_device[1:]) for conn in established)


def test_rfc8950_ipv4_only_link_resolves_scoped_eui64():
    from netsim.model import forwarding as fw
    from netsim.model.routing import Nexthop

    net = Network()
    a, b = net.add_device('a'), net.add_device('b')
    net.add_p2p(a, 'a0', b, 'b0', ipv4=('192.0.2.0/31', '192.0.2.1/31'))
    ll = MacAddress(b['b0'].mac).link_local_int()
    a.add_route('198.51.100.0/24', [Nexthop.via('a0', ll, 6)])
    a.add_route('203.0.113.0/24', [Nexthop.via('a0', ll + 1, 6)])
    net.converge()
    entry = a.fib(4).entries.get(0xC6336400, 24)
    assert entry is not None and entry.action == fw.FORWARD
    assert a.fib(4).group(entry).adjacencies[0].mac == b['b0'].mac
    assert a.fib(4).entries.get(0xCB007100, 24) is None


def test_heartbeat_and_sequence_refresh_do_not_run_spf_fib_or_placement():
    from tests.agents.fixtures import assert_equivalent, counter, state, twins

    sim, oracle = twins()
    sim.run_until(2)
    before = {name: state(sim, name) for name in sim.state.devices}
    views = {
        name: node.agents['ref'].srdb_view for name, node in sim.state.devices.items()
    }
    spf = counter(sim, 'spf')
    sim.run_until(8)
    assert counter(sim, 'spf') == spf
    assert all(state(sim, name) is old for name, old in before.items())
    # The refresh at t=10 increments sequences, but its bodies are unchanged.
    sim.run_until(12)
    assert counter(sim, 'spf') == spf
    assert all(
        node.agents['ref'].srdb_view is views[name]
        for name, node in sim.state.devices.items()
    )
    assert all(
        record.origin.name not in ('fib', 'placement')
        for record in sim.timeline.records
        if record.time > 2
    )
    assert_equivalent(sim, oracle)


def test_local_down_at_ten_is_immediate_and_floods_oracle_result():
    from netsim.agents import ReferenceConfig
    from tests.agents.fixtures import assert_equivalent, state, twins

    sim, oracle = twins(config=ReferenceConfig(run_delay=0))
    sim.run_until(2)
    key = ('e1', 2)
    assert state(sim).adjacencies[key].up
    sim.at(10, sim.network.links['r0:e1--r1:e0'].fail)
    oracle.links['r0:e1--r1:e0'].fail()
    oracle.converge()
    sim.run_until(10)
    assert key not in state(sim).adjacencies
    assert all(edge.peer != 2 for edge in state(sim).lsdb[1, 'ADJACENCY'].body)
    sim.run_until(11)
    assert_equivalent(sim, oracle)


def test_muted_hello_channel_uses_hold_not_wire_or_session_rejection():
    from netsim.agents import ReferenceConfig
    from tests.agents.fixtures import state, twins

    sim, _ = twins(config=ReferenceConfig(run_delay=0))
    for name, iface in (('r0', 'e1'), ('r1', 'e0')):
        sim.network.device(name)[iface].configure(carrier_delay_down=100)
    sim.run_until(9.5)
    key = ('e1', 2)
    sim.at(10, sim.network.links['r0:e1--r1:e0'].fail)
    sim.run_until(11.5)
    assert sim.state.devices['r0'].interfaces['e1'].oper.oper == 1
    assert state(sim).adjacencies[key].up
    sim.run_until(12.1)
    assert sim.state.devices['r0'].interfaces['e1'].oper.oper == 1
    assert key not in state(sim).adjacencies
    assert ('e0', 1) not in state(sim, 'r1').adjacencies


def test_partition_expiry_and_heal_replace_finite_lsdb():
    from netsim.agents import ReferenceConfig
    from tests.agents.fixtures import assert_equivalent, state, twins

    config = ReferenceConfig(max_age=6, refresh_interval=2)
    sim, oracle = twins(config=config)
    sim.run_until(3)
    links = [key for key in sim.network.links if 'r3:' in key]
    for key in links:
        sim.network.links[key].fail()
        oracle.links[key].fail()
    oracle.converge()
    sim.run_until(10)
    assert all(origin != 4 for origin, _ in state(sim).lsdb)
    assert {origin for origin, _ in state(sim, 'r3').lsdb} == {4}
    assert_equivalent(sim, oracle)
    for key in links:
        sim.network.links[key].restore()
        oracle.links[key].restore()
    oracle.converge()
    sim.run_until(13)
    assert all(len(state(sim, name).lsdb) == 12 for name in sim.state.devices)
    assert_equivalent(sim, oracle)


def test_restart_replaces_stale_lsa_and_resynchronizes():
    from tests.agents.fixtures import assert_equivalent, state, twins

    sim, oracle = twins()
    sim.run_until(3)
    old = {
        kind: lsa.seq for (origin, kind), lsa in state(sim).lsdb.items() if origin == 2
    }
    generation = sim.state.devices['r1'].agents['ref'].generation
    sim.reset_agent('r1', 'ref')
    sim.run_until(5)
    assert sim.state.devices['r1'].agents['ref'].generation != generation
    for name in sim.state.devices:
        assert all(
            state(sim, name).lsdb[2, kind].seq > seq for kind, seq in old.items()
        )
    assert_equivalent(sim, oracle)


def inject(sim, name, lsas):
    from netsim.agents.reference import LsUpdate
    from tests.agents.fixtures import state

    connection = next(iter(state(sim, name).sessions.values()))
    assert sim.agents.deliver(
        name,
        'ref',
        c.Delivery(sim.env.now, LsUpdate(tuple(lsas)), connection=connection),
    )


def test_duplicate_out_of_order_and_expired_advertisements_are_identity_noops():
    from dataclasses import replace

    from tests.agents.fixtures import counter, state, twins

    sim, _ = twins()
    sim.run_until(2)
    old = state(sim)
    lsa = old.lsdb[4, 'PREFIX']
    inject(
        sim,
        'r0',
        [
            lsa,
            replace(lsa, seq=lsa.seq - 1, body=()),
            replace(lsa, seq=lsa.seq + 100, originated_at=-100),
            replace(lsa, seq=lsa.seq + 100, originated_at=100),
        ],
    )
    spf = counter(sim, 'spf')
    sim.run_until(2.1)
    assert state(sim) is old
    assert counter(sim, 'spf') == spf


def test_self_sequence_fightback_handles_same_time_restart_collision():
    from dataclasses import replace

    from tests.agents.fixtures import assert_equivalent, state, twins

    sim, oracle = twins()
    sim.run_until(2)
    old = state(sim).lsdb[1, 'PREFIX']
    injected = replace(old, seq=10**12, body=(), originated_at=2)
    inject(sim, 'r0', [injected])
    sim.run_until(2.5)
    assert state(sim).lsdb[1, 'PREFIX'].seq == injected.seq + 1
    sim.reset_agent('r0', 'ref')
    sim.run_until(4)
    assert all(
        state(sim, name).lsdb[1, 'PREFIX'].seq > injected.seq
        for name in sim.state.devices
    )
    assert_equivalent(sim, oracle)


def test_metric_change_has_no_adjacency_or_session_churn():
    from tests.agents.fixtures import assert_equivalent, counter, state, twins

    sim, oracle = twins()
    sim.run_until(2)
    before = state(sim)
    spf = counter(sim, 'spf')
    sim.network.device('r0')['e1'].configure(metric=7)
    oracle.device('r0')['e1'].configure(metric=7)
    oracle.converge()
    sim.run_until(2.5)
    after = state(sim)
    assert after.adjacencies is before.adjacencies
    assert after.sessions == before.sessions
    assert counter(sim, 'spf') > spf
    assert_equivalent(sim, oracle)


@pytest.mark.parametrize('seed,size', [(11, 6), (29, 8), (73, 10)])
def test_random_connected_graphs_match_oracle(seed, size):
    import random

    from tests.agents.fixtures import assert_equivalent, twins

    rng = random.Random(seed)
    edges = {(i, i + 1) for i in range(size - 1)}
    edges |= {
        (a, b) for a in range(size) for b in range(a + 1, size) if rng.random() < 0.25
    }
    sim, oracle = twins(edges=tuple(sorted(edges)), size=size, numbered=seed != 29)
    for net in (sim.network, oracle):
        for i, key in enumerate(sorted(net.links)):
            link = net.links[key]
            # Metrics, including ties, from stable link order.
            for device, iface in (link.node.a, link.node.b):
                net.device(device)[iface].configure(metric=1 + i % 3)
    oracle.converge()
    sim.run_until(3)
    assert_equivalent(sim, oracle)
    key = sorted(sim.network.links)[-1]
    sim.network.links[key].fail()
    oracle.links[key].fail()
    oracle.converge()
    sim.run_until(4)
    assert_equivalent(sim, oracle)


def test_explicit_empty_body_purge_withdraws_on_every_router():
    from tests.agents.fixtures import assert_equivalent, state, twins

    sim, oracle = twins(numbered=False, demands=False)
    sim.run_until(2)
    previous = state(sim, 'r3').lsdb[4, 'PREFIX']
    sim.network.device('r3').remove_interface('lo')
    oracle.device('r3').remove_interface('lo')
    oracle.converge()
    sim.run_until(2.5)
    for name in sim.state.devices:
        purge = state(sim, name).lsdb[4, 'PREFIX']
        assert purge.body == () and purge.seq == previous.seq + 1
        assert not any(row.distinguisher == (4,) for row in state(sim, name).rows)
    assert_equivalent(sim, oracle)


def test_route_publication_initial_sync_small_diff_and_large_sync():
    from netsim.agents import ReferenceConfig
    from tests.agents.fixtures import twins

    sim, _ = twins(numbered=False, config=ReferenceConfig(delta_limit=4))
    outputs = []
    apply = sim.agents._apply

    def capture(state, device, client, output):
        if device == 'r0':
            outputs.append(output)
        return apply(state, device, client, output)

    sim.agents._apply = capture
    sim.run_until(2)
    assert len(outputs[0].route_ops) == 2
    assert all(op.sync == () for op in outputs[0].route_ops)
    outputs.clear()
    sim.network.device('r3').add_loopback('extra', ipv4=['198.51.100.1/32'])
    sim.run_until(2.5)
    ops = [op for out in outputs for op in out.route_ops]
    assert len(ops) == 1 and len(ops[0].add) == 1 and ops[0].sync is None
    outputs.clear()
    sim.network.device('r3').remove_interface('extra')
    sim.run_until(2.75)
    ops = [op for out in outputs for op in out.route_ops]
    assert len(ops) == 1 and len(ops[0].delete) == 1 and ops[0].sync is None
    outputs.clear()
    sim.network.device('r3').add_loopback(
        'many', ipv4=[f'198.51.100.{i}/32' for i in range(1, 9)]
    )
    sim.run_until(3.5)
    ops = [op for out in outputs for op in out.route_ops]
    assert len(ops) == 1 and ops[0].sync is not None
    outputs.clear()
    sim.run_until(5)
    assert all(not out.route_ops for out in outputs)


def test_agent_rounds_publish_one_delta_with_all_successful_receipts():
    from netsim.runtime.timeline import AgentRunEvent
    from tests.agents.fixtures import twins

    sim, _ = twins()
    sim.run_until(2)
    records = [
        record for record in sim.timeline.records if record.origin.name == 'agent'
    ]
    assert records and len({(r.time, r.round) for r in records}) == len(records)
    deltas = dict(sim.timeline.deltas)
    events = sim.timeline.select(kind=AgentRunEvent)
    for record in records:
        runs = [event for event in events if event.seq == record.seq]
        assert runs
        for run in runs:
            receipt = (
                deltas[record.seq].new.devices[run.device].agents[run.agent].receipt
            )
            assert receipt.status == 'PUBLISHED'
            assert receipt.run_id == run.run_id
            assert receipt.time == record.time
            assert receipt.ops_count == run.ops_count
    assert len([event for event in events if event.time == 0]) == 4


def test_delayed_fib_exposes_pending_routes_and_transient_placement_drop():
    from tests.agents.fixtures import assert_equivalent, state, twins

    sim, oracle = twins()
    sim.run_until(2)
    sim.network.device('r0').configure(fib_delay=0.5)
    sim.run_until(3)
    sim.at(10, sim.network.links['r0:e1--r1:e0'].fail)
    oracle.links['r0:e1--r1:e0'].fail()
    oracle.converge()
    sim.run_until(10.125)
    dev = sim.network.device('r0')
    assert any(
        dev.route_status(row.af, row.key)[0] == 'PENDING' for row in state(sim).rows
    )
    assert sim.network.placement.delivered_total < oracle.placement.delivered_total
    assert sim.network.placement.dropped_by_reason
    assert any(
        record.origin.name == 'placement' and 10 <= record.time <= 10.125
        for record in sim.timeline.records
    )
    sim.run_until(11)
    assert_equivalent(sim, oracle)


def test_learned_sr_claims_and_literal_policy_follow_sid_advertisements():
    from netsim.model import srv6 as sr
    from netsim.model.addressing import to_int
    from tests.agents.fixtures import state, twins

    sim, oracle = twins(sr=True)
    sim.network.device('r0').configure(srdb_source=('agent', 'ref'))
    # Adjacency SID advertises peer router ID, never peer device/interface name.
    sid = sim.network.device('r1').add_local_sid(
        sr.END_X, interface='e3', structure=sr.UNCOMPRESSED
    )
    oracle.device('r1').add_local_sid(
        sr.END_X, interface='e3', structure=sr.UNCOMPRESSED
    )
    oracle.converge()
    endpoint = to_int('2001:db8::4')[0]
    terminal = next(iter(sim.state.devices['r3'].srv6_sids.sids.values()))
    policy = sr.SrPolicy(
        c.STATIC,
        10,
        endpoint,
        candidate_paths=(sr.CandidatePath(200, (sr.SegmentList((terminal.sid,)),)),),
    )
    sim.network.device('r0').policy_client().add(policy)
    # No advertised claims yet: agent mode cannot consult remote inventory.
    sim.settle()
    assert (
        sim.state.devices['r0'].srv6_policies.states[policy.key].status
        == sr.POLICY_DOWN
    )
    sim.run_until(2)
    node = sim.state.devices['r0']
    assert node.srv6_policies.states[policy.key].status == sr.POLICY_UP
    view = node.agents['ref'].srdb_view
    claim = next(claim for claim in view.sids if claim.sid == sid.sid)
    assert claim.owner == '2' and claim.peer == '4' and claim.adjacency_up
    assert claim.interface is None
    assert ('4', (endpoint, 128)) in view.locators
    assert any(
        prefix == sim.state.devices['r3'].srv6_sids.locators['sr'].prefix
        for owner, prefix in view.locators
        if owner == '4'
    )
    before = state(sim).lsdb[4, 'SID']
    sim.network.device('r3').remove_local_sid(terminal.sid)
    sim.run_until(2.5)
    assert state(sim).lsdb[4, 'SID'].seq > before.seq
    assert (
        sim.state.devices['r0'].srv6_policies.states[policy.key].status
        == sr.POLICY_DOWN
    )
    # A local DOWN is flooded into learned adjacency-up claims.
    sim.network.links['r1:e3--r3:e1'].fail()
    sim.run_until(3)
    claim = next(
        claim
        for claim in sim.state.devices['r0'].agents['ref'].srdb_view.sids
        if claim.sid == sid.sid
    )
    assert not claim.adjacency_up and claim.peer is None


def test_fresh_runtime_resynchronizes_reference_fork():
    from netsim import Environment
    from netsim.runtime import Simulation
    from tests.agents.fixtures import assert_equivalent, twins

    sim, oracle = twins()
    sim.run_until(2)
    fork = sim.network.fork()
    old = fork.state.devices['r0'].agents['ref'].generation
    fresh = Simulation(Environment(), fork)
    fresh.run_until(2)
    assert fork.state.devices['r0'].agents['ref'].generation != old
    assert_equivalent(fresh, oracle)


@pytest.mark.slow
@pytest.mark.timeout(180)
def test_64_router_ring_qualification():
    from tests.agents.fixtures import measure_ring

    result = measure_ring(64)
    assert result['cold_simulated'] <= 2
    assert result['failure_simulated'] <= 2
    assert result['cold_spf'] > 0 and result['cold_messages'] > 0
    assert result['failure_spf'] >= 64
    print(result)


@pytest.mark.parametrize(
    'options',
    [
        {'hello_interval': 0},
        {'hold_time': 1},
        {'max_age': float('inf')},
        {'refresh_interval': 30},
        {'processing_delay': 0},
        {'run_delay': -1},
        {'port': 65536},
        {'delta_limit': -1},
        {'session_timeout': -1},
    ],
)
def test_invalid_configuration(options):
    from netsim.agents import ReferenceConfig

    with pytest.raises(ValueError):
        ReferenceConfig(**options)


def test_invalid_link_control_family_and_lsa_metadata():
    from netsim.agents.reference import Lsa

    with pytest.raises(ValueError, match='af=6'):
        c.Datagram('e0', 'hello', af=4, link_local=True)
    with pytest.raises(ValueError, match='kind'):
        Lsa(1, 1, 'unsupported', (), 0)
    with pytest.raises(ValueError, match='version/time'):
        Lsa(1, -1, 'PREFIX', (), 0)


def test_out_of_order_refresh_does_not_extend_absolute_expiry():
    from dataclasses import replace

    from netsim.agents import ReferenceConfig
    from netsim.agents.reference import Prefix
    from tests.agents.fixtures import state, twins

    sim, _ = twins(config=ReferenceConfig(max_age=6, refresh_interval=2))
    sim.run_until(2)
    sample = state(sim).lsdb[4, 'PREFIX']
    lsa = replace(
        sample, origin=900, seq=2, body=(Prefix(4, (0xC0000200, 24)),), originated_at=2
    )
    inject(sim, 'r0', [lsa])
    sim.run_until(7)
    assert (900, 'PREFIX') in state(sim).lsdb
    # Delivery time is recent, origin time is not; seq/age dedup never refreshes it.
    inject(sim, 'r0', [lsa, replace(lsa, seq=1, originated_at=7)])
    sim.run_until(8.1)
    assert all(
        (900, 'PREFIX') not in state(sim, name).lsdb for name in sim.state.devices
    )
    inject(sim, 'r0', [lsa])
    sim.run_until(8.5)
    assert (900, 'PREFIX') not in state(sim).lsdb


def test_three_way_discovery_requires_our_id_in_peer_hello():
    from dataclasses import replace

    from netsim.agents.reference import Hello
    from tests.agents.fixtures import state, twins

    sim, _ = twins(size=2, edges=((0, 1),), numbered=False)
    send = sim.transport.send_datagram

    def no_echo(device, agent, generation, datagram):
        if device == 'r1' and isinstance(datagram.payload, Hello):
            datagram = replace(datagram, payload=replace(datagram.payload, seen=()))
        return send(device, agent, generation, datagram)

    sim.transport.send_datagram = no_echo
    sim.run_until(3)
    assert not state(sim).adjacencies['e1', 2].up
    assert not state(sim).sessions and not state(sim).rows
    sim.transport.send_datagram = send
    sim.run_until(5)
    assert state(sim).adjacencies['e1', 2].up
    assert state(sim).sessions and state(sim).rows
