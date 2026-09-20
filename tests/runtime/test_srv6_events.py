"""SR input invalidation and semantic timeline records."""

from dataclasses import replace

import pytest

import netsim
from netsim.model import derive, srv6
from netsim.model.addressing import IPV4, IPV6
from netsim.model.contracts import SRV6_LOCAL, STATIC
from netsim.model.network import Network
from netsim.model.routing import SRV6_LOCAL_NH
from netsim.model.state import StateDelta
from netsim.runtime import Simulation
from netsim.runtime.pipeline import fib_affected, placement_affected
from netsim.runtime.timeline import LocatorEvent, PolicyEvent, SidEvent, Timeline


def topology():
    net = Network()
    head = net.add_device('head', fib_delay=2)
    a, b = net.add_device('a'), net.add_device('b')
    link = net.add_p2p(a, 'p', b, 'p', unnumbered=True)
    a.add_locator('loc', structure=srv6.F3216_GIB)
    sid = a.add_local_sid(
        srv6.END_X,
        flavors=srv6.NEXT_CSID | srv6.PSP,
        structure=srv6.F3216_LIB,
        interface='p',
    )
    head.policy_client().add(srv6.SrPolicy(STATIC, 10, 1))
    return net, head, a, b, link, sid


def local_rows(device):
    return tuple(
        row
        for row in device.rib(IPV6).rows_of(SRV6_LOCAL)
        if row.nexthops[0].special == SRV6_LOCAL_NH
    )


def test_adjacency_withdrawal_persists_sid_and_invalidates_remote_consumer():
    net, head, a, _, link, sid = topology()
    net.converge()
    assert a.node.srv6_sids.sids[sid.sid].adjacency_up
    assert local_rows(a)[0].nexthops[0].behavior == a.node.srv6_sids.sids[sid.sid]
    before, ribs = head.node.resolver_input_epoch, head.node.ribs
    link.fail()
    net.converge()
    assert not a.node.srv6_sids.sids[sid.sid].adjacency_up
    assert not local_rows(a)
    assert head.node.ribs is ribs
    assert all(head.node.resolver_input_epoch[af] > before[af] for af in (IPV4, IPV6))
    link.restore()
    net.converge()
    assert a.node.srv6_sids.sids[sid.sid].adjacency_up
    assert len(local_rows(a)) == 1


def test_timed_pipeline_schedules_both_families_and_honors_delay():
    net, head, a, _, link, sid = topology()
    sim = Simulation(netsim.Environment(), net)
    epochs = head.node.resolver_input_epoch
    sim.at(10, link.fail)
    sim.run_until(10.5)
    assert not a.node.srv6_sids.sids[sid.sid].adjacency_up
    assert not local_rows(a)
    for af in (IPV4, IPV6):
        assert head.node.resolver_input_epoch[af] > epochs[af]
        assert (
            head.node.resolver_outcomes[af].processed_epoch
            < head.node.resolver_input_epoch[af]
        )
    sim.run_until(13)
    for af in (IPV4, IPV6):
        assert (
            head.node.resolver_outcomes[af].processed_epoch
            == head.node.resolver_input_epoch[af]
        )
    events = sim.timeline.select(kind=SidEvent)
    assert any(
        e.action == 'adjacency_down'
        and e.behavior == 'End.X'
        and e.flavors == ('PSP', 'NEXT-C-SID')
        for e in events
    )
    assert 'fib' in sim.timeline.stage_names(12)


@pytest.mark.parametrize(
    'field,value',
    [('srv6_source', 123), ('srv6_hop_limit', 128), ('srdb_source', ('agent', 'test'))],
)
def test_sr_config_is_both_family_fib_and_placement_input(field, value):
    net, head, *_ = topology()
    net.add_demand('d', 'head', '10.0.0.1', 100)
    net.converge()
    before = net.state
    head.configure(**{field: value})
    delta = StateDelta(before, net.state)
    assert {('head', IPV4), ('head', IPV6)} <= fib_affected(delta, net.state)
    assert placement_affected(delta, net.state) == {'*'}
    for af in (IPV4, IPV6):
        assert (
            head.node.resolver_input_epoch[af]
            > before.devices['head'].resolver_input_epoch[af]
        )


def test_missing_sid_appearance_deletion_and_batch_reversion_dirty_epochs():
    net, head, a, *_ = topology()
    net.converge()
    before = net.state
    with net.batch():
        sid = a.add_local_sid(
            srv6.END_DT46, structure=srv6.F3216_TERMINAL, sid='5f00:0:e800::'
        )
        a.remove_local_sid(sid.sid)
    assert a.node.srv6_sids == before.devices['a'].srv6_sids
    assert {('head', IPV4), ('head', IPV6)} <= fib_affected(
        StateDelta(before, net.state), net.state
    )
    assert all(
        head.node.resolver_input_epoch[af]
        > before.devices['head'].resolver_input_epoch[af]
        for af in (IPV4, IPV6)
    )
    net.converge()
    assert all(
        head.node.resolver_input_epoch[af]
        == head.node.resolver_outcomes[af].processed_epoch
        for af in (IPV4, IPV6)
    )


def test_policy_state_output_does_not_trigger_fib_feedback():
    net, head, *_ = topology()
    net.converge()
    before = net.state

    def change(state):
        dev = state.devices['head']
        table = replace(
            dev.srv6_policies,
            states=dev.srv6_policies.states.set((10, 1), srv6.PolicyState(status='UP')),
        )
        return replace(
            state, devices=state.devices.set('head', replace(dev, srv6_policies=table))
        )

    net.update(change)
    assert not fib_affected(StateDelta(before, net.state), net.state)
    assert head.node.resolver_input_epoch is before.devices['head'].resolver_input_epoch


def test_timeline_add_replace_delete_state_and_removed_device():
    net = Network()
    timeline = Timeline()
    net.on_delta.append(timeline.on_delta)
    dev = net.add_device('r')
    dev.add_locator('loc', structure=srv6.F3216_GIB)
    sid = dev.add_local_sid(srv6.END, flavors=srv6.NEXT_CSID, structure=srv6.F3216_GIB)
    policy = srv6.SrPolicy(STATIC, 10, 1, name='test')
    client = dev.policy_client()
    client.add(policy)
    client.replace(replace(policy, name='changed'))

    def set_state(state):
        dev = state.devices['r']
        result = srv6.PolicyState(
            active_path=1,
            status='UP',
            reasons=((0, 0, 'FIRST_SID_UNRESOLVABLE'),),
            programmed_version=7,
        )
        table = replace(
            dev.srv6_policies, states=dev.srv6_policies.states.set(policy.key, result)
        )
        return replace(
            state, devices=state.devices.set('r', replace(dev, srv6_policies=table))
        )

    net.update(set_state)
    client.delete(policy.key)
    dev.remove_local_sid(sid.sid)
    net.remove_device('r')
    assert [e.action for e in timeline.select(kind=LocatorEvent)] == ['add', 'remove']
    assert [e.action for e in timeline.select(kind=SidEvent)] == ['add', 'remove']
    events = timeline.select(kind=PolicyEvent)
    assert [e.action for e in events] == ['add', 'replace', 'state', 'delete']
    assert events[2].active_path == 1 and events[2].programmed_version == 7
    assert events[2].row()['reasons'] == ((0, 0, 'FIRST_SID_UNRESOLVABLE'),)
    assert events[0].endpoint == '::1'
    assert timeline.select(kind=SidEvent)[0].behavior == 'End'
    assert timeline.select(kind=SidEvent)[0].flavors == ('NEXT-C-SID',)


def test_numbered_neighbor_required_and_scoped_l3():
    net = Network()
    a, b = net.add_device('a'), net.add_device('b')
    net.add_p2p(a, 'p', b, 'p', ipv6=('2001:db8::1/64', '2001:db8::2/64'))
    a.add_locator('l', structure=srv6.F3216_GIB)
    sid = a.add_local_sid(
        srv6.END_X, structure=srv6.F3216_LIB, interface='p', nexthop='2001:db8::3'
    )
    net.converge()
    assert not a.node.srv6_sids.sids[sid.sid].adjacency_up
    b['p'].configure(ipv6=['2001:db8::3/64'])
    state = derive.derive_l3(net.state, 0, [('a', frozenset({'p'}))])
    assert state.devices['a'].srv6_sids.sids[sid.sid].adjacency_up
    net.converge()
    assert a.node.srv6_sids.sids[sid.sid].adjacency_up
    a.configure(enabled=False)
    net.converge()
    assert not local_rows(a)


def test_unknown_cover_returns_on_sid_withdrawal_and_yields_on_restore():
    from netsim.model import forwarding as fw
    from netsim.model import packets

    net = Network()
    a, b = net.add_device('a'), net.add_device('b')
    ranges = srv6.SidRanges(gib=(1, 100), lib=(0xE001, 0xE00F), wlib=(0xFFFE, 0xFFFF))
    for dev in (a, b):
        dev.add_locator('loc', structure=srv6.F3216_GIB, ranges=ranges)
    b.add_loopback('lo', ipv4=['10.0.0.2/32'])
    link = net.add_p2p(a, 'p', b, 'p', unnumbered=True)
    adjacency = a.add_local_sid(
        srv6.END_X,
        structure=srv6.F3216_LIB,
        flavors=srv6.NEXT_CSID,
        sid='5f00:0:e001::',
        interface='p',
    )
    terminal = b.add_local_sid(
        srv6.END_DT46, structure=srv6.F3216_TERMINAL, sid='5f00:0:e008::'
    )
    packet = packets.encapsulate(
        packets.IPv4Packet(1, 0x0A000002, 17),
        (adjacency.sid, terminal.sid),
        behavior=srv6.H_ENCAPS_RED,
        source=1,
        hop_limit=64,
        flow_label=0,
        transit=False,
    )
    sim = Simulation(netsim.Environment(), net)
    assert net.trace('a', packet).outcome == fw.DELIVER
    assert a.fib(IPV6).lookup(adjacency.sid).action == fw.SRV6_LOCAL
    sim.at(10, link.fail)
    sim.at(20, link.restore)
    sim.run_until(11)
    assert not a.node.srv6_sids.sids[adjacency.sid].adjacency_up
    assert a.fib(IPV6).lookup(adjacency.sid).action == fw.DROP_UNREACHABLE
    assert net.trace('a', packet).reason == srv6.SID_UNKNOWN
    sim.run_until(21)
    assert a.node.srv6_sids.sids[adjacency.sid].adjacency_up
    assert a.fib(IPV6).lookup(adjacency.sid).action == fw.SRV6_LOCAL
    assert net.trace('a', packet).outcome == fw.DELIVER
