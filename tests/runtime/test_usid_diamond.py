"""B3 timed probes and encoding-aware placement on the SR-DB diamond."""

import pytest

from netsim import Environment
from netsim.model import flows
from netsim.model import forwarding as fw
from netsim.model import srv6 as sr
from netsim.model.hashing import flow_label_for
from netsim.model.packets import FlowKey, frame_bytes
from netsim.runtime import Simulation
from tests.model.test_usid import (
    diamond,
    form_path,
    inner_template,
    install_path,
    install_policy_path,
    ip,
    loose_sids,
    strict_sids,
)


@pytest.mark.parametrize('path', ['strict', 'loose'])
@pytest.mark.parametrize('af', [4, 6])
def test_timed_send_matches_full_trace_and_waits_each_link(path, af):
    net, _ = diamond(delay=0.25)
    sids = loose_sids(net, delay=0.25) if path == 'loose' else strict_sids(net)
    install_path(net, sids)
    packet = inner_template(af).to_packet()
    expected = net.trace('R1', packet)
    sim = Simulation(Environment(), net)
    process = sim.send('R1', packet)
    transmissions = 3 if path == 'loose' else 2
    sim.run_until(transmissions * 0.25 - 0.01)
    assert not process.triggered
    sim.env.run(until=process)
    assert sim.env.now == transmissions * 0.25
    assert process.value == expected
    assert process.value.outcome == fw.DELIVER


@pytest.mark.parametrize('form', ['bare', 'composite', 'wlib', 'two-blocks'])
@pytest.mark.parametrize('numbered', [True, False])
def test_sid_forms_reach_the_intended_edges_in_probes_hash_placement_and_send(
    form, numbered
):
    net, _ = diamond(numbered=numbered, delay=0.25)
    sids, expected, path = form_path(net, form)
    encap = install_path(net, sids, destination=9)
    assert encap.entries == expected
    net.add_demand('d', 'R1', '10.0.0.9', 100e6, mode=flows.HASH, flows=16)
    sim = Simulation(Environment(), net)
    packet = inner_template(destination=9).to_packet()
    trace = net.trace('R1', packet)
    assert (trace.outcome, trace.path) == (fw.DELIVER, (*path, 'R9'))
    sizes = [1098, 1098, 1034] if form == 'two-blocks' else [1074, 1074, 1034]
    assert [frame_bytes(h.packet) for h in trace.hops[:3]] == sizes
    report = net.placement
    assert report.delivered_total == 100e6 and not report.dropped_by_reason
    edges = [h.edge_id for h in trace.hops[:3]]
    for edge, size in zip(edges, sizes, strict=True):
        assert report.offered[edge] == pytest.approx(100e6 * size / 1000)
        assert report.carried[edge] == report.offered[edge]
    assert all(
        load == 0 for edge, load in enumerate(report.offered) if edge not in edges
    )
    process = sim.send('R1', packet)
    sim.env.run(until=process)
    assert sim.env.now == 0.75 and process.value == trace


@pytest.mark.parametrize('compressed', [True, False])
@pytest.mark.parametrize(
    'mode',
    [
        flows.HASH,
        pytest.param(
            flows.FLUID,
            marks=pytest.mark.xfail(
                strict=True,
                reason='netsim/model/flows.py:381 _egress_edges ignores adj.encap; '
                'FLUID charges plain-IP frame sizes along the SR path (G5 owns flows)',
            ),
        ),
    ],
    ids=['hash', 'fluid'],
)
def test_per_edge_wire_loads_follow_actual_encoding_and_decap(compressed, mode):
    net, links = diamond()
    install_path(net, strict_sids(net), compressed=compressed, destination=9)
    net.add_demand('d', 'R1', '10.0.0.9', 100e6, mode=mode, flows=16)
    net.converge()
    report = net.placement
    assert report.delivered_total == 100e6 and not report.dropped_by_reason
    expected = (
        [107.4e6, 107.4e6, 103.4e6] if compressed else [111.4e6, 111.4e6, 103.4e6]
    )
    edges = [links[1, 2].edge('R1'), links[2, 4].edge('R2'), links[4, 9].edge('R4')]
    assert [report.offered[e] for e in edges] == pytest.approx(expected)
    assert [report.carried[e] for e in edges] == pytest.approx(expected)
    assert all(load == 0 for e, load in enumerate(report.offered) if e not in edges)
    assert dict(report.demands['d'].edges) == dict.fromkeys(edges, 100e6)


@pytest.mark.parametrize('compressed', [True, False])
@pytest.mark.parametrize(
    'mode',
    [
        flows.HASH,
        pytest.param(
            flows.FLUID,
            marks=pytest.mark.xfail(
                strict=True,
                reason='netsim/model/flows.py:355 _egress_edges computes MTU from the inner '
                'packet only, omitting adj.encap headers and wire load (G5 owns flows)',
            ),
        ),
    ],
    ids=['hash', 'fluid'],
)
@pytest.mark.parametrize('af', [4, 6])
def test_mtu_decisions_use_ip_stack_for_each_encoding(compressed, mode, af):
    # Inner (1020 or 1040) + outer 40 fits; a raw list adds a 40-byte SRH.
    net, links = diamond(mtu=1060 if af == 4 else 1080)
    install_path(net, strict_sids(net), compressed=compressed, destination=9)
    packet = inner_template(af, destination=9).to_packet()
    trace = net.trace('R1', packet)
    net.add_demand(
        'd', 'R1', '10.0.0.9' if af == 4 else '2001:db8::9', 100e6, mode=mode
    )
    net.converge()
    report = net.placement
    if compressed:
        assert trace.outcome == fw.DELIVER
        assert report.delivered_total == 100e6 and not report.dropped_by_reason
        assert report.offered[links[1, 2].edge('R1')] == pytest.approx(
            107.4e6 if af == 4 else 109.4e6
        )
    else:
        assert (trace.path, trace.reason) == (('R1',), fw.MTU_EXCEEDED)
        assert report.delivered_total == 0
        assert dict(report.dropped_by_reason) == {fw.MTU_EXCEEDED: 100e6}
        assert not any(report.offered)


@pytest.mark.parametrize('psp', [0, sr.PSP])
def test_psp_mtu_is_checked_after_popping_the_second_container_srh(psp):
    net, links = diamond(mtu=1084)
    install_path(net, loose_sids(net, psp=psp))
    net['R3']['toR4'].configure(mtu=1060)
    net['R4']['toR3'].configure(mtu=1060)
    net.add_demand('d', 'R1', '10.0.0.4', 100e6, mode=flows.HASH)
    net.converge()
    trace = net.trace('R1', inner_template().to_packet())
    if psp:
        assert trace.outcome == fw.DELIVER
        assert trace.hops[2].packet.srh is None
        assert net.placement.delivered_total == 100e6
        assert net.placement.offered[links[3, 4].edge('R3')] == pytest.approx(107.4e6)
    else:
        assert (trace.path, trace.reason) == (('R1', 'R2', 'R3'), fw.MTU_EXCEEDED)
        assert net.placement.delivered_total == 0
        assert dict(net.placement.dropped_by_reason) == {fw.MTU_EXCEEDED: 100e6}


def test_hash_flow_label_derivation_is_identical_with_and_without_srh():
    labels = []
    for compressed in (True, False):
        net, _ = diamond()
        install_path(net, strict_sids(net), compressed=compressed)
        encoding_labels = []
        for af in (4, 6):
            for sport in range(10000, 10032):
                packet = inner_template(af, sport=sport, dscp=17).to_packet()
                trace = net.trace('R1', packet)
                assert trace.outcome == fw.DELIVER
                expected = flow_label_for(FlowKey.from_packet(packet), 0)
                encoding_labels.append(expected)
                for hop in trace.hops[:2]:
                    assert hop.packet.flow_label == expected
                    key = FlowKey.from_packet(hop.packet)
                    assert key.flow_label == expected
                    assert (key.sport, key.dport) == (0, 0)
        labels.append(encoding_labels)
    assert labels[0] == labels[1] and len(set(labels[0])) > 60


@pytest.mark.parametrize('compressed', [True, False])
def test_inflight_sid_withdrawal_and_restore_are_visible(compressed):
    net, _ = diamond(delay=1)
    sids = strict_sids(net)
    install_path(net, sids, compressed=compressed)
    sim = Simulation(Environment(), net)
    packet = inner_template().to_packet()
    process = sim.send('R1', packet)
    sim.at(0.5, lambda: net['R2'].remove_local_sid(sids[1].sid))
    sim.env.run(until=process)
    assert sim.env.now == 1
    assert (process.value.path, process.value.reason) == (('R1', 'R2'), sr.SID_UNKNOWN)
    # Recreate the same explicit address. The encoded packet is valid again.
    net['R2'].add_local_sid(
        sr.END_X,
        sid=sids[1].sid,
        structure=sids[1].structure,
        flavors=sids[1].flavors,
        interface='toR4',
    )
    sim.settle()
    restored = sim.send('R1', packet)
    sim.env.run(until=restored)
    assert restored.value.outcome == fw.DELIVER
    assert [h.packet.hop_limit for h in restored.value.hops[:2]] == [63, 62]


def test_delayed_carrier_and_fib_expose_stale_program_before_sid_withdrawal():
    net, links = diamond()
    sids = strict_sids(net)
    install_path(net, sids)
    net['R2'].configure(fib_delay=0.5)
    for node, peer in ((2, 4), (4, 2)):
        net[f'R{node}'][f'toR{peer}'].configure(
            carrier_delay_down=0.25, carrier_delay_up=0.25
        )
    net.add_demand('d', 'R1', '10.0.0.4', 100e6, mode=flows.HASH)
    sim = Simulation(Environment(), net)
    packet = inner_template().to_packet()
    sim.at(1, links[2, 4].fail)
    sim.run_until(1.1)
    assert net['R2'].node.srv6_sids.sids[sids[1].sid].adjacency_up
    assert net.trace('R1', packet).reason == fw.LINK_DOWN
    sim.run_until(1.3)
    assert not net['R2'].node.srv6_sids.sids[sids[1].sid].adjacency_up
    assert net['R2'].fib(6).lookup(sids[1].sid).action == fw.SRV6_LOCAL
    assert net.trace('R1', packet).reason == fw.EGRESS_DOWN
    sim.run_until(1.8)
    assert net.trace('R1', packet).reason == sr.SID_UNKNOWN
    assert dict(net.placement.dropped_by_reason) == {sr.SID_UNKNOWN: 100e6}
    sim.at(2, links[2, 4].restore)
    sim.run_until(2.8)
    assert net['R2'].node.srv6_sids.sids[sids[1].sid].adjacency_up
    assert net.trace('R1', packet).outcome == fw.DELIVER
    assert net.placement.delivered_total == 100e6
    settled = net.fork()
    settled.converge()
    assert settled.trace('R1', packet) == net.trace('R1', packet)


@pytest.mark.skip(reason='needs G5 policies')
@pytest.mark.parametrize('mode', [flows.HASH, flows.FLUID], ids=['hash', 'fluid'])
def test_policy_counterpart_timed_delivery_and_placement(mode):
    net, links = diamond(delay=0.25)
    policy = install_policy_path(net, strict_sids(net), symbolic=True, destination=9)
    net.add_demand('d', 'R1', '10.0.0.9', 100e6, mode=mode)
    sim = Simulation(Environment(), net)
    assert net['R1'].node.srv6_policies.states[policy.key].status == sr.POLICY_UP
    process = sim.send('R1', inner_template(destination=9).to_packet())
    sim.env.run(until=process)
    assert (process.value.outcome, process.value.path) == (
        fw.DELIVER,
        ('R1', 'R2', 'R4', 'R9'),
    )
    assert process.value.hops[0].packet.dst == ip('5f00:0:e002:e104::')
    assert [frame_bytes(h.packet) for h in process.value.hops[:3]] == [1074, 1074, 1034]
    assert net.placement.delivered_total == 100e6
    assert [
        net.placement.offered[links[a, b].edge(f'R{a}')]
        for a, b in ((1, 2), (2, 4), (4, 9))
    ] == pytest.approx([107.4e6, 107.4e6, 103.4e6])
