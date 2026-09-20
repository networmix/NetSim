"""Timed packet loss across a short link outage: single link and ECMP, with
the control plane (``fib_delay``) and data plane (``fast_failover``) knobs."""

from __future__ import annotations

from collections import Counter

import pytest

import netsim
from netsim.model import forwarding as fw
from netsim.model.addressing import IPV4, to_int
from netsim.model.network import Network
from netsim.model.packets import PacketTemplate
from netsim.runtime import Simulation

FAIL, RESTORE = 0.500, 0.550  # 50 ms outage


def build(links: int, **dev):
    net = Network()
    r1, r2 = net.add_device('R1', **dev), net.add_device('R2', **dev)
    r1.add_loopback('lo0', ipv4=['10.0.0.1/32'])
    r2.add_loopback('lo0', ipv4=['10.0.0.2/32'])
    nhs = []
    for i in range(links):
        net.add_p2p(
            r1,
            f'eth{i}',
            r2,
            f'eth{i}',
            ipv4=(f'10.1.{i}.0/31', f'10.1.{i}.1/31'),
            speed=10e9,
        )
        nhs.append((f'eth{i}', f'10.1.{i}.1'))
    r1.add_route('10.0.0.2/32', nhs)
    return net


def send_train(net, n=100, spacing=0.01, offset=0.005, **kw):
    """*n* packets, distinct flows, one every *spacing* s; returns traces by send time."""
    env = netsim.Environment()
    sim = Simulation(env, net)
    link0 = net.links['R1:eth0--R2:eth0']
    sim.at(FAIL, link0.fail)
    sim.at(RESTORE, link0.restore)
    src, _ = to_int('10.0.0.1')
    dst, _ = to_int('10.0.0.2')
    procs = []
    for k in range(n):
        pkt = PacketTemplate(IPV4, src, dst, sport=40000 + k).to_packet()
        sim.at(offset + k * spacing, lambda p=pkt: procs.append(sim.send('R1', p)))
    sim.run_until(offset + n * spacing + 1)
    return [(offset + k * spacing, p.value) for k, p in enumerate(procs)], sim


def lost(traces):
    return [(t, tr.reason) for t, tr in traces if tr.outcome != fw.DELIVER]


class TestSingleLink:
    def test_exactly_the_packets_in_the_outage_are_lost(self):
        traces, _ = send_train(build(1))
        bad = lost(traces)
        assert [t for t, _ in bad] == [t for t, _ in traces if FAIL < t < RESTORE]
        assert {r for _, r in bad} == {fw.NO_ROUTE}  # interface DOWN withdrew the route
        assert len(bad) == 5

    def test_carrier_delay_keeps_routing_but_the_link_is_still_dead(self):
        net = build(1)
        for d in ('R1', 'R2'):
            net.devices[d].interface('eth0').configure(carrier_delay_down=0.1)
        traces, _ = send_train(net)
        bad = lost(traces)
        assert len(bad) == 5 and {r for _, r in bad} == {fw.LINK_DOWN}

    def test_a_packet_at_the_restore_instant_sees_the_old_state(self):
        traces, _ = send_train(build(1), n=1, spacing=1, offset=RESTORE)
        assert (
            traces[0][1].outcome == fw.DROP
        )  # ops and sends at t run before the derivation band


class TestEcmp:
    def test_instant_control_plane_loses_nothing(self):
        traces, _ = send_train(build(2, fib_delay=0.0))
        assert lost(traces) == []

    def test_slow_control_plane_loses_flows_hashed_to_the_dead_leg(self):
        traces, _ = send_train(
            build(2, fib_delay=0.1), n=1000, spacing=0.001, offset=0.0005
        )
        bad = lost(traces)
        assert {r for _, r in bad} == {fw.EGRESS_DOWN}
        assert all(FAIL < t < RESTORE for t, _ in bad)
        assert 10 <= len(bad) <= 40  # about half of the 50 packets in the window

    def test_control_plane_reaction_time_bounds_the_loss(self):
        traces, _ = send_train(
            build(2, fib_delay=0.02), n=1000, spacing=0.001, offset=0.0005
        )
        bad = lost(traces)
        assert all(FAIL < t < FAIL + 0.02 for t, _ in bad)
        assert 3 <= len(bad) <= 17

    def test_fast_failover_loses_nothing_with_a_slow_control_plane(self):
        traces, sim = send_train(
            build(2, fib_delay=0.1, fast_failover=True),
            n=1000,
            spacing=0.001,
            offset=0.0005,
        )
        assert lost(traces) == []
        via = Counter(tr.hops[0].egress for t, tr in traces if FAIL < t < RESTORE)
        assert via == {'eth1': 50}  # everything re-hashed onto the live leg
        assert sim.timeline.stage_names(FAIL) == ['carrier', 'l3']  # FIB still pending

    def test_fast_failover_uses_raw_carrier_during_debounce(self):
        net = build(2, fast_failover=True)
        net.devices['R1'].interface('eth0').configure(carrier_delay_down=0.1)
        traces, _ = send_train(net, n=1000, spacing=0.001, offset=0.0005)
        assert lost(traces) == []  # the control plane still thinks eth0 is UP


class TestPlacementAgrees:
    def test_fluid_prunes_the_dead_leg_only_with_fast_failover(self):
        for ff, expect_drop in ((False, True), (True, False)):
            net = build(2, fib_delay=0.1, fast_failover=ff)
            net.add_demand('d', 'R1', '10.0.0.2', 100e6)
            env = netsim.Environment()
            sim = Simulation(env, net)
            sim.at(FAIL, net.links['R1:eth0--R2:eth0'].fail)
            sim.run_until(0.52)  # inside the outage, before the FIB reacts
            rep = net.state.placement
            e1 = net.links['R1:eth1--R2:eth1'].edge('R1')
            if expect_drop:
                assert rep.dropped_by_reason[fw.EGRESS_DOWN] == pytest.approx(50e6)
                assert rep.utilization(e1) == pytest.approx(50e6 * 1.034 / 10e9)
            else:
                assert dict(rep.dropped_by_reason) == {}
                assert rep.utilization(e1) == pytest.approx(100e6 * 1.034 / 10e9)
                assert rep.delivered_total == pytest.approx(100e6)
