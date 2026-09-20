"""The timeline: records, semantic events, series, export and retention."""

from __future__ import annotations

import csv

import pytest

import netsim
from netsim.model.addressing import IPV4
from netsim.runtime import Simulation
from netsim.runtime.timeline import (
    CarrierRawEvent,
    ConfigEvent,
    DemandEvent,
    DeviceEvent,
    FibEvent,
    InterfaceOperEvent,
    LagEvent,
    LinkConfigEvent,
    LinkStateEvent,
    Origin,
    PlacementEvent,
    RouteEvent,
    Timeline,
    origin_from,
)
from tests.model.test_network import build_diamond

LINK = 'R1:eth1--R2:eth1'


def flap(t_fail=10, t_restore=None, **sim_kw):
    net, R = build_diamond(min_links=2)
    net.add_demand('d1', 'R1', '10.0.0.4', 100e6)
    env = netsim.Environment()
    sim = Simulation(env, net, **sim_kw)
    link = net.links[LINK]
    sim.at(t_fail, link.fail)
    if t_restore is not None:
        sim.at(t_restore, link.restore)
    sim.run_until(40)
    return net, R, sim


class TestRecords:
    def test_records_carry_seq_round_and_origin(self):
        net, R, sim = flap()
        tl = sim.timeline
        seqs = [r.seq for r in tl.records]
        assert seqs == list(range(1, len(seqs) + 1))
        at10 = [r for r in tl.records if r.time == 10]
        assert at10[0].origin == Origin('op', 'link', f'{LINK},fail')
        assert [r.origin.name for r in at10[1:]] == [
            'carrier',
            'lag',
            'l3',
            'fib',
            'placement',
        ]
        assert all(r.origin.kind == 'stage' for r in at10[1:])
        assert all(r.round == 0 for r in at10)
        assert tl.stage_names(10) == ['carrier', 'lag', 'l3', 'fib', 'placement']
        assert tl.records[0].origin == Origin('init', 'converge')
        assert sum(r.event_count for r in tl.records) == len(tl.events)

    def test_origin_from(self):
        assert origin_from(('kind', 'fib', 3)) == (Origin('stage', 'fib'), 3)
        assert origin_from(('link', LINK, 'fail')) == (
            Origin('op', 'link', f'{LINK},fail'),
            None,
        )
        assert origin_from('converge') == (Origin('op', 'converge'), None)
        assert origin_from('converge', initializing=True) == (
            Origin('init', 'converge'),
            None,
        )
        assert str(Origin('op', 'link', 'x')) == 'op:link(x)'
        assert str(Origin('stage', 'fib')) == 'stage:fib'


class TestEvents:
    def test_link_failure_events(self):
        net, R, sim = flap()
        tl = sim.timeline
        ev = tl.at(10)
        links = [e for e in ev if isinstance(e, LinkStateEvent)]
        assert [(e.link, e.old, e.new, e.origin) for e in links] == [
            (LINK, 'UP', 'FAILED', Origin('op', 'link', f'{LINK},fail'))
        ]
        assert (links[0].seq, links[0].idx, links[0].round) == (2, 0, 0)
        opers = {
            (e.device, e.interface): (e.old, e.new, e.reason)
            for e in ev
            if isinstance(e, InterfaceOperEvent)
        }
        assert opers[('R1', 'eth1')] == ('UP', 'DOWN', 'CARRIER')
        assert opers[('R2', 'eth1')] == ('UP', 'DOWN', 'CARRIER')
        assert opers[('R1', 'Po1')] == ('UP', 'LOWER_LAYER_DOWN', 'MIN_LINKS')
        lags = {(e.device, e.portchannel): e for e in ev if isinstance(e, LagEvent)}
        assert lags[('R1', 'Po1')].active_members == ()
        assert lags[('R1', 'Po1')].bandwidth == 0.0
        routes = {
            (e.device, e.prefix, e.source, e.action)
            for e in ev
            if isinstance(e, RouteEvent)
        }
        assert ('R1', '10.1.12.0/31', 'connected', 'delete') in routes
        fibs = {(e.device, e.prefix): e for e in ev if isinstance(e, FibEvent)}
        assert fibs[('R1', '10.0.0.4/32')].action == 'update'
        assert fibs[('R1', '10.0.0.4/32')].nexthops == ('eth3 10.1.13.1',)
        assert fibs[('R1', '10.0.0.2/32')].action == 'withdraw'
        assert fibs[('R1', '10.1.12.0/32')].fib_action == 'RECEIVE'
        placements = [e for e in ev if isinstance(e, PlacementEvent)]
        assert len(placements) == 1
        assert placements[0].version == 2
        assert placements[0].oversubscribed == 2
        assert placements[0].delivered == pytest.approx(100e6)

    def test_fib_events_only_when_forwarding_changes(self):
        """A re-interned group id or a bookkeeping bump is not an event."""
        net, R, sim = flap()
        ev = sim.timeline.at(10)
        fibs = {(e.device, e.prefix) for e in ev if isinstance(e, FibEvent)}
        assert ('R1', '10.0.0.3/32') not in fibs  # still via eth3
        assert ('R1', '10.1.13.0/31') not in fibs
        assert ('R3', '10.0.0.4/32') not in fibs  # R3 untouched

    def test_carrier_debounce_is_two_events(self):
        net, R = build_diamond(min_links=2)
        R['R1'].interface('eth1').configure(carrier_delay_down=0.5)
        env = netsim.Environment()
        sim = Simulation(env, net)
        sim.at(10, net.links[LINK].fail)
        sim.run_until(20)
        tl = sim.timeline
        raw = [e for e in tl.select(kind=CarrierRawEvent, device='R1')]
        assert [(e.time, e.interface, e.carrier_raw, e.effective) for e in raw] == [
            (10, 'eth1', False, True)
        ]
        assert raw[0].origin == Origin('stage', 'carrier')
        assert tl.interface_series('R1', 'eth1') == [
            (0, 'UP', 'UP'),
            (10.5, 'DOWN', 'CARRIER'),
        ]
        assert tl.interface_series('R2', 'eth1') == [
            (0, 'UP', 'UP'),
            (10, 'DOWN', 'CARRIER'),
        ]

    def test_select_filters(self):
        net, R, sim = flap(t_restore=30)
        tl = sim.timeline
        assert len(tl.select(kind=LinkStateEvent)) == 2
        assert tl.select(kind=LinkStateEvent, since=20) == tl.select(
            kind=LinkStateEvent, until=40, since=30
        )
        r1 = tl.select(kind=(FibEvent, RouteEvent), device='R1', since=10, until=10)
        assert r1 and all(e.device == 'R1' and e.time == 10 for e in r1)
        assert tl.select(
            predicate=lambda e: getattr(e, 'prefix', '') == '10.0.0.4/32', since=10
        )


class TestSeries:
    def test_utilization_and_delivered_series(self):
        net, R, sim = flap(t_restore=30)
        tl = sim.timeline
        edge = net.links['R1:eth3--R3:eth1'].edge('R1')
        series = tl.utilization_series(edge)
        assert [t for t, _ in series] == [0, 10, 30]
        assert series[0][1] == pytest.approx(0.517, abs=1e-3)
        assert series[1][1] == pytest.approx(1.034, abs=1e-3)
        assert series[2][1] == pytest.approx(0.517, abs=1e-3)
        assert tl.delivered_series() == [
            (0, pytest.approx(100e6)),
            (10, pytest.approx(100e6)),
            (30, pytest.approx(100e6)),
        ]
        assert tl.delivered_series('d1') == tl.delivered_series()
        assert tl.delivered_series('nope') == []

    def test_series_do_not_need_roots_or_reports(self):
        net, R, sim = flap(t_restore=30, keep_roots=1, keep_deltas=0)
        tl = sim.timeline
        assert len(tl.roots) == 1 and len(tl.deltas) == 0
        edge = net.links['R1:eth3--R3:eth1'].edge('R1')
        assert [t for t, _ in tl.utilization_series(edge)] == [0, 10, 30]
        assert all(e.report is None for e in tl.placement_events())

    def test_keep_reports(self):
        tl = Timeline(keep_reports=True)
        net, R = build_diamond(min_links=2)
        net.add_demand('d1', 'R1', '10.0.0.4', 100e6)
        env = netsim.Environment()
        sim = Simulation(env, net)
        sim.timeline = tl
        sim.at(10, net.links[LINK].fail)
        sim.run_until(20)
        events = list(tl.placement_events())
        assert events and events[-1].report is net.state.placement


class TestExport:
    def test_rows_and_csv(self, tmp_path):
        net, R, sim = flap()
        tl = sim.timeline
        rows = tl.rows()
        assert len(rows) == len(tl.events)
        assert all(r['event'] and isinstance(r['origin'], str) for r in rows)
        fib_rows = tl.rows(tl.select(kind=FibEvent, device='R1', since=10))
        assert fib_rows[0]['origin'] == 'stage:fib'
        assert fib_rows[0]['event'] == 'FibEvent'
        path = tmp_path / 'events.csv'
        n = tl.to_csv(str(path))
        with open(path) as f:
            read = list(csv.DictReader(f))
        assert n == len(read) == len(tl.events)
        assert {r['event'] for r in read} >= {
            'LinkStateEvent',
            'InterfaceOperEvent',
            'FibEvent',
            'PlacementEvent',
        }
        assert read[0]['seq'] == '1'

    def test_summary_is_one_line_per_event(self):
        net, R, sim = flap()
        text = sim.timeline.summary(10)
        lines = text.splitlines()
        assert len(lines) == len(sim.timeline.at(10))
        assert any('LinkStateEvent' in ln and 'new=FAILED' in ln for ln in lines)
        assert any('reason=CARRIER' in ln for ln in lines)


class TestRetention:
    def test_bounded_deltas_and_roots(self):
        net, R, sim = flap(t_restore=30, keep_deltas=2, keep_roots=2)
        tl = sim.timeline
        assert len(tl.deltas) == 2 and len(tl.roots) == 2
        assert tl.deltas[-1][0] == tl.records[-1].seq
        assert tl.snapshot_at(0) is None  # evicted
        assert tl.snapshot_at(30) is net.state
        assert tl.snapshot_at(10) is not None
        assert len(tl.records) == len(set(r.seq for r in tl.records))

    def test_extract_can_be_disabled(self):
        net, R, sim = flap(extract_events=False)
        tl = sim.timeline
        assert tl.events == []
        assert len(tl.records) > 1 and all(r.event_count == 0 for r in tl.records)
        assert tl.stage_names(10) == ['carrier', 'lag', 'l3', 'fib', 'placement']

    def test_recorder_alias(self):
        net, R, sim = flap()
        assert sim.recorder is sim.timeline


class TestDeltaNoise:
    def test_changed_paths_hide_bookkeeping(self):
        net, R = build_diamond(min_links=2)
        delta = net.update(lambda s: s, origin='noop')
        assert delta is None
        net.converge()
        deltas = []
        net.on_delta.append(lambda t, o, d: deltas.append(d))
        R['R1'].add_route('10.9.9.9/32', [('eth3', '10.1.13.1')])
        d = deltas[-1]
        paths = d.changed_paths()
        assert not any('resolver' in str(p) for p in paths)
        assert any(
            'resolver_input_epoch' in str(p) for p in d.changed_paths(bookkeeping=True)
        )
        assert not d.is_empty()
        assert d.interface_changes('R1') is d.interface_changes('R1')  # memoized


class TestPlacementCache:
    def test_rate_change_reuses_class(self):
        net, R = build_diamond(min_links=2)
        net.add_demand('d1', 'R1', '10.0.0.4', 100e6)
        net.converge()
        before = net.state.placement
        key = next(iter(before.classes))
        net.remove_demand('d1')
        net.add_demand('d1', 'R1', '10.0.0.4', 50e6)
        net.converge()
        after = net.state.placement
        assert after.classes[key] is before.classes[key]
        edge = net.links['R1:eth3--R3:eth1'].edge('R1')
        assert after.utilization(edge) == pytest.approx(before.utilization(edge) / 2)

    def test_topology_change_recomputes_class(self):
        net, R = build_diamond(min_links=2)
        net.add_demand('d1', 'R1', '10.0.0.4', 100e6)
        net.converge()
        before = net.state.placement
        key = next(iter(before.classes))
        net.links[LINK].fail()
        net.converge()
        after = net.state.placement
        assert after.classes[key] is not before.classes[key]
        assert after.deps['R1'].same(before.deps['R1']) is False
        assert after.deps['R3'].same(before.deps['R3'])

    def test_peer_admin_down_during_carrier_delay_recomputes_class(self):
        """The token pins the peer endpoint: RX_DOWN drops appear at once
        even though the local interface is still oper UP (debounced)."""
        net, R = build_diamond(min_links=1)
        R['R1'].interface('eth3').configure(carrier_delay_down=5)
        net.add_demand('d1', 'R1', '10.0.0.3', 100e6)
        env = netsim.Environment()
        sim = Simulation(env, net)
        before = net.state.placement
        key = next(iter(before.classes))
        sim.at(10, R['R3'].interface('eth1').admin_down)
        sim.run_until(11)
        after = net.state.placement
        assert after.classes[key] is not before.classes[key]
        assert net.state.devices['R1'].interfaces['eth3'].oper.oper == 1  # still UP
        assert dict(after.dropped_by_reason) == {'RX_DOWN': pytest.approx(100e6)}
        assert not after.deps['R1'].same(before.deps['R1'])

    def test_per_demand_edges_only_for_single_source_classes(self):
        net, R = build_diamond(min_links=2)
        net.add_demand('d1', 'R1', '10.0.0.4', 100e6)
        net.add_demand('d2', 'R3', '10.0.0.4', 10e6)
        net.converge()
        rep = net.state.placement
        assert rep.demands['d1'].edges == () and rep.demands['d2'].edges == ()
        net.remove_demand('d2')
        net.converge()
        rep = net.state.placement
        edges = dict(rep.demands['d1'].edges)
        assert edges[net.links['R1:eth3--R3:eth1'].edge('R1')] == pytest.approx(50e6)
        assert rep.demands['d1'].delivered == pytest.approx(100e6)


def test_ipv4_constant_smoke():
    assert IPV4 == 4


class TestReviewRegressions:
    """Defects found by the logging review, each pinned here."""

    def test_fib_event_when_only_group_weights_change(self):
        from netsim.model.addressing import to_int
        from netsim.model.routing import Nexthop

        net, R = build_diamond(min_links=2)
        env = netsim.Environment()
        sim = Simulation(env, net)
        h13, _ = to_int('10.1.13.1')
        h12, _ = to_int('10.1.12.1')
        sim.at(
            5,
            lambda: R['R1'].add_route(
                '10.0.0.4/32',
                [
                    Nexthop(interface='eth3', address=h13, af=IPV4, weight=3),
                    Nexthop(interface='Po1', address=h12, af=IPV4, weight=1),
                ],
            ),
        )
        sim.run_until(6)
        evs = [
            e
            for e in sim.timeline.at(5)
            if isinstance(e, FibEvent) and e.prefix == '10.0.0.4/32'
        ]
        assert [(e.action, e.nexthops) for e in evs] == [
            ('update', ('Po1 10.1.12.1', 'eth3 10.1.13.1 w3'))
        ]
        routes = [
            e
            for e in sim.timeline.at(5)
            if isinstance(e, RouteEvent) and e.prefix == '10.0.0.4/32'
        ]
        assert routes[0].action == 'update'
        assert routes[0].nexthops == ('eth3 10.1.13.1 w3', 'Po1 10.1.12.1')
        assert routes[0].distance == 1 and routes[0].source == 'static'

    def test_distinct_rows_have_distinct_route_events(self):
        net, R = build_diamond(min_links=2)
        env = netsim.Environment()
        sim = Simulation(env, net)
        sim.at(
            5,
            lambda: (
                R['R1'].add_route(
                    '10.9.9.9/32', [('eth3', '10.1.13.1')], distinguisher=('a',)
                ),
                R['R1'].add_route(
                    '10.9.9.9/32', [('Po1', '10.1.12.1')], distinguisher=('b',)
                ),
            ),
        )
        sim.run_until(6)
        rows = sim.timeline.rows(
            e for e in sim.timeline.at(5) if isinstance(e, RouteEvent)
        )
        assert len(rows) == 2 and rows[0] != rows[1]
        assert {r['distinguisher'] for r in rows} == {"('a',)", "('b',)"}

    def test_device_and_link_lifecycle_events(self):
        import dataclasses

        net, R = build_diamond(min_links=2)
        net.add_device('R9')  # standalone: nothing references it
        env = netsim.Environment()
        sim = Simulation(env, net)
        sim.at(5, lambda: net.links[LINK].configure(capacity=5e6, delay=0.001))
        sim.at(
            6,
            lambda: net.update(
                lambda s: dataclasses.replace(s, devices=s.devices.remove('R9')),
                origin=('remove_device', 'R9'),
            ),
        )
        sim.at(7, lambda: net.add_device('R8'))
        sim.run_until(8)
        cfg = [e for e in sim.timeline.at(5) if isinstance(e, LinkConfigEvent)]
        assert cfg and cfg[0].link == LINK
        assert dict((f, n) for f, _, n in cfg[0].changes) == {
            'capacity': '5000000.0',
            'delay': '0.001',
        }
        removed = [e for e in sim.timeline.at(6) if isinstance(e, DeviceEvent)]
        assert [(e.device, e.action) for e in removed] == [('R9', 'removed')]
        assert removed[0].origin == Origin('op', 'remove_device', 'R9')
        added = [e for e in sim.timeline.at(7) if isinstance(e, DeviceEvent)]
        assert [(e.device, e.action) for e in added] == [('R8', 'added')]
        assert ('enabled', '', 'True') in added[0].changes

    def test_config_events_carry_values(self):
        net, R = build_diamond(min_links=2)
        env = netsim.Environment()
        sim = Simulation(env, net)
        sim.at(5, lambda: R['R1'].interface('eth3').configure(mtu=1400, metric=7))
        sim.run_until(6)
        cfg = [e for e in sim.timeline.at(5) if isinstance(e, ConfigEvent)]
        assert cfg[0].interface == 'eth3' and cfg[0].action == 'changed'
        assert set(cfg[0].changes) == {('mtu', '1500', '1400'), ('metric', '1', '7')}
        dev = [e for e in sim.timeline.at(5) if isinstance(e, DeviceEvent)]
        assert dev == []

    def test_demand_events_carry_endpoints(self):
        net, R = build_diamond(min_links=2)
        env = netsim.Environment()
        sim = Simulation(env, net)
        sim.at(5, lambda: net.add_demand('d9', 'R1', '10.0.0.4', 5e6))
        sim.run_until(6)
        ev = [e for e in sim.timeline.at(5) if isinstance(e, DemandEvent)]
        assert ev[0].row() == {
            'event': 'DemandEvent',
            'seq': ev[0].seq,
            'idx': 0,
            'time': 5,
            'round': 0,
            'origin': str(ev[0].origin),
            'demand': 'd9',
            'action': 'add',
            'source': 'R1',
            'dst': '10.0.0.4',
            'af': 4,
            'mode': 'FLUID',
            'rate': 5e6,
        }

    def test_baseline_for_already_converged_network(self):
        net, R = build_diamond(min_links=2)
        net.add_demand('d1', 'R1', '10.0.0.4', 100e6)
        net.converge()
        env = netsim.Environment()
        sim = Simulation(env, net)
        tl = sim.timeline
        assert [r.origin for r in tl.records] == [Origin('init', 'baseline')]
        assert tl.delivered_series() == [(0, pytest.approx(100e6))]
        assert tl.records[0].version == net.state.version

    def test_series_after_demand_removal_and_for_new_edges(self):
        net, R = build_diamond(min_links=2)
        net.add_demand('d1', 'R1', '10.0.0.4', 100e6)
        net.add_demand('d2', 'R3', '10.0.0.4', 10e6)
        env = netsim.Environment()
        sim = Simulation(env, net)
        sim.at(5, lambda: net.remove_demand('d2'))
        sim.at(
            6,
            lambda: net.add_p2p(
                R['R1'],
                'eth9',
                R['R4'],
                'eth9',
                ipv4=('10.1.14.0/31', '10.1.14.1/31'),
                speed=100e6,
            ),
        )
        sim.run_until(7)
        tl = sim.timeline
        assert tl.delivered_series('d2') == [(0, pytest.approx(10e6)), (5, 0.0)]
        assert tl.delivered_series('d1')[-1] == (6, pytest.approx(100e6))
        new_edge = net.links['R1:eth9--R4:eth9'].edge('R1')
        assert [t for t, _ in tl.utilization_series(new_edge)] == [6]

    def test_settled_delivery_without_a_round(self):
        net, R = build_diamond(min_links=2)
        env = netsim.Environment()
        sim = Simulation(env, net)
        seen = []
        sim.bus.subscribe(lambda t, o, d, root: seen.append((t, o)))
        sim.at(5, lambda: net.links[LINK].configure(delay=0.25))  # derives nothing
        sim.run_until(6)
        assert sim.bus._queued == []
        assert seen and seen[0][0] == 5

    def test_event_records_are_slotted(self):
        net, R, sim = flap()
        e = sim.timeline.events[0]
        assert not hasattr(e, '__dict__')

    def test_kind_generation_is_the_round(self):
        net, R = build_diamond(min_links=2)
        env = netsim.Environment()
        sim = Simulation(env, net)

        # a settled delivery that writes opens a second round at the same time
        def react(t, o, d, root):
            if t == 5 and not any(r.round == 1 for r in sim.timeline.records):
                net.links['R3:eth2--R4:eth2'].fail()

        sim.bus.subscribe(react)
        sim.at(5, lambda: net.links[LINK].fail())
        sim.run_until(6)
        rounds = sorted({r.round for r in sim.timeline.records if r.time == 5})
        assert rounds == [0, 1]
        assert sim.timeline.stage_names(5).count('carrier') == 2


def test_event_and_record_budgets_bound_a_long_run():
    net, R = build_diamond(min_links=2)
    env = netsim.Environment()
    sim = Simulation(
        env, net, keep_events=200, keep_records=50, keep_roots=1, keep_deltas=0
    )
    link = net.links[LINK]
    for k in range(400):
        sim.at(1 + k, link.fail if k % 2 == 0 else link.restore)
    sim.run_until(500)
    tl = sim.timeline
    assert len(tl.events) <= 200 + max(64, 200 // 8)
    assert len(tl.records) <= 50 + 64
    assert tl.dropped_events > 0 and tl.dropped_records > 0
    assert tl.events[-1].time == tl.records[-1].time  # the newest survive
    assert tl.records[-1].seq == sim.timeline._seq
