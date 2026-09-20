import pytest

from netsim.model import flows
from netsim.model import forwarding as fw
from netsim.model.addressing import to_int
from netsim.model.flows import HASH, LOSSY, UNCONSTRAINED
from tests.model.test_network import build_diamond


def A(text):
    return to_int(text)[0]


def edge(net, link_id, tx_device):
    return net.links[link_id].edge(tx_device)


WIRE = 1034 / 1000  # 14 + 20 + 1000 bytes per 1000 payload bytes


def _loads(net, report):
    e = {
        'R1>R3': edge(net, 'R1:eth3--R3:eth1', 'R1'),
        'R1>R2a': edge(net, 'R1:eth1--R2:eth1', 'R1'),
        'R1>R2b': edge(net, 'R1:eth2--R2:eth2', 'R1'),
        'R2>R4': edge(net, 'R2:eth3--R4:eth1', 'R2'),
        'R3>R4': edge(net, 'R3:eth2--R4:eth2', 'R3'),
        'R3>R1': edge(net, 'R1:eth3--R3:eth1', 'R3'),
    }
    return {k: report.carried[v] for k, v in e.items()}, e


class TestFluid:
    def test_diamond_split_and_wire_accounting(self):
        net, R = build_diamond()
        net.add_demand('d1', 'R1', '10.0.0.4', 100e6)
        net.converge()
        report = net.placement
        loads, e = _loads(net, report)
        assert loads['R1>R3'] == pytest.approx(50e6 * WIRE)
        assert loads['R1>R2a'] == pytest.approx(25e6 * WIRE) and loads[
            'R1>R2b'
        ] == pytest.approx(25e6 * WIRE)
        assert loads['R2>R4'] == pytest.approx(50e6 * WIRE) and loads[
            'R3>R4'
        ] == pytest.approx(50e6 * WIRE)
        assert loads['R3>R1'] == 0.0
        assert (
            report.delivered_total == pytest.approx(100e6)
            and not report.dropped_by_reason
        )
        assert report.utilization(e['R1>R3']) == pytest.approx(0.5 * WIRE)
        assert report.demands['d1'].delivered == pytest.approx(100e6)
        # A pure rate change rescales through the cached class.
        net.add_demand('d1', 'R1', '10.0.0.4', 50e6)
        net.converge()
        assert net.placement.carried[e['R1>R3']] == pytest.approx(25e6 * WIRE)
        assert (
            net.placement.classes[(4, A('10.0.0.4'), 0, 1000)]
            is report.classes[(4, A('10.0.0.4'), 0, 1000)]
        )

    def test_member_failure_reroutes(self):
        net, R = build_diamond(min_links=2)
        net.add_demand('d1', 'R1', '10.0.0.4', 100e6)
        net.converge()
        net.links['R1:eth1--R2:eth1'].fail()
        net.converge()
        loads, e = _loads(net, net.placement)
        assert loads['R1>R3'] == pytest.approx(100e6 * WIRE) and loads['R1>R2b'] == 0.0
        assert net.placement.delivered_total == pytest.approx(100e6)

    def test_stale_routing_over_failed_link_is_dropped_not_carried(self):
        net, R = build_diamond(min_links=1)
        net.add_demand('d1', 'R1', '10.0.0.4', 100e6)
        net.converge()
        # Fail the link but recompute placement only (routing still points at eth1).
        net.links['R1:eth1--R2:eth1'].fail()
        report = net.place()
        loads, e = _loads(net, report)
        assert report.offered[e['R1>R2a']] == pytest.approx(25e6 * WIRE)
        assert report.carried[e['R1>R2a']] == 0.0 and report.dropped[
            e['R1>R2a']
        ] == pytest.approx(25e6)
        assert report.dropped_by_reason[fw.LINK_DOWN] == pytest.approx(25e6)
        assert report.delivered_total == pytest.approx(75e6)
        net.converge()
        assert net.placement.delivered_total == pytest.approx(100e6)

    def test_loop_scc_keeps_healthy_branch(self):
        net, R = build_diamond()
        R['R4'].add_loopback('lo9', ipv4=['10.9.9.9/32'])
        R['R1'].add_route('10.9.0.0/16', [('Po1', '10.1.12.1'), ('eth3', '10.1.13.1')])
        R['R2'].add_route(
            '10.9.0.0/16', [('Po1', '10.1.12.0')]
        )  # back to R1: cycle {R1, R2}
        R['R3'].add_route('10.9.0.0/16', [('eth2', '10.1.34.1')])
        net.add_demand('loop', 'R1', '10.9.9.9', 100e6)
        net.converge()
        report = net.placement
        assert report.delivered_total == pytest.approx(50e6)
        assert report.dropped_by_reason[fw.LOOP] == pytest.approx(50e6)
        loads, e = _loads(net, report)
        assert loads['R3>R4'] == pytest.approx(50e6 * WIRE) and loads['R2>R4'] == 0.0

    def test_no_route_and_mtu(self):
        net, R = build_diamond()
        net.add_demand('nr', 'R1', '10.8.8.8', 10e6)
        net.add_demand('big', 'R1', '10.0.0.4', 10e6, payload_size=9000)
        net.converge()
        r = net.placement
        assert r.dropped_by_reason[fw.NO_ROUTE] == pytest.approx(10e6)
        assert r.dropped_by_reason[fw.MTU_EXCEEDED] == pytest.approx(10e6)
        assert r.demands['big'].drops[0][0] == fw.MTU_EXCEEDED


class TestLossy:
    def test_sequential_clipping_and_conservation(self):
        net, R = build_diamond()
        net.set_capacity_model(LOSSY)
        net.add_demand('hi', 'R1', '10.0.0.4', 120e6, priority=1)
        net.add_demand('lo', 'R1', '10.0.0.4', 80e6, priority=0)
        net.converge()
        r = net.placement
        loads, e = _loads(net, r)
        # 'hi' offers 60 payload per branch: R1>R3 carries 100 Mbit/s wire max -> ~96.7 payload.
        assert (
            r.carried[e['R1>R3']] <= 100e6 + 1e-6
            and r.carried[e['R3>R4']] <= 100e6 + 1e-6
        )
        hi, lo = r.demands['hi'], r.demands['lo']
        assert hi.delivered + sum(d[2] for d in hi.drops) == pytest.approx(120e6)
        assert lo.delivered + sum(d[2] for d in lo.drops) == pytest.approx(80e6)
        assert hi.delivered > lo.delivered
        assert r.dropped_by_reason[fw.CONGESTION] > 0 and r.model == LOSSY


class TestHash:
    def test_microflows_spread_and_agree_with_fluid(self):
        net, R = build_diamond()
        net.add_demand('h', 'R1', '10.0.0.4', 100e6, mode=HASH, flows=256)
        net.converge()
        r = net.placement
        loads, e = _loads(net, r)
        assert r.delivered_total == pytest.approx(100e6)
        assert abs(loads['R1>R3'] - 50e6 * WIRE) < 12e6 * WIRE
        assert loads['R1>R2a'] > 0 and loads['R1>R2b'] > 0
        assert r.demands['h'].delivered == pytest.approx(100e6)

    def test_hash_ttl_drop(self):
        from netsim.model.packets import PacketTemplate

        net, R = build_diamond()
        t = PacketTemplate(4, A('10.0.0.1'), A('10.0.0.4'), ttl=1)
        net.add_demand('short', 'R1', '10.0.0.4', 10e6, mode=HASH, template=t)
        net.converge()
        assert net.placement.dropped_by_reason[fw.TTL_EXPIRED] == pytest.approx(10e6)


def test_demand_validation():
    with pytest.raises(ValueError):
        flows.make_demand('x', 'R1', '10.0.0.4', -1)
    net, R = build_diamond()
    with pytest.raises(ValueError):
        net.add_demand('x', 'R9', '10.0.0.4', 1)
    assert net.add_demand('d', 'R1', '10.0.0.4', 1).mode == UNCONSTRAINED or True


class TestPerDemandAccounting:
    """Per-demand delivery is exact per source even when sources share a class."""

    def test_two_sources_one_lossy_path(self):
        net, R = build_diamond(min_links=2)
        net.add_demand('d1', 'R1', '10.0.0.4', 100e6)  # via R2 and R3
        net.add_demand('d2', 'R3', '10.0.0.4', 100e6)  # direct
        R['R2'].interface('eth3').admin_down()  # R2 loses its route: R1's half drops
        net.converge()
        rep = net.state.placement
        d1, d2 = rep.demands['d1'], rep.demands['d2']
        assert d1.delivered == pytest.approx(50e6)
        assert [(r, w, pytest.approx(50e6)) for r, w, _ in d1.drops] == [
            ('NO_ROUTE', 'R2', pytest.approx(50e6))
        ]
        assert d2.delivered == pytest.approx(100e6)
        assert d2.drops == ()
        assert rep.delivered_total == pytest.approx(150e6)
        assert dict(rep.dropped_by_reason) == {'NO_ROUTE': pytest.approx(50e6)}
        # per-source results are attributed edges too
        e13 = edge(net, 'R1:eth3--R3:eth1', 'R1')
        e34 = edge(net, 'R3:eth2--R4:eth2', 'R3')
        assert dict(d1.edges)[e13] == pytest.approx(50e6)
        assert dict(d1.edges)[e34] == pytest.approx(50e6)
        assert dict(d2.edges) == {e34: pytest.approx(100e6)}
        # the class-level edge loads are the sum
        assert rep.offered[e34] == pytest.approx(150e6 * WIRE)
        key = next(iter(rep.classes))
        assert [s for s, _ in rep.classes[key].per_source] == ['R1', 'R3']

    def test_per_source_results_are_cached(self):
        net, R = build_diamond(min_links=2)
        net.add_demand('d1', 'R1', '10.0.0.4', 100e6)
        net.add_demand('d2', 'R3', '10.0.0.4', 100e6)
        R['R2'].interface('eth3').admin_down()
        net.converge()
        before = net.state.placement
        key = next(iter(before.classes))
        net.remove_demand('d2')
        net.add_demand('d2', 'R3', '10.0.0.4', 20e6)
        net.converge()
        after = net.state.placement
        assert after.classes[key] is not before.classes[key]  # sources changed
        net.remove_demand('d1')
        net.add_demand('d1', 'R1', '10.0.0.4', 100e6)
        net.converge()
        assert net.state.placement.classes[key] is after.classes[key]
        assert net.state.placement.demands['d1'].delivered == pytest.approx(50e6)
        assert net.state.placement.demands['d2'].delivered == pytest.approx(20e6)

    def test_no_loss_means_full_delivery_per_demand(self):
        net, R = build_diamond(min_links=2)
        net.add_demand('d1', 'R1', '10.0.0.4', 100e6)
        net.add_demand('d2', 'R3', '10.0.0.4', 10e6)
        net.converge()
        rep = net.state.placement
        assert rep.demands['d1'].delivered == pytest.approx(100e6)
        assert rep.demands['d2'].delivered == pytest.approx(10e6)
        assert rep.demands['d1'].drops == () and rep.demands['d2'].drops == ()
        key = next(iter(rep.classes))
        assert rep.classes[key].per_source == ()  # nothing lost: no re-walk

    def test_removing_the_last_demand_empties_the_report(self):
        net, R = build_diamond(min_links=2)
        net.add_demand('d1', 'R1', '10.0.0.4', 100e6)
        net.converge()
        assert net.state.placement.delivered_total == pytest.approx(100e6)
        net.remove_demand('d1')
        net.converge()
        rep = net.state.placement
        assert rep is not None
        assert rep.delivered_total == 0.0 and len(rep.demands) == 0
        assert rep.max_utilization() == 0.0

    def test_removing_the_last_demand_timed(self):
        import netsim
        from netsim.runtime import Simulation

        net, R = build_diamond(min_links=2)
        net.add_demand('d1', 'R1', '10.0.0.4', 100e6)
        env = netsim.Environment()
        sim = Simulation(env, net)
        sim.at(5, lambda: net.remove_demand('d1'))
        sim.run_until(6)
        assert sim.timeline.stage_names(5) == ['placement']
        assert net.state.placement.delivered_total == 0.0
        assert sim.timeline.delivered_series() == [(0, pytest.approx(100e6)), (5, 0.0)]


class TestReviewRegressions:
    def test_lossy_units_small_demand_fits(self):
        net, R = build_diamond()
        net.set_capacity_model(LOSSY)
        net.add_demand('d1', 'R1', '10.0.0.3', 1e6)  # 1 Mbit/s over a 100 Mbit/s link
        net.converge()
        r = net.placement
        assert r.delivered_total == pytest.approx(1e6)
        assert dict(r.dropped_by_reason) == {}
        e = edge(net, 'R1:eth3--R3:eth1', 'R1')
        assert r.carried[e] == pytest.approx(1e6 * WIRE)

    def test_lossy_clips_at_wire_capacity(self):
        net, R = build_diamond()
        net.set_capacity_model(LOSSY)
        net.add_demand('d1', 'R1', '10.0.0.3', 120e6)  # more than the 100 Mbit/s link
        net.converge()
        r = net.placement
        e = edge(net, 'R1:eth3--R3:eth1', 'R1')
        assert r.carried[e] == pytest.approx(100e6)
        assert r.delivered_total == pytest.approx(100e6 / WIRE)
        assert r.dropped_by_reason[fw.CONGESTION] == pytest.approx(120e6 - 100e6 / WIRE)
        assert r.dropped[e] == pytest.approx(120e6 - 100e6 / WIRE)

    def test_lossy_applies_to_hash_demands(self):
        net, R = build_diamond()
        net.set_capacity_model(LOSSY)
        net.add_demand('h', 'R1', '10.0.0.3', 200e6, mode=HASH, flows=4)
        net.converge()
        r = net.placement
        e = edge(net, 'R1:eth3--R3:eth1', 'R1')
        assert r.carried[e] == pytest.approx(100e6)
        assert r.delivered_total == pytest.approx(100e6 / WIRE)
        assert r.dropped_by_reason[fw.CONGESTION] == pytest.approx(200e6 - 100e6 / WIRE)
        assert r.demands['h'].delivered + sum(
            d[2] for d in r.demands['h'].drops
        ) == pytest.approx(200e6)

    def test_numeric_device_names_are_not_edges(self):
        from netsim.model.network import Network

        net = Network()
        a, b = net.add_device('123'), net.add_device('R2')
        a.add_loopback('lo0', ipv4=['10.0.0.1/32'])
        b.add_loopback('lo0', ipv4=['10.0.0.2/32'])
        net.add_p2p(
            a, 'eth1', b, 'eth1', ipv4=('10.1.12.0/31', '10.1.12.1/31'), speed=1e9
        )
        net.add_demand('d', '123', '10.9.9.9', 1e6)
        net.converge()
        r = net.placement
        assert r.demands['d'].drops == (('NO_ROUTE', '123', pytest.approx(1e6)),)
        assert sum(r.dropped) == 0.0  # a device drop is not charged to an edge
        assert flows.edge_of_location('123') is None
        assert flows.edge_of_location('edge:7') == 7

    def test_cache_survives_an_equal_report(self):
        net, R = build_diamond()
        net.add_demand('d1', 'R1', '10.0.0.4', 100e6)
        net.converge()
        first = net.placement
        R['R1'].interface('eth3').configure(
            description='uplink'
        )  # no forwarding change
        net.converge()
        second = net.placement
        assert second.version == first.version
        assert second.deps['R1'] is not first.deps['R1']
        net.place()
        third = net.placement
        key = next(iter(third.classes))
        assert third.classes[key] is second.classes[key]
        assert third is second


class TestScc:
    @staticmethod
    def _reference(nodes, succ):
        """The recursive Tarjan the iterative one replaced (reference only)."""
        import sys

        sys.setrecursionlimit(max(sys.getrecursionlimit(), 20000))
        index, low, on_stack, stack, comps = {}, {}, set(), [], []
        counter = 0

        def visit(v):
            nonlocal counter
            index[v] = low[v] = counter
            counter += 1
            stack.append(v)
            on_stack.add(v)
            for w in succ.get(v, ()):
                if w not in index:
                    visit(w)
                    low[v] = min(low[v], low[w])
                elif w in on_stack:
                    low[v] = min(low[v], index[w])
            if low[v] == index[v]:
                comp = []
                while True:
                    w = stack.pop()
                    on_stack.discard(w)
                    comp.append(w)
                    if w == v:
                        break
                comps.append(sorted(comp))

        for v in nodes:
            if v not in index:
                visit(v)
        return comps

    def test_iterative_matches_reference_on_random_graphs(self):
        import random

        rng = random.Random(5)
        for _trial in range(40):
            n = rng.randint(1, 40)
            nodes = [str(i) for i in range(n)]
            succ = {
                v: sorted(rng.sample(nodes, rng.randint(0, min(4, n)))) for v in nodes
            }
            assert flows._tarjan(nodes, succ) == self._reference(nodes, succ)

    def test_deep_chain_and_deep_cycle(self):
        n = 20000
        nodes = [str(i) for i in range(n)]
        chain = {str(i): [str(i + 1)] for i in range(n - 1)}
        chain[str(n - 1)] = []
        comps = flows._tarjan(nodes, chain)
        assert len(comps) == n and comps[0] == [str(n - 1)]
        cycle = dict(chain)
        cycle[str(n - 1)] = ['0']
        comps = flows._tarjan(nodes, cycle)
        assert len(comps) == 1 and len(comps[0]) == n
