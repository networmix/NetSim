"""NetGraph adapter tests on duck-typed objects (no ``ngraph`` needed), plus
a real-``ngraph`` test that is skipped when the extra is not installed."""

from dataclasses import dataclass, field

import pytest

from netsim.adapters import ngraph as adapter
from netsim.adapters.core import edge_arrays
from netsim.model import forwarding as fw


@dataclass
class Node:
    name: str
    disabled: bool = False
    risk_groups: set = field(default_factory=set)
    attrs: dict = field(default_factory=dict)


@dataclass
class Link:
    source: str
    target: str
    capacity: float = 1.0
    cost: float = 1.0
    disabled: bool = False
    risk_groups: set = field(default_factory=set)
    attrs: dict = field(default_factory=dict)
    id: str = ''


class StubNetwork:
    def __init__(self):
        self.nodes: dict[str, Node] = {}
        self.links: dict[str, Link] = {}

    def add_node(self, node):
        self.nodes[node.name] = node

    def add_link(self, link):
        n = sum(
            1
            for lk in self.links.values()
            if (lk.source, lk.target) == (link.source, link.target)
        )
        link.id = f'{link.source}|{link.target}|{n}'
        self.links[link.id] = link


@dataclass
class TrafficDemand:
    source: str
    target: str
    volume: float
    priority: int = 0
    mode: str = 'combine'


class DemandSet:
    def __init__(self, sets):
        self.sets = sets


class Scenario:
    def __init__(self, network, demand_set, seed=0):
        self.network = network
        self.demand_set = demand_set
        self.failure_policy_set = None
        self.seed = seed


def diamond_stub():
    g = StubNetwork()
    for n in ('R1', 'R2', 'R3', 'R4'):
        g.add_node(Node(n))
    g.add_link(
        Link('R1', 'R2', capacity=100, cost=1, attrs={'lag': 'Po1', 'min_links': 2})
    )
    g.add_link(
        Link('R1', 'R2', capacity=100, cost=1, attrs={'lag': 'Po1', 'min_links': 2})
    )
    g.add_link(Link('R1', 'R3', capacity=100, cost=1))
    g.add_link(Link('R2', 'R4', capacity=100, cost=1))
    g.add_link(Link('R3', 'R4', capacity=100, cost=1))
    return g


class TestFromNetwork:
    @pytest.mark.parametrize('addressing', ['unnumbered', 'p2p'])
    def test_import_routes_and_places(self, addressing):
        g = diamond_stub()
        net = adapter.from_network(g, addressing=addressing, capacity_unit=1e6)
        scenario = Scenario(
            g, DemandSet({'traffic': [TrafficDemand('^R1$', '^R4$', 100)]})
        )
        ids = adapter.demands_from(g, net, scenario.demand_set.sets, capacity_unit=1e6)
        assert ids == ['traffic:0:R1>R4']
        net.converge()
        rep = net.placement
        assert rep.delivered_total == pytest.approx(100e6)
        po = net['R1']['Po1']
        assert po.oper.bandwidth == 200e6 and po.config.min_links == 2
        e = net.links[net.ngraph_link_ids['R1|R3|0']].edge('R1')
        assert rep.carried[e] == pytest.approx(50e6 * 1034 / 1000)
        # Disabled link maps through the recorded id.
        assert set(net.ngraph_link_ids) == set(g.links)

    def test_disabled_and_failures(self):
        g = diamond_stub()
        g.links['R1|R3|0'].disabled = True
        net = adapter.from_network(g, capacity_unit=1e6)
        net.add_demand('d', 'R1', adapter._loopback_v4(net, 'R4'), 100e6)
        net.converge()
        assert net.links[net.ngraph_link_ids['R1|R3|0']].state == 0
        assert net.placement.delivered_total == pytest.approx(100e6)  # all via Po1
        schedule = adapter.FailureSchedule((adapter.FailureIteration(0, ('R2',), ()),))
        import netsim
        from netsim.runtime import Simulation

        env = netsim.Environment()
        sim = Simulation(env, net)
        schedule.apply(net, sim, start=10, dwell=2)
        sim.run_until(10)
        assert (
            net.placement.delivered_total == 0.0
        )  # R2 disabled and R1-R3 down: nothing reaches R4
        sim.run_until(12)
        assert net.placement.delivered_total == pytest.approx(100e6)

    def test_export_and_results(self):
        g = diamond_stub()
        net = adapter.from_network(g, capacity_unit=1e6)
        net.add_demand('d', 'R1', adapter._loopback_v4(net, 'R4'), 100e6)
        net.converge()
        out = adapter.to_network(
            net,
            net.placement,
            capacity_unit=1e6,
            node_cls=Node,
            link_cls=Link,
            network_cls=StubNetwork,
        )
        assert set(out.nodes) == {'R1', 'R2', 'R3', 'R4'} and len(out.links) == 5
        link = next(
            lk for lk in out.links.values() if {lk.source, lk.target} == {'R1', 'R3'}
        )
        assert link.capacity == pytest.approx(100) and link.attrs[
            'utilization'
        ] == pytest.approx(0.5 * 1.034)
        doc = adapter.results_json(net, net.placement)
        assert (
            doc['delivered'] == pytest.approx(100e6)
            and len(doc['edges']) == 10
            and doc['oversubscribed'] == []
        )
        names, src, dst, cap, cost, ext, mask = edge_arrays(net)
        assert (
            names == ['R1', 'R2', 'R3', 'R4']
            and len(src) == 10
            and all(mask)
            and sorted(ext) == list(range(10))
        )

    def test_selectors_and_modes(self):
        g = diamond_stub()
        net = adapter.from_network(g, capacity_unit=1e6)
        ids = adapter.demands_from(
            g,
            net,
            {'m': [TrafficDemand('^R[12]$', '^R[34]$', 40, mode='pairwise')]},
            capacity_unit=1e6,
        )
        assert len(ids) == 4 and all(
            net.state.demands[i].rate == pytest.approx(40e6) for i in ids
        )
        ids2 = adapter.demands_from(
            g,
            net,
            {'c': [TrafficDemand('^R[12]$', '^R[34]$', 40, mode='combine')]},
            capacity_unit=1e6,
        )
        assert all(net.state.demands[i].rate == pytest.approx(10e6) for i in ids2)
        with pytest.raises(ValueError):
            adapter.from_network(g, addressing='weird')


def test_real_ngraph_readme_topology():
    pytest.importorskip('ngraph', reason='netsim[ngraph] extra not installed')
    from ngraph import Link as NLink
    from ngraph import Network as NNetwork
    from ngraph import Node as NNode

    g = NNetwork()
    for n in ('A', 'B', 'C'):
        g.add_node(NNode(n))
    g.add_link(NLink('A', 'B', capacity=10.0, cost=1.0))
    g.add_link(NLink('B', 'C', capacity=10.0, cost=1.0))
    net = adapter.from_network(g, capacity_unit=1e9)
    net.add_demand('d', 'A', adapter._loopback_v4(net, 'C'), 5e9)
    net.converge()
    assert net.placement.delivered_total == pytest.approx(5e9)
    assert (
        net.placement.dropped_by_reason == {}
        or fw.LOOP not in net.placement.dropped_by_reason
    )
