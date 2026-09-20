"""NetGraph adapter tests on duck-typed objects (no ``ngraph`` needed), plus
a real-``ngraph`` test that is skipped when the extra is not installed."""

from dataclasses import dataclass, field

import pytest

from netsim.adapters import ngraph as adapter
from netsim.adapters.core import edge_arrays
from netsim.model import forwarding as fw
from netsim.model.addressing import to_int


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
        assert ids == ['traffic:0:R1>*']  # combine: one anycast destination
        assert net.netsim_demand_destinations[ids[0]] == '{R4}'
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
            net.state.demands[i].rate == pytest.approx(10e6) for i in ids
        )
        ids2 = adapter.demands_from(
            g,
            net,
            {'c': [TrafficDemand('^R[12]$', '^R[34]$', 40, mode='combine')]},
            capacity_unit=1e6,
        )
        # combine: an even share per source towards one anycast destination
        assert len(ids2) == 2 and all(
            net.state.demands[i].rate == pytest.approx(20e6) for i in ids2
        )
        anycast = {net.state.demands[i].dst for i in ids2}
        assert len(anycast) == 1
        for t in ('R3', 'R4'):
            addrs = {h for h, _ in net.device(t)['lo0'].node.config.ipv4}
            assert anycast <= addrs
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


@pytest.mark.parametrize('entry', ['from_network', 'from_scenario', 'demands_from'])
def test_adapter_bulk_construction_commits_once(entry, monkeypatch):
    from contextlib import contextmanager

    from netsim.model.network import Network
    from netsim.model.state import tree_equal

    graph = diamond_stub()
    sets = {'traffic': [TrafficDemand('^R1$', '^R[234]$', 100)]}
    scenario = Scenario(graph, DemandSet(sets))
    calls = []

    def factory(**kw):
        net = Network(**kw)
        net.on_delta.append(lambda *args: calls.append(args))
        return net

    monkeypatch.setattr(adapter, 'Network', factory)

    def build():
        if entry == 'from_scenario':
            return adapter.from_scenario(scenario)[0]
        net = adapter.from_network(graph)
        if entry == 'demands_from':
            calls.clear()
            adapter.demands_from(graph, net, sets)
        return net

    net = build()
    assert len(calls) == 1
    assert calls[0][1][0] == 'batch'
    assert len(calls[0][2].new.devices) == 4

    @contextmanager
    def unbatched(self):
        yield self

    monkeypatch.setattr(Network, 'batch', unbatched)
    expected = build()
    assert tree_equal(net.state, expected.state)
    net.converge()
    expected.converge()
    assert tree_equal(net.state, expected.state)


def test_failure_schedules_share_overlapping_leases():
    import netsim
    from netsim.runtime import Simulation

    net = adapter.from_network(diamond_stub())
    sim = Simulation(netsim.Environment(), net)
    lid = 'R1|R3|0'
    schedule = adapter.FailureSchedule((adapter.FailureIteration(0, (), (lid,)),))
    schedule.apply(net, sim, start=1, dwell=8)
    schedule.apply(net, sim, start=2, dwell=8)
    sim.run_until(5.5)
    assert net.link(net.ngraph_link_ids[lid]).state == 0
    sim.run_until(6)
    assert net.link(net.ngraph_link_ids[lid]).state == 1


def test_demand_volume_follows_netgraph_expansion_semantics():
    """``pairwise`` splits the volume over the pairs; ``combine`` is one
    aggregate of the same total. Both offer ``volume`` in total."""
    graph = diamond_stub()
    sets = {'t': [TrafficDemand('^R1$', '^R[234]$', 12.0, mode='pairwise')]}
    net = adapter.from_network(graph)
    ids = adapter.demands_from(graph, net, sets, capacity_unit=1.0)
    rates = [net.state.demands[i].rate for i in ids]
    assert len(rates) == 3 and all(r == pytest.approx(4.0) for r in rates)
    sets = {'t': [TrafficDemand('^R1$', '^R[234]$', 12.0, mode='combine')]}
    net = adapter.from_network(graph)
    ids = adapter.demands_from(graph, net, sets, capacity_unit=1.0)
    assert [net.state.demands[i].rate for i in ids] == [pytest.approx(12.0)]
    net.converge()
    assert net.placement.delivered_total == pytest.approx(12.0)


def test_combine_reaches_only_reachable_targets_like_netgraph():
    """A target cut off from the graph takes no share: the anycast prefix is
    reached at the nearest connected target, as NetGraph's pseudo sink does."""
    graph = diamond_stub()
    graph.nodes['R4'].disabled = True  # only R2 and R3 stay reachable from R1
    sets = {'t': [TrafficDemand('^R1$', '^R[234]$', 20.0, mode='combine')]}
    net = adapter.from_network(graph)
    ids = adapter.demands_from(graph, net, sets, capacity_unit=1.0)
    net.converge()
    assert net.placement.delivered_total == pytest.approx(20.0)
    assert net.placement.demands[ids[0]].drops == ()


def test_netgraph_priority_direction_is_translated():
    """NetGraph serves lower priority numbers first; NetSim higher ones."""
    from netsim.model.flows import LOSSY

    graph = diamond_stub()
    net = adapter.from_network(graph, capacity_unit=1e6)
    net.set_capacity_model(LOSSY)
    adapter.demands_from(
        graph,
        net,
        {
            'p': [
                TrafficDemand('^R1$', '^R4$', 150, mode='pairwise', priority=10),
                TrafficDemand('^R1$', '^R4$', 150, mode='pairwise', priority=0),
            ]
        },
        capacity_unit=1e6,
    )
    net.converge()
    urgent = net.placement.demands['p:1:R1>R4']
    later = net.placement.demands['p:0:R1>R4']
    assert urgent.delivered == pytest.approx(150e6)
    assert later.delivered < 150e6
    assert net.netsim_demand_priorities == {'p:0:R1>R4': 10, 'p:1:R1>R4': 0}
    assert net.state.demands['p:1:R1>R4'].priority == 0
    assert net.state.demands['p:0:R1>R4'].priority == -10


def test_real_ngraph_total_demand_matches_netgraph():
    pytest.importorskip('ngraph', reason='netsim[ngraph] extra not installed')
    import pathlib

    import yaml
    from ngraph.scenario import Scenario as NgScenario

    text = (
        pathlib.Path(__file__)
        .with_name('data')
        .joinpath('square_mesh.yaml')
        .read_text()
    )
    doc = yaml.safe_load(text)
    doc['workflow'] = [
        {
            'type': 'TrafficMatrixPlacement',
            'name': 'tm',
            'demand_set': 'baseline_traffic_matrix',
            'iterations': 0,
        }
    ]
    scenario = NgScenario.from_yaml(yaml.safe_dump(doc))
    scenario.run()
    total = scenario.results.to_dict()['steps']['tm']['data']['baseline']['summary'][
        'total_demand'
    ]
    net, ids, _ = adapter.from_scenario(scenario, capacity_unit=1.0)
    assert sum(net.state.demands[i].rate for i in ids) == pytest.approx(total)


def test_real_ngraph_combine_placed_volume_matches_netgraph():
    pytest.importorskip('ngraph', reason='netsim[ngraph] extra not installed')
    from ngraph.scenario import Scenario as NgScenario

    text = """
network:
  nodes:
    A: {}
    B: {}
    C: {}
  links:
    - source: A
      target: B
      capacity: 100
demands:
  d:
    - source: ^A$
      target: ^[BC]$
      mode: combine
      volume: 20
workflow:
  - type: TrafficMatrixPlacement
    name: tm
    demand_set: d
    iterations: 0
"""
    scenario = NgScenario.from_yaml(text)
    scenario.run()
    ng = scenario.results.to_dict()['steps']['tm']['data']['baseline']['summary']
    net, ids, _ = adapter.from_scenario(scenario, capacity_unit=1.0)
    net.converge()
    assert ng['total_placed'] == pytest.approx(20.0)
    assert net.placement.delivered_total == pytest.approx(ng['total_placed'])


class TestCombineAllocation:
    def test_anycast_skips_addresses_already_in_the_tree(self):
        graph = diamond_stub()
        net = adapter.from_network(graph)
        first = str(
            next(
                __import__('ipaddress').ip_network(adapter.DEFAULT_ANYCAST_POOL).hosts()
            )
        )
        net.device('R1')['lo0'].configure(
            ipv4=[f'{adapter._loopback_v4(net, "R1")}/32', f'{first}/32']
        )  # R1 already owns the pool's first address
        sets = {'t': [TrafficDemand('^R1$', '^R4$', 20.0, mode='combine')]}
        ids = adapter.demands_from(graph, net, sets, capacity_unit=1.0)
        dst = net.state.demands[ids[0]].dst
        assert dst != to_int(first)[0]
        net.converge()
        assert net.placement.demands[ids[0]].delivered == pytest.approx(20.0)
        e = net.links[net.ngraph_link_ids['R1|R3|0']].edge('R1')
        assert net.placement.offered[e] > 0  # traffic really leaves R1

    def test_fork_keeps_the_allocator_and_the_labels(self):
        graph = diamond_stub()
        net = adapter.from_network(graph)
        ids = adapter.demands_from(
            graph,
            net,
            {'a': [TrafficDemand('^R1$', '^R4$', 20.0, mode='combine', priority=3)]},
            capacity_unit=1.0,
        )
        fork = net.fork()
        ids2 = adapter.demands_from(
            graph,
            fork,
            {'b': [TrafficDemand('^R1$', '^R3$', 30.0, mode='combine')]},
            capacity_unit=1.0,
        )
        assert fork.state.demands[ids2[0]].dst != net.state.demands[ids[0]].dst
        assert fork.netsim_demand_priorities[ids[0]] == 3
        assert fork.netsim_demand_destinations[ids[0]] == '{R4}'
        assert ids[0] not in net.netsim_demand_destinations or 'b' not in str(
            net.netsim_anycast.keys()
        )
        fork.converge()
        assert fork.placement.demands[ids2[0]].delivered == pytest.approx(30.0)

    def test_full_overlap_yields_no_demand(self):
        graph = diamond_stub()
        net = adapter.from_network(graph)
        ids = adapter.demands_from(
            graph,
            net,
            {'o': [TrafficDemand('^R[12]$', '^R[12]$', 20.0, mode='combine')]},
            capacity_unit=1.0,
        )
        assert ids == [] and len(net.state.demands) == 0

    def test_metadata_rolls_back_when_an_import_aborts(self):
        graph = diamond_stub()
        net = adapter.from_network(graph)
        adapter.demands_from(
            graph,
            net,
            {'p': [TrafficDemand('^R1$', '^R4$', 20.0, mode='pairwise', priority=1)]},
            capacity_unit=1.0,
        )
        before = dict(net.netsim_demand_priorities)
        root = net.state
        with pytest.raises(ValueError):
            adapter.demands_from(
                graph,
                net,
                {
                    'p': [
                        TrafficDemand('^R1$', '^R4$', 20.0, mode='pairwise', priority=9)
                    ],
                    'q': [TrafficDemand('^R1$', '^R4$', -1.0, mode='pairwise')],
                },
                capacity_unit=1.0,
            )
        assert net.state is root
        assert net.netsim_demand_priorities == before

    def test_strict_rejects_untranslated_options(self):
        graph = diamond_stub()
        net = adapter.from_network(graph)

        class Rich(TrafficDemand):
            pass

        td = Rich('^R1$', '^R4$', 1.0, mode='pairwise')
        td.flow_policy = type('P', (), {'name': 'TE_WCMP_UNLIM', 'value': 3})()
        with pytest.raises(ValueError, match='flow_policy'):
            adapter.demands_from(graph, net, {'x': [td]}, capacity_unit=1.0)
        ids = adapter.demands_from(
            graph, net, {'x': [td]}, capacity_unit=1.0, strict=False
        )
        assert len(ids) == 1
        td2 = Rich('^R1$', '^R4$', 1.0, mode='pairwise')
        td2.static_paths = (('R1', 'R4'),)
        with pytest.raises(ValueError, match='static_paths'):
            adapter.demands_from(graph, net, {'y': [td2]}, capacity_unit=1.0)
        td3 = Rich('^R1$', '^R4$', 1.0, mode='pairwise')
        td3.group_mode = 'per_group'
        with pytest.raises(ValueError, match='group_mode'):
            adapter.demands_from(graph, net, {'z': [td3]}, capacity_unit=1.0)
