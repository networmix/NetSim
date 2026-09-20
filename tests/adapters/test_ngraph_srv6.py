"""Structural SR imports; forwarding assertions await the policy slice."""

from types import SimpleNamespace as NS

import pytest

from netsim.adapters.ngraph import demands_from, from_scenario
from netsim.model import srv6 as sr
from tests.adapters.test_ngraph import (
    DemandSet,
    Link,
    Scenario,
    TrafficDemand,
    diamond_stub,
)


def scenario(path=None, **options):
    td = TrafficDemand('^R1$', '^R4$', 100, priority=2, mode='pairwise')
    td.static_paths = (NS(nodes=tuple(path or ('R1', 'R2', 'R4')), links=()),)
    for key, value in options.items():
        setattr(td, key, value)
    return Scenario(diamond_stub(), DemandSet({'traffic': [td]}), seed=7)


def test_explicit_path_policy_and_steering():
    net, ids, _ = from_scenario(scenario(), srv6=True, capacity_unit=1e6)
    demand = net.state.demands[ids[0]]
    assert isinstance(demand.steer, sr.PolicyRef)
    policy = net['R1'].node.srv6_policies.policies[
        (demand.steer.color, demand.steer.endpoint)
    ]
    assert policy.fallback == sr.FALLBACK_DROP
    assert policy.candidate_paths[0].segment_lists[0].segments == (
        sr.AdjSeg('R1', 'Po1'),
        sr.AdjSeg('R2', 'eth0'),
        sr.TermSeg('R4'),
    )
    assert len(policy.candidate_paths) == 1
    assert demand.rate == 100e6 and demand.priority == -2
    for dev in net.state.devices.values():
        assert dev.srv6_sids.locators['ngraph'].structure == sr.F3216_GIB
        assert any(s.behavior == sr.END_DT46 for s in dev.srv6_sids.sids.values())
    assert net.validate() == []


@pytest.mark.skip(reason='needs G5 policy derivation')
def test_explicit_policy_delivery_and_failure_drop():
    net, _, _ = from_scenario(scenario(), srv6=True, capacity_unit=1e6)
    net.converge()
    assert net.placement.delivered_total == pytest.approx(100e6)
    net['R2'].disable()
    net.converge()
    assert net.placement.delivered_total == 0
    assert dict(net.placement.dropped_by_reason.items()) == {'POLICY_DOWN': 100e6}


@pytest.mark.parametrize(
    'policy', ['TE_WCMP_UNLIM', 'TE_ECMP_UP_TO_256_LSP', 'TE_ECMP_16_LSP', 3, 4, 5]
)
def test_te_preset_with_pin(policy):
    net, ids, _ = from_scenario(scenario(flow_policy=policy), srv6=True)
    assert net.state.demands[ids[0]].steer is not None


def test_seeded_gibs_and_sid_allocation_are_repeatable():
    first, _, _ = from_scenario(scenario(), srv6=True)
    second, _, _ = from_scenario(scenario(), srv6=True)
    for name in first.state.devices:
        assert first[name].node.srv6_sids == second[name].node.srv6_sids
    ids = [
        first[name].node.srv6_sids.locators['ngraph'].node_id
        for name in sorted(first.state.devices)
    ]
    assert ids == [3, 2, 4, 1]  # Random(7) permutation of sorted device names.
    a = first['R1'].node.srv6_sids
    assert sum(s.interface == 'Po1' for s in a.sids.values()) == 1
    assert not any(s.interface in ('eth1', 'eth2') for s in a.sids.values())


def test_link_form_reverse_and_parallel_node_selection():
    sc = scenario()
    sc.network.add_link(Link('R1', 'R3', cost=9))
    td = sc.demand_set.sets['traffic'][0]
    td.source, td.target = '^R4$', '^R1$'
    td.static_paths = (NS(nodes=(), links=('R3|R4|0', 'R1|R3|1')),)
    net, ids, _ = from_scenario(sc, srv6=True)
    policy = next(iter(net['R4'].node.srv6_policies.policies.values()))
    segments = policy.candidate_paths[0].segment_lists[0].segments
    assert segments[-1] == sr.TermSeg('R1')
    assert segments[1].interface == net.link(net.ngraph_link_ids['R1|R3|1']).node.b[1]
    assert net.state.demands[ids[0]].source == 'R4'
    td.source, td.target = '^R1$', '^R4$'
    td.static_paths = (NS(nodes=('R1', 'R3', 'R4'), links=()),)
    net, _, _ = from_scenario(sc, srv6=True)
    policy = next(iter(net['R1'].node.srv6_policies.policies.values()))
    assert (
        policy.candidate_paths[0].segment_lists[0].segments[0].interface
        == net.link(net.ngraph_link_ids['R1|R3|0']).node.a[1]
    )


@pytest.mark.parametrize(
    'path,match',
    [
        (NS(nodes=('R1', 'R4'), links=()), 'no enabled link'),
        (NS(nodes=('R2', 'R4'), links=()), 'start at source'),
        (NS(nodes=('R1', 'R3', 'R1', 'R2', 'R4'), links=()), 'simple'),
        (NS(nodes=(), links=('missing',)), 'unknown link'),
        (NS(nodes=(), links=('R3|R4|0',)), 'does not leave'),
        (NS(nodes=(), links=('R1|R3|0',)), 'end at target'),
        (NS(nodes=(), links=('R1|R2|0', 'R2|R4|0')), 'member-link pin'),
        ({'nodes': ['R1', 'R3', 'R4'], 'extra': True}, 'unsupported path fields'),
        (NS(nodes=(), links=()), 'exactly one'),
    ],
)
def test_reject_invalid_static_paths(path, match):
    sc = scenario(static_paths=(path,))
    with pytest.raises(ValueError, match=match):
        from_scenario(sc, srv6=True)


@pytest.mark.parametrize(
    'options,match',
    [
        ({'flow_policy': 'TE_UNKNOWN'}, 'flow_policy'),
        ({'flow_policy': 'TE_WCMP_UNLIM', 'static_paths': ()}, 'flow_policy'),
        ({'group_mode': 'per_group'}, 'group_mode'),
        ({'mode': 'unknown'}, 'demand mode'),
        ({'source': '^R'}, 'one distinct'),
        (
            {
                'static_paths': (
                    NS(nodes=('R1', 'R3', 'R4')),
                    NS(nodes=('R1', 'R2', 'R4')),
                )
            },
            'one pinned route',
        ),
    ],
)
def test_reject_unsupported_te(options, match):
    with pytest.raises(ValueError, match=match):
        from_scenario(scenario(**options), srv6=True)


def test_static_path_requires_srv6_and_ipv6():
    with pytest.raises(ValueError, match='static_paths require'):
        from_scenario(scenario())
    with pytest.raises(ValueError, match='IPv6 underlay'):
        from_scenario(scenario(), srv6=True, ipv6=False)


def test_bad_followup_import_rolls_back_policies_and_metadata():
    net, _, _ = from_scenario(scenario(), srv6=True)
    root, metadata = net.state, dict(net.netsim_demand_destinations)
    sc = scenario()
    good = sc.demand_set.sets['traffic'][0]
    bad = scenario(path=('R1', 'R4')).demand_set.sets['traffic'][0]
    with pytest.raises(ValueError, match='no enabled link'):
        demands_from(sc.network, net, {'new': [good, bad]})
    assert net.state is root
    assert net.netsim_demand_destinations == metadata


def real_pinned():
    ng = pytest.importorskip('ngraph')
    from ngraph.model.demand.spec import StaticPath, TrafficDemand
    from ngraph.model.flow.policy_config import FlowPolicyPreset

    graph = ng.Network()
    for name in ('A', 'B', 'C', 'D'):
        graph.add_node(ng.Node(name))
    for a, b, cost in (('A', 'B', 1), ('B', 'D', 1), ('A', 'C', 9), ('C', 'D', 9)):
        graph.add_link(ng.Link(a, b, capacity=1000, cost=cost))
    td = TrafficDemand(
        '^A$',
        '^D$',
        10,
        mode='pairwise',
        priority=3,
        flow_policy=FlowPolicyPreset.TE_WCMP_UNLIM,
        static_paths=(StaticPath(nodes=('A', 'C', 'D')),),
    )
    return graph, td


def core_pinned(graph, td, exclusions=()):
    import netgraph_core
    from ngraph.analysis import analyze
    from ngraph.analysis.demand import expand_demands
    from ngraph.analysis.placement import place_demands

    expansion = expand_demands(graph, [td])
    ctx = analyze(graph)
    return place_demands(
        expansion.demands,
        [td.volume],
        netgraph_core.FlowGraph(ctx.multidigraph),
        ctx,
        ctx.build_node_mask(None),
        ctx.build_edge_mask(set(exclusions)),
        collect_entries=True,
        include_used_edges=True,
    )


def test_real_netgraph_core_pin_matches_translated_interfaces():
    graph, td = real_pinned()
    expected = core_pinned(graph, td)
    assert expected.summary.total_placed == 10
    net, ids, _ = from_scenario(
        NS(network=graph, demand_set=NS(sets={'pinned': [td]}), seed=7), srv6=True
    )
    policy = next(iter(net['A'].node.srv6_policies.policies.values()))
    pins = policy.candidate_paths[0].segment_lists[0].segments[:-1]
    selected = set()
    for pin in pins:
        for lid, imported in net.ngraph_link_ids.items():
            link = net.link(imported).node
            if link.a == (pin.device, pin.interface):
                selected.add(lid + ':fwd')
            elif link.b == (pin.device, pin.interface):
                selected.add(lid + ':rev')
    assert selected == expected.entries[0].used_edges == {'A|C|0:fwd', 'C|D|0:fwd'}
    assert net.state.demands[ids[0]].priority == -3
    assert core_pinned(graph, td, {'C|D|0'}).summary.total_placed == 0


@pytest.mark.skip(reason='needs G5 policy derivation')
def test_real_netgraph_core_drop_equals_policy_delivery():
    graph, td = real_pinned()
    net, _, _ = from_scenario(
        NS(network=graph, demand_set=NS(sets={'pinned': [td]}), seed=7), srv6=True
    )
    net.converge()
    assert net.placement.delivered_total / 1e9 == pytest.approx(
        core_pinned(graph, td).summary.total_placed
    )
    net.link(net.ngraph_link_ids['C|D|0']).fail()
    net.converge()
    assert (
        net.placement.delivered_total / 1e9
        == core_pinned(graph, td, {'C|D|0'}).summary.total_placed
        == 0
    )


def test_pins_require_pairwise_enabled_endpoints_and_enabled_links():
    with pytest.raises(ValueError, match='pairwise'):
        from_scenario(scenario(mode='combine'), srv6=True)
    sc = scenario()
    sc.network.nodes['R1'].disabled = True
    with pytest.raises(ValueError, match='must be enabled'):
        from_scenario(sc, srv6=True)
    sc = scenario(static_paths=(NS(nodes=(), links=('R1|R3|0', 'R3|R4|0')),))
    sc.network.links['R1|R3|0'].disabled = True
    with pytest.raises(ValueError, match='disabled link'):
        from_scenario(sc, srv6=True)
