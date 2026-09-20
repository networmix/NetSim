"""Optional integration checks against NetGraph's actual policy/result contracts."""

import pytest

pytest.importorskip('ngraph', reason='optional NetGraph integration')

from ngraph.analysis.failure_manager import FailureManager
from ngraph.scenario import Scenario

from netsim.adapters.ngraph import failure_schedule


def scenario_doc():
    # The square_mesh.yaml topology/policy, with a bounded serial workflow.
    return {
        'seed': 42,
        'network': {
            'nodes': {f'N{i}': {} for i in range(1, 5)},
            'links': [
                {'source': f'N{i}', 'target': f'N{j}', 'capacity': 2.0}
                for i in range(1, 5)
                for j in range(i + 1, 5)
            ],
        },
        'failures': {
            'single_link_failure': {
                'modes': [
                    {
                        'weight': 1.0,
                        'rules': [{'scope': 'link', 'mode': 'choice', 'count': 1}],
                    }
                ]
            }
        },
        'demands': {
            'traffic': [
                {
                    'source': '^N[1-4]$',
                    'target': '^N[1-4]$',
                    'volume': 0.1,
                    'mode': 'pairwise',
                }
            ]
        },
        'workflow': [],
    }


def scenario_from(doc):
    import yaml

    return Scenario.from_yaml(yaml.safe_dump(doc))


def test_existing_schedule_matches_policy_seed_fallback():
    scenario = scenario_from(scenario_doc())
    policy = scenario.failure_policy_set.get_policy('single_link_failure')
    manager = FailureManager(scenario.network, scenario.failure_policy_set)
    expected = [
        manager.compute_exclusions(policy, seed_offset=policy.seed + i)
        for i in range(20)
    ]
    actual = failure_schedule(
        scenario.network,
        scenario.failure_policy_set,
        policy='single_link_failure',
        iterations=20,
    )
    assert [
        (set(x.excluded_nodes), set(x.excluded_links)) for x in actual.iterations
    ] == expected


def square_doc():
    from pathlib import Path

    import yaml

    return yaml.safe_load(
        (Path(__file__).parent / 'data' / 'square_mesh.yaml').read_text()
    )


def test_draws_match_actual_square_mesh_monte_carlo_and_replay(tmp_path):
    import json
    from collections import Counter

    from ngraph.results.flow import FlowEntry, FlowIterationResult, FlowSummary

    from netsim import Environment
    from netsim.runtime import Draws, Schedule, Simulation
    from netsim.study import Study

    doc = square_doc()
    step = next(x for x in doc['workflow'] if x['type'] == 'TrafficMatrixPlacement')
    step.update(iterations=50, parallelism=1, seed=42)
    step.pop('alpha_from_step')
    step.pop('alpha_from_field')
    doc['workflow'] = [step]
    scenario = scenario_from(doc)
    scenario.run()
    exported = scenario.results.to_dict()
    expected = exported['steps']['tm_placement']['data']['flow_results']
    draws = Draws.from_policy(
        scenario.network,
        scenario.failure_policy_set,
        policy=step['failure_policy'],
        iterations=50,
        seed=42,
    )
    assert Counter(d.failure_id for d in draws) == {
        row['failure_id']: row['occurrence_count'] for row in expected
    }
    study = Study.from_scenario(
        scenario, demand_set=step['demand_set'], capacity_unit=1e9
    )
    result = study.iterations(draws)
    assert [row['failure_id'] for row in result.flow_results] == [
        row['failure_id'] for row in expected
    ]
    assert [row['occurrence_count'] for row in result.flow_results] == [
        row['occurrence_count'] for row in expected
    ]
    for row in [result.baseline, *result.flow_results]:
        assert set(row) == set(FlowIterationResult().to_dict())
        assert all(
            set(flow) == set(FlowEntry('a', 'b', 0, 1, 1, 0).to_dict())
            for flow in row['flows']
        )
        container = FlowIterationResult(
            **{
                **row,
                'flows': [FlowEntry(**flow) for flow in row['flows']],
                'summary': FlowSummary(**row['summary']),
            }
        )
        json.dumps(container.to_dict(), allow_nan=False)
    # Preserve the existing adapter's per-pair volume semantics. NetGraph
    # aggregates this selector demand; T6 does not alter placement semantics.
    # pairwise volume is split over the expanded pairs, as NetGraph does
    assert result.baseline['summary']['total_demand'] == 12
    assert expected[0]['summary']['total_demand'] == 12
    assert {row['destination'] for row in result.rows()} == set(scenario.network.nodes)
    path = tmp_path / 'results.json'
    path.write_text(json.dumps(exported))
    selected = expected[0]['failure_id']
    replay = study.replay(path, 'tm_placement', selected)
    assert len(replay.flow_results) == 1
    assert replay.flow_results[0]['failure_state'] == expected[0]['failure_state']
    assert replay.flow_results[0]['occurrence_count'] == expected[0]['occurrence_count']
    sim = Simulation(Environment(), study.network.fork())
    sim.failures(Schedule.replay(path, 'tm_placement', selected))
    sim.run_until(1)
    for lid in expected[0]['failure_state']['excluded_links']:
        assert not sim.network.link(sim.network.ngraph_link_ids[lid]).state


def test_seed_formula_and_policy_process_reproducibility():
    from ngraph.utils.seed_manager import SeedManager

    from netsim.runtime import Process
    from netsim.runtime.failures import derive_seed

    assert derive_seed(42, 'link', 'x') == SeedManager(42).derive_seed(
        'netsim', 'link', 'x'
    )
    scenario = scenario_from(scenario_doc())
    kwargs = {
        'policy': 'single_link_failure',
        'rate': 2,
        'duration': {'mean': 2, 'kind': 'weibull', 'shape': 2},
    }
    p = Process.from_policy(
        scenario.network, scenario.failure_policy_set, seed=42, **kwargs
    )
    assert p.events(20) == p.events(20)
    q = Process.from_policy(
        scenario.network, scenario.failure_policy_set, seed=43, **kwargs
    )
    assert p.events(20) != q.events(20)
    manager = FailureManager(scenario.network, scenario.failure_policy_set)
    policy = scenario.failure_policy_set.get_policy('single_link_failure')
    base = derive_seed(42, 'policy', 'single_link_failure')
    for i, event in enumerate(p.events(20)):
        nodes, links = manager.compute_exclusions(policy, seed_offset=base + i)
        assert event.entities == tuple(
            [('device', n) for n in sorted(nodes)]
            + [('link', n) for n in sorted(links)]
        )
    zero = Process.from_policy(
        scenario.network, scenario.failure_policy_set, seed=42, **{**kwargs, 'rate': 0}
    )
    assert zero.events(100) == ()


def test_workflow_iterations_process_and_replay_inside_scenario():
    doc = scenario_doc()
    doc['network']['nodes']['N1']['attrs'] = {'netsim': {'mtbf': 2, 'mttr': 1}}
    doc['workflow'] = [
        {
            'type': 'NetSimStudy',
            'name': 'ns',
            'demand_set': 'traffic',
            'failure_policy': 'single_link_failure',
            'iterations': 12,
            'seed': 42,
        },
        {
            'type': 'NetSimStudy',
            'name': 'process',
            'mode': 'process',
            'failure_policy': 'single_link_failure',
            'horizon': 4,
            'rate': 2,
            'duration': {'mean': 1, 'kind': 'exponential'},
            'seed': 3,
        },
        {
            'type': 'NetSimStudy',
            'name': 'renewal',
            'mode': 'process',
            'horizon': 10,
            'keep': {'timeline': True},
            'seed': 4,
        },
        {'type': 'NetSimStudy', 'name': 'replay', 'mode': 'replay', 'step': 'ns'},
    ]
    scenario = scenario_from(doc)
    scenario.run()
    steps = scenario.results.to_dict()['steps']
    assert set(steps) == {'ns', 'process', 'renewal', 'replay'}
    assert steps['ns']['metadata']['iterations'] == 12
    assert (
        steps['ns']['data']['flow_results'] == steps['replay']['data']['flow_results']
    )
    assert steps['process']['data']['netsim']['fault_events'] > 0
    assert steps['renewal']['data']['netsim']['fault_events'] > 0
    assert steps['renewal']['data']['netsim']['timeline']


def test_no_policy_and_bad_workflow_mode():
    from netsim.runtime import Draws

    doc = scenario_doc()
    scenario = scenario_from(doc)
    assert list(Draws.from_policy(scenario.network, scenario.failure_policy_set)) == []
    with pytest.raises(ValueError, match='iterations'):
        Draws.from_policy(scenario.network, scenario.failure_policy_set, iterations=-1)
    doc['workflow'] = [{'type': 'NetSimStudy', 'mode': 'bad'}]
    with pytest.raises(ValueError, match='mode'):
        scenario_from(doc).run()


def test_attrs_netsim_timing_and_nested_risk_groups():
    from ngraph import Link, Network, Node
    from ngraph.model.network import RiskGroup

    from netsim.adapters.ngraph import from_network
    from netsim.runtime import Draws, Process
    from netsim.study import Study

    graph = Network()
    graph.add_node(
        Node(
            'a',
            risk_groups={'inner'},
            attrs={
                'netsim': {
                    'mtbf': 10,
                    'mttr': 2,
                    'fib_delay': 0.02,
                    'fast_failover': True,
                    'loopback_ipv4': '10.3.0.1/32',
                }
            },
        )
    )
    graph.add_node(Node('b'))
    graph.add_link(
        Link(
            'a',
            'b',
            risk_groups={'inner'},
            attrs={
                'netsim': {
                    'mtbf': 10,
                    'mttr': 2,
                    'carrier_delay_down': 0.1,
                    'source_interface': 'port0',
                    'target_interface': 'port1',
                }
            },
        )
    )
    graph.risk_groups['outer'] = RiskGroup('outer', children=[RiskGroup('inner')])
    net = from_network(graph)
    lid = next(iter(graph.links))
    assert net.device('a').node.config.fib_delay == 0.02
    assert net.device('a').node.config.fast_failover
    assert net.device('a')['port0'].config.carrier_delay_down == 0.1
    assert net.netsim_risk_groups['outer'] == (('device', 'a'), ('link', lid))
    assert list(Draws.enumerate(net, 'risk_groups'))[0].excluded_links == (lid,)
    source = Process.from_network(net)
    assert set(source.parameters) == {('device', 'a'), ('link', lid)}
    study = Study(net)
    assert len(study.enumerate('risk_groups').flow_results) == 1


def test_cli_square_mesh_console_entry(tmp_path):
    import json
    import subprocess
    import sys
    from pathlib import Path

    import yaml

    doc = square_doc()
    doc['workflow'] = [
        {
            'type': 'NetSimStudy',
            'name': 'ns',
            'demand_set': 'baseline_traffic_matrix',
            'failure_policy': 'single_link_failure',
            'iterations': 6,
            'seed': 42,
        }
    ]
    source = tmp_path / 'square_mesh.yaml'
    source.write_text(yaml.safe_dump(doc))
    output = tmp_path / 'results.json'
    root = Path(__file__).resolve().parents[2]
    # Exercise the actual installed console script under NetGraph's interpreter.
    script = root / 'venv-ft' / 'bin' / 'netsim'
    if not script.exists():
        script = root / 'venv' / 'bin' / 'netsim'
    if script.exists():
        command = [sys.executable, str(script)]
    else:
        command = [sys.executable, '-m', 'netsim']
    run = subprocess.run(
        [*command, 'run', str(source), '--results', str(output)],
        capture_output=True,
        text=True,
    )
    assert run.returncode == 0, run.stderr
    data = json.loads(output.read_text())
    assert data['steps']['ns']['metadata']['iterations'] == 6
    assert (
        sum(
            row['occurrence_count']
            for row in data['steps']['ns']['data']['flow_results']
        )
        == 6
    )
