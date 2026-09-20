import json
import subprocess
import sys

import pytest

from netsim.runtime import Draws, FailureSet, Process, Schedule
from netsim.study import Study
from tests.model.test_network import build_diamond


def diamond(**kwargs):
    net, devices = build_diamond(**kwargs)
    net.add_demand('d', 'R1', '10.0.0.4', 100e6)
    return net, devices


def test_iterations_equal_direct_convergence_and_preserve_baseline():
    net, _ = diamond()
    study = Study(net)
    root = net.state
    draws = list(Draws.enumerate(net)) + list(Draws.enumerate(net, 'devices'))
    result = study.iterations(Draws(draws + draws))
    assert net.state is root
    assert len(result.flow_results) == 9
    for draw, record in zip(draws, result.flow_results, strict=False):
        fork = study.network.fork()
        for name in draw.excluded_nodes:
            fork.device(name).configure(enabled=False)
        for name in draw.excluded_links:
            fork.link(name).fail()
        fork.converge()
        assert record['summary']['total_placed'] == fork.placement.delivered_total
        assert record['failure_id'] == draw.failure_id
        assert record['occurrence_count'] == 2
    assert result.metadata['iterations'] == 18
    assert result.baseline['summary']['total_placed'] == 100e6
    assert all(r['occurrence_count'] == 2 for r in result.rows())
    exported = result.to_ngraph()
    json.dumps(exported, allow_nan=False)
    exported['data']['flow_results'].clear()
    assert len(result.flow_results) == 9


def test_exact_transient_loss_fib_delay_and_short_settle():
    net, _ = diamond(fib_delay=0.05)
    result = Study(net).iterations(
        [FailureSet(excluded_links=('R1:eth1--R2:eth1',))], settle=0.01
    )
    row = result.flow_results[0]
    extras = row['data']['netsim']
    assert row['summary']['total_placed'] == 100e6
    assert extras['settle_time'] == pytest.approx(0.05)
    assert extras['bits_lost_transient'] == pytest.approx(50e6 * 0.05)
    assert extras['per_demand']['d']['downtime'] == pytest.approx(0.05)
    assert extras['loss_integral'] == pytest.approx(50e6 * 0.05)
    assert extras['event_counts']['LinkStateEvent'] == 2
    assert extras['observed_drop_reasons']


def test_process_integral_availability_and_concurrent_union():
    net, _ = diamond()
    events = Schedule(
        [
            ((('device', 'R1'),), 1, 3),
            ((('device', 'R1'),), 2, 1),
            ((('device', 'R4'),), 3, 2),
        ]
    )
    result = Study(net).process(events, 6)
    metrics = result.netsim
    # The source fails [1,4); destination fails [3,5): union [1,5).
    assert metrics['per_demand']['d']['downtime'] == 4
    assert metrics['per_demand']['d']['unavailability'] == pytest.approx(4 / 6)
    assert metrics['loss_integral'] == pytest.approx(400e6)
    assert metrics['concurrent_failure_histogram'] == {'0': 2, '1': 3, '2': 1}
    assert result.flow_results[0]['summary']['total_placed'] == 100e6
    assert metrics['fault_events'] == 3
    json.dumps(result.to_ngraph(), allow_nan=False)


def test_process_horizon_does_not_run_future_repairs():
    net, _ = diamond()
    result = Study(net).process(
        Schedule([((('device', 'R1'),), 1, 100), ((('device', 'R4'),), 4, 1)]), 3
    )
    assert result.netsim['fault_events'] == 1
    assert result.netsim['per_demand']['d']['downtime'] == 2
    assert result.flow_results[0]['failure_state']['excluded_nodes'] == ['R1']
    assert result.flow_results[0]['summary']['total_placed'] == 0


def test_enumerate_replay_retention_and_empty_cases(tmp_path):
    net, _ = diamond()
    study = Study(
        net, keep={'arrays': True, 'timeline': True, 'reports': True, 'roots': 1}
    )
    result = study.enumerate('devices', restore=False)
    assert len(result.flow_results) == 4
    assert all(
        row['data']['netsim']['recovery_settle_time'] is None
        for row in result.flow_results
    )
    assert result.flow_results[0]['data']['netsim']['utilization_series']
    assert result.flow_results[0]['data']['netsim']['timeline']
    path = tmp_path / 'results.json'
    path.write_text(
        json.dumps({'steps': {'test': result.to_ngraph()}}, allow_nan=False)
    )
    replay = study.replay(path, 'test', [result.flow_results[0]['failure_id']])
    assert replay.flow_results[0]['flows'] == result.flow_results[0]['flows']
    assert replay.metadata['mode'] == 'replay'
    assert study.iterations([]).flow_results == []
    assert (
        study.process(Process({}), 1).netsim['per_demand']['d']['unavailability'] == 0
    )
    empty = Study(__import__('netsim.model.network', fromlist=['Network']).Network())
    assert (
        empty.iterations([FailureSet()]).flow_results[0]['summary']['overall_ratio']
        == 1
    )


@pytest.mark.parametrize(
    'options', [{'parallelism': 2}, {'t0': -1}, {'settle': float('nan')}]
)
def test_invalid_iteration_options(options):
    net, _ = diamond()
    with pytest.raises(ValueError):
        Study(net).iterations([], **options)


def test_invalid_keep_and_process_horizon():
    net, _ = diamond()
    with pytest.raises(ValueError, match='keep'):
        Study(net, keep={'bad': 1})
    with pytest.raises(ValueError, match='horizon'):
        Study(net).process(Process({}), 0)


def test_core_import_and_native_study_without_site_packages():
    result = subprocess.run(
        [
            sys.executable,
            '-S',
            '-c',
            'import sys; from netsim.study import Study; from netsim.runtime import Process; '
            'assert not any(m.startswith("ngraph") for m in sys.modules)',
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_cli_missing_optional_dependency():
    result = subprocess.run(
        [sys.executable, '-S', '-m', 'netsim', 'run', 'unused.yaml'],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert 'requires the optional ngraph package' in result.stderr


def two_rate_network(rates=(15, 7)):
    from netsim.model.network import Network

    net = Network()
    with net.batch():
        a, b = net.add_device('a'), net.add_device('b')
        a.add_loopback('lo', ipv4=['10.0.0.1/32'])
        b.add_loopback('lo', ipv4=['10.0.0.2/32'])
        net.add_p2p(a, 'e', b, 'e', unnumbered=True, speed=1e9)
        a.add_route('10.0.0.2/32', ['e'])
        for i, rate in enumerate(rates):
            net.add_demand(f'd{i:04}', 'a', '10.0.0.2', rate)
    return net


def test_healthy_roundoff_is_not_loss_or_downtime():
    study = Study(two_rate_network())
    # This real placement is slightly below the mathematically exact 15 bit/s.
    assert study.network.placement.demands['d0000'].delivered < 15
    result = study.process(Process({}), 10)
    for row in [result.baseline, *result.flow_results]:
        assert row['summary']['dropped_flows'] == 0
        assert row['summary']['overall_ratio'] == 1
        assert all(flow['dropped'] == 0 for flow in row['flows'])
        assert all(flow['placed'] == flow['demand'] for flow in row['flows'])
    assert result.netsim['loss_integral'] == 0
    assert all(
        metric == {'downtime': 0, 'unavailability': 0, 'loss_integral': 0}
        for metric in result.netsim['per_demand'].values()
    )


@pytest.mark.parametrize('size', [100, 1000])
def test_metrics_visit_delivery_tuples_linearly(size):
    from dataclasses import replace

    from netsim import Environment
    from netsim.runtime import Simulation
    from netsim.runtime.timeline import PlacementEvent

    class Visits:
        def __init__(self, pairs):
            self.pairs = pairs
            self.count = 0

        def __iter__(self):
            for pair in self.pairs:
                self.count += 1
                yield pair

    study = Study(two_rate_network([1] * size))
    sim = Simulation(Environment(), study.network.fork())
    registry = sim.failures(Schedule([]))
    samples = []
    for i, event in enumerate(sim.timeline.events):
        if isinstance(event, PlacementEvent):
            pairs = Visits(event.demand_delivered)
            samples.append(pairs)
            sim.timeline.events[i] = replace(event, demand_delivered=pairs)
    metrics = study._metrics(sim, registry, 0, 10)
    assert len(metrics['per_demand']) == size
    assert sum(sample.count for sample in samples) <= 3 * size * len(samples)


@pytest.mark.parametrize(
    'keys', [('events', 'records'), ('keep_events', 'keep_records')]
)
def test_study_history_budgets_preserve_full_run_metrics(keys):
    net = two_rate_network()
    events = Schedule([((('device', 'a'),), 1 + i * 2, 1) for i in range(40)])
    full = Study(net).process(events, 82)
    bounded = Study(net, keep=dict.fromkeys(keys, 0))
    sim = bounded._simulation()
    assert sim.timeline.keep_events == sim.timeline.keep_records == 0
    result = bounded.process(events, 82)
    assert result.netsim['per_demand'] == full.netsim['per_demand']
    assert result.netsim['loss_integral'] == full.netsim['loss_integral']
    assert result.netsim['per_demand']['d0000']['downtime'] == 40
    assert result.netsim['commits'] == full.netsim['commits']
    assert result.netsim['dropped_events'] > 0
    assert result.netsim['dropped_records'] > 0


def test_bounded_records_preserve_settle_time():
    net, _ = diamond(fib_delay=0.05)
    draws = [FailureSet(excluded_nodes=('R1', 'R2', 'R3', 'R4'))]
    full = Study(net).iterations(draws)
    bounded = Study(net, keep={'events': 0, 'records': 0}).iterations(draws)
    assert (
        bounded.flow_results[0]['data']['netsim']['settle_time']
        == full.flow_results[0]['data']['netsim']['settle_time']
    )
    assert (
        bounded.flow_results[0]['data']['netsim']['bits_lost_transient']
        == full.flow_results[0]['data']['netsim']['bits_lost_transient']
    )


@pytest.mark.parametrize(
    'rate,residual,is_loss',
    [
        (1.0, 0.5e-9, False),
        (1.0, 2e-9, True),
        (1e-4, 0.5e-12, False),
        (1e-4, 2e-12, True),
    ],
)
@pytest.mark.parametrize('capacity_unit', [1.0, 1e9])
def test_same_shortfall_tolerance_for_exports_and_integrals(
    rate, residual, is_loss, capacity_unit
):
    from dataclasses import replace

    from netsim.study import _DeliveryIntegrals

    study = Study(two_rate_network([rate]))
    study.capacity_unit = capacity_unit
    report = study.network.placement
    delivered = rate - residual
    report = replace(
        report,
        demands=report.demands.set(
            'd0000', replace(report.demands['d0000'], delivered=delivered)
        ),
    )
    study.network.update(lambda state: replace(state, placement=report))
    record = study._record(study.network, FailureSet())
    integral = _DeliveryIntegrals({'d0000': rate}, 0)
    integral.sample(0, [('d0000', delivered)])
    metrics = integral.metrics(10)['d0000']
    assert record['summary']['dropped_flows'] == int(is_loss)
    assert metrics['downtime'] == (10 if is_loss else 0)
    assert metrics['unavailability'] == int(is_loss)
    assert metrics['loss_integral'] == pytest.approx(
        record['flows'][0]['dropped'] * capacity_unit * 10
    )
    assert (record['flows'][0]['dropped'] > 0) == is_loss


def test_integrals_timestamp_coalescing_missing_demand_and_window_edges():
    from netsim.study import _DeliveryIntegrals

    integral = _DeliveryIntegrals({'large': 1e12, 'small': 1}, 1)
    integral.sample(0, [('large', 1e12), ('small', 1)])
    integral.sample(2, [('large', 1e12)])
    integral.sample(2, [('large', 1e12), ('small', 0.5)])
    integral.sample(4, [('large', 1e12), ('small', 1)])
    metrics = integral.metrics(5)
    assert metrics['large']['loss_integral'] == 0
    assert metrics['small'] == {
        'loss_integral': 1,
        'downtime': 2,
        'unavailability': 0.5,
    }
    assert integral.metrics(5) == metrics  # Reporting does not advance/mutate it.


def test_conflicting_retention_aliases_are_rejected():
    with pytest.raises(ValueError, match='conflicting'):
        Study(two_rate_network(), keep={'events': 1, 'keep_events': 2})
