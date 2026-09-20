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
