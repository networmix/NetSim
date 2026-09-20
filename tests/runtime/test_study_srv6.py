"""Policy exports are independent of derivation and forwarding availability."""

import csv
import json
from dataclasses import replace

from netsim.adapters.ngraph import from_scenario
from netsim.model import srv6 as sr
from netsim.runtime.failures import FailureSet
from netsim.study import Study, StudyResult, _policy_rows
from tests.adapters.test_ngraph_srv6 import scenario


def network():
    net, ids, _ = from_scenario(scenario(), srv6=True, capacity_unit=1e6)
    return net, ids[0]


def test_derived_validity_and_exports_are_detached(tmp_path):
    net, did = network()
    study = Study(net, keep={'timeline': True})
    result = study.iterations([FailureSet(excluded_links=('R1|R2|0',))])
    row = result.rows()[0]
    assert row['destination'] == 'R4' and row['priority'] == 2
    assert row['demand'] == 100
    assert row['policy_status'] == 'DOWN'
    assert row['policy_basic_valid'] is False and row['policy_strict_valid'] is False
    assert row['policy_active_path'] is None and row['policy_programmed_version'] > 0
    assert (
        row['policy_delivered'] == row['placed']
    )  # observation only, no policy-delivery claim
    assert row['data']['demand_id'] == did
    document = result.to_ngraph()
    json.dumps(document, allow_nan=False)
    assert (
        document['data']['netsim']['policy_iterations'][0]['policies']
        == result.flow_results[0]['data']['netsim']['policies']
    )
    document['data']['netsim']['policy_iterations'][0]['policies'].clear()
    assert result.to_ngraph()['data']['netsim']['policy_iterations'][0]['policies']
    result.to_csv(tmp_path / 'flows.csv')
    with (tmp_path / 'flows.csv').open() as f:
        exported = next(csv.DictReader(f))
    assert exported['policy_status'] == 'DOWN'
    assert exported['policy_basic_valid'] == 'False'
    assert exported['priority'] == '2'
    assert json.loads(exported['policy_basic_valid_lists']) == []


def test_status_fields_are_read_from_committed_state_without_inference():
    net, _ = network()
    dev = net['R1']
    table = dev.node.srv6_policies
    key = next(iter(table.policies))
    # Synthetic producer output tests the exporter, not G5 derivation.
    value = sr.PolicyState(
        active_path=0,
        valid_lists=(),
        status='PENDING',
        basic_valid=((0, 0),),
        reasons=((0, 0, sr.PATH_UNREACHABLE),),
        programmed_version=17,
    )
    net.update(
        lambda state: replace(
            state,
            devices=state.devices.set(
                'R1',
                replace(
                    state.devices['R1'],
                    srv6_policies=replace(table, states=table.states.set(key, value)),
                ),
            ),
        )
    )
    root = net.state
    row = _policy_rows(net, 1e6)[0]
    assert row['basic_valid'] is True and row['strict_valid'] is False
    assert row['status'] == 'PENDING' and row['programmed_version'] == 17
    assert row['active_path'] == 0 and row['delivered'] is None
    assert row['reasons'] == [[0, 0, sr.PATH_UNREACHABLE]]
    assert net.state is root


def test_policy_and_sid_event_rows_and_csv(tmp_path):
    net, _ = network()
    study = Study(net, keep={'timeline': True})
    original = study._simulation

    def simulation(start=0.0):
        sim = original(start)
        dev = sim.network['R1']
        policy = next(iter(dev.node.srv6_policies.policies.values()))
        sim.at(
            1.1, lambda: dev.policy_client().replace(replace(policy, name='changed'))
        )
        return sim

    study._simulation = simulation
    result = study.iterations([FailureSet(excluded_links=('R1|R2|0',))])
    rows = result.rows(events=True)
    assert {r['event'] for r in rows} == {'SidEvent', 'PolicyEvent'}
    assert any(r['event'] == 'SidEvent' and not r['adjacency_up'] for r in rows)
    assert any(r['event'] == 'PolicyEvent' and r['name'] == 'changed' for r in rows)
    assert all('failure_id' in r and r['occurrence_count'] == 1 for r in rows)
    result.to_csv(tmp_path / 'events.csv', events=True)
    with (tmp_path / 'events.csv').open() as f:
        data = list(csv.DictReader(f))
    assert len(data) == len(rows)
    assert {
        'seq',
        'time',
        'round',
        'origin',
        'event',
        'programmed_version',
        'adjacency_up',
    } <= data[0].keys()


def test_failure_snapshot_is_not_replaced_by_recovery_snapshot():
    net, _ = network()
    study = Study(net)
    # iterations drains all scheduled events; use its _metrics boundary to
    # verify the failure snapshot was already captured before recovery.
    metrics = study._metrics

    def after_recovery(sim, registry, start, end):
        policy = next(iter(sim.network['R1'].node.srv6_policies.policies.values()))
        sim.network['R1'].policy_client().replace(
            replace(policy, name='after-recovery')
        )
        return metrics(sim, registry, start, end)

    study._metrics = after_recovery
    result = study.iterations([FailureSet()])
    assert (
        result.flow_results[0]['data']['netsim']['policies'][0]['name']
        != 'after-recovery'
    )
    assert result.baseline['data']['netsim']['policies'][0]['status'] == 'UP'


def test_repeatable_study_and_empty_csv(tmp_path):
    net, _ = network()
    study = Study(net, keep={'timeline': True})
    draws = [FailureSet(excluded_nodes=('R2',)), FailureSet(excluded_nodes=('R2',))]
    assert study.iterations(draws).to_ngraph() == study.iterations(draws).to_ngraph()
    empty = StudyResult({}, [])
    empty.to_csv(tmp_path / 'empty.csv')
    assert (tmp_path / 'empty.csv').read_text().strip() == ''
    assert empty.rows(events=True) == []


def test_study_policy_failover_and_delivered_status():
    net, _ = network()
    result = Study(net).iterations([FailureSet(excluded_nodes=('R2',))])
    row = result.rows()[0]
    assert row['policy_basic_valid'] is False
    assert row['policy_strict_valid'] is False
    assert row['policy_active_path'] is None
    assert row['policy_delivered'] == row['placed'] == 0
