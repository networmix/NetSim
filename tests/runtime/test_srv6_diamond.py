"""Gate B diamond: validity, delayed programming and observed delivery."""

import pytest

import netsim
from netsim.model import forwarding as fw
from netsim.model import srv6 as sr
from netsim.model.state import tree_equal
from netsim.runtime import Simulation
from netsim.runtime.timeline import PolicyEvent
from tests.model.test_policies import diamond, policy, state_of


@pytest.mark.parametrize('backup', [False, True])
def test_timed_failover_preserves_old_program_and_visible_drops(backup):
    net, routers = diamond()
    p = policy(routers, backup=backup)
    routers['R1'].policy_client().set_steering(
        [sr.SteeringRule('class', p.key, dscp=10)]
    )
    net.add_demand('d', 'R1', '10.0.0.4', 100e6, dscp=10)
    sim = Simulation(netsim.Environment(), net)
    routers['R1'].configure(fib_delay=0.05)
    sim.run_until(1)
    old_fib = routers['R1'].fib(6)
    old_version = state_of(routers, p).programmed_version
    assert net.placement.demands['d'].policies[0].delivered == 100e6
    link = net.links['R1:eth1--R2:eth1']
    sim.at(10, link.fail)
    sim.run_until(10)
    status = state_of(routers, p)
    assert status.active_path == (1 if backup else None)
    assert status.programming == 'PENDING'
    assert status.programmed_version == old_version
    assert routers['R1'].fib(6) is old_fib
    assert net.placement.delivered_total == 0
    assert net.placement.dropped_by_reason[fw.EGRESS_DOWN] == 100e6
    observation = net.placement.demands['d'].policies[0]
    assert observation.delivered == 0
    assert observation.drops == ((fw.EGRESS_DOWN, 'R1', 100e6),)
    sim.run_until(10.1)
    assert state_of(routers, p).programming == 'INSTALLED'
    if backup:
        assert net.placement.delivered_total == 100e6
    else:
        assert dict(net.placement.dropped_by_reason) == {fw.POLICY_DOWN: 100e6}
    # The zero-delivery interval is not overwritten by successful programming.
    assert any(
        event.time == 10 and dict(event.demand_delivered)['d'] == 0
        for event in sim.timeline.placement_events()
    )
    events = sim.timeline.select(kind=PolicyEvent)
    assert any(e.time == 10 and e.programming == 'PENDING' for e in events)
    assert any(e.time > 10 and e.programming == 'INSTALLED' for e in events)
    sim.at(20, link.restore)
    sim.run_until(21)
    assert state_of(routers, p).active_path == 0
    assert net.placement.delivered_total == 100e6


def test_policy_clock_free_equals_settled_timed_execution():
    instant, a = diamond()
    timed, b = diamond()
    for net, routers in ((instant, a), (timed, b)):
        p = policy(routers, backup=True)
        net.add_demand('d', 'R1', '10.0.0.4', 100e6, steer=sr.PolicyRef(*p.key))
    instant.converge()
    sim = Simulation(netsim.Environment(), timed)
    assert tree_equal(instant.state, timed.state)
    for t, action in ((10, 'fail'), (20, 'restore')):
        getattr(instant.links['R1:eth1--R2:eth1'], action)()
        instant.converge()
        sim.at(t, getattr(timed.links['R1:eth1--R2:eth1'], action))
        sim.run_until(t + 1)
        assert tree_equal(instant.state, timed.state)


def test_study_exports_failure_policy_status_and_observed_delivery():
    import json

    from netsim.runtime.failures import FailureSet
    from netsim.study import Study

    net, routers = diamond()
    p = policy(routers)
    net.add_demand('d', 'R1', '10.0.0.4', 100e6, steer=sr.PolicyRef(*p.key))
    result = Study(net).iterations([FailureSet(excluded_links=('R1:eth1--R2:eth1',))])
    before = result.baseline['data']['policies'][0]
    after = result.flow_results[0]['data']['policies'][0]
    assert before['status'] == 'UP'
    assert before['observed_delivery'][0]['delivered'] == 100e6
    assert after['basic_valid'] == [[0, 0]]
    assert after['strict_valid'] == []
    assert after['status'] == 'DOWN' and after['programming'] == 'INSTALLED'
    assert after['observed_delivery'][0]['delivered'] == 0
    assert after['observed_delivery'][0]['drops'] == [[fw.POLICY_DOWN, 'R1', 100e6]]
    json.dumps(result.to_ngraph())


def test_prefer_installed_uses_fib_identity_during_failure_and_restore():
    from dataclasses import replace

    from netsim.model.routing import ResolutionPolicy
    from tests.model.test_policies import path

    net, routers = diamond()
    p = policy(routers)
    routers['R1'].configure(resolution_policy=ResolutionPolicy(prefer_installed=True))
    first = sr.CandidatePath(100, (path(),), originator=(2, 0))
    second = sr.CandidatePath(100, (path('R3'),), originator=(1, 0))
    routers['R1'].policy_client().replace(replace(p, candidate_paths=(first,)))
    net.converge()
    routers['R1'].policy_client().replace(replace(p, candidate_paths=(first, second)))
    net.converge()
    assert state_of(routers, p).active_path == 0
    sim = Simulation(netsim.Environment(), net)
    routers['R1'].configure(fib_delay=2)
    sim.run_until(3)
    link = net.links['R1:eth1--R2:eth1']
    sim.at(10, link.fail)
    sim.at(10.5, link.restore)
    sim.run_until(10)
    assert state_of(routers, p).active_path == 1
    sim.run_until(10.6)
    assert state_of(routers, p).programming == 'PENDING'
    assert state_of(routers, p).active_path == 0
