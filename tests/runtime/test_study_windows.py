"""Gate C observation windows are independent of liveness and retention."""

import pytest

from netsim.runtime import FailureSet, Schedule
from netsim.study import Study
from tests.runtime.test_study import diamond, two_rate_network


def test_horizon_is_a_deadline_for_delayed_fib():
    net, _ = diamond(fib_delay=0.5)
    result = Study(net).iterations(
        [FailureSet(excluded_links=('R1:eth1--R2:eth1',))],
        horizon=0.125,
        quiet=0.0625,
        restore=False,
    )
    metrics = result.flow_results[0]['data']['netsim']
    assert metrics['status'] == 'deadline_exceeded'
    assert metrics['converged_at'] is None
    assert metrics['end'] == 1.125
    assert metrics['loss_integral'] == pytest.approx(50e6 * 0.125)


def test_streaming_event_counts_survive_eviction():
    net = two_rate_network()
    events = Schedule([((('device', 'a'),), 1 + i * 2, 1) for i in range(40)])
    full = Study(net).process(events, 82)
    bounded = Study(net, keep={'events': 0, 'records': 0}).process(events, 82)
    assert bounded.netsim['event_counts'] == full.netsim['event_counts']


def metrics(result):
    return result.flow_results[0]['data']['netsim']


def with_timer(study, period=0.001, operation=None):
    create = study._simulation
    ticks, sims = [], []

    def timed(*args, **kwargs):
        sim = create(*args, **kwargs)
        sims.append(sim)

        def tick():
            ticks.append(sim.env.now)
            if operation is not None:
                operation(sim)
            sim.at(sim.env.now + period, tick)

        sim.at(sim.env.now + period, tick)
        return sim

    study._simulation = timed
    return ticks, sims


@pytest.mark.parametrize('selected', ['routing', 'programming', 'delivery', 'all'])
def test_periodic_normal_timers_do_not_prevent_convergence(selected):
    study = Study(two_rate_network())
    ticks, _ = with_timer(study)
    result = study.iterations(
        [FailureSet()],
        horizon=5,
        quiet=0.25,
        stability=selected,
        restore=False,
    )
    row = metrics(result)
    assert row['status'] == 'converged'
    assert row['converged_at'] == 1
    assert row['convergence_time'] == 0
    assert row['end'] == 6
    assert 5990 < len(ticks) <= 6000
    assert max(ticks) <= 6
    assert row['engine_events'] == len(ticks) + 1  # includes failure lease event
    assert row['rounds'] == 0


def test_quiet_begins_on_output_change_and_includes_deadline_events():
    from netsim.study import Stability

    net, _ = diamond(fib_delay=0.25)
    draws = [FailureSet(excluded_links=('R1:eth1--R2:eth1',))]
    good = metrics(
        Study(net).iterations(
            draws,
            horizon=0.5,
            quiet=0.25,
            stability=Stability.ALL,
            restore=False,
        )
    )
    assert good['status'] == 'converged'
    assert good['converged_at'] == 1.25
    assert good['convergence_time'] == 0.25
    assert good['bits_lost_transient'] == 50e6 * 0.25
    assert sum(good['bits_lost_by_reason'].values()) == 50e6 * 0.25
    short = metrics(
        Study(net).iterations(draws, horizon=0.375, quiet=0.25, restore=False)
    )
    assert short['status'] == 'deadline_exceeded'
    assert short['converged_at'] is None


def test_settle_alias_becomes_upper_bound_and_explicit_horizon_wins():
    net, _ = diamond(fib_delay=0.5)
    draws = [FailureSet(excluded_links=('R1:eth1--R2:eth1',))]
    study = Study(net)
    bounded = metrics(
        study.iterations(draws, settle=0.125, stability='all', restore=False)
    )
    assert bounded['status'] == 'deadline_exceeded'
    assert bounded['end'] == 1.125
    legacy = metrics(study.iterations(draws, settle=0.125, restore=False))
    assert legacy['settle_time'] == 0.5
    assert legacy['end'] == 1.5
    explicit = metrics(study.iterations(draws, settle=0.125, horizon=1, restore=False))
    assert explicit['end'] == 2
    assert explicit['status'] == 'converged'


@pytest.mark.parametrize('budget', [0, 1, 17])
def test_budget_caps_dispatch_and_returns_partial_metrics(budget):
    study = Study(two_rate_network())
    ticks, _ = with_timer(study, period=0.125)
    row = metrics(
        study.iterations(
            [FailureSet(excluded_nodes=('a',))],
            horizon=5,
            event_budget=budget,
        )
    )
    assert row['status'] == 'budget_exceeded'
    assert row['engine_events'] == budget
    assert len(ticks) <= budget
    assert row['end'] <= 6
    assert row['recovery_status'] is None


def test_budget_exactly_enough_and_budget_shared_with_recovery():
    study = Study(two_rate_network())
    # Empty fault/repair each dispatch one event, with no derivation work.
    exact = metrics(study.iterations([FailureSet()], horizon=1, event_budget=2))
    assert exact['status'] == 'converged'
    assert exact['engine_events'] == 2
    partial = metrics(study.iterations([FailureSet()], horizon=1, event_budget=1))
    assert partial['status'] == partial['recovery_status'] == 'budget_exceeded'
    assert partial['engine_events'] == 1


def test_budget_applies_to_warmup_and_process_faults_are_counted_on_dispatch():
    study = Study(two_rate_network())
    with_timer(study, period=0.125)
    result = study.process(
        Schedule([((('device', 'a'),), 0, 1)]), 2, warmup=1, event_budget=3
    )
    assert result.netsim['status'] == 'budget_exceeded'
    assert result.netsim['warmup_events'] == result.costs['warmup_events'] == 3
    assert result.netsim['fault_events'] == 0
    assert result.netsim['loss_integral'] == 0
    assert result.netsim['per_demand']['d0000']['downtime'] == 0
    assert result.costs['warmup_seconds'] > 0
    assert result.netsim['observation_end'] == -0.625


def test_process_window_after_warmup_uses_same_failure_clock():
    study = Study(two_rate_network())
    ticks, _ = with_timer(study, period=0.125)
    events = Schedule([((('device', 'a'),), 0.25, 0.25)])
    result = study.process(events, 1, warmup=0.5, quiet=0.25)
    assert result.netsim['status'] == 'converged'
    assert result.netsim['converged_at'] == 0.5
    assert result.netsim['warmup_events'] == 4
    assert result.netsim['per_demand']['d0000']['downtime'] == 0.25
    assert min(ticks) == -0.375
    assert max(ticks) == 1


def test_fresh_agent_runtime_uses_configuration_and_warms_every_iteration(monkeypatch):
    from dataclasses import replace

    from netsim.model.contracts import TransportState
    from netsim.runtime.agents import AgentRuntime
    from tests.model.test_agent_contract import Minimal

    net = two_rate_network()
    net.add_agent('a', Minimal(), name='ref')
    device = net.state.devices['a']
    old = device.agents['ref']
    stale = replace(old, initialized=True, state=('stale',), runs=9)
    net.update(
        lambda state: replace(
            state,
            transport=TransportState(),
            devices=state.devices.set(
                'a', replace(device, agents=device.agents.set('ref', stale))
            ),
        )
    )
    seen, runtimes, ticks = [], [], []

    def bind(runtime):
        runtimes.append(runtime)
        sim = runtime.sim
        seen.append(sim.state.devices['a'].agents['ref'])
        assert sim.state.transport is None

        def tick():
            ticks.append((len(runtimes), sim.env.now))
            sim.at(sim.env.now + 0.125, tick)

        sim.at(sim.env.now + 0.125, tick)

    monkeypatch.setattr(AgentRuntime, 'bind', bind)
    root = net.state
    result = Study(net).iterations(
        [FailureSet(), FailureSet(excluded_nodes=('b',))],
        t0=0,
        warmup=0.5,
        horizon=0.5,
        quiet=0.25,
        restore=False,
    )
    assert net.state is root
    assert len(runtimes) == 3  # baseline preparation plus two iterations
    assert all(a is not b for i, a in enumerate(runtimes) for b in runtimes[i + 1 :])
    assert all(node.generation != stale.generation for node in seen)
    assert seen[0].generation == seen[1].generation  # deterministic independent forks
    assert all(
        not node.initialized and node.state is None and node.runs == 0 for node in seen
    )
    assert [row['data']['netsim']['warmup_events'] for row in result.flow_results] == [
        4,
        4,
    ]
    assert result.baseline['data']['netsim']['warmup_events'] == 4
    assert result.costs['warmup_events'] == 12
    assert result.costs['warmup_seconds'] > 0
    assert [time for i, time in ticks if i == 2] == [
        time for i, time in ticks if i == 3
    ]
    assert [time for i, time in ticks if i == 1] == [-0.375, -0.25, -0.125, 0]


def test_all_streamed_metrics_equal_with_tiny_retention():
    events = Schedule([((('device', 'a'),), 1 + i * 2, 1) for i in range(80)])
    full = Study(two_rate_network()).process(events, 162, quiet=1)
    study = Study(two_rate_network(), keep={'keep_events': 1, 'keep_records': 1})
    _, sims = with_timer(study, period=1)
    # Compare counts without a liveness timer first; timers emit no timeline rows.
    bounded = study.process(events, 162, quiet=1)
    a, b = dict(full.netsim), dict(bounded.netsim)
    for key in ('dropped_events', 'dropped_records', 'engine_events'):
        a.pop(key)
        b.pop(key)
    assert a == b
    assert bounded.netsim['rounds'] > 0
    assert len(sims[0].timeline.events) <= 65  # amortized retention block
    assert len(sims[0].timeline.records) <= 65
    assert sims[0]._failure_registry.history == []
    assert sims[0]._failure_registry.trace == []
    assert bounded.netsim['fault_events'] == 80


def test_describe_counts_baseline_and_does_not_invent_event_rate():
    from dataclasses import FrozenInstanceError

    study = Study(two_rate_network(), keep={'events': 2, 'records': 3})
    description = study.describe()
    assert description.devices == 2
    assert description.links == 1
    assert description.demands == 2
    assert description.prefixes == 2
    assert description.rib_rows >= description.prefixes
    assert description.classes > 0
    assert description.agents == description.sessions == 0
    assert description.event_rate is description.duration is None
    assert dict(description.retention)['events'] == 2
    assert study.describe(event_rate=1000, duration=5).duration == 5
    with pytest.raises(FrozenInstanceError):
        description.devices = 10


@pytest.mark.parametrize(
    'options',
    [
        {'horizon': -1},
        {'horizon': float('inf')},
        {'warmup': -1},
        {'event_budget': -1},
        {'event_budget': 1.5},
        {'event_budget': True},
        {'quiet': float('nan')},
        {'stability': 'never'},
        {'t0': 1e30, 'horizon': 0.01},
        {'t0': 1e30, 'quiet': 0.01},
    ],
)
def test_invalid_windows(options):
    with pytest.raises(ValueError):
        Study(two_rate_network()).iterations([], **options)


def test_enumerate_and_replay_forward_window_options():
    study = Study(two_rate_network())
    result = study.enumerate('devices', t0=0, horizon=0.5, quiet=0.25, warmup=0.125)
    replay = study.replay(
        {'steps': {'s': result.to_ngraph()}},
        's',
        t0=0,
        horizon=0.5,
        quiet=0.25,
        warmup=0.125,
    )
    assert result.flow_results == replay.flow_results
    assert all(
        row['data']['netsim']['status'] == 'converged' for row in result.flow_results
    )


def test_repeatability_fingerprint():
    import hashlib
    import json

    from netsim.runtime import Process

    def run():
        return Study(two_rate_network(), keep={'events': 1, 'records': 1}).process(
            Process({('device', 'a'): {'mtbf': 0.5, 'mttr': 0.125}}, seed=42),
            4,
            warmup=0.25,
            quiet=0.125,
        )

    first, second = run(), run()
    assert first.rows() == second.rows()
    assert first.to_ngraph() == second.to_ngraph()
    document = json.dumps(first.to_ngraph(), sort_keys=True, allow_nan=False)
    assert (
        hashlib.sha256(document.encode()).hexdigest()
        == '8f7451ae26a6e619312ffe14786c079bc7478fc5bf583e5de7c240e5f8e41ecf'
    )


@pytest.mark.parametrize('selected', ['routing', 'delivery', 'all'])
def test_unrelated_commits_do_not_reset_quiet_interval(selected):
    study = Study(two_rate_network())
    original = study._simulation

    def simulation(*args, **kwargs):
        sim = original(*args, **kwargs)
        # Changing wire latency emits LinkConfigEvent but preserves all selected
        # forwarding/programming/delivery outputs. Last commit is at 1.875.
        link = sim.network.link(next(iter(sim.state.links)))
        sim.at(1.875, lambda: link.configure(delay=0.25))
        return sim

    study._simulation = simulation
    row = metrics(
        study.iterations(
            [FailureSet()], horizon=1, quiet=0.5, stability=selected, restore=False
        )
    )
    assert row['event_counts']['LinkConfigEvent'] == 1
    assert row['status'] == 'converged'
    assert row['converged_at'] == 1


def test_no_premature_convergence_at_earlier_quiet_period():
    study = Study(two_rate_network())
    original = study._simulation

    def simulation(*args, **kwargs):
        sim = original(*args, **kwargs)
        sim.at(1.875, lambda: sim.network.device('a').configure(enabled=False))
        return sim

    study._simulation = simulation
    row = metrics(
        study.iterations([FailureSet()], horizon=1, quiet=0.25, restore=False)
    )
    assert row['status'] == 'deadline_exceeded'
    assert row['converged_at'] is None
    assert row['per_demand']['d0000']['downtime'] == 0.125


def test_selected_delivery_and_routing_are_independent():
    net, _ = diamond(fib_delay=1)
    draw = [FailureSet(excluded_links=('R1:eth1--R2:eth1',))]
    # FIBs stay unchanged while programming is pending. Selection is explicit;
    # routing/delivery alone do not promise that programming has caught up.
    for selected in ('routing', 'delivery'):
        row = metrics(
            Study(net).iterations(
                draw, horizon=0.5, quiet=0.25, stability=selected, restore=False
            )
        )
        assert row['status'] == 'converged'
        assert row['converged_at'] == 1
    row = metrics(
        Study(net).iterations(
            draw, horizon=0.5, quiet=0.25, stability='programming', restore=False
        )
    )
    assert row['status'] == 'deadline_exceeded'


def test_programming_requires_installed_policies():
    from dataclasses import replace

    from netsim.model import srv6
    from netsim.study import Stability, _StabilityWindow
    from tests.runtime.test_study_srv6 import network

    net, _ = network()
    sim = Study(net)._simulation()
    dev = sim.state.devices['R1']
    table = dev.srv6_policies
    key = next(iter(table.policies))
    pending = replace(
        sim.state,
        devices=sim.state.devices.set(
            'R1',
            replace(
                dev,
                srv6_policies=replace(
                    table, states=table.states.set(key, srv6.PolicyState())
                ),
            ),
        ),
    )
    window = _StabilityWindow(sim, 0, Stability.PROGRAMMING, 0.25)
    assert window.ready(sim.state)
    assert not window.ready(pending)


def test_transient_loss_stops_at_start_of_confirmed_quiet_interval():
    row = metrics(
        Study(two_rate_network()).iterations(
            [FailureSet(excluded_nodes=('a',))],
            horizon=1,
            quiet=0.5,
            restore=False,
        )
    )
    assert row['status'] == 'converged'
    assert row['converged_at'] == 1
    assert row['loss_integral'] == 22
    assert sum(row['bits_lost_by_reason'].values()) == 22
    assert row['bits_lost_transient'] == 0
    assert sum(row['bits_lost_transient_by_reason'].values()) == 0


def test_actual_ngraph_workflow_forwards_windows():
    pytest.importorskip('ngraph', reason='optional actual NetGraph workflow')
    from tests.adapters.test_study_ngraph import scenario_doc, scenario_from

    doc = scenario_doc()
    doc['workflow'] = [
        {
            'type': 'NetSimStudy',
            'name': 'windows',
            'demand_set': 'traffic',
            'failure_policy': 'single_link_failure',
            'iterations': 2,
            'warmup': 0.25,
            'horizon': 0.5,
            'quiet': 0.125,
            'stability': 'delivery',
            'event_budget': 1000,
            'restore': False,
        },
        {
            'type': 'NetSimStudy',
            'name': 'replay',
            'mode': 'replay',
            'step': 'windows',
            'demand_set': 'traffic',
            'warmup': 0.25,
            'horizon': 0.5,
            'quiet': 0.125,
            'stability': 'delivery',
            'event_budget': 1000,
            'restore': False,
        },
        {
            'type': 'NetSimStudy',
            'name': 'process',
            'mode': 'process',
            'demand_set': 'traffic',
            'horizon': 0.5,
            'warmup': 0.25,
            'quiet': 0.125,
            'stability': 'routing',
            'event_budget': 0,
        },
    ]
    scenario = scenario_from(doc)
    scenario.run()
    steps = scenario.results.to_dict()['steps']
    assert (
        steps['windows']['data']['flow_results']
        == steps['replay']['data']['flow_results']
    )
    row = steps['windows']['data']['flow_results'][0]['data']['netsim']
    assert row['status'] == 'converged'
    assert row['end'] == 1.5
    assert steps['process']['data']['netsim']['end'] == 0.5


def test_warmup_changes_are_excluded_from_observation_metrics():
    study = Study(two_rate_network(), keep={'events': 0, 'records': 0})
    original = study._simulation

    def simulation(*args, **kwargs):
        sim = original(*args, **kwargs)
        sim.at(-0.25, lambda: sim.network.device('a').configure(enabled=False))
        sim.at(-0.125, lambda: sim.network.device('a').configure(enabled=True))
        return sim

    study._simulation = simulation
    result = study.process(Schedule(()), 1, warmup=0.5, quiet=0.25)
    assert result.netsim['warmup_events'] > 2
    assert result.netsim['loss_integral'] == 0
    assert result.netsim['observed_drop_reasons'] == []
    assert result.netsim['event_counts'] == {'PlacementEvent': 1}
    assert result.netsim['rounds'] == 0
    assert result.netsim['max_utilization'] > 0


def test_real_initialized_agents_restart_and_warm_repeatably():
    from tests.runtime.test_agent_fresh_runtime import running_network

    network, original_runtime = running_network()
    original_root = network.state
    generations = {
        device: network.state.devices[device].agents['ref'].generation
        for device in ('R1', 'R2')
    }
    study = Study(network)
    _, simulations = with_timer(study, period=0.125)
    options = dict(t0=0, warmup=0.5, horizon=0.25, quiet=0.125, restore=False)
    first = study.iterations([FailureSet()], **options)
    second = study.iterations([FailureSet()], **options)
    assert first.to_ngraph() == second.to_ngraph()
    assert metrics(first)['warmup_events'] > 4  # agent initialization + NORMAL timer
    assert metrics(first)['status'] == 'converged'
    for sim in simulations:
        for device, old_generation in generations.items():
            node = sim.state.devices[device].agents['ref']
            assert node.generation != old_generation
            assert node.initialized
            if sim.env.now == 0:  # baseline stops before the timer's agent run
                assert node.runs == 1 and node.state == ('init', -0.5)
            else:
                assert node.runs == 2
                assert node.state == ('run', 0.001, ('init', -0.5))
    assert network.state is original_root
    assert original_runtime.env.now == 2


def test_default_processing_delay_delivers_during_real_agent_warmup():
    from netsim.model import contracts as c
    from tests.model.test_agent_contract import Minimal

    class Hello(Minimal):
        config = c.AgentConfig(run_delay=0, listen_ports=(179,))

        def on_init(self, ctx):
            return c.AgentOutput(
                state=(),
                datagrams=(c.Datagram('e', b'hello', port=179),),
            )

        def on_run(self, ctx):
            return c.AgentOutput(
                state=ctx.agent_state
                + tuple(
                    (entry.time, entry.payload)
                    for entry in ctx.inbox
                    if isinstance(entry, c.Delivery)
                )
            )

    network = two_rate_network()
    network.add_agent('a', Hello(), name='ref')
    network.add_agent('b', Hello(), name='ref')
    assert all(link.config.delay == 0 for link in network.state.links.values())
    study = Study(network)
    _, simulations = with_timer(study, period=0.125)
    result = study.iterations(
        [FailureSet()], t0=0, warmup=0.125, horizon=0.25, quiet=0.125, restore=False
    )
    assert metrics(result)['status'] == 'converged'
    for device in ('a', 'b'):
        node = simulations[0].state.devices[device].agents['ref']
        assert node.config.processing_delay == 0.001
        assert node.state == ((-0.124, b'hello'),)
        assert node.runs == 2


@pytest.mark.parametrize('mode', ['iterations', 'process'])
@pytest.mark.parametrize('initialized_until', [None, 0.03125, 0.5])
def test_reference_baseline_matches_prepared_no_fault_iteration(
    mode, initialized_until
):
    from netsim import Environment
    from netsim.runtime import Simulation
    from tests.agents.fixtures import topology

    network = topology(numbered=False)
    if initialized_until is not None:
        runtime = Simulation(Environment(), network)
        runtime.run_until(initialized_until)
        assert all(
            dev.agents['ref'].initialized for dev in network.state.devices.values()
        )
    root = network.state
    study = Study(network, keep={'events': 0, 'records': 0})

    def run():
        if mode == 'iterations':
            return study.iterations(
                [FailureSet()], t0=0.5, warmup=2, horizon=1, quiet=0.25, restore=False
            )
        return study.process(Schedule([]), 1, warmup=2, quiet=0.25)

    result = run()
    assert result.baseline['summary']['total_placed'] == 2000
    assert result.baseline['summary'] == result.flow_results[0]['summary']
    baseline = result.baseline['data']['netsim']
    assert baseline['baseline_complete'] is True
    assert baseline['preparation_status'] == 'complete'
    assert (
        baseline['preparation_end']
        == baseline['preparation_deadline']
        == (0.5 if mode == 'iterations' else 0)
    )
    assert baseline['warmup_events'] > 0
    assert result.costs['warmup_events'] == (
        baseline['warmup_events'] + metrics(result)['warmup_events']
    )
    assert result.costs['warmup_seconds'] > 0
    assert metrics(result)['status'] == 'converged'
    assert result.to_ngraph() == run().to_ngraph()
    assert network.state is root
    if initialized_until is not None:
        assert runtime.env.now == initialized_until


def test_agent_baseline_runs_to_t0_even_without_warmup_or_draws():
    from tests.agents.fixtures import topology

    study = Study(topology(numbered=False))
    result = study.iterations([], t0=0.5)
    assert result.baseline['summary']['total_placed'] == 2000
    baseline = result.baseline['data']['netsim']
    assert baseline['baseline_complete'] is True
    assert baseline['preparation_end'] == 0.5
    assert baseline['engine_events'] > 0
    assert baseline['warmup_events'] == result.costs['warmup_events'] == 0
    assert result.flow_results == []


@pytest.mark.parametrize('mode', ['iterations', 'process'])
def test_agent_baseline_has_independent_budget_and_its_wall_cost_is_included(
    mode, monkeypatch
):
    from tests.agents.fixtures import topology

    study = Study(topology(numbered=False))

    def run(budget=None):
        if mode == 'iterations':
            return study.iterations(
                [FailureSet()],
                t0=0,
                warmup=2,
                horizon=1,
                event_budget=budget,
                restore=False,
            )
        return study.process(Schedule([]), 1, warmup=2, event_budget=budget)

    # Exactly enough events for preparation must not mark the baseline partial,
    # even when subsequent observation needs another event and exhausts its budget.
    budget = run().baseline['data']['netsim']['engine_events']
    times = iter((10.0, 13.0, 20.0, 25.0))
    monkeypatch.setattr('netsim.study.perf_counter', lambda: next(times))
    result = run(budget)
    baseline = result.baseline['data']['netsim']
    assert baseline['baseline_complete'] is True
    assert baseline['engine_events'] == budget
    assert metrics(result)['status'] == 'budget_exceeded'
    assert metrics(result)['engine_events'] == budget
    assert result.costs == {'warmup_seconds': 8.0, 'warmup_events': 2 * budget}


@pytest.mark.parametrize('mode', ['iterations', 'process'])
def test_agent_baseline_does_not_include_later_observation(mode):
    from tests.agents.fixtures import topology

    study = Study(topology(numbered=False))
    if mode == 'iterations':
        result = study.iterations([FailureSet()], t0=0, horizon=1, restore=False)
    else:
        result = study.process(Schedule([]), 1)
    assert result.baseline['summary']['total_placed'] == 0
    assert result.flow_results[0]['summary']['total_placed'] == 2000
    baseline = result.baseline['data']['netsim']
    assert baseline['preparation_end'] == 0
    assert baseline['baseline_complete'] is True  # requested zero-time preparation


@pytest.mark.parametrize('mode', ['iterations', 'process'])
def test_agent_baseline_is_captured_before_faults(mode):
    from tests.agents.fixtures import topology

    study = Study(topology(numbered=False))
    if mode == 'iterations':
        result = study.iterations(
            [FailureSet(excluded_nodes=('r0',))],
            t0=0.5,
            warmup=2,
            horizon=1,
            restore=False,
        )
    else:
        result = study.process(Schedule([((('device', 'r0'),), 0, 2)]), 1, warmup=2)
    assert result.baseline['summary']['total_placed'] == 2000
    assert result.flow_results[0]['summary']['total_placed'] == 0


@pytest.mark.parametrize('mode', ['iterations', 'process'])
@pytest.mark.parametrize('warmup', [0, 2])
@pytest.mark.parametrize('budget', [0, 1])
def test_agent_baseline_marks_incomplete_preparation(mode, warmup, budget):
    from tests.agents.fixtures import topology

    study = Study(topology(numbered=False), keep={'events': 0, 'records': 0})
    if mode == 'iterations':
        result = study.iterations(
            [FailureSet()], warmup=warmup, event_budget=budget, restore=False
        )
    else:
        result = study.process(Schedule([]), 1, warmup=warmup, event_budget=budget)
    baseline = result.baseline['data']['netsim']
    assert baseline['baseline_complete'] is False
    assert baseline['preparation_status'] == 'budget_exceeded'
    assert baseline['engine_events'] == budget
    assert baseline['preparation_end'] <= baseline['preparation_deadline']
    assert result.costs['warmup_events'] == (
        baseline['warmup_events'] + metrics(result)['warmup_events']
    )
    assert result.to_ngraph()['data']['baseline']['data']['netsim'] == baseline


@pytest.mark.parametrize('mode', ['iterations', 'process'])
def test_oracle_baseline_requires_no_additional_runtime(mode):
    study = Study(two_rate_network())
    _, simulations = with_timer(study)
    if mode == 'iterations':
        result = study.iterations([], warmup=1)
        assert simulations == []
    else:
        result = study.process(Schedule([]), 1, warmup=1)
        assert len(simulations) == 1
    assert result.baseline == study._record(study.network, FailureSet())
