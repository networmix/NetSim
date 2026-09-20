import pytest

import netsim
from netsim.model import forwarding as fw
from netsim.model.addressing import IPV4, to_int
from netsim.model.contracts import STATIC
from netsim.model.interfaces import OperState, StateReason
from netsim.model.packets import PacketTemplate
from netsim.runtime import Simulation
from netsim.runtime.pipeline import ConvergenceError
from tests.model.test_flows import WIRE, _loads
from tests.model.test_network import build_diamond


def A(text):
    return to_int(text)[0]


def bound(min_links=2, demand=True, **kw):
    net, R = build_diamond(min_links=min_links, **kw)
    if demand:
        net.add_demand('d1', 'R1', '10.0.0.4', 100e6)
    env = netsim.Environment()
    sim = Simulation(env, net)
    return env, net, R, sim


def kind_origins(sim, t):
    return sim.timeline.stage_names(t)


class TestRounds:
    def test_link_failure_one_round_per_band(self):
        env, net, R, sim = bound()
        assert net.placement.delivered_total == pytest.approx(100e6)
        sim.at(10, lambda: net.links['R1:eth1--R2:eth1'].fail())
        sim.run_until(20)
        assert kind_origins(sim, 10) == ['carrier', 'lag', 'l3', 'fib', 'placement']
        fib = R['R1'].fib(IPV4)
        assert [
            a.interface for a in fib.group(fib.lookup(A('10.0.0.4'))).adjacencies
        ] == ['eth3']
        loads, e = _loads(net, net.placement)
        assert loads['R1>R3'] == pytest.approx(100e6 * WIRE)
        assert net.placement.delivered_total == pytest.approx(100e6)
        assert env.now == 20

    def test_fib_delay_shows_stale_forwarding(self):
        env, net, R, sim = bound()
        R['R1'].configure(fib_delay=0.05)
        sim.settle()
        sim.at(10, lambda: net.links['R1:eth1--R2:eth1'].fail())
        sim.run_until(10)
        key = (A('10.0.0.4'), 32, STATIC, ())
        assert R['R1'].route_status(IPV4, key) == ('PENDING', None)
        rep = net.placement
        assert rep.dropped_by_reason.get(fw.EGRESS_DOWN, 0) == pytest.approx(50e6)
        assert rep.delivered_total == pytest.approx(50e6)
        sim.run_until(10.05)
        assert R['R1'].route_status(IPV4, key) == ('INSTALLED', None)
        assert net.placement.delivered_total == pytest.approx(100e6)
        assert 'fib' in kind_origins(sim, 10.05) and 'placement' in kind_origins(
            sim, 10.05
        )

    def test_carrier_delay_debounce(self):
        env, net, R, sim = bound(min_links=2)
        for dev, name in (('R1', 'eth1'), ('R2', 'eth1')):
            R[dev][name].configure(carrier_delay_down=1.0)
        sim.settle()
        link = net.links['R1:eth1--R2:eth1']
        sim.at(10, link.fail)
        sim.at(10.5, link.restore)
        sim.run_until(12)
        assert (
            R['R1']['eth1'].oper.oper == OperState.UP
            and R['R1']['eth1'].oper.since == 0.0
        )
        assert 'lag' not in kind_origins(sim, 10) and 'lag' not in kind_origins(sim, 11)
        # Physical failure during the window: stale routing offers 25 on the failed member and drops it.
        sim.at(20, link.fail)
        sim.run_until(20.5)
        rep = net.placement
        loads, e = _loads(net, rep)
        assert rep.offered[e['R1>R2a']] == pytest.approx(25e6 * WIRE)
        assert rep.carried[e['R1>R2a']] == 0.0 and rep.dropped[
            e['R1>R2a']
        ] == pytest.approx(25e6)
        assert rep.dropped_by_reason[fw.LINK_DOWN] == pytest.approx(
            25e6
        ) and rep.delivered_total == pytest.approx(75e6)
        sim.run_until(21)
        assert (
            R['R1']['eth1'].oper.oper == OperState.DOWN
            and R['R1']['eth1'].oper.since == 21
        )
        assert net.placement.delivered_total == pytest.approx(100e6)

    def test_asymmetric_delays_partner_check(self):
        env, net, R, sim = bound(min_links=2)
        R['R1']['eth1'].configure(carrier_delay_down=0.2)
        sim.settle()
        sim.at(10, net.links['R1:eth1--R2:eth1'].fail)
        sim.run_until(10)
        assert R['R2']['Po1'].oper.reason == StateReason.MIN_LINKS
        assert R['R1']['eth1'].oper.oper == OperState.UP
        assert (R['R1']['Po1'].oper.oper, R['R1']['Po1'].oper.reason) == (
            OperState.LOWER_LAYER_DOWN,
            StateReason.PARTNER,
        )
        sim.run_until(10.2)
        assert R['R1']['Po1'].oper.reason == StateReason.MIN_LINKS

    def test_reactive_process_sees_settled_root(self):
        env, net, R, sim = bound()
        seen = []

        def waiter(env):
            delta, root = yield sim.bus.wait_for(
                lambda d: any(
                    i == 'eth1' and o for i, _, o in d.interface_changes('R1')
                )
            )
            po = root.devices['R1'].interfaces['Po1'].oper
            seen.append((env.now, po.oper, root.placement.delivered_total))
            net.links[
                'R1:eth3--R3:eth1'
            ].fail()  # a write from a settled delivery opens new rounds

        env.process(waiter(env))
        sim.at(10, net.links['R1:eth1--R2:eth1'].fail)
        sim.run_until(11)
        assert seen == [(10, OperState.LOWER_LAYER_DOWN, pytest.approx(100e6))]
        assert net.placement.delivered_total == 0.0  # both branches gone
        assert R['R1']['eth3'].oper.oper == OperState.DOWN

    def test_failed_derivation_retries(self):
        env, net, R, sim = bound()
        calls = []

        def bad_source(state, now):
            calls.append(now)
            if len(calls) == 1:
                raise RuntimeError('boom')
            return state

        net.add_source(bad_source)
        sim.at(10, net.links['R1:eth1--R2:eth1'].fail)
        with pytest.raises(RuntimeError, match='boom'):
            sim.run_until(20)
        assert env.now == 10 and 'fib' not in kind_origins(sim, 10)
        sim.retry()
        sim.run_until(20)
        assert 'fib' in kind_origins(sim, 10) and len(calls) == 2

    def test_run_until_semantics_and_convergence_guard(self):
        env, net, R, sim = bound()
        sim.at(5, lambda: None)
        sim.run_until(5)
        assert env.now == 5
        with pytest.raises(ValueError):
            sim.at(1, lambda: None)
        sim.pipeline.max_rounds = 1

        def flip(t, origin, delta, root):
            if t == 7 and origin == ('kind', 'placement', 0):
                net.add_demand('d2', 'R2', '10.0.0.4', 1e6)

        sim.bus.subscribe(flip)
        sim.at(7, lambda: net.add_demand('d3', 'R3', '10.0.0.4', 1e6))
        # The settled delivery writes at t=7 which opens another round; with max_rounds=1 this overflows.
        with pytest.raises(ConvergenceError):
            sim.run_until(8)

    def test_send_honours_link_delay(self):
        env, net, R, sim = bound(demand=False)
        pkt = PacketTemplate(IPV4, A('10.0.0.1'), A('10.0.0.4')).to_packet()
        proc = sim.send('R1', pkt)
        env.run()
        trace = proc.value
        assert trace.outcome == fw.DELIVER and trace.path[-1] == 'R4'


def test_round_bookkeeping_does_not_grow_with_time():
    env, net, R, sim = bound(demand=False)
    link = net.links['R1:eth3--R3:eth1']
    for k in range(200):
        sim.at(1 + k, link.fail if k % 2 == 0 else link.restore)
    sim.run_until(300)
    assert len(sim.pipeline.round_end_scheduled) == 0
    assert len(sim.pipeline.scheduled) == 0
    assert all(not kind.pending for kind in sim.pipeline.kinds)


def test_kind_that_dirties_itself_runs_again_in_a_successor_round():
    import dataclasses

    from netsim.model.network import Network
    from netsim.runtime.pipeline import COALESCE, Kind, Pipeline

    net = Network()
    net.add_device('A')
    env = netsim.Environment()
    runs = []

    def run(state, now, entities):
        runs.append(sorted(entities))
        dev = state.devices['A']
        cfg = dataclasses.replace(dev.config, seed=dev.config.seed + 1)
        return dataclasses.replace(
            state, devices=state.devices.set('A', dataclasses.replace(dev, config=cfg))
        )

    def affected(delta, state):
        return (
            {'A'}
            if delta.config_changed('A') and state.devices['A'].config.seed < 3
            else set()
        )

    kind = Kind(0, COALESCE, run, affected)
    kind.name = 'self'
    pipe = Pipeline(env, net, [kind])
    net.on_delta.append(pipe.on_delta)
    net.clock = lambda: env.now
    net.devices['A'].configure(seed=1)
    env.run()
    assert runs == [['A'], ['A']]
    assert net.state.devices['A'].config.seed == 3
    assert not kind.pending


def test_observer_failure_after_commit_does_not_re_execute_the_run():
    """A kind's commit that an observer rejects afterwards is consumed once:
    retry() has nothing to re-run and the committed state stands."""
    import dataclasses

    from netsim.model import derive
    from netsim.model.network import Network, published_failure
    from netsim.runtime.pipeline import COALESCE, Kind, Pipeline

    net = Network()
    net.add_device('R')
    net.converge()
    env = netsim.Environment()
    runs = []

    def run(root, now, due):
        runs.append(now)
        dev = root.devices['R']
        dev = dataclasses.replace(
            dev, agents=dev.agents.set('count', dev.agents.get('count', 0) + 1)
        )
        return dataclasses.replace(root, devices=root.devices.set('R', dev))

    kind = Kind(derive.AGENT, COALESCE, run, lambda delta, state: set())
    kind.name = 'agent'
    pipeline = Pipeline(env, net, [kind])

    def observer(*args):
        raise RuntimeError('post-publication failure')

    net.on_delta.append(observer)
    pipeline.mark(kind, {'R'}, 0)
    with pytest.raises(RuntimeError, match='post-publication failure') as info:
        env.run()
    assert published_failure(info.value)
    assert net.state.devices['R'].agents['count'] == 1
    assert not kind.retryable and not kind.pending
    net.on_delta.clear()
    pipeline.retry()
    env.run()
    assert net.state.devices['R'].agents['count'] == 1 and runs == [0]

    # a failure before publication is still retryable
    def boom(root, now, due):
        raise ValueError('prepare failed')

    kind.run = boom
    pipeline.mark(kind, {'R'}, 0)
    with pytest.raises(ValueError):
        env.run()
    assert kind.retryable
