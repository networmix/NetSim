"""Throughput scenarios for the core loop and resources.

Run with ``pytest --benchmark-enable tests/test_benchmarks.py`` to get
timings; under the default ``--benchmark-disable`` each scenario runs once
as a smoke test.
"""

import pytest

import netsim

pytestmark = pytest.mark.benchmark


def _timeout_loop(n):
    env = netsim.Environment()

    def p(env):
        for _ in range(n):
            yield env.timeout(1)

    env.process(p(env))
    env.run()
    return env.now


def _store_pingpong(n):
    env = netsim.Environment()
    st = netsim.Store(env, capacity=1)

    def prod(env):
        for i in range(n):
            yield st.put(i)

    def cons(env):
        for _ in range(n):
            yield st.get()

    env.process(prod(env))
    env.process(cons(env))
    env.run()
    return len(st.items)


def _resource_backlog(n):
    env = netsim.Environment()
    res = netsim.Resource(env, capacity=1)

    def u(env):
        with res.request() as r:
            yield r
            yield env.timeout(1)

    for _ in range(n):
        env.process(u(env))
    env.run()
    return env.now


def _anyof_timeout(n):
    env = netsim.Environment()
    st = netsim.Store(env)

    def p(env):
        for _ in range(n):
            g = st.get()
            r = yield g | env.timeout(1)
            if g not in r:
                g.cancel()

    env.process(p(env))
    env.run()
    return env.now


def _interrupts(n):
    env = netsim.Environment()

    def victim(env):
        while True:
            try:
                yield env.timeout(1000)
            except netsim.Interrupt:
                pass

    def attacker(env, v):
        for _ in range(n):
            yield env.timeout(1)
            v.interrupt()

    v = env.process(victim(env))
    env.process(attacker(env, v))
    env.run(until=n + 1)
    return env.now


def test_timeout_loop(benchmark):
    assert benchmark(_timeout_loop, 20_000) == 20_000


def test_store_pingpong(benchmark):
    assert benchmark(_store_pingpong, 10_000) == 0


def test_resource_backlog(benchmark):
    assert benchmark(_resource_backlog, 10_000) == 10_000


def test_anyof_timeout(benchmark):
    assert benchmark(_anyof_timeout, 10_000) == 10_000


def test_interrupts(benchmark):
    assert benchmark(_interrupts, 10_000) == 10_001


# ---------------------------------------------------------------------------
# Network layer
# ---------------------------------------------------------------------------


def _clos_converge(leaves, spines):
    from tests.model.clos import build_clos

    net = build_clos(leaves, spines)
    net.converge()
    return net.placement.delivered_total


def _clos_failure_cascade(leaves, spines):
    import netsim
    from netsim.runtime import Simulation
    from tests.model.clos import build_clos

    net = build_clos(leaves, spines)
    env = netsim.Environment()
    sim = Simulation(env, net)
    links = list(net.links.values())
    for i, link in enumerate(links[:spines]):
        sim.at(10 + i, link.fail)
    sim.run_until(10 + spines + 1)
    return net.placement.delivered_total


def _lpm_lookups(n):
    import random

    from netsim.model.lpm import PrefixTable

    rng = random.Random(1)
    t = PrefixTable(32)
    for _ in range(20_000):
        plen = rng.choice([8, 16, 24, 32])
        t.insert(rng.getrandbits(32) & (0xFFFFFFFF ^ ((1 << (32 - plen)) - 1)), plen, 1)
    f = t.freeze()
    hits = 0
    for _ in range(n):
        if f.lookup(rng.getrandbits(32)) is not None:
            hits += 1
    return hits


def test_clos8_converge(benchmark):
    assert benchmark(_clos_converge, 8, 4) > 0


def test_clos8_failure_cascade(benchmark):
    assert benchmark(_clos_failure_cascade, 8, 4) > 0


def test_lpm_lookups(benchmark):
    benchmark(_lpm_lookups, 20_000)


@pytest.mark.slow
@pytest.mark.timeout(300)
def test_clos64_converge(benchmark):
    assert benchmark(_clos_converge, 64, 8) > 0


@pytest.mark.parametrize('fixture', ['diamond', 'clos8x4', 'clos16x4'])
def test_study_windows(benchmark, fixture):
    """Explicit windows; baseline construction/convergence is outside timing."""
    if benchmark.disabled:
        pytest.skip('study qualification requires --benchmark-enable')
    from dataclasses import asdict

    from netsim.runtime import FailureSet
    from netsim.study import Study
    from tests.model.clos import build_clos
    from tests.runtime.test_study import diamond

    network = (
        diamond()[0]
        if fixture == 'diamond'
        else build_clos(8 if fixture == 'clos8x4' else 16, 4)
    )
    study = Study(network, keep={'events': 1, 'records': 1})
    draws = [
        FailureSet(excluded_links=(name,)) for name in sorted(network.state.links)[:4]
    ]
    benchmark.extra_info['workload'] = asdict(study.describe(duration=0.5))
    benchmark.extra_info['iterations_per_call'] = len(draws)
    result = benchmark(study.iterations, draws, horizon=0.25, quiet=0.125)
    assert all(
        row['data']['netsim']['status'] == 'converged' for row in result.flow_results
    )
