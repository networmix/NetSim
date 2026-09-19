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
