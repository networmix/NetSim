"""Independent environments must run correctly in parallel threads.

An Environment is single-threaded by design and must not be shared between
threads. Separate environments share nothing, so on a free-threaded build
they run in parallel; on a GIL build they interleave. Either way the results
must equal a sequential run.
"""

import sys
from concurrent.futures import ThreadPoolExecutor

import pytest

import netsim


def _simulate(seed, n=2000):
    env = netsim.Environment()
    store = netsim.Store(env, capacity=3)
    res = netsim.PreemptiveResource(env, capacity=1)
    log = []

    def producer(env):
        for i in range(n):
            yield store.put((seed, i))

    def consumer(env):
        total = 0
        for _ in range(n):
            _, i = yield store.get()
            req = res.request(priority=i % 3)
            try:
                yield req
                yield env.timeout(1 + (i * seed) % 3)
                total += i * (seed + 1)
            except netsim.Interrupt:
                log.append(env.now)
            finally:
                res.release(req)
        return total

    def preemptor(env):
        while True:
            yield env.timeout(7)
            with res.request(priority=-1) as req:
                yield req
                yield env.timeout(1)

    env.process(producer(env))
    c = env.process(consumer(env))
    env.process(preemptor(env))
    env.run(until=c)
    return env.now, c.value, tuple(log)


@pytest.mark.parametrize('workers', [2, 4])
def test_environments_in_threads_match_sequential(workers):
    expected = [_simulate(s) for s in range(workers)]
    with ThreadPoolExecutor(workers) as ex:
        got = list(ex.map(_simulate, range(workers)))
    assert got == expected
    # The seeds produce different runs, so a mix-up between threads shows.
    assert len(set(expected)) == workers


def test_reports_gil_state():
    """Documents which build the suite ran on; the assertion cannot fail."""
    gil = getattr(sys, '_is_gil_enabled', lambda: True)()
    assert gil in (True, False)
