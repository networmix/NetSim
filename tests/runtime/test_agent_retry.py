"""Rejected agent receipts stay parked until an explicit retry."""

import pytest

from netsim import Environment
from netsim.model import contracts as c
from netsim.model import derive
from netsim.model.network import Network
from netsim.runtime import Simulation, pipeline
from netsim.runtime.agents import AgentBatchError
from tests.runtime.test_agents import Plugin, fixture


@pytest.mark.parametrize('count', [10, 1000])
def test_unrelated_run_never_visits_parked_rejections(monkeypatch, count):
    def reject(ctx):
        raise ValueError('needs operator correction')

    net = Network()
    with net.batch():
        for index in range(count):
            device = f'r{index:04}'
            net.add_device(device)
            net.add_agent(device, Plugin(c.ClientId('bad'), callback=reject))
        net.add_device('good')
        net.add_agent('good', Plugin(c.ClientId('ok')))
    sim = Simulation(
        Environment(), net, keep_roots=0, keep_deltas=0, keep_records=0, keep_events=0
    )
    with pytest.raises(AgentBatchError) as failure:
        sim.settle()
    assert len(failure.value.rejections) == count
    sim.settle()

    visited = []
    pop = pipeline.heappop

    def counted(heap):
        item = pop(heap)
        visited.append(item[2])
        return item

    monkeypatch.setattr(pipeline, 'heappop', counted)
    # A fresh arrival for a parked owner must stay queued without making its
    # rejected prefix runnable. A different owner may still run normally.
    assert sim.agents.deliver('r0000', 'bad', c.TimerFired(0, 'later'))
    assert sim.agents.deliver('good', 'ok', c.TimerFired(0, 'unrelated'))
    sim.settle()
    assert net.state.devices['good'].agents['ok'].runs == 2
    assert [entity for entity in visited if entity[1] == 'bad'] == []
    budget = sim.agents.budget()
    assert budget['inbox_entries'] == 1
    assert budget['pending_runs'] == 0 and budget['parked_runs'] == count
    assert sim.agents.kind()._heap == []


@pytest.mark.parametrize('action', ['reset', 'remove', 'replace'])
def test_retry_does_not_resurrect_parked_generation(action):
    seen = []
    bad = [True]

    def callback(ctx):
        seen.append((ctx.generation, ctx.inbox))
        if bad[0]:
            raise ValueError('reject')
        return c.AgentOutput()

    sim = fixture(Plugin(callback=callback))
    assert sim.agents.deliver('r', 'test', c.TimerFired(0, 'old'))
    with pytest.raises(AgentBatchError):
        sim.settle()
    bad[0] = False
    if action == 'reset':
        sim.reset_agent('r', 'test')
    else:
        sim.network.remove_agent('r', 'test')
        if action == 'replace':
            sim.network.add_agent('r', Plugin(callback=callback))
    sim.retry()
    sim.settle()
    if action != 'remove':
        assert len(seen) == 2
        assert seen[1][0] != seen[0][0]
        assert seen[1][1] == ()
    else:
        assert len(seen) == 1
    assert sim.agents.budget()['inbox_entries'] == 0
    assert sim.agents.budget()['parked_runs'] == 0


@pytest.mark.parametrize('delay', [0, 0.25])
def test_retry_keeps_initialized_state_rng_and_later_arrivals(delay):
    seen = []
    bad = [False]

    def callback(ctx):
        seen.append((ctx.agent_state, ctx.inbox, ctx.rng.random()))
        if bad[0]:
            raise ValueError('reject')
        return c.AgentOutput(state=(ctx.agent_state or 0) + 1)

    sim = fixture(Plugin(config=c.AgentConfig(run_delay=delay), callback=callback))
    sim.settle()
    admitted = sim.state.devices['r'].agents['test']
    first, later = c.TimerFired(0, 'first'), c.TimerFired(0, 'later')
    bad[0] = True
    assert sim.agents.deliver('r', 'test', first)
    with pytest.raises(AgentBatchError):
        sim.run_until(1)
    rejected = sim.state.devices['r'].agents['test']
    assert rejected.state is admitted.state and rejected.rng is admitted.rng
    assert sim.agents.deliver('r', 'test', later)
    sim.run_until(5)
    assert len(seen) == 2
    bad[0] = False
    sim.retry()
    sim.settle()
    assert seen[2] == seen[1]  # pre-run state, captured prefix and RNG
    sim.run_until(6)
    assert seen[3][:2] == (2, (later,))
    assert len(seen) == 4
    assert sim.agents.budget()['inbox_entries'] == 0


def test_unpublished_batch_parks_until_retry(monkeypatch):
    seen = []

    def callback(ctx):
        seen.append((ctx.inbox, ctx.rng.random()))
        return c.AgentOutput(state=1)

    sim = fixture(Plugin(callback=callback))
    first, later = c.TimerFired(0, 'first'), c.TimerFired(0, 'later')
    assert sim.agents.deliver('r', 'test', first)
    update = sim.network.update
    fail = [True]

    def reject_commit(fn, origin='op'):
        if origin[:2] == ('kind', 'agent') and fail[0]:
            fail[0] = False
            fn(sim.state)  # prepare the captured run, then reject before commit
            raise RuntimeError('commit failed')
        return update(fn, origin)

    monkeypatch.setattr(sim.network, 'update', reject_commit)
    with pytest.raises(RuntimeError, match='commit failed'):
        sim.settle()
    assert sim.agents.deliver('r', 'test', later)
    sim.network.add_agent('r', Plugin(c.ClientId('other')))
    sim.settle()
    assert len(seen) == 1
    assert sim.state.devices['r'].agents['test'].runs == 0
    assert sim.agents.budget()['parked_runs'] == 1
    assert sim.agents.kind()._heap == []
    sim.retry()
    sim.settle()
    assert seen[0] == seen[1]
    assert seen[2][0] == (later,)
    assert sim.state.devices['r'].agents['test'].runs == 2


def test_retry_from_later_band_uses_successor_round(monkeypatch):
    bad = [True]

    def callback(ctx):
        if bad[0]:
            raise ValueError('reject')
        return c.AgentOutput()

    sim = fixture(Plugin(callback=callback))
    with pytest.raises(AgentBatchError):
        sim.settle()
    sim.run_until(1)
    bad[0] = False
    fib_kind = sim.pipeline.by_offset[derive.FIB]
    run = fib_kind.run
    rounds = []

    def retry_from_fib(state, now, due):
        rounds.append(sim.pipeline.round_of(now))
        sim.retry()
        return run(state, now, due)

    monkeypatch.setattr(fib_kind, 'run', retry_from_fib)
    sim.pipeline.mark(fib_kind, {('r', 4)}, sim.env.now)
    sim.settle()
    records = [rec for rec in sim.timeline.records if rec.origin.name == 'agent']
    assert len(records) == 2
    assert records[-1].round > rounds[0]
    assert sim.state.devices['r'].agents['test'].runs == 1
