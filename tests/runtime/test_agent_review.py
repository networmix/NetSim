"""Adversarial Gate C review regressions, with real publication boundaries."""

import dataclasses
import inspect
from dataclasses import dataclass, replace

import pytest

from netsim import Environment
from netsim.model import contracts as c
from netsim.model import nht, srv6
from netsim.model.network import published_failure
from netsim.model.routing import Nexthop, Route
from netsim.model.state import NetworkState, PMap, StateDelta
from netsim.runtime import agents
from netsim.runtime.events import Stats
from netsim.runtime.pipeline import ConvergenceError
from netsim.runtime.simulation import Simulation
from tests.model.test_network import A, build_diamond
from tests.model.test_policies import diamond, policy
from tests.runtime.test_agents import Plugin, fixture, row


@pytest.mark.parametrize('malformed', [False, True])
def test_commit_time_sr_validation_rejects_only_its_journal(malformed):
    net, routers = diamond(compressed=False)
    policy(routers)
    bad = True
    calls = []
    claim = c.RemoteSid(A('2001:db8:ffff::1'), 128, srv6.END, owner='remote')
    view = c.SrDbView(
        (claim, replace(claim, behavior=srv6.END_DT46))
        if not malformed
        else (claim, replace(claim, sid='malformed'))
    )

    def callback(ctx):
        calls.append((ctx.agent, ctx.inbox, ctx.rng.random()))
        return c.AgentOutput(
            state=1,
            srdb_view=view if ctx.agent == 'b' and bad else c.SrDbView(),
            route_ops=(c.RouteOp(4, add=(row(ctx.client),)),),
        )

    for name in ('a', 'b'):
        net.add_agent('R1', Plugin(c.ClientId(name), callback=callback))
    routers['R1'].configure(srdb_source=('agent', 'b'))
    sim = Simulation(Environment(), net)
    first = c.Delivery(0, 'captured', connection=1)
    sim.agents.deliver('R1', 'b', first)
    with pytest.raises(agents.AgentBatchError):
        sim.settle()
    a, b = (sim.state.devices['R1'].agents[name] for name in ('a', 'b'))
    assert a.runs == 1 and a.receipt.status == c.RECEIPT_PUBLISHED
    assert b.runs == 0 and b.receipt.status == c.RECEIPT_REJECTED
    assert b.state is b.rng is None
    assert sim.state.devices['R1'].ribs[4].rows_of(a.client)
    assert not sim.state.devices['R1'].ribs[4].rows_of(b.client)
    later = c.Delivery(0, 'later', connection=1)
    sim.agents.deliver('R1', 'b', later)
    sim.settle()  # successful FIB work can generate a fresh CAUSE_ROUTES for a
    a_calls = [call[0] for call in calls].count('a')
    bad = False
    sim.retry()
    sim.settle()
    assert [call[0] for call in calls].count('a') == a_calls
    retries = [call for call in calls if call[0] == 'b']
    assert retries[0][1:] == retries[1][1:]  # prefix and RNG preserved
    assert retries[2][1] == (later,)
    assert sim.agents.budget()['inbox_entries'] == 0


@pytest.mark.parametrize('failing_step', ['stats', 'transport'])
def test_finalization_failure_consumes_every_receipt_and_finishes_peers(
    monkeypatch, failing_step
):
    calls, sent, flushed = [], [], []

    def callback(ctx):
        calls.append(ctx.agent)
        return c.AgentOutput(
            state=1,
            messages=(c.Message(1, ctx.agent),),
            stats=((ctx.agent, 1), ('after-' + ctx.agent, 1)),
            timers=(c.TimerOp('armed', 2),),
        )

    sim = fixture(*(Plugin(c.ClientId(name), callback=callback) for name in ('a', 'b')))
    for name in ('a', 'b'):
        sim.agents.deliver('r', name, c.Delivery(0, 'captured', connection=1))

    def stat(name, time, value):
        # All accepted prefixes have been consumed before ANY finalization.
        assert sim.agents.budget()['inbox_entries'] == 0
        flushed.append(name)
        if name == 'a' and failing_step == 'stats':
            raise RuntimeError('finalization failed')

    def send(*args):
        sent.append(args[-1].payload)
        if sent[-1] == 'a' and failing_step == 'transport':
            raise RuntimeError('finalization failed')

    monkeypatch.setattr(sim.stats, 'add', stat)
    monkeypatch.setattr(sim.transport, 'send_message', send)
    with pytest.raises(RuntimeError, match='finalization failed') as caught:
        sim.settle()
    assert published_failure(caught.value)
    assert flushed == ['a', 'after-a', 'b', 'after-b'] and sent == ['a', 'b']
    assert sim.agents.budget()['armed_timers'] == 2
    assert all(n.runs == 1 for n in sim.state.devices['r'].agents.values())
    sim.retry()
    sim.settle()
    assert calls == ['a', 'b'] and sent == ['a', 'b']


@pytest.mark.parametrize(
    'entry',
    [
        c.TimerFired(0, 'timer'),
        c.Rejection(0, c.OVERFLOW),
        c.SessionEvent(0, 1, c.ESTABLISHED),
    ],
)
def test_control_allowance_is_separate_and_exhaustion_is_explicit(entry):
    sim = fixture(Plugin(config=c.AgentConfig(run_delay=1, inbox_limit=1)))
    sim.settle()
    assert sim.agents.deliver('r', 'test', c.Delivery(0, 'data', connection=1))
    assert not sim.agents.deliver('r', 'test', c.Delivery(0, 'more data', connection=1))
    assert sim.agents.deliver('r', 'test', entry)
    with pytest.raises(RuntimeError, match='control inbox'):
        sim.agents.deliver('r', 'test', entry)
    assert sim.agents.budget()['inbox_entries'] == 2
    sim.run_until(1)
    assert sim.agents.deliver('r', 'test', entry)


def test_transport_control_overflow_propagates_out_of_run_until():
    # Real transport rejects two messages to missing connections. The second
    # control entry cannot fit: it must terminate the run explicitly.
    def callback(ctx):
        return c.AgentOutput(messages=(c.Message(1, 'one'), c.Message(2, 'two')))

    sim = fixture(Plugin(config=c.AgentConfig(inbox_limit=1), callback=callback))
    with pytest.raises(RuntimeError, match='control inbox') as caught:
        sim.run_until(1)
    assert published_failure(caught.value)
    assert sim.state.devices['r'].agents['test'].runs == 1


def test_prospective_nht_is_detached_and_excludes_the_candidate():
    net, routers = build_diamond()
    key = c.NhtKey(c.ClientId('test'), 4, A('192.0.2.1'))
    net.register_client(c.ClientProfile(key.owner, 115))
    candidate = Route(
        (A('192.0.2.0'), 24), 4, key.owner, 115, (Nexthop.recursive(key.address, 4),)
    )
    routers['R1'].rib_client(key.owner).add_routes((candidate,))
    contexts = []

    def callback(ctx):
        contexts.append(ctx)
        result = ctx.resolve(key, exclude_rows=frozenset({candidate.key}))
        assert not result.eligible and result.reason == 'SELF_COVERED'
        assert result.via_prefix == candidate.prefix
        return c.AgentOutput(state=result)

    net.add_agent('R1', Plugin(callback=callback))
    sim = Simulation(Environment(), net)
    sim.settle()
    ctx = contexts[-1]
    assert ctx.installed(key) == nht.installed(sim.state, 'R1', key)
    connected = replace(
        key,
        address=A('10.1.12.1'),
        interface='Po1',
        interface_generation=ctx.interfaces['Po1'].generation,
    )
    assert ctx.installed(connected) == nht.installed(sim.state, 'R1', connected)
    stale = replace(connected, interface_generation=connected.interface_generation + 1)
    assert ctx.installed(stale) == nht.installed(sim.state, 'R1', stale)
    assert ctx.installed(stale).prefix is None
    # Traverse slots AND bound-method closures, including the query service.
    seen, stack = set(), [ctx]
    while stack:
        obj = stack.pop()
        if id(obj) in seen:
            continue
        seen.add(id(obj))
        assert not isinstance(obj, NetworkState)
        assert not hasattr(obj, 'carrier_raw')
        if inspect.ismethod(obj):
            stack.append(obj.__self__)
            stack.extend(cell.cell_contents for cell in obj.__func__.__closure__ or ())
        elif dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            stack.extend(getattr(obj, f.name) for f in dataclasses.fields(obj))
            stack.extend(
                getattr(obj, name)
                for name in dir(type(obj))
                if not name.startswith('__') and inspect.ismethod(getattr(obj, name))
            )
        elif isinstance(obj, PMap):
            stack.extend(obj.values())
        elif isinstance(obj, (tuple, list)):
            stack.extend(obj)
        elif hasattr(obj, '__slots__'):
            stack.extend(getattr(obj, s) for s in obj.__slots__ if hasattr(obj, s))


def test_fresh_equal_state_is_a_change_without_structural_comparison():
    comparisons = []

    @dataclass(frozen=True)
    class Opaque:
        value: int

        def __eq__(self, other):
            comparisons.append(other)
            return isinstance(other, Opaque) and self.value == other.value

    sim = fixture(
        Plugin(
            paths=(('agents', 'test', 'state'),),
            callback=lambda ctx: c.AgentOutput(state=Opaque(1)),
        ),
        max_rounds_per_timestamp=3,
    )
    with pytest.raises(ConvergenceError):
        sim.settle()
    assert comparisons == []
    # No bookkeeping difference is needed to classify an opaque identity change.
    old = sim.state
    dev = old.devices['r']
    node = replace(dev.agents['test'], state=Opaque(1))
    new = replace(
        old,
        devices=old.devices.set('r', replace(dev, agents=dev.agents.set('test', node))),
    )
    assert ('devices', 'r', 'agents', 'test') in StateDelta(old, new).changed_paths()
    assert comparisons == []


def test_normal_state_admission_is_shallow_and_identity_return_is_not_walked(
    monkeypatch,
):
    reads, walks = [], []

    @dataclass(frozen=True)
    class Leaf:
        value: int

        def __getattribute__(self, name):
            if name == 'value':
                reads.append(name)
            return object.__getattribute__(self, name)

    state = tuple(Leaf(i) for i in range(1000))
    view = c.SrDbView((c.RemoteSid(A('2001:db8::1'), 128, srv6.END, owner='remote'),))
    real = agents.validate_immutable

    def validate(obj, path='root'):
        walks.append(path)
        real(obj, path)

    sim = fixture(
        Plugin(
            callback=lambda ctx: c.AgentOutput(
                state=state if ctx.agent_state is None else ctx.agent_state,
                srdb_view=view,
            )
        )
    )
    monkeypatch.setattr(agents, 'validate_immutable', validate)
    sim.settle()
    assert reads == walks == []
    sim.agents.deliver('r', 'test', c.TimerFired(0, 'tick'))
    walks.clear()
    sim.settle()
    assert reads == walks == []
    assert sim.state.devices['r'].agents['test'].state is state


@pytest.mark.parametrize('field', ['state', 'srdb_view'])
def test_debug_validation_rejects_nested_mutable_state_without_losing_peer(field):
    value = ([],) if field == 'state' else c.SrDbView(sids=([],))
    sim = fixture(
        Plugin(c.ClientId('a')),
        Plugin(c.ClientId('b'), callback=lambda ctx: c.AgentOutput(**{field: value})),
    )
    sim.network.debug_validate = True
    with pytest.raises(agents.AgentBatchError, match='mutable list'):
        sim.settle()
    assert sim.state.devices['r'].agents['a'].runs == 1
    assert sim.state.devices['r'].agents['b'].runs == 0


def test_stats_default_retention_is_bounded_and_aggregates_are_exact():
    def callback(ctx):
        ctx.stats.add('ticks', ctx.now)
        return c.AgentOutput(timers=(c.TimerOp('tick', 1),))

    sim = fixture(
        Plugin(callback=callback),
        keep_roots=0,
        keep_deltas=0,
        keep_events=0,
        keep_records=0,
    )
    sim.run_until(1000)
    assert len(sim.stats.counters['ticks']) <= 1
    aggregate = sim.stats.aggregates['ticks']
    assert (
        aggregate.count,
        aggregate.sum,
        aggregate.min,
        aggregate.max,
        aggregate.last,
        aggregate.last_time,
    ) == (1001, 500500, 0, 1000, 1000, 1000)
    assert not sim.stats.samples
    assert sum(value for _, value in sim.stats.counters['ticks']) == aggregate.sum


def test_stats_opt_in_samples_are_bounded_independently_of_aggregates():
    stats = Stats(samples=3)
    for time in range(10):
        stats.add('key', time, time - 5)
    assert list(stats.samples['key']) == [(7, 2), (8, 3), (9, 4)]
    assert stats.samples['key'].maxlen == 3
    assert stats.aggregates['key'].count == 10
    assert stats.aggregates['key'].sum == -5
    assert sum(value for _, value in stats.counters['key']) == -5


@pytest.mark.parametrize('samples', [-1, True, 1.5, None])
def test_stats_rejects_unbounded_or_invalid_sample_limits(samples):
    with pytest.raises(ValueError, match='non-negative integer'):
        Stats(samples=samples)


def test_simulation_can_opt_in_to_bounded_stat_samples():
    sim = fixture(
        Plugin(callback=lambda ctx: c.AgentOutput(stats=(('x', 2),))), stats_samples=2
    )
    sim.settle()
    assert list(sim.stats.samples['x']) == [(0, 2)]


@pytest.mark.parametrize('observer', [False, True])
def test_all_finalization_errors_are_reported_without_replaying_receipts(
    monkeypatch, observer
):
    calls, sent = [], []

    def callback(ctx):
        calls.append(ctx.agent)
        return c.AgentOutput(
            stats=((ctx.agent, 1),), messages=(c.Message(1, ctx.agent),)
        )

    sim = fixture(*(Plugin(c.ClientId(name), callback=callback) for name in ('a', 'b')))

    def stat(name, *_):
        raise ValueError('stat-' + name)

    def send(*args):
        sent.append(args[-1].payload)
        raise RuntimeError('send-' + args[-1].payload)

    def observe(_now, origin, _delta):
        if origin[:2] == ('kind', 'agent'):
            raise ValueError('observer')

    monkeypatch.setattr(sim.stats, 'add', stat)
    monkeypatch.setattr(sim.transport, 'send_message', send)
    if observer:
        sim.network.on_delta.append(observe)
    with pytest.raises(ExceptionGroup) as caught:
        sim.settle()
    assert published_failure(caught.value)
    leaves, stack = [], [caught.value]
    while stack:
        error = stack.pop()
        if isinstance(error, ExceptionGroup):
            stack.extend(error.exceptions)
        else:
            leaves.append(str(error))
    assert sorted(leaves) == sorted(
        ['stat-a', 'send-a', 'stat-b', 'send-b'] + (['observer'] if observer else [])
    )
    sim.retry()
    sim.settle()
    assert calls == sent == ['a', 'b']


def test_timer_finalization_failure_does_not_skip_other_outboxes(monkeypatch):
    calls, sent, attempts = [], [], []

    def callback(ctx):
        calls.append(ctx.agent)
        return c.AgentOutput(
            timers=(c.TimerOp('one', 1), c.TimerOp('two', 2)),
            messages=(c.Message(1, ctx.agent),),
        )

    sim = fixture(*(Plugin(c.ClientId(name), callback=callback) for name in ('a', 'b')))
    timeout = sim.env.timeout

    def fail_once(delay, *args, **kwargs):
        attempts.append(delay)
        if len(attempts) == 1:
            raise RuntimeError('timer creation')
        return timeout(delay, *args, **kwargs)

    monkeypatch.setattr(sim.env, 'timeout', fail_once)
    monkeypatch.setattr(
        sim.transport, 'send_message', lambda *args: sent.append(args[-1].payload)
    )
    with pytest.raises(RuntimeError, match='timer creation') as caught:
        sim.settle()
    assert published_failure(caught.value)
    assert attempts == [1, 2, 1, 2]
    assert sim.agents.budget()['armed_timers'] == 3
    sim.retry()
    sim.settle()
    assert calls == sent == ['a', 'b']


@pytest.mark.parametrize('unrelated', [10, 1000])
def test_context_uses_only_the_owners_connection_index(monkeypatch, unrelated):
    contexts = []
    sim = fixture(Plugin(callback=lambda ctx: contexts.append(ctx) or c.AgentOutput()))
    sim.settle()
    gen = sim.agents.generation('r', 'test')
    endpoint = c.Endpoint(6, A('fe80::1'), 179, 'local')
    own = c.ConnectionState(
        1,
        'r',
        'test',
        endpoint,
        gen,
        b_local=replace(endpoint, scope='remote'),
        state=c.DOWN,
    )
    connections = PMap(
        (i, c.ConnectionState(i, 'elsewhere', 'p', endpoint, 1, state=c.DOWN))
        for i in range(2, unrelated + 2)
    ).set(1, own)
    sim.network.update(
        lambda state: replace(
            state, transport=c.TransportState(connections=connections)
        )
    )
    seen = []

    def owners(device, agent, generation):
        assert (device, agent, generation) == ('r', 'test', gen)
        return (1,)

    def counters(cid):
        seen.append(cid)
        return ({'messages': 2, 'bytes': 5}, {'messages': 3, 'bytes': 9})

    monkeypatch.setattr(sim.transport, 'connections_of', owners, raising=False)
    monkeypatch.setattr(sim.transport, 'connection_counters', counters, raising=False)
    monkeypatch.setattr(
        sim.transport, 'budget', lambda: pytest.fail('global budget scan')
    )
    sim.agents.deliver('r', 'test', c.TimerFired(0, 'tick'))
    sim.settle()
    assert tuple(contexts[-1].connections) == (1,) and seen == [1]
    view = contexts[-1].connections[1]
    assert (
        view.remote.scope == 'local'
        and view.queued_messages == 2
        and view.queued_bytes == 5
    )
