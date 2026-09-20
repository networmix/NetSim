import hashlib
import json
import random

import pytest

from netsim import Environment
from netsim.model.network import Network
from netsim.runtime import (
    Distribution,
    Draws,
    FailureSet,
    FaultEvent,
    Process,
    Schedule,
    Simulation,
)
from netsim.runtime.failures import derive_seed, resolve_groups
from tests.model.test_network import build_diamond


def small_net():
    net = Network()
    a, b = net.add_device('a'), net.add_device('b')
    link = net.add_p2p(a, 'eth0', b, 'eth0')
    return net, link.id


def test_random_overlapping_leases_match_interval_union():
    net, lid = small_net()
    sim = Simulation(
        Environment(), net, extract_events=False, keep_roots=0, keep_deltas=0
    )
    rng = random.Random(472)
    events = [FaultEvent((('link', lid),), 1, None)]
    # Permanent lease is on a device; links must actually restore in this test.
    events[0] = FaultEvent((('device', 'a'),), 40, None)
    for i in range(100):
        start = rng.randrange(30)
        entity = ('risk_group', 'rack') if i % 3 == 0 else ('link', lid)
        events.append(FaultEvent((entity,), start, rng.randrange(1, 5)))
    registry = sim.failures(Schedule(events), risk_groups={'rack': (('link', lid),)})
    for t in sorted(
        {e.start for e in events}
        | {e.start + e.duration for e in events if e.duration is not None}
    ):
        sim.run_until(t)
        count = sum(
            e.start <= t < e.start + e.duration
            for e in events
            if e.duration is not None
        )
        assert net.link(lid).state == int(count == 0)
        assert registry.counts.get(('link', lid), 0) == count
    assert not net.device('a').enabled
    assert registry.counts == {('device', 'a'): 1}


def test_groups_nested_overlaps_and_already_disabled_restore():
    net, lid = small_net()
    net.device('a').configure(enabled=False)
    net.link(lid).fail()
    sim = Simulation(Environment(), net)
    groups = {
        'outer': (('risk_group', 'inner'), ('device', 'a')),
        'inner': (('link', lid),),
    }
    first = sim.failures(
        Schedule([((('risk_group', 'outer'),), 1, 2)]), risk_groups=groups
    )
    second = sim.failures(
        Schedule([{'entities': [('link', lid)], 'start': 2, 'duration': 5}])
    )
    assert first is second
    sim.run_until(7)
    assert not net.device('a').enabled
    assert not net.link(lid).state
    assert not first.counts
    assert first.risk_groups['outer'] == (('device', 'a'), ('link', lid))
    with pytest.raises(ValueError, match='cannot change'):
        sim.failures(Schedule([]), risk_groups={})


def test_canonical_noop_leases_and_zero_duration():
    net, lid = small_net()
    sim = Simulation(Environment(), net)
    reg = sim.failures(Schedule([]))
    first = reg.acquire([('link', lid)])
    failed = net.state
    second = reg.acquire([('link', lid), ('link', lid)])
    assert net.state is failed
    reg.release(first)
    assert net.state is failed
    reg.release(second)
    sim.failures(Schedule([((('link', lid),), 1, 0)]))
    sim.run_until(1)
    assert net.link(lid).state == 1
    assert not reg.counts


def test_bad_schedule_is_validated_before_enqueue():
    net, lid = small_net()
    sim = Simulation(Environment(), net)
    with pytest.raises(KeyError):
        sim.failures(
            Schedule([((('link', lid),), 1, 1), ((('device', 'unknown'),), 2, 1)])
        )
    assert sim.env.peek() == float('inf')
    sim.run_until(3)
    with pytest.raises(ValueError, match='past'):
        sim.failures(Schedule([((('link', lid),), 1, 1)]))
    with pytest.raises(ValueError, match='horizon'):
        sim.failures(Process({}))
    with pytest.raises(ValueError, match='unknown risk group'):
        sim.failures(Schedule([((('risk_group', 'missing'),), 4, 1)]))


@pytest.mark.parametrize('kind', ['exponential', 'lognormal', 'weibull', 'constant'])
def test_renewal_unavailability_and_seed(kind):
    params = {
        ('link', 'first'): {
            'mtbf': 10,
            'mttr': 2,
            'ttf': {'kind': kind, 'shape': 1.5, 'sigma': 0.7},
            'ttr': {'kind': kind, 'shape': 2, 'sigma': 0.5},
        },
        ('device', 'second'): {'mtbf': 5, 'mttr': 5, 'ttf': kind, 'ttr': kind},
    }
    process = Process(params, seed=7)
    horizon = 100_000
    events = process.events(horizon)
    assert events == process.events(horizon)
    if kind != 'constant':
        assert events != Process(params, seed=8).events(horizon)
    for entity, expected in [(('link', 'first'), 2 / 12), (('device', 'second'), 0.5)]:
        downtime = sum(
            min(e.duration, horizon - e.start) for e in events if entity in e.entities
        )
        assert downtime / horizon == pytest.approx(expected, abs=0.015)
    # Adding/reordering entities must not perturb an existing stream.
    single = Process({('link', 'first'): params[('link', 'first')]}, seed=7)
    assert single.events(horizon) == tuple(
        e for e in events if ('link', 'first') in e.entities
    )


def test_distribution_means_and_seed_formula():
    expected = (
        int.from_bytes(hashlib.sha256(b'42:netsim:link:x').digest()[:4], 'big')
        & 0x7FFFFFFF
    )
    assert derive_seed(42, 'link', 'x') == expected
    rng = random.Random(42)
    for kind in ('exponential', 'lognormal', 'weibull', 'constant'):
        dist = Distribution(4, kind, sigma=0.5, shape=2)
        assert sum(dist.sample(rng) for _ in range(30000)) / 30000 == pytest.approx(
            4, rel=0.025
        )
    assert Distribution(0).sample(rng) == 0
    assert Distribution.from_spec(2) == Distribution(2, 'constant')
    dist = Distribution(3)
    assert Distribution.from_spec(dist) is dist


@pytest.mark.parametrize(
    'constructor',
    [
        lambda: FaultEvent((('wrong', 'x'),), 0),
        lambda: FaultEvent((), float('nan')),
        lambda: FaultEvent((), 0, -1),
        lambda: Distribution(-1),
        lambda: Distribution(1, 'other'),
        lambda: Distribution(1, shape=0),
        lambda: Distribution.from_spec('constant'),
        lambda: Distribution.from_spec(None),
        lambda: Process({('link', 'x'): {'mtbf': 0, 'mttr': 1}}),
        lambda: Process({}).events(float('inf')),
        lambda: FailureSet(occurrence_count=0),
        lambda: FailureSet(excluded_nodes=(1,)),
        lambda: FailureSet(risk_groups=('x',)).failure_id,
        lambda: resolve_groups({'a': [('risk_group', 'b')]}),
        lambda: resolve_groups(
            {'a': [('risk_group', 'b')], 'b': [('risk_group', 'a')]}
        ),
    ],
)
def test_invalid_sources(constructor):
    with pytest.raises(ValueError):
        constructor()


def test_enumeration_and_group_ids():
    net, _ = build_diamond()
    links = list(Draws.enumerate(net))
    assert len(links) == 5 and len({d.failure_id for d in links}) == 5
    assert len(list(Draws.enumerate(net, k=2))) == 10
    assert len(list(Draws.enumerate(net, 'devices'))) == 4
    group = {
        'rack': [('device', 'R2'), ('device', 'R3')],
        'parent': [('risk_group', 'rack')],
    }
    draws = list(Draws.enumerate(net, 'risk_groups', risk_groups=group))
    assert [d.excluded_nodes for d in draws] == [('R2', 'R3')] * 2
    assert list(Draws.enumerate(net, k=6)) == []
    with pytest.raises(ValueError):
        Draws.enumerate(net, k=0)
    with pytest.raises(ValueError):
        Draws.enumerate(net, 'oops')
    assert FailureSet(('z', 'a', 'z')).excluded_nodes == ('a', 'z')


def test_replay_selects_ids_preserves_weights_and_applies(tmp_path):
    net, lid = small_net()
    failure = FailureSet(excluded_links=(lid,), occurrence_count=3)
    row = {
        'failure_id': failure.failure_id,
        'failure_state': {'excluded_nodes': [], 'excluded_links': [lid]},
        'occurrence_count': 3,
    }
    doc = {'steps': {'mc': {'data': {'flow_results': [row]}}}}
    path = tmp_path / 'results.json'
    path.write_text(json.dumps(doc))
    assert tuple(Draws.replay(path, 'mc', failure.failure_id)) == (failure,)
    schedule = Schedule.replay(path, 'mc', [failure.failure_id], start=2, dwell=4)
    sim = Simulation(Environment(), net)
    sim.failures(schedule)
    sim.run_until(2)
    assert net.link(lid).state == 0
    sim.run_until(6)
    assert net.link(lid).state == 1
    with pytest.raises(ValueError, match='unknown failure_ids'):
        Draws.replay(doc, 'mc', ['missing'])
    row['failure_id'] = 'bad'
    with pytest.raises(ValueError, match='does not match'):
        Draws.replay(doc, 'mc')
    row['failure_state'] = None
    with pytest.raises(ValueError, match='failure_state'):
        Draws.replay(doc, 'mc')


def _raising_observer(*_):
    raise ValueError('observer failed after commit')


@pytest.mark.parametrize('operation', ['acquire', 'release'])
def test_lease_post_commit_observer_error_preserves_consistency(operation):
    net, lid = small_net()
    sim = Simulation(Environment(), net)
    registry = sim.failures(Schedule([]))
    members = [('device', 'a'), ('link', lid)]
    token = registry.acquire(members) if operation == 'release' else registry._next
    # Observe both the atomic tree transition and its corresponding lease state.
    seen = []
    net.on_delta.append(
        lambda *_: seen.append((net.device('a').enabled, net.link(lid).state))
    )
    net.on_delta.append(_raising_observer)
    with pytest.raises(ValueError, match='observer failed'):
        if operation == 'acquire':
            registry.acquire(members)
        else:
            registry.release(token)
    net.on_delta.remove(_raising_observer)
    if operation == 'acquire':
        assert seen == [(False, 0)]
        assert registry.counts == {('device', 'a'): 1, ('link', lid): 1}
        assert registry._leases[token] == tuple(members)
        registry.release(token)
    else:
        assert seen == [(True, 1)]
        # Retrying a committed release must not decrement another lease.
        registry.release(token)
    assert net.device('a').enabled and net.link(lid).state == 1
    assert registry.counts == registry._leases == registry._original == {}


@pytest.mark.parametrize('operation', ['acquire', 'release'])
def test_lease_pre_commit_validation_error_aborts_all_entities(monkeypatch, operation):
    import netsim.model.network as network_module

    net, lid = small_net()
    sim = Simulation(Environment(), net)
    registry = sim.failures(Schedule([]))
    members = [('device', 'a'), ('link', lid)]
    token = registry.acquire(members) if operation == 'release' else None
    before = net.state
    counts = dict(registry.counts)
    leases = dict(registry._leases)
    original = dict(registry._original)
    net.debug_validate = True
    with monkeypatch.context() as patch:
        patch.setattr(
            network_module,
            'validate_immutable',
            lambda _: (_ for _ in ()).throw(ValueError('abort before commit')),
        )
        with pytest.raises(ValueError, match='abort before commit'):
            if operation == 'acquire':
                registry.acquire(members)
            else:
                registry.release(token)
    assert net.state is before
    assert registry.counts == counts
    assert registry._leases == leases
    assert registry._original == original
    if token is not None:
        registry.release(token)
    else:
        registry.release(registry.acquire(members))
    assert net.device('a').enabled and net.link(lid).state == 1


def test_scheduled_committed_fault_arms_repair_even_when_observer_raises():
    net, lid = small_net()
    sim = Simulation(Environment(), net)
    registry = sim.failures(Schedule([((('link', lid),), 1, 3)]))
    net.on_delta.append(_raising_observer)
    with pytest.raises(ValueError, match='observer failed'):
        sim.run_until(1)
    net.on_delta.remove(_raising_observer)
    assert registry.counts == {('link', lid): 1}
    sim.run_until(4)
    assert net.link(lid).state == 1 and not registry.counts


def test_scheduled_pre_commit_abort_does_not_arm_repair(monkeypatch):
    import netsim.model.network as network_module

    net, lid = small_net()
    sim = Simulation(Environment(), net)
    registry = sim.failures(Schedule([((('link', lid),), 1, 3)]))
    net.debug_validate = True
    with monkeypatch.context() as patch:
        patch.setattr(
            network_module,
            'validate_immutable',
            lambda _: (_ for _ in ()).throw(ValueError('abort before commit')),
        )
        with pytest.raises(ValueError, match='abort before commit'):
            sim.run_until(1)
    assert not registry.counts and not registry._leases
    assert net.link(lid).state == 1
    sim.run_until(4)
    assert not registry.counts


def test_committed_release_retry_keeps_an_overlapping_lease():
    net, lid = small_net()
    sim = Simulation(Environment(), net)
    registry = sim.failures(Schedule([]))
    first = registry.acquire([('device', 'a'), ('link', lid)])
    second = registry.acquire([('link', lid)])
    net.on_delta.append(_raising_observer)
    with pytest.raises(ValueError, match='observer failed'):
        registry.release(first)
    net.on_delta.remove(_raising_observer)
    registry.release(first)
    assert registry.active_leases == (second,)
    assert registry.counts == {('link', lid): 1}
    assert net.device('a').enabled and net.link(lid).state == 0
    registry.release(second)
    assert net.link(lid).state == 1
    with pytest.raises(KeyError):
        registry.release(999)


def test_lease_change_inside_existing_batch_is_rejected_without_registry_mutation():
    net, lid = small_net()
    sim = Simulation(Environment(), net)
    registry = sim.failures(Schedule([]))
    with net.batch():
        with pytest.raises(RuntimeError, match='nested'):
            registry.acquire([('link', lid)])
    assert registry.active_leases == () and not registry.counts
    token = registry.acquire([('link', lid)])
    with net.batch():
        with pytest.raises(RuntimeError, match='nested'):
            registry.release(token)
    assert registry.active_leases == (token,) and registry.counts == {('link', lid): 1}
    registry.release(token)


@pytest.mark.parametrize('failure', ['abort', 'observer'])
def test_failed_acquire_preserves_preexisting_overlap(monkeypatch, failure):
    import netsim.model.network as network_module

    net, lid = small_net()
    sim = Simulation(Environment(), net)
    registry = sim.failures(Schedule([]))
    first = registry.acquire([('link', lid)])
    with monkeypatch.context() as patch:
        if failure == 'abort':
            net.debug_validate = True
            patch.setattr(network_module, 'validate_immutable', _raising_observer)
        else:
            net.on_delta.append(_raising_observer)
        with pytest.raises(ValueError, match='observer failed'):
            registry.acquire([('device', 'a'), ('link', lid)])
    if failure == 'observer':
        net.on_delta.remove(_raising_observer)
        (second,) = set(registry.active_leases) - {first}
        assert registry.counts == {('device', 'a'): 1, ('link', lid): 2}
        registry.release(second)
    assert registry.active_leases == (first,)
    assert registry.counts == {('link', lid): 1}
    assert net.device('a').enabled and net.link(lid).state == 0
    registry.release(first)
    assert net.link(lid).state == 1
