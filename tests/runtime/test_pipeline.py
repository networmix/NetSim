"""Deadline bookkeeping and its cost, independently of network derivations."""

import dataclasses

import pytest

from netsim import Environment
from netsim.model.network import Network
from netsim.model.state import StateDelta
from netsim.runtime import pipeline as coordinator
from netsim.runtime.pipeline import COALESCE, DEBOUNCE, Kind, Pipeline, build_kinds


def pipeline(mode=DEBOUNCE, run=None):
    env = Environment()
    net = Network()
    calls = []

    def record(state, now, entities):
        calls.append((now, entities))
        return state

    kind = Kind(0, mode, record if run is None else run, lambda d, s: set())
    pipe = Pipeline(env, net, [kind])
    return env, kind, pipe, calls


@pytest.mark.parametrize('mode', [COALESCE, DEBOUNCE])
@pytest.mark.parametrize(
    'source,target,expected_round', [(3, 0, 1), (0, 3, 0), (3, 3, 1)]
)
def test_due_now_invalidation_respects_round_frontier(
    mode, source, target, expected_round
):
    env = Environment()
    calls = []

    def runner(offset):
        def run(state, now, entities):
            assert pipe.round_open(now)
            calls.append((offset, pipe.round_of(now)))
            if entities == ['initial']:
                pipe.schedule_entity(kinds[target], 'followup', now, now)
            return state

        return run

    kinds = {
        offset: Kind(offset, mode, runner(offset), lambda d, s: set())
        for offset in {source, target}
    }
    pipe = Pipeline(env, Network(), list(kinds.values()))
    pipe.schedule_entity(kinds[source], 'initial', 0, 0)
    env.run()
    assert calls == [(source, 0), (target, expected_round)]
    assert not pipe.round_open(0)
    assert not pipe.scheduled
    assert not pipe.round_end_scheduled
    assert all(not kind.pending for kind in kinds.values())


def test_interleaved_normal_event_respects_round_frontier():
    env = Environment()
    calls = []

    def early(state, now, entities):
        calls.append(('early', pipe.round_of(now)))
        return state

    def late(state, now, entities):
        calls.append(('late', pipe.round_of(now)))
        return state

    first = Kind(0, COALESCE, early, lambda d, s: set())
    last = Kind(3, COALESCE, late, lambda d, s: set())
    pipe = Pipeline(env, Network(), [first, last])
    pipe.schedule_entity(last, 'initial', 0, 0)
    env.step()  # The higher band ran, but ROUND_END has not run yet.
    normal = env.timeout(0)
    assert normal.callbacks is not None
    normal.callbacks.append(lambda _: pipe.mark(first, {'interleaved'}, env.now))
    env.run()
    assert calls == [('late', 0), ('early', 1)]


@pytest.mark.parametrize('trigger', ['successor', 'settled', 'future'])
def test_new_round_resets_frontier_for_forward_work(trigger):
    env = Environment()
    calls = []

    def early(state, now, entities):
        calls.append(('early', now, pipe.round_of(now)))
        pipe.mark(middle_kind, {'forward'}, now)
        return state

    def middle(state, now, entities):
        calls.append(('middle', now, pipe.round_of(now)))
        return state

    def late(state, now, entities):
        calls.append(('late', now, pipe.round_of(now)))
        if trigger != 'settled':
            pipe.schedule_entity(early_kind, 'backward', int(trigger == 'future'), now)
        return state

    def settled(now):
        if trigger == 'settled' and len(calls) == 1:
            pipe.mark(early_kind, {'after-close'}, now)

    early_kind = Kind(0, COALESCE, early, lambda d, s: set())
    middle_kind = Kind(2, COALESCE, middle, lambda d, s: set())
    late_kind = Kind(3, COALESCE, late, lambda d, s: set())
    pipe = Pipeline(
        env, Network(), [early_kind, middle_kind, late_kind], on_settled=settled
    )
    pipe.mark(late_kind, {'initial'}, 0)
    env.run()
    time, generation = (1, 0) if trigger == 'future' else (0, 1)
    assert calls == [
        ('late', 0, 0),
        ('early', time, generation),
        ('middle', time, generation),
    ]
    assert not pipe.scheduled
    assert not pipe.round_end_scheduled


def test_retry_does_not_move_frontier_back_before_intervening_band():
    env = Environment()
    calls = []

    def early(state, now, entities):
        calls.append(('early', pipe.round_of(now)))
        if len(calls) == 1:
            raise RuntimeError('boom')
        pipe.mark(middle_kind, {'backward-from-frontier'}, now)
        return state

    def middle(state, now, entities):
        calls.append(('middle', pipe.round_of(now)))
        return state

    def late(state, now, entities):
        calls.append(('late', pipe.round_of(now)))
        return state

    early_kind = Kind(0, COALESCE, early, lambda d, s: set())
    middle_kind = Kind(2, COALESCE, middle, lambda d, s: set())
    late_kind = Kind(3, COALESCE, late, lambda d, s: set())
    pipe = Pipeline(env, Network(), [early_kind, middle_kind, late_kind])
    pipe.mark(early_kind, {'initial'}, 0)
    pipe.mark(late_kind, {'initial'}, 0)
    with pytest.raises(RuntimeError, match='boom'):
        env.run()
    env.step()  # Advance to the higher band before retrying the failed one.
    pipe.retry()
    env.run()
    assert calls == [('early', 0), ('late', 0), ('early', 0), ('middle', 1)]


def test_device_disable_enumerates_ethernet_interfaces_once(monkeypatch):
    n = 2000
    net = Network()
    with net.batch():
        dev = net.add_device('A')
        for i in range(n):
            dev.add_ethernet(f'e{i}')
    before = net.state
    dev.configure(enabled=False)
    delta = StateDelta(before, net.state)
    original = coordinator._ethernets_of
    visits = []

    def counted(state, device):
        visits.append(len(state.devices[device].interfaces))
        return original(state, device)

    monkeypatch.setattr(coordinator, '_ethernets_of', counted)
    assert coordinator.carrier_affected(delta, net.state) == {
        ('A', f'e{i}') for i in range(n)
    }
    assert (len(visits), sum(visits)) == (1, n)


@pytest.mark.parametrize('n', [250, 4000])
def test_distinct_deadlines_do_not_scan_all_pending_work(n):
    class CountVisits(dict):
        visited = 0

        def items(self):
            self.visited += len(self)
            return super().items()

    env, kind, pipe, calls = pipeline()
    pending = CountVisits()
    kind.pending = pending
    for i in reversed(range(n)):
        pipe.schedule_entity(kind, i, i + 1, 0)
    env.run()
    assert calls == [(i + 1, [i]) for i in range(n)]
    # A complexity regression guard, without machine-dependent timing limits.
    assert pending.visited <= 2 * n
    assert not kind.pending
    assert not pipe.scheduled
    assert not pipe.round_end_scheduled


@pytest.mark.parametrize('mode', [COALESCE, DEBOUNCE])
@pytest.mark.parametrize('targets', [(5, 7, 3), (5, 3, 7), (5, 5), (5, 7, 5)])
def test_reschedule_earlier_later_and_same_deadline(mode, targets):
    env, kind, pipe, calls = pipeline(mode)
    for target in targets:
        pipe.schedule_entity(kind, 'entity', target, 0)
    env.run()
    expected = min(targets) if mode == COALESCE else targets[-1]
    assert calls == [(expected, ['entity'])]
    assert not kind.pending


def test_many_equal_deadlines_with_incomparable_entities():
    env, kind, pipe, calls = pipeline()
    entities = [e for i in range(1000) for e in (i, str(i), ('tuple', i))]
    for entity in reversed(entities):
        pipe.schedule_entity(kind, entity, 1, 0)
    assert len(pipe.scheduled) == 1
    env.run()
    assert calls == [(1, sorted(entities, key=repr))]
    assert not kind.pending


@pytest.mark.parametrize('reschedule', [False, True])
def test_stale_entry_after_cancellation(reschedule):
    env, kind, pipe, calls = pipeline()
    pipe.schedule_entity(kind, 'removed', 1, 0)
    pipe.schedule_entity(kind, 'live', 3, 0)
    del kind.pending['removed']
    if reschedule:
        pipe.schedule_entity(kind, 'removed', 2, 0)
    env.run()
    expected = [(2, ['removed'])] if reschedule else []
    assert calls == expected + [(3, ['live'])]
    assert not kind.pending


def test_carrier_deadline_after_interface_removal_is_a_noop():
    net = Network()
    dev = net.add_device('A')
    dev.add_ethernet('removed')
    env = Environment()
    ref = {}
    carrier = build_kinds(net, ref)[0]
    pipe = Pipeline(env, net, [carrier])
    ref['pipeline'] = pipe
    pipe.schedule_entity(carrier, ('A', 'removed'), 1, 0)
    pipe.schedule_entity(carrier, ('A', 'removed'), 2, 0)
    old = net.state.devices['A']
    net.update(
        lambda state: dataclasses.replace(
            state,
            devices=state.devices.set(
                'A',
                dataclasses.replace(old, interfaces=old.interfaces.remove('removed')),
            ),
        )
    )
    removed = net.state
    env.run()
    assert net.state is removed
    assert not carrier.pending


def test_tickets_are_per_kind_and_do_not_reset_after_draining():
    env, kind, pipe, _ = pipeline()
    pipe.schedule_entity(kind, 'entity', 1, 0)
    first = kind.pending['entity'][1]
    pipe.schedule_entity(kind, 'entity', 1, 0)
    assert kind.pending['entity'][1] == first
    env.run()
    pipe.schedule_entity(kind, 'entity', 2, env.now)
    assert kind.pending['entity'][1] > first
    _, independent, other, _ = pipeline()
    other.schedule_entity(independent, 'entity', 1, 0)
    assert independent.pending['entity'][1] == first


def test_heap_compacts_when_stale_entries_outnumber_live_work():
    env, kind, pipe, calls = pipeline()
    pipe.schedule_entity(kind, 'live', 200, 0)
    for target in range(1, 101):
        pipe.schedule_entity(kind, 'moving', target, 0)
        assert len(kind._heap) <= 2 * len(kind.pending)
    # Compaction does not cancel the old StageEvents in the engine queue.
    assert len(pipe.scheduled) == 101
    env.run()
    assert calls == [(100, ['moving']), (200, ['live'])]
    assert not kind._heap
    assert not pipe.scheduled
    assert not pipe.round_end_scheduled


def test_claiming_due_work_also_compacts_future_garbage():
    env, kind, pipe, calls = pipeline()
    for entity in range(10):
        pipe.schedule_entity(kind, entity, 100, 0)
        pipe.schedule_entity(kind, entity, 1, 0)
    assert len(kind._heap) == 2 * len(kind.pending)
    env.run(until=2)
    assert calls == [(1, list(range(10)))]
    assert not kind.pending
    assert not kind._heap
    # The future stale StageEvent is retained until its exact key is consumed.
    assert pipe.scheduled == {(kind.offset, 100, 0)}
    env.run()
    assert not pipe.scheduled


def test_stale_generation_keeps_the_newer_scheduled_keys():
    env, kind, pipe, calls = pipeline()
    pipe.schedule_entity(kind, 'entity', 1, 0)
    pipe._generation_for(1)
    pipe.round_gen = 1
    pipe._ensure_event(kind, 1, 1)
    env.step()  # stale kind event for generation 0
    assert calls == []
    assert pipe.scheduled == {(kind.offset, 1, 1)}
    assert pipe.round_end_scheduled == {1: 1}
    env.step()  # live kind event
    assert calls == [(1, ['entity'])]
    env.step()  # stale ROUND_END must not remove the current round's key
    assert pipe.round_open(1)
    assert pipe.round_end_scheduled == {1: 1}
    env.run()
    assert not pipe.round_open(1)
    assert not pipe.scheduled
    assert not pipe.round_end_scheduled


def test_retry_restores_the_entire_failed_batch():
    attempts = []

    def run(state, now, entities):
        attempts.append((now, entities))
        assert not kind.pending  # claimed before invoking the derivation
        if len(attempts) <= 2:
            raise RuntimeError('boom')
        return state

    env, kind, pipe, _ = pipeline(run=run)
    for entity in ('z', 'a', 2):
        pipe.schedule_entity(kind, entity, 1, 0)
    for _ in range(2):
        with pytest.raises(RuntimeError, match='boom'):
            env.run()
        assert set(kind.pending) == {'z', 'a', 2}
        pipe.retry()
    env.run()
    assert attempts == [(1, sorted(['z', 'a', 2], key=repr))] * 3
    assert not kind.pending
    assert not kind.retryable


@pytest.mark.parametrize('target', [1, 5])
def test_retry_preserves_new_requests_from_failed_run(target):
    attempts = []

    def run(state, now, entities):
        attempts.append((now, entities))
        if len(attempts) == 1:
            pipe.schedule_entity(kind, 'newer', target, now)
            raise RuntimeError('boom')
        return state

    env, kind, pipe, _ = pipeline(run=run)
    pipe.mark(kind, {'restore', 'newer'}, 1)
    with pytest.raises(RuntimeError, match='boom'):
        env.run()
    newer = kind.pending['newer']
    pipe.retry()
    assert kind.pending['newer'] == newer
    env.run()
    expected = [(1, ['newer', 'restore'])]
    expected += (
        [(1, ['newer', 'restore'])]
        if target == 1
        else [(1, ['restore']), (5, ['newer'])]
    )
    assert attempts == expected
    assert not kind.pending


@pytest.mark.parametrize('cancel', [False, True])
def test_retry_does_not_resurrect_completed_or_cancelled_newer_work(cancel):
    attempts = []

    def run(state, now, entities):
        attempts.append((now, entities))
        if len(attempts) == 1:
            pipe.schedule_entity(kind, 'entity', 5, now)
            raise RuntimeError('boom')
        return state

    env, kind, pipe, _ = pipeline(run=run)
    pipe.schedule_entity(kind, 'entity', 1, 0)
    with pytest.raises(RuntimeError, match='boom'):
        env.run()
    if cancel:
        del kind.pending['entity']
    env.run(until=6)
    pipe.retry()
    env.run()
    assert attempts == [(1, ['entity'])] + ([] if cancel else [(5, ['entity'])])


def test_retry_after_time_advances_moves_only_the_failed_deadline_to_now():
    attempts = []

    def run(state, now, entities):
        attempts.append((now, entities))
        if len(attempts) == 1:
            raise RuntimeError('boom')
        return state

    env, kind, pipe, _ = pipeline(run=run)
    pipe.schedule_entity(kind, 'entity', 1, 0)
    with pytest.raises(RuntimeError, match='boom'):
        env.run()
    env.run(until=3)
    pipe.retry()
    env.run()
    assert attempts == [(1, ['entity']), (3, ['entity'])]
