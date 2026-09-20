import dataclasses
import os
import pickle
import random
import subprocess
import sys

import pytest

from netsim.model.derive import bump_epochs
from netsim.model.state import (
    Allocators,
    DeviceState,
    NetworkState,
    PMap,
    StateDelta,
    canon,
    diff_pmap,
    tree_equal,
)


class CountingKey(str):
    probes = 0

    def __hash__(self):
        type(self).probes += 1
        return super().__hash__()


def test_one_device_delta_and_epochs_probe_only_changed_shards():
    keys = [CountingKey(f'r{i}') for i in range(10000)]
    old = NetworkState(devices=PMap((k, DeviceState(k, i)) for i, k in enumerate(keys)))
    dev = old.devices[keys[0]]
    new = dataclasses.replace(
        old,
        devices=old.devices.set(
            keys[0],
            dataclasses.replace(dev, config=dataclasses.replace(dev.config, seed=7)),
        ),
    )
    CountingKey.probes = 0
    bumped = bump_epochs(old, new)
    assert CountingKey.probes < 1000
    CountingKey.probes = 0
    delta = StateDelta(old, bumped)
    assert delta.devices().changed == (keys[0],)
    assert delta.changed_paths() == [('devices', keys[0], 'config')]
    assert not delta.is_empty()
    assert CountingKey.probes < 1000
    assert bumped.devices[keys[0]].resolver_input_epoch == PMap({4: 1, 6: 1})
    assert bump_epochs(old, bumped) is bumped


def test_large_maps_share_all_but_one_shard_including_allocators():
    base = PMap((f'r{i}', 1) for i in range(10000))
    changed = base.set('r0', 2)
    assert len(base._shards) > 1
    assert (
        sum(a is not b for a, b in zip(base._shards, changed._shards, strict=False))
        == 1
    )
    alloc = Allocators(next_ifindex=base)
    next_alloc, index = alloc.take_ifindex('r0')
    assert index == 1 and next_alloc.next_ifindex['r0'] == 2
    assert (
        sum(
            a is not b
            for a, b in zip(base._shards, next_alloc.next_ifindex._shards, strict=False)
        )
        == 1
    )


@pytest.mark.parametrize('size', [0, 512, 513, 4096])
def test_map_api_and_ownership(size):
    source = {f'k{i}': (i,) for i in range(size)}
    expected = dict(source)
    m = PMap(source)
    source['caller'] = (7,)
    assert dict(m) == expected
    assert list(m.items()) == list(expected.items())
    assert list(m.values()) == list(expected.values())
    assert m.keys() == expected.keys()
    assert m.keys() == set(expected)
    assert m.keys() & {'missing'} == set()
    assert len(m.items()) == len(m.values()) == size
    assert m.get('missing', 7) == 7
    assert m.remove('missing') is m
    assert m.update([]) is m
    assert m.update(m) is m
    assert m != object()
    assert pickle.loads(pickle.dumps(m)) == m
    with pytest.raises(KeyError):
        _ = m['missing']
    with pytest.raises(TypeError):
        hash(m)
    if size:
        assert ('k0', (0,)) in m.items()
        assert m.set('k0', m['k0']) is m
        changed = m.set('k0', (99,))
        assert m['k0'] == (0,)
        assert list(changed)[0] == 'k0'
        assert list(changed.remove('k0').set('k0', (100,)))[-1] == 'k0'


@pytest.mark.parametrize('key', [lambda i: i << 8, lambda i: f'router-{i}'])
def test_mixed_hash_spreads_aligned_integer_and_string_keys(key):
    m = PMap((key(i), i) for i in range(10000))
    assert len(m._shards) == 256
    assert sum(bool(s) for s in m._shards) == 256
    assert max(map(len, m._shards)) < 100


def test_builder_copies_each_touched_shard_once_and_detaches():
    base = PMap((i << 8, (i,)) for i in range(2000))
    b = base.builder()
    assert b.build() is base
    b.set(0, base[0])
    b.remove(-1)
    assert b.build() is base
    b.set(0, ('changed',))
    index = hash((0,)) & base._mask
    shard = b._shards[index]
    collision = next(k for k in base._shards[index] if k != 0)
    b.set(collision, ('also changed',))
    assert b._shards[index] is shard
    assert b[0] == b.get(0) == ('changed',)
    assert 0 in b and b.get(-1) is None and -1 not in b
    b.remove(256)
    b.set(-1, ('new',))
    first = b.build()
    assert b.build() is first
    assert first[0] == ('changed',) and 256 not in first
    b.set(0, ('later',))
    b.set(256, ('restored',))
    b.remove(-1)
    second = b.build()
    assert first[0] == ('changed',) and -1 in first and 256 not in first
    assert second[0] == ('later',) and -1 not in second and 256 in second
    assert list(first)[-1] == -1 and list(second)[-1] == 256
    assert base[0] == (0,) and 256 in base and -1 not in base


def test_promotion_at_publish_and_layout_fallbacks(monkeypatch):
    monkeypatch.setattr(PMap, '_FLAT_LIMIT', 3)
    flat = PMap([(2, (2,)), (0, (0,)), (1, (1,))])
    b = flat.builder()
    b.set(3, (3,))
    promoted = b.build()
    assert len(flat._shards) == 1 and len(promoted._shards) == 256
    assert diff_pmap(flat, promoted).added == (3,)
    assert diff_pmap(promoted, flat).removed == (3,)
    shrunk = promoted.remove(3)
    assert len(shrunk._shards) == 256  # no representation churn on shrink
    assert list(shrunk) == list(flat)
    assert shrunk == flat and flat == shrunk and tree_equal(flat, shrunk)
    assert not diff_pmap(flat, shrunk) and not diff_pmap(shrunk, flat)
    assert canon(flat, shrunk) is flat
    b.set(4, (4,))
    assert 4 not in promoted and 4 in b.build()
    monkeypatch.setattr(PMap, '_SHARD_COUNT', 512)
    different_layout = PMap(promoted)
    assert len(different_layout._shards) == 512
    assert different_layout == promoted and not diff_pmap(promoted, different_layout)
    assert diff_pmap(promoted, different_layout.set(0, ('x',))).changed == (0,)
    assert not tree_equal(flat, shrunk.set(0, ('x',)))
    assert not tree_equal(flat, promoted)


def test_identity_first_then_value_and_shard_aware_equality():
    @dataclasses.dataclass(frozen=True)
    class Value:
        value: int

    class TrackedDict(dict):
        comparisons = 0

        def __eq__(self, other):
            type(self).comparisons += 1
            return super().__eq__(other)

    original = PMap((i, Value(i)) for i in range(2000))
    old = PMap._from_parts(
        tuple(TrackedDict(s) for s in original._shards),
        original._order,
        len(original),
        original._next,
    )
    equal = old.set(0, Value(0))
    assert old == equal
    assert TrackedDict.comparisons == 1
    assert not diff_pmap(old, equal)
    assert diff_pmap(old, equal, by_identity=True).changed == (0,)
    new = equal.set(1, Value(999)).remove(2).set(-1, Value(-1))
    delta = diff_pmap(old, new)
    assert (delta.added, delta.removed, delta.changed) == ((-1,), (2,), (1,))
    assert diff_pmap(old, new, by_identity=True).changed == (0, 1)
    assert diff_pmap(None, old).added == tuple(range(2000))
    assert diff_pmap(old, None).removed == tuple(range(2000))


@pytest.mark.parametrize('field', ['devices', 'links', 'demands', 'traffic_classes'])
def test_state_delta_top_level_maps_skip_shared_shards(field):
    keys = [CountingKey(f'k{i}') for i in range(10000)]
    m = PMap((k, DeviceState(k, i)) for i, k in enumerate(keys))
    old = dataclasses.replace(NetworkState(), **{field: m})
    new = dataclasses.replace(old, **{field: m.set(keys[0], DeviceState(keys[0], -1))})
    CountingKey.probes = 0
    delta = StateDelta(old, new)
    assert getattr(delta, field)().changed == (keys[0],)
    assert not delta.is_empty()
    assert CountingKey.probes < 1000


def test_builder_and_persistent_edits_match_dict_with_retained_roots(monkeypatch):
    monkeypatch.setattr(PMap, '_FLAT_LIMIT', 8)
    rng = random.Random(47)
    reference = {}
    m = PMap()
    snapshots = []
    for _ in range(80):
        snapshots.append((m, dict(reference)))
        b = m.builder()
        for _ in range(10):
            key = rng.randrange(80)
            if rng.randrange(4):
                value = (rng.randrange(100),)
                b.set(key, value)
                reference[key] = value
            else:
                b.remove(key)
                reference.pop(key, None)
        m = b.build()
        assert list(m.items()) == list(reference.items())
        assert m == PMap(reference)
        assert m.sorted_items() == sorted(reference.items())
    for snap, expected in snapshots:
        assert list(snap.items()) == list(expected.items())


def test_iteration_handles_incomparable_keys_and_is_hash_seed_independent(monkeypatch):
    monkeypatch.setattr(PMap, '_FLAT_LIMIT', 0)
    keys = [3, 'hello', (1, 'x'), frozenset({2})]
    m = PMap((k, i) for i, k in enumerate(keys))
    assert list(m) == keys
    assert list(m.keys()) == keys
    assert list(m.values()) == list(range(4))
    script = """
from netsim.model.state import PMap
m = PMap((f'r{i}', i) for i in range(700))
m = m.remove('r10').set('r10', -1).set('r1', 42)
assert list(m) == [f'r{i}' for i in range(700) if i != 10] + ['r10']
print(list(m.items()))
"""
    outputs = [
        subprocess.check_output(
            [sys.executable, '-c', script],
            env={**os.environ, 'PYTHONHASHSEED': str(seed)},
        )
        for seed in (1, 937)
    ]
    assert outputs[0] == outputs[1]


def test_placement_cache_comparison_is_shard_scoped():
    from netsim.model.flows import DepToken, _same_cache
    from tests.model.test_network import build_diamond

    net, _ = build_diamond()
    net.add_demand('d', 'R1', '10.0.0.4', 1e6)
    net.converge()
    keys = [CountingKey(f'r{i}') for i in range(10000)]
    token = DepToken(None, None, None, None, ())
    deps = PMap((k, token) for k in keys)
    classes = PMap((k, object()) for k in keys)
    old = dataclasses.replace(net.placement, classes=classes, deps=deps)
    new = dataclasses.replace(old, deps=deps.set(keys[0], dataclasses.replace(token)))
    CountingKey.probes = 0
    assert _same_cache(old, new)
    assert CountingKey.probes < 1000
    assert not _same_cache(
        old, dataclasses.replace(new, classes=classes.set(keys[0], object()))
    )
    assert not _same_cache(old, dataclasses.replace(new, deps=deps.remove(keys[0])))
    assert not _same_cache(
        old,
        dataclasses.replace(
            new, deps=deps.set(keys[0], dataclasses.replace(token, config=object()))
        ),
    )


def test_timeline_route_comparison_does_not_enumerate_whole_row_map(monkeypatch):
    from netsim.model.contracts import STATIC
    from netsim.model.routing import Nexthop, RibState, Route, rib_apply
    from netsim.runtime.timeline import RouteEvent, _route_events

    rows = tuple(
        Route((i, 32), 4, STATIC, 1, (Nexthop.blackhole(),)) for i in range(2000)
    )
    rib = rib_apply(RibState.empty(4), add=rows)
    rib2 = rib_apply(rib, add=(dataclasses.replace(rows[0], metric=8),))
    old_dev = DeviceState('r', 1, ribs=PMap({4: rib}))
    new_dev = dataclasses.replace(old_dev, ribs=PMap({4: rib2}))
    old = NetworkState(devices=PMap({'r': old_dev}))
    new = dataclasses.replace(old, devices=PMap({'r': new_dev}))
    map_type = type(rib.shards[32])
    keys = map_type.keys

    def guarded_keys(m):
        assert m is not rib.shards[32] and m is not rib2.shards[32]
        return keys(m)

    monkeypatch.setattr(map_type, 'keys', guarded_keys)
    events = []
    _route_events(
        lambda kind, **kw: events.append((kind, kw)),
        old_dev,
        new_dev,
        'r',
        StateDelta(old, new),
    )
    assert len(events) == 1
    assert events[0][0] is RouteEvent
    assert events[0][1]['metric'] == 8
