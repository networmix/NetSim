import dataclasses

import pytest

from netsim.model import state as st
from netsim.model.state import (
    Allocators,
    DeviceState,
    FloatArray,
    NetworkState,
    PMap,
    StateDelta,
    canon,
    check_name,
    diff_pmap,
    path_parse,
    path_str,
    record,
    tree_equal,
    validate_immutable,
)


@record
class _Leaf:
    x: int
    since: float = 0.0


class TestPMap:
    def test_copy_on_set_and_identity(self):
        m = PMap({'a': 1})
        m2 = m.set('b', 2)
        assert m2 is not m and 'b' not in m and m2['b'] == 2
        assert m2.set('b', 2) is m2  # identical object: no copy
        assert m2.remove('zzz') is m2
        assert m2.remove('a') == PMap({'b': 2})
        assert m.update({'a': 1}) is m
        assert m.update([('c', 3)]) == PMap({'a': 1, 'c': 3})

    def test_owns_backing_dict(self):
        src = {'a': 1}
        m = PMap(src)
        src['a'] = 2
        assert m['a'] == 1

    def test_equality_and_unhashable(self):
        assert PMap({'a': 1}) == PMap({'a': 1})
        assert PMap({'a': 1}) != PMap({'a': 2})
        with pytest.raises(TypeError):
            hash(PMap())

    def test_builder_copies_once(self):
        m = PMap({'a': 1, 'b': 2})
        b = m.builder()
        assert b.build() is m
        b.set('a', 1)  # identical value: still no copy
        assert b.build() is m
        b.set('c', 3)
        b.remove('b')
        out = b.build()
        assert out == PMap({'a': 1, 'c': 3}) and m == PMap({'a': 1, 'b': 2})

    def test_sorted_items(self):
        assert PMap({'b': 1, 'a': 2}).sorted_items() == [('a', 2), ('b', 1)]


class TestFloatArray:
    def test_views_are_fresh_and_release_safe(self):
        arr = FloatArray.from_iterable([1.0, 2.5])
        v = arr.view()
        assert v.readonly and v.tolist() == [1.0, 2.5]
        v.release()
        assert arr.tolist() == [1.0, 2.5] and arr[1] == 2.5 and len(arr) == 2
        with pytest.raises(TypeError):
            arr.view()[0] = 9.0
        assert FloatArray.zeros(3).tolist() == [0.0, 0.0, 0.0]
        assert arr == FloatArray.from_iterable([1.0, 2.5])
        with pytest.raises(TypeError):
            hash(arr)
        with pytest.raises(ValueError):
            FloatArray(b'123')


class TestImmutability:
    def test_accepts_tree_types(self):
        validate_immutable(NetworkState(devices=PMap({'R1': DeviceState('R1', 1)})))
        validate_immutable(
            (1, 'a', frozenset({2}), FloatArray.zeros(1), None, 2.0, b'x')
        )

    @pytest.mark.parametrize(
        'bad', [[1], {'a': 1}, {1}, bytearray(b'x'), memoryview(b'x')]
    )
    def test_rejects_mutable_containers(self, bad):
        with pytest.raises(TypeError):
            validate_immutable((bad,))

    def test_rejects_unfrozen_dataclass_and_unknown_objects(self):
        @dataclasses.dataclass
        class Mutable:
            x: int = 1

        with pytest.raises(TypeError):
            validate_immutable(Mutable())

        class Unknown:
            pass

        with pytest.raises(TypeError):
            validate_immutable(Unknown())


class TestPaths:
    def test_round_trip_with_escaping(self):
        p = ('devices', 'dc1/rack1', 'interfaces', 'eth1', '50%')
        s = path_str(p)
        assert s == 'devices/dc1%2Frack1/interfaces/eth1/50%25'
        assert path_parse(s) == p
        assert path_parse('') == ()

    def test_check_name(self):
        assert check_name('R1') == 'R1'
        for bad in ('a|b', 'a:b', 'a--b', 'a/b', ''):
            with pytest.raises(ValueError):
                check_name(bad)
        assert check_name('a/b', allow_slash=True) == 'a/b'


class TestAllocators:
    def test_counters_advance_functionally(self):
        a0 = Allocators()
        a1, g = a0.take_generation()
        a2, i = a1.take_ifindex('R1')
        a3, j = a2.take_ifindex('R1')
        a4, k = a3.take_ifindex('R2')
        assert (g, i, j, k) == (1, 1, 2, 1)
        assert a0.next_generation == 1 and a4.next_generation == 2
        assert a4.take_link_index()[1] == 0 and a4.take_mac_index()[1] == 0


class TestDelta:
    def test_identity_first_then_value(self):
        leaf = _Leaf(1)
        old = PMap({'a': leaf, 'b': _Leaf(2), 'c': _Leaf(3)})
        new = PMap({'a': leaf, 'b': _Leaf(2), 'c': _Leaf(4), 'd': _Leaf(5)})
        d = diff_pmap(old, new)
        assert d.added == ('d',) and d.removed == () and d.changed == ('c',)
        assert bool(d) and not diff_pmap(old, old)
        # by identity: an equal-but-distinct object counts as changed
        assert diff_pmap(old, new, by_identity=True).changed == ('b', 'c')

    def test_state_delta_sections_and_paths(self):
        dev = DeviceState('R1', 1)
        s0 = NetworkState(devices=PMap({'R1': dev}))
        s1 = dataclasses.replace(
            s0,
            version=1,
            devices=s0.devices.set(
                'R1', dataclasses.replace(dev, interfaces=PMap({'eth1': _Leaf(1)}))
            ),
            links=PMap({'L': _Leaf(0)}),
        )
        d = StateDelta(s0, s1)
        assert d.devices().changed == ('R1',)
        assert d.interfaces('R1').added == ('eth1',)
        assert d.links().added == ('L',)
        assert not d.placement_changed()
        assert set(d.changed_paths()) == {
            ('links', 'L'),
            ('devices', 'R1', 'interfaces', 'eth1'),
        }
        assert StateDelta(s0, s0).is_empty()

    def test_canon_and_tree_equal(self):
        old = _Leaf(1, since=5.0)
        assert canon(old, _Leaf(1, since=5.0)) is old
        assert canon(old, _Leaf(2)) == _Leaf(2)
        a = NetworkState(
            version=3,
            devices=PMap(
                {'R1': DeviceState('R1', 1, config=st.DeviceConfig(enabled_since=9.0))}
            ),
        )
        b = NetworkState(
            version=7,
            devices=PMap(
                {'R1': DeviceState('R1', 1, config=st.DeviceConfig(enabled_since=2.0))}
            ),
        )
        assert tree_equal(a, b)
        c = dataclasses.replace(b, devices=b.devices.set('R1', DeviceState('R1', 2)))
        assert not tree_equal(a, c)


def test_builder_detaches_from_published_map():
    from netsim.model.state import PMap

    b = PMap({'a': 1}).builder()
    b.set('a', 2)
    snap = b.build()
    b.set('a', 3)
    assert snap['a'] == 2
    assert b.build()['a'] == 3
    assert snap['a'] == 2


def test_traffic_class_change_is_not_empty():
    import dataclasses

    from tests.model.test_network import build_diamond

    net, R = build_diamond()
    delta = net.update(
        lambda s: dataclasses.replace(
            s, traffic_classes=s.traffic_classes.set('gold', ('gold', 46, 1))
        ),
        origin='tc',
    )
    assert delta is not None
    assert ('traffic_classes', 'gold') in delta.changed_paths()
