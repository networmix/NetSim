import ipaddress
import random

import pytest

from netsim.model.lpm import FrozenPrefixTable, PrefixTable


def _brute(prefixes, addr_int, bits, *, exclude=(), min_len=0):
    """Reference LPM over ``ipaddress`` objects."""
    best = None
    a = (
        ipaddress.ip_address(addr_int)
        if bits == 32
        else ipaddress.IPv6Address(addr_int)
    )
    for net_int, plen, value in prefixes:
        if plen < min_len or (net_int, plen) in exclude:
            continue
        net = (
            ipaddress.IPv4Network((net_int, plen))
            if bits == 32
            else ipaddress.IPv6Network((net_int, plen))
        )
        if a in net and (best is None or plen > best[1]):
            best = (net_int, plen, value)
    return best


def _random_prefixes(rng, bits, n):
    out = {}
    for _ in range(n):
        plen = rng.choice(
            [0, 8, 16, 24, 30, 31, 32] if bits == 32 else [0, 32, 48, 64, 127, 128]
        )
        net = rng.getrandbits(bits) & (((1 << bits) - 1) ^ ((1 << (bits - plen)) - 1))
        out[(net, plen)] = f'{net:x}/{plen}'
    return [(net, plen, v) for (net, plen), v in out.items()]


class TestPrefixTable:
    @pytest.mark.parametrize('bits', [32, 128])
    def test_matches_brute_force(self, bits):
        rng = random.Random(1234)
        prefixes = _random_prefixes(rng, bits, 200)
        table = PrefixTable(bits)
        for net, plen, v in prefixes:
            table.insert(net, plen, v)
        assert len(table) == len(prefixes)
        for _ in range(500):
            addr = rng.getrandbits(bits)
            # Bias half the probes toward existing prefixes so long matches happen.
            if rng.random() < 0.5:
                net, plen, _ = rng.choice(prefixes)
                addr = net | (rng.getrandbits(bits) & ((1 << (bits - plen)) - 1))
            assert table.lookup(addr) == _brute(prefixes, addr, bits)
            assert table.lookup(addr, min_len=8) == _brute(
                prefixes, addr, bits, min_len=8
            )

    def test_exclude_and_iteration_order(self):
        t = PrefixTable(32)
        t.insert(0, 0, 'default')
        t.insert(0x0A000000, 8, '10/8')
        t.insert(0x0A010000, 16, '10.1/16')
        t.insert(0x0A010203, 32, 'host')
        addr = 0x0A010203
        assert [v for _, _, v in t.lookup_iter(addr)] == [
            'host',
            '10.1/16',
            '10/8',
            'default',
        ]
        assert t.lookup(addr, exclude=lambda n, p: p == 32)[2] == '10.1/16'
        assert t.lookup(addr, min_len=1)[2] == 'host'
        assert t.lookup(0x0B000000, min_len=1) is None
        assert t.lookup(0x0B000000)[2] == 'default'

    def test_insert_remove_and_lengths(self):
        t = PrefixTable(32)
        t.insert(0x0A000000, 8, 'a')
        t.insert(0x0A000000, 8, 'b')  # replace keeps the count
        assert len(t) == 1 and t.get(0x0A000000, 8) == 'b'
        assert t.lengths() == (8,)
        assert t.remove(0x0A000000, 8) == 'b'
        assert len(t) == 0 and t.lengths() == ()
        with pytest.raises(KeyError):
            t.remove(0x0A000000, 8)
        with pytest.raises(ValueError):
            t.insert(0x0A000001, 8, 'host bits')
        with pytest.raises(ValueError):
            t.insert(0, 33, 'too long')
        with pytest.raises(ValueError):
            PrefixTable(64)

    def test_freeze_is_independent_and_readonly(self):
        t = PrefixTable(32)
        t.insert(0x0A000000, 8, 'a')
        f = t.freeze()
        t.insert(0x0B000000, 8, 'b')
        assert isinstance(f, FrozenPrefixTable) and not hasattr(f, 'insert')
        assert len(f) == 1 and (0x0B000000, 8) not in f and (0x0A000000, 8) in f
        assert f.items() == [(0x0A000000, 8, 'a')]
        assert f.bits == 32

    def test_items_sorted(self):
        t = PrefixTable(32)
        t.insert(0x0B000000, 8, 'b')
        t.insert(0x0A000000, 8, 'a')
        t.insert(0x0A000000, 16, 'a16')
        assert [v for _, _, v in t.items()] == ['a', 'a16', 'b']


def test_frozen_shards_are_not_writable():
    from types import MappingProxyType

    from netsim.model.lpm import PrefixTable

    t = PrefixTable(32)
    t.insert(10, 32, 'a')
    frozen = t.freeze()
    view = frozen.shards()
    assert isinstance(view, MappingProxyType) and isinstance(view[32], MappingProxyType)
    with pytest.raises(TypeError):
        view[32][10] = 'hacked'  # type: ignore[index]
    with pytest.raises(TypeError):
        view[33] = {}  # type: ignore[index]
    assert frozen.get(10, 32) == 'a'


def test_frozen_table_copies_constructor_input():
    from netsim.model.lpm import FrozenPrefixTable, PrefixTable

    tables = {32: {10: 'a'}}
    masks = PrefixTable(32).freeze()._masks
    frozen = FrozenPrefixTable(32, tables, masks)
    tables[32][10] = 'hacked'
    tables[32][11] = 'extra'
    assert frozen.get(10, 32) == 'a' and frozen.get(11, 32) is None
    assert len(frozen) == 1
