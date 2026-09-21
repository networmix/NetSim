"""Longest-prefix-match table.

A dict per prefix length plus a descending tuple of populated lengths.
``lookup`` masks the address for each populated length and probes, so the
cost is O(k) dict probes with k = number of distinct lengths present
(typically 3 to 6), and insert/remove are O(1). The mask table is built
per instance: nothing mutable is shared between tables (free-threading
rule from ``AGENTS.md``).

``PrefixTable`` is the mutable builder; ``freeze()`` returns a
``FrozenPrefixTable`` with the same lookups and no mutators, which is what
the state tree stores.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Callable, Generic, Iterator, Mapping, TypeVar

from netsim.model.addressing import mask_for

V = TypeVar('V')

PrefixKey = tuple[int, int]
"""``(network_int, prefix_len)``."""


class FrozenPrefixTable(Generic[V]):
    """Read-only LPM table."""

    __slots__ = ('_bits', '_masks', '_tables', '_lengths', '_count')

    def __init__(
        self, bits: int, tables: dict[int, dict[int, V]], masks: tuple[int, ...]
    ) -> None:
        # Public construction copies: a frozen table never aliases caller dicts.
        self._init(bits, {plen: dict(t) for plen, t in tables.items()}, masks)

    def _init(
        self,
        bits: int,
        tables: dict[int, Mapping[int, V]],
        masks: tuple[int, ...],
    ) -> None:
        if type(bits) is not int:
            bits = int(bits)  # exact metadata, never a caller's int subclass
        if bits not in (32, 128):
            raise ValueError('bits must be 32 or 128')
        self._bits = bits
        # Own an exact tuple of exact ints: a caller-owned list or int
        # subclass could change lookups after the table was committed.
        if type(masks) is not tuple or any(type(m) is not int for m in masks):
            masks = tuple(int(m) for m in masks)
        self._masks = masks
        self._tables = tables
        self._lengths: tuple[int, ...] = tuple(sorted(tables, reverse=True))
        self._count = sum(len(t) for t in tables.values())

    @classmethod
    def _owned(
        cls,
        bits: int,
        tables: dict[int, Mapping[int, V]],
        masks: tuple[int, ...],
    ) -> FrozenPrefixTable[V]:
        """Own fresh dicts, or share immutable PMaps for incremental RIB edits.

        ``freeze`` has already copied the mutable builder's dicts.
        """
        self = cls.__new__(cls)
        self._init(bits, tables, masks)
        return self

    @property
    def bits(self) -> int:
        return self._bits

    def shards(self) -> Mapping[int, Mapping[int, V]]:
        """Read-only view of the per-length tables (network int → value);
        the inner tables are wrapped too, so nothing reachable is writable."""
        return MappingProxyType(
            {k: MappingProxyType(v) for k, v in self._tables.items()}
        )

    def __eq__(self, other: object) -> bool:
        if isinstance(other, FrozenPrefixTable):
            if self._bits != other._bits:
                return False
            if self._tables == other._tables:
                return True
            if self._tables.keys() != other._tables.keys():
                return False
            for plen, table in self._tables.items():
                peer = other._tables[plen]
                if table is peer or table == peer:
                    continue
                # PMap deliberately only compares equal to another PMap.
                # Prefix tables compare by content even across storage layouts.
                if isinstance(table, dict) == isinstance(peer, dict):
                    return False
                if len(table) != len(peer) or any(
                    net not in peer or value != peer[net]
                    for net, value in table.items()
                ):
                    return False
            return True
        return NotImplemented

    __hash__ = None  # type: ignore[assignment]

    def __len__(self) -> int:
        return self._count

    def __contains__(self, key: PrefixKey) -> bool:
        net, plen = key
        table = self._tables.get(plen)
        return table is not None and net in table

    def get(self, net: int, plen: int, default: Any = None) -> V | Any:
        table = self._tables.get(plen)
        if table is None:
            return default
        return table.get(net, default)

    def lookup_iter(
        self,
        addr: int,
        *,
        exclude: Callable[[int, int], bool] | None = None,
        min_len: int = 0,
    ) -> Iterator[tuple[int, int, V]]:
        """Matching prefixes from longest to shortest.

        ``exclude(net, plen)`` skips a match (used by the resolver's gray
        stack); ``min_len`` stops the walk before shorter prefixes (used to
        refuse resolution via a default route).
        """
        masks = self._masks
        tables = self._tables
        for plen in self._lengths:
            if plen < min_len:
                break
            net = addr & masks[plen]
            table = tables[plen]
            if net in table:
                if exclude is not None and exclude(net, plen):
                    continue
                yield net, plen, table[net]

    def lookup(
        self,
        addr: int,
        *,
        exclude: Callable[[int, int], bool] | None = None,
        min_len: int = 0,
    ) -> tuple[int, int, V] | None:
        for match in self.lookup_iter(addr, exclude=exclude, min_len=min_len):
            return match
        return None

    def items(self) -> list[tuple[int, int, V]]:
        """All entries sorted by ``(network_int, prefix_len)``."""
        out = [
            (net, plen, v)
            for plen, table in self._tables.items()
            for net, v in table.items()
        ]
        out.sort(key=lambda e: (e[0], e[1]))
        return out

    def lengths(self) -> tuple[int, ...]:
        return self._lengths


class PrefixTable(FrozenPrefixTable[V]):
    """Mutable LPM table; ``freeze()`` snapshots it."""

    __slots__ = ()

    def __init__(self, bits: int) -> None:
        if bits not in (32, 128):
            raise ValueError('bits must be 32 or 128')
        masks = tuple(mask_for(n, bits) for n in range(bits + 1))
        super().__init__(bits, {}, masks)

    def insert(self, net: int, plen: int, value: V) -> None:
        if type(net) is not int or type(plen) is not int:
            net, plen = int(net), int(plen)  # keys are exact ints, never subclasses
        if not 0 <= plen <= self._bits:
            raise ValueError(f'prefix length {plen} out of range')
        if net & ~self._masks[plen]:
            raise ValueError(f'host bits set in {net:#x}/{plen}')
        table = self._tables.get(plen)
        if table is None:
            table = self._tables[plen] = {}
            self._lengths = tuple(sorted(self._tables, reverse=True))
        assert isinstance(table, dict)  # mutable builders never own persistent shards
        if net not in table:
            self._count += 1
        table[net] = value

    def remove(self, net: int, plen: int) -> V:
        table = self._tables.get(plen)
        if table is None or net not in table:
            raise KeyError((net, plen))
        assert isinstance(table, dict)
        value = table.pop(net)
        self._count -= 1
        if not table:
            del self._tables[plen]
            self._lengths = tuple(sorted(self._tables, reverse=True))
        return value

    def freeze(self) -> FrozenPrefixTable[V]:
        return FrozenPrefixTable._owned(
            self._bits, {plen: dict(t) for plen, t in self._tables.items()}, self._masks
        )
