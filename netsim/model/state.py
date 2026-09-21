"""The state tree: immutable records, owning maps, paths, deltas.

All simulated state lives in one immutable tree (``NetworkState``), in the
style of FBOSS ``SwitchState``: records are frozen slotted dataclasses,
maps are ``PMap`` (owning, structurally shared shards), an update produces a new
root plus a ``StateDelta`` computed identity-first. Every committed leaf is
transitively immutable; ``validate_immutable`` checks that in debug mode.
"""

from __future__ import annotations

import dataclasses
import enum
import os
import types
from collections.abc import ItemsView, KeysView, ValuesView
from dataclasses import dataclass, field
from fractions import Fraction
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Hashable,
    Iterable,
    Iterator,
    Mapping,
    TypeVar,
    cast,
    dataclass_transform,
)

from netsim.model.lpm import FrozenPrefixTable

if TYPE_CHECKING:
    from netsim.model.srv6 import Srv6Policies, Srv6Sids

K = TypeVar('K', bound=Hashable)
V = TypeVar('V')
T = TypeVar('T')

DEBUG_VALIDATE: bool = os.environ.get('NETSIM_DEBUG', '') not in ('', '0')
"""When set, ``Network.update`` validates transitive immutability of the candidate root."""


@dataclass_transform(frozen_default=True)
def record(cls: type[T]) -> type[T]:
    """Decorator for tree records: frozen, slotted dataclass."""
    return dataclass(frozen=True, slots=True)(cls)


def empty_pmap() -> PMap[Any, Any]:
    """Default factory for map fields (typed so field declarations check)."""
    return PMap()


# ---------------------------------------------------------------------------
# PMap
# ---------------------------------------------------------------------------


class PMap(Mapping[K, V]):
    """Owning persistent map, with insertion-order iteration and set-like keys.

    Up to 512 entries a map owns one flat dict. Above that it promotes to
    256 shards, selected by the mixed hash of ``(key,)`` (the tuple mixer
    also spreads aligned integers). Promotion happens at construction or
    publication; maps never demote. A mutation copies one shard and the
    shard vector. No backing dict is exposed or mutated after publication.

    Hashes select storage only, never iteration/output order. Shard-local
    insertion ordinals preserve dict order across promotion and hash seeds;
    value-only edits share this metadata. Full iteration merges that order,
    whereas ``sorted_items`` sorts keys directly for canonical output.
    Unordered input still needs sorting by the caller, just as with dict.

    ``set`` returns self for an identical value; equality skips identical
    shards before comparing values. Differing layouts fall back to content
    comparison. Costs are O(S + N/S) for set, O(S + q*N/S) for a diff of q
    changed shards, and O(N log S) for insertion-order iteration after
    promotion (fixed S, not an asymptotically sublinear trie).
    """

    # Small/empty maps keep the original one-slot footprint and direct lookup.
    # Only the private promoted subclass carries shard/order metadata.
    __slots__ = ('_d',)
    _FLAT_LIMIT = 512
    _SHARD_COUNT = 256

    def __new__(
        cls, items: Mapping[K, V] | Iterable[tuple[K, V]] | None = None
    ) -> PMap[K, V]:
        d = dict(items) if items is not None else {}
        return cls._wrap(d)

    def __init__(
        self, items: Mapping[K, V] | Iterable[tuple[K, V]] | None = None
    ) -> None:
        pass  # __new__ owns the input and selects the representation.

    @classmethod
    def _wrap(cls, d: dict[K, V]) -> PMap[K, V]:
        if len(d) <= cls._FLAT_LIMIT:
            obj = object.__new__(PMap)
            obj._d = d
            return obj
        mask = cls._SHARD_COUNT - 1
        shards: tuple[dict[K, V], ...] = tuple({} for _ in range(mask + 1))
        order: tuple[dict[K, int], ...] = tuple({} for _ in range(mask + 1))
        for ordinal, (key, value) in enumerate(d.items()):
            index = hash((key,)) & mask
            shards[index][key] = value
            order[index][key] = ordinal
        return cls._from_parts(shards, order, len(d), len(d))

    @property
    def _shards(self) -> tuple[dict[K, V], ...]:
        return (self._d,)

    @property
    def _order(self) -> tuple[dict[K, int], ...] | None:
        return None

    @property
    def _size(self) -> int:
        return len(self._d)

    @property
    def _next(self) -> int:
        return len(self._d)

    @property
    def _mask(self) -> int:
        return 0

    @classmethod
    def _from_parts(
        cls,
        shards: tuple[dict[K, V], ...],
        order: tuple[dict[K, int], ...] | None,
        size: int,
        next_ordinal: int,
    ) -> PMap[K, V]:
        if len(shards) == 1:
            return cls._wrap(shards[0])
        obj = object.__new__(_ShardedPMap)
        obj._buckets, obj._ordinals = shards, order
        obj._n, obj._next_ordinal = size, next_ordinal
        obj._hash_mask = len(shards) - 1
        return obj

    def __getitem__(self, key: K) -> V:
        return self._d[key]

    def get(self, key: K, default: Any = None) -> V | Any:
        return self._d.get(key, default)

    def __contains__(self, key: object) -> bool:
        return key in self._d

    def __iter__(self) -> Iterator[K]:
        return iter(self._d)

    def __len__(self) -> int:
        return len(self._d)

    def _items(self) -> Iterator[tuple[K, V]]:
        if self._order is None:
            yield from self._shards[0].items()
        else:
            # Ordinals are unique: keys/values are never used as sort keys,
            # so even heterogeneous, non-orderable Hashable keys work.
            rows = sorted(
                (order[k], k, v)
                for shard, order in zip(self._shards, self._order, strict=True)
                for k, v in shard.items()
            )
            for _, k, v in rows:
                yield k, v

    def keys(self):
        return self._d.keys()

    def values(self):
        return self._d.values()

    def items(self):
        return self._d.items()

    def sorted_items(self) -> list[tuple[K, V]]:
        return sorted(
            (kv for shard in self._shards for kv in shard.items()),
            key=lambda kv: kv[0],  # type: ignore[arg-type, return-value]
        )

    def _changed_shards(
        self, other: PMap[K, V]
    ) -> Iterator[tuple[Mapping[K, V], Mapping[K, V]]]:
        """Comparison boundary: never enumerate entries in shared shards."""
        if self is other:
            return
        if self._mask != other._mask:
            yield self, other
        else:
            for a, b in zip(self._shards, other._shards, strict=True):
                if a is not b:
                    yield a, b

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PMap):
            return NotImplemented
        if self is other:
            return True
        if self._size != other._size:
            return False
        if self._mask == other._mask:
            return all(a == b for a, b in self._changed_shards(other))
        return all(
            k in other and (v is other[k] or v == other[k]) for k, v in self.items()
        )

    __hash__ = None  # type: ignore[assignment]

    def __repr__(self) -> str:
        return f'PMap({dict(self.items())!r})'

    def __reduce__(self):
        return PMap, (list(self.items()),)

    def set(self, key: K, value: V) -> PMap[K, V]:
        index = hash((key,)) & self._mask
        shard = self._shards[index]
        present = key in shard
        if present and shard[key] is value:
            return self
        d = dict(shard)
        d[key] = value
        shards = list(self._shards)
        shards[index] = d
        order = self._order
        if not present and order is not None:
            orders = list(order)
            orders[index] = dict(orders[index])
            orders[index][key] = self._next
            order = tuple(orders)
        return self._from_parts(
            tuple(shards), order, self._size + (not present), self._next + (not present)
        )

    def remove(self, key: K) -> PMap[K, V]:
        index = hash((key,)) & self._mask
        if key not in self._shards[index]:
            return self
        shards = list(self._shards)
        shards[index] = dict(shards[index])
        del shards[index][key]
        order = self._order
        if order is not None:
            orders = list(order)
            orders[index] = dict(orders[index])
            del orders[index][key]
            order = tuple(orders)
        return self._from_parts(tuple(shards), order, self._size - 1, self._next)

    def update(self, items: Mapping[K, V] | Iterable[tuple[K, V]]) -> PMap[K, V]:
        """Apply sets, copying each touched shard once."""
        builder = self.builder()
        pairs = (
            cast(Mapping[K, V], items).items() if isinstance(items, Mapping) else items
        )
        for key, value in pairs:
            builder.set(key, value)
        return builder.build()

    def builder(self) -> PMapBuilder[K, V]:
        return PMapBuilder(self)


class _ShardedPMap(PMap[K, V]):
    __slots__ = ('_buckets', '_ordinals', '_n', '_next_ordinal', '_hash_mask')

    _buckets: tuple[dict[K, V], ...]
    _ordinals: tuple[dict[K, int], ...] | None
    _n: int
    _next_ordinal: int
    _hash_mask: int

    @property
    def _shards(self) -> tuple[dict[K, V], ...]:
        return self._buckets

    @property
    def _order(self) -> tuple[dict[K, int], ...] | None:
        return self._ordinals

    @property
    def _size(self) -> int:
        return self._n

    @property
    def _next(self) -> int:
        return self._next_ordinal

    @property
    def _mask(self) -> int:
        return self._hash_mask

    def __getitem__(self, key: K) -> V:
        return self._buckets[hash((key,)) & self._hash_mask][key]

    def get(self, key: K, default: Any = None) -> V | Any:
        return self._buckets[hash((key,)) & self._hash_mask].get(key, default)

    def __contains__(self, key: object) -> bool:
        return key in self._buckets[hash((key,)) & self._hash_mask]

    def __iter__(self) -> Iterator[K]:
        return (k for k, _ in self._items())

    def __len__(self) -> int:
        return self._n

    def keys(self):
        return KeysView(self)

    def values(self):
        return _PMapValues(self)

    def items(self):
        return _PMapItems(self)


class _PMapItems(ItemsView[K, V]):
    _mapping: PMap[K, V]

    def __iter__(self) -> Iterator[tuple[K, V]]:
        return cast(PMap[K, V], self._mapping)._items()


class _PMapValues(ValuesView[V]):
    _mapping: PMap[Any, V]

    def __iter__(self) -> Iterator[V]:
        return (v for _, v in cast(PMap[Any, V], self._mapping)._items())


class PMapBuilder(Generic[K, V]):
    """Mutable staging: copy each touched shard once, detach on publication."""

    __slots__ = (
        '_base',
        '_shards',
        '_order',
        '_dirty',
        '_order_dirty',
        '_size',
        '_next',
    )

    def __init__(self, base: PMap[K, V]) -> None:
        self._base = base
        self._shards: list[dict[K, V]] | None = None
        self._order: list[dict[K, int]] | None = None
        self._dirty: set[int] = set()
        self._order_dirty: set[int] = set()
        self._size, self._next = base._size, base._next

    def _shard(self, index: int) -> dict[K, V]:
        return (self._base._shards if self._shards is None else self._shards)[index]

    def _writable(self, index: int) -> dict[K, V]:
        if self._shards is None:
            self._shards = list(self._base._shards)
        if index not in self._dirty:
            self._shards[index] = dict(self._shards[index])
            self._dirty.add(index)
        return self._shards[index]

    def _writable_order(self, index: int) -> dict[K, int]:
        if self._order is None:
            assert self._base._order is not None
            self._order = list(self._base._order)
        if index not in self._order_dirty:
            self._order[index] = dict(self._order[index])
            self._order_dirty.add(index)
        return self._order[index]

    def __getitem__(self, key: K) -> V:
        return self._shard(hash((key,)) & self._base._mask)[key]

    def get(self, key: K, default: Any = None) -> V | Any:
        return self._shard(hash((key,)) & self._base._mask).get(key, default)

    def __contains__(self, key: object) -> bool:
        return key in self._shard(hash((key,)) & self._base._mask)

    def set(self, key: K, value: V) -> None:
        index = hash((key,)) & self._base._mask
        current = self._shard(index)
        present = key in current
        if present and current[key] is value:
            return
        self._writable(index)[key] = value
        if not present:
            if self._base._order is not None:
                self._writable_order(index)[key] = self._next
            self._size += 1
            self._next += 1

    def remove(self, key: K) -> None:
        index = hash((key,)) & self._base._mask
        if key in self._shard(index):
            del self._writable(index)[key]
            if self._base._order is not None:
                del self._writable_order(index)[key]
            self._size -= 1

    def build(self) -> PMap[K, V]:
        """Publish, then forget mutable ownership of every published shard."""
        if self._shards is None:
            return self._base
        order = self._base._order if self._order is None else tuple(self._order)
        built = PMap._from_parts(tuple(self._shards), order, self._size, self._next)
        self._base = built
        self._shards = self._order = None
        self._dirty.clear()
        self._order_dirty.clear()
        self._size, self._next = built._size, built._next
        return built


# ---------------------------------------------------------------------------
# Read-only numeric leaf
# ---------------------------------------------------------------------------


class FloatArray:
    """Immutable ``float64`` sequence stored as ``bytes``.

    ``view()`` returns a *fresh* read-only ``memoryview`` per call, so no
    caller receives the object stored in a snapshot and releasing a
    returned view cannot invalidate the array or another reader.
    """

    __slots__ = ('_data', '_n')
    _FMT = 'd'
    _SIZE = 8

    def __init__(self, data: bytes) -> None:
        if not isinstance(data, bytes):
            raise TypeError('FloatArray needs bytes')
        if type(data) is not bytes:
            # Copy the buffer itself; ``bytes(subclass)`` may return the
            # caller's object through an overridden ``__bytes__``.
            data = memoryview(data).tobytes()
        assert type(data) is bytes
        if len(data) % self._SIZE:
            raise ValueError('FloatArray bytes must be a multiple of 8')
        self._data = data
        self._n = len(data) // self._SIZE

    @classmethod
    def from_iterable(cls, values: Iterable[float]) -> FloatArray:
        import array as _array

        arr = _array.array('d', values)
        return cls(arr.tobytes())

    @classmethod
    def zeros(cls, n: int) -> FloatArray:
        return cls(bytes(n * cls._SIZE))

    def view(self) -> memoryview[float]:
        return memoryview(self._data).cast(self._FMT)  # type: ignore[return-value]

    def __len__(self) -> int:
        return self._n

    def __getitem__(self, index: int) -> float:
        return self.view()[index]

    def __iter__(self) -> Iterator[float]:
        return iter(self.tolist())

    def tolist(self) -> list[float]:
        return list(self.view().tolist())

    def __eq__(self, other: object) -> bool:
        if isinstance(other, FloatArray):
            return self._data == other._data
        return NotImplemented

    __hash__ = None  # type: ignore[assignment]

    def __repr__(self) -> str:
        return f'FloatArray({self.tolist()!r})'


# ---------------------------------------------------------------------------
# Immutability validation
# ---------------------------------------------------------------------------

_LEAF_TYPES = (int, float, str, bytes, bool, type(None), FloatArray, Fraction)
_FORBIDDEN = (list, dict, set, bytearray, memoryview)


_LEAF_EXACT = frozenset(
    (int, float, str, bytes, bool, type(None), FloatArray, Fraction)
)
_BUILTIN_BASES = frozenset(
    (object, int, float, str, bytes, bool, tuple, frozenset, Fraction, enum.Enum)
)

# Node classes of the immutability walk (fail-closed: anything else raises).
_LEAF = 0
_PREFIX_TABLE = 1
_PMAP = 2
_TUPLE = 3
_FROZENSET = 4
_RECORD = 5


def _extra_storage(t: type) -> bool:
    """Whether instances of *t* can hold attributes beyond their builtin
    base's value. The check reads the real descriptors of every
    user-defined class in the MRO: an instance ``__dict__`` descriptor or
    any slot member descriptor means writable storage (a ``__slots__``
    declaration is not trusted: Python consumes an iterator-valued one at
    class creation). Builtin bases and ``Enum`` machinery are exempt;
    audited aliases such as ``MacAddress`` (``__slots__ = ()``) and
    ``NamedTuple`` classes pass."""
    for cls in t.__mro__:
        if cls in _BUILTIN_BASES or cls.__module__ == 'enum':
            continue
        namespace = cls.__dict__
        if '__dict__' in namespace:
            return True
        for name, value in namespace.items():
            if name == '__weakref__':
                continue
            if isinstance(value, types.MemberDescriptorType):
                return True
    return False


_ENUM_ATTRS = frozenset(('_value_', '_name_', '__objclass__', '_sort_order_', '_hash_'))


def _enum_ok(o: Any) -> bool:
    """An enum member is admitted only with the enum machinery's own
    instance attributes and no user-declared slot storage: a member that
    gained attributes (in an initializer, through a slot, or later) carries
    caller-owned state. Members are class-level singletons: like
    declared-immutable types they are trusted by their declaration, never
    copied into the tree. The member's value is classified by the walk."""
    extras = set(vars(o)) - _ENUM_ATTRS
    return not extras and not _extra_storage(type(o))


_OBJECT_SIZE = object.__basicsize__


def _check_record_type(t: type, p: str) -> None:
    """A frozen record may inherit only frozen dataclasses and storage-free
    Python mixins: a base with a larger instance layout than ``object``
    (``deque``, ``list``, ``int`` ... whether or not CPython builds it as a
    heap type) carries a payload no field walk visits, and a mixin with an
    instance ``__dict__`` or slot descriptors is writable storage."""
    for cls in t.__mro__:
        if cls is object:
            continue
        own = cls.__dict__
        if '__dataclass_params__' in own:
            if not own['__dataclass_params__'].frozen:
                raise TypeError(f'non-frozen dataclass base {cls.__name__} at {p}')
            continue  # its fields are walked
        if cls.__basicsize__ != _OBJECT_SIZE or cls.__itemsize__:
            raise TypeError(
                f'record {t.__name__} inherits payload storage from '
                f'{cls.__name__} at {p}'
            )
        if '__dict__' in own or any(
            isinstance(v, types.MemberDescriptorType)
            for k, v in own.items()
            if k != '__weakref__'
        ):
            raise TypeError(
                f'record {t.__name__} inherits storage from {cls.__name__} at {p}'
            )


def _classify(o: Any, p: str) -> int:
    """Classify a node for the immutability walks, or raise ``TypeError``.

    Fail-closed: a value is admitted only when it is an exact leaf type, a
    leaf, tuple or frozenset subclass without extra storage (no instance
    ``__dict__``, no non-empty slots: ``MacAddress``, ``NamedTuple``), an
    enum member (its value is walked), a class decorated itself as a frozen
    dataclass, a ``PMap``, a frozen prefix table or a type that declares
    ``__netsim_immutable__``. Everything else (mutable builtins, C-level
    containers such as ``deque``, iterators, subclasses with attribute
    storage, arbitrary objects) is rejected.
    """
    t = type(o)
    if isinstance(o, Fraction):
        # Public constructors keep the components ``as_integer_ratio``
        # returned; a subclass must also be storage-free.
        if t is not Fraction and _extra_storage(t):
            raise TypeError(f'mutable {t.__name__} (leaf subclass with storage) at {p}')
        if type(o.numerator) is not int or type(o.denominator) is not int:
            raise TypeError(f'Fraction with non-exact int components at {p}')
        return _LEAF
    if t in _LEAF_EXACT:
        return _LEAF
    if isinstance(o, _FORBIDDEN):
        raise TypeError(f'mutable {t.__name__} at {p}')
    if isinstance(o, enum.Enum):
        if not _enum_ok(o):
            raise TypeError(f'enum member {o!r} with instance attributes at {p}')
        return _LEAF  # the member's value is walked by the caller
    if isinstance(o, _LEAF_TYPES):
        if _extra_storage(t):
            raise TypeError(f'mutable {t.__name__} (leaf subclass with storage) at {p}')
        return _LEAF
    if isinstance(o, FrozenPrefixTable):
        if t is not FrozenPrefixTable:
            # PrefixTable (the mutable builder) subclasses the frozen table.
            raise TypeError(f'mutable {t.__name__} at {p}')
        return _PREFIX_TABLE
    if isinstance(o, PMap):
        return _PMAP
    if isinstance(o, tuple):
        if t is not tuple and _extra_storage(t):
            raise TypeError(
                f'mutable {t.__name__} (tuple subclass with storage) at {p}'
            )
        return _TUPLE
    if isinstance(o, frozenset):
        if t is not frozenset and _extra_storage(t):
            raise TypeError(
                f'mutable {t.__name__} (frozenset subclass with storage) at {p}'
            )
        return _FROZENSET
    if dataclasses.is_dataclass(o) and not isinstance(o, type):
        params = t.__dict__.get('__dataclass_params__')
        if params is None:
            raise TypeError(
                f'{t.__name__} at {p} inherits a frozen record but is not a '
                'frozen dataclass itself'
            )
        if not params.frozen:
            raise TypeError(f'non-frozen dataclass {t.__name__} at {p}')
        _check_record_type(t, p)
        # Instance storage must be accounted for by the fields the walk
        # visits: a populated cached_property or any other extra entry in
        # the instance dictionary is caller-owned state.
        instance_dict = getattr(o, '__dict__', None)
        if instance_dict:
            names = {f.name for f in dataclasses.fields(o)}
            extra = [k for k in instance_dict if k not in names]
            if extra:
                raise TypeError(
                    f'record {t.__name__} with non-field instance storage '
                    f'{extra[0]!r} at {p}'
                )
        return _RECORD
    if getattr(t, '__netsim_immutable__', False):
        return _LEAF
    raise TypeError(f'unrecognized object {t.__name__} at {p}')


def _prefix_key_ok(net_: Any, plen: Any) -> bool:
    return type(net_) is int and type(plen) is int


def validate_immutable(obj: Any, path: str = 'root') -> None:
    """Raise ``TypeError`` if *obj* is not transitively immutable.

    Allowed: ints, floats, strs, bytes, bools, None, ``FloatArray``,
    tuples, frozensets, ``PMap`` (keys and values), enums, frozen prefix
    tables and frozen dataclasses whose fields are all allowed. The walk is
    fail-closed: any other object is rejected.
    """
    stack: list[tuple[Any, str]] = [(obj, path)]
    seen: set[int] = set()
    while stack:
        o, p = stack.pop()
        kind = _classify(o, p)
        if kind == _LEAF:
            if isinstance(o, enum.Enum):
                stack.append((o.value, f'{p}.value'))
            continue
        if id(o) in seen:
            continue
        seen.add(id(o))
        if kind == _PREFIX_TABLE:
            if type(o.bits) is not int:
                raise TypeError(f'prefix table bits is not an exact int at {p}')
            for plen, table in o.shards().items():
                if type(plen) is not int:
                    raise TypeError(f'prefix length is not an exact int at {p}')
                for net_, value in table.items():
                    if not _prefix_key_ok(net_, plen):
                        raise TypeError(f'prefix key is not an exact int at {p}')
                    stack.append((value, f'{p}[{net_}/{plen}]'))
        elif kind == _PMAP:
            for k, v in o.items():
                stack.append((k, f'{p}.key({k!r})'))
                stack.append((v, f'{p}[{k!r}]'))
        elif kind in (_TUPLE, _FROZENSET):
            for i, v in enumerate(o):
                stack.append((v, f'{p}[{i}]'))
        else:
            for f in dataclasses.fields(o):
                stack.append((getattr(o, f.name), f'{p}.{f.name}'))


def validate_admitted(new: Any, old: Any, path: str = 'root') -> None:
    """Validate *new* transitively, trusting every subtree that is the very
    object found at the same place in *old* (a previously admitted value).

    Admission of agent state and advertised views uses this instead of a
    whole-tree walk: a value returned by identity costs nothing, a record
    that reuses most of its previous fields costs only its changed subtrees
    (the incremental trusted-builder rule of the design), and a mutable
    value anywhere in a *new* subtree is still rejected with the same
    fail-closed classification as ``validate_immutable``. Pairing follows
    structure: dataclass fields by name (same type only), ``PMap`` entries
    by key, tuples by index when lengths match; anything unpaired is walked
    in full. ``old`` must itself have been admitted through this function or
    ``validate_immutable``.
    """
    stack: list[tuple[Any, Any, str]] = [(new, old, path)]
    seen: set[int] = set()
    while stack:
        o, prev, p = stack.pop()
        if o is prev:
            continue  # trusted by identity: admitted before
        kind = _classify(o, p)
        if kind == _LEAF:
            if isinstance(o, enum.Enum):
                stack.append((o.value, None, f'{p}.value'))
            continue
        if id(o) in seen:
            continue
        seen.add(id(o))
        if kind == _PREFIX_TABLE:
            if type(o.bits) is not int:
                raise TypeError(f'prefix table bits is not an exact int at {p}')
            prev_shards = prev.shards() if type(prev) is FrozenPrefixTable else {}
            for plen, table in o.shards().items():
                if type(plen) is not int:
                    raise TypeError(f'prefix length is not an exact int at {p}')
                prev_table = prev_shards.get(plen, {})
                for net_, value in table.items():
                    if not _prefix_key_ok(net_, plen):
                        raise TypeError(f'prefix key is not an exact int at {p}')
                    stack.append((value, prev_table.get(net_), f'{p}[{net_}/{plen}]'))
        elif kind == _PMAP:
            paired = isinstance(prev, PMap)
            for k, v in o.items():
                # Keys are validated in full: a key equal to a previous key
                # need not be the same object.
                stack.append((k, None, f'{p}.key({k!r})'))
                stack.append((v, prev.get(k) if paired else None, f'{p}[{k!r}]'))
        elif kind == _TUPLE:
            paired = isinstance(prev, tuple) and len(prev) == len(o)
            for i, v in enumerate(o):
                stack.append((v, prev[i] if paired else None, f'{p}[{i}]'))
        elif kind == _FROZENSET:
            for i, v in enumerate(o):
                stack.append((v, None, f'{p}[{i}]'))
        else:
            paired = type(prev) is type(o)
            for f in dataclasses.fields(o):
                stack.append(
                    (
                        getattr(o, f.name),
                        getattr(prev, f.name) if paired else None,
                        f'{p}.{f.name}',
                    )
                )


def _is_enum(o: Any) -> bool:
    import enum

    return isinstance(o, enum.Enum)


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

Path = tuple[str, ...]

_NAME_FORBIDDEN = ('|', ':', '--')


def check_name(name: str, *, allow_slash: bool = False) -> str:
    """Validate an entity name; ``/`` is allowed only for imported names."""
    if not isinstance(name, str) or not name:
        raise ValueError('names must be non-empty strings')
    for bad in _NAME_FORBIDDEN:
        if bad in name:
            raise ValueError(f'name {name!r} may not contain {bad!r}')
    if '/' in name and not allow_slash:
        raise ValueError(f'name {name!r} may not contain "/"')
    return name


def _escape(component: str) -> str:
    return component.replace('%', '%25').replace('/', '%2F')


def _unescape(component: str) -> str:
    return component.replace('%2F', '/').replace('%25', '%')


def path_str(path: Path) -> str:
    return '/'.join(_escape(c) for c in path)


def path_parse(text: str) -> Path:
    if text == '':
        return ()
    return tuple(_unescape(c) for c in text.split('/'))


def path_startswith(path: Path, prefix: Path) -> bool:
    return len(path) >= len(prefix) and path[: len(prefix)] == prefix


# ---------------------------------------------------------------------------
# Allocators and generations
# ---------------------------------------------------------------------------


@record
class Allocators:
    """Counters that live in the tree so a failed update leaves no gaps."""

    next_generation: int = 1
    next_mac_index: int = 0
    next_link_index: int = 0
    next_ifindex: PMap[str, int] = field(default_factory=empty_pmap)
    next_srv6_node: PMap[tuple[int, int], int] = field(default_factory=empty_pmap)

    def take_generation(self) -> tuple[Allocators, int]:
        return dataclasses.replace(
            self, next_generation=self.next_generation + 1
        ), self.next_generation

    def take_mac_index(self) -> tuple[Allocators, int]:
        return dataclasses.replace(
            self, next_mac_index=self.next_mac_index + 1
        ), self.next_mac_index

    def take_link_index(self) -> tuple[Allocators, int]:
        return dataclasses.replace(
            self, next_link_index=self.next_link_index + 1
        ), self.next_link_index

    def take_ifindex(self, device: str) -> tuple[Allocators, int]:
        current = self.next_ifindex.get(device, 1)
        return dataclasses.replace(
            self, next_ifindex=self.next_ifindex.set(device, current + 1)
        ), current


# ---------------------------------------------------------------------------
# Tree skeleton
# ---------------------------------------------------------------------------


@record
class DeviceConfig:
    enabled: bool = True
    enabled_since: float = 0.0
    seed: int = 0
    fib_delay: float = 0.0
    fast_failover: bool = False
    """Data-plane pruning of ECMP legs: a next-hop group member whose egress
    port has no link (raw carrier down) or is not usable is skipped at
    selection time and the flow is re-hashed over the live members, without
    waiting for the control plane to reprogram the FIB (``fib_delay``).
    Off, packets hashed to a dead leg are dropped until the FIB changes."""
    router_id: int | None = None
    resolution_policy: Any = None
    srv6_source: int | None = None
    srv6_hop_limit: int = 64
    srdb_source: Any = None


@record
class DeviceOper:
    router_id: int = 0


@record
class DeviceState:
    """Per-device subtree. Field types are opaque here; owning modules
    define the records (``interfaces.py``, ``routing.py`` ...)."""

    name: str
    generation: int
    config: DeviceConfig = field(default_factory=DeviceConfig)
    oper: DeviceOper = field(default_factory=DeviceOper)
    interfaces: PMap[str, Any] = field(default_factory=empty_pmap)
    ribs: PMap[int, Any] = field(default_factory=empty_pmap)
    resolver_input_epoch: PMap[int, int] = field(default_factory=empty_pmap)
    resolver_outcomes: PMap[int, Any] = field(default_factory=empty_pmap)
    fibs: PMap[int, Any] = field(default_factory=empty_pmap)
    neighbors: Any = None
    nexthop_groups: Any = None
    load_balancers: Any = None
    srv6_config: Any = None
    srv6_sids: Srv6Sids | None = None
    srv6_policies: Srv6Policies | None = None
    agents: PMap[str, Any] = field(default_factory=empty_pmap)
    """``contracts.AgentNode`` per agent name; state and srdb_view are
    identity-compared (``StateDelta.agents`` diffs by identity)."""
    nht: Any = None
    """``contracts.NhtTable`` or ``None`` (registrations and results)."""
    l3_interfaces: PMap[str, Any] | None = None
    """Per-interface L3 contributions, owned by derive_l3; None before initialization."""
    interface_index: Any = None
    """Immutable bundle membership index, maintained with committed interface changes."""


@record
class NetworkState:
    version: int = 0
    devices: PMap[str, DeviceState] = field(default_factory=empty_pmap)
    links: PMap[str, Any] = field(default_factory=empty_pmap)
    demands: PMap[str, Any] = field(default_factory=empty_pmap)
    traffic_classes: PMap[str, Any] = field(default_factory=empty_pmap)
    placement: Any = None
    transport: Any = None
    """``contracts.TransportState`` or ``None`` (listeners and connections)."""
    allocators: Allocators = field(default_factory=Allocators)
    srv6_consumers: frozenset[str] = field(default_factory=frozenset)
    """Bookkeeping index of policy headends, maintained incrementally by bump_epochs."""


# ---------------------------------------------------------------------------
# Deltas
# ---------------------------------------------------------------------------


@record
class MapDiff:
    added: tuple[Any, ...]
    removed: tuple[Any, ...]
    changed: tuple[Any, ...]

    def __bool__(self) -> bool:
        return bool(self.added or self.removed or self.changed)

    @property
    def keys(self) -> tuple[Any, ...]:
        return self.added + self.removed + self.changed


BOOKKEEPING_FIELDS = frozenset(
    {'resolver_input_epoch', 'resolver_outcomes', 'l3_interfaces', 'interface_index'}
)


def diff_pmap(
    old: PMap | None,
    new: PMap | None,
    *,
    by_identity: bool = False,
    sort_key: Callable[[Any], Any] | None = None,
) -> MapDiff:
    """Keys added, removed and changed between two maps.

    Identity first: an entry whose value is the same object is unchanged
    without comparison; otherwise values are compared by content unless
    ``by_identity`` (used for opaque agent state). Shared shards are skipped.
    ``sort_key`` supports keys with a domain-specific canonical ordering.
    """
    if old is new:
        return MapDiff((), (), ())  # fresh: no shared sentinel on this path
    if old is None:
        old = PMap()
    if new is None:
        new = PMap()
    added, removed, changed = [], [], []
    for od, nd in old._changed_shards(new):
        removed.extend(k for k in od if k not in nd)
        for k, v in nd.items():
            if k not in od:
                added.append(k)
            else:
                ov = od[k]
                if ov is not v and (by_identity or ov != v):
                    changed.append(k)
    return MapDiff(
        tuple(sorted(added, key=sort_key)),
        tuple(sorted(removed, key=sort_key)),
        tuple(sorted(changed, key=sort_key)),
    )


class StateDelta:
    """Difference between two roots; sections are computed lazily and
    identity-first so unchanged subtrees cost nothing."""

    __slots__ = ('old', 'new', '_cache')

    def __init__(self, old: NetworkState, new: NetworkState) -> None:
        self.old = old
        self.new = new
        self._cache: dict[str, Any] = {}

    def _memo(self, key: str, compute: Callable[[], Any]) -> Any:
        try:
            return self._cache[key]
        except KeyError:
            value = self._cache[key] = compute()
            return value

    # -- top level ----------------------------------------------------------

    def devices(self) -> MapDiff:
        return self._memo(
            'devices', lambda: diff_pmap(self.old.devices, self.new.devices)
        )

    def links(self) -> MapDiff:
        return self._memo('links', lambda: diff_pmap(self.old.links, self.new.links))

    def demands(self) -> MapDiff:
        return self._memo(
            'demands', lambda: diff_pmap(self.old.demands, self.new.demands)
        )

    def traffic_classes(self) -> MapDiff:
        return self._memo(
            'traffic_classes',
            lambda: diff_pmap(self.old.traffic_classes, self.new.traffic_classes),
        )

    def placement_changed(self) -> bool:
        o, n = self.old.placement, self.new.placement
        return not (o is n or o == n)

    def transport_changed(self) -> bool:
        o, n = self.old.transport, self.new.transport
        return not (o is n or o == n)

    # -- per device ---------------------------------------------------------

    def _device_pair(self, name: str) -> tuple[DeviceState | None, DeviceState | None]:
        return self.old.devices.get(name), self.new.devices.get(name)

    def device_field_changed(self, name: str, field_name: str) -> bool:
        o, n = self._device_pair(name)
        ov = getattr(o, field_name, None)
        nv = getattr(n, field_name, None)
        return not (ov is nv or ov == nv)

    def device_map_diff(
        self, name: str, field_name: str, *, by_identity: bool = False
    ) -> MapDiff:
        key = f'{field_name}:{name}'

        def compute() -> MapDiff:
            o, n = self._device_pair(name)
            return diff_pmap(
                getattr(o, field_name, None),
                getattr(n, field_name, None),
                by_identity=by_identity,
            )

        return self._memo(key, compute)

    def interfaces(self, name: str) -> MapDiff:
        return self.device_map_diff(name, 'interfaces')

    def ribs(self, name: str) -> MapDiff:
        return self.device_map_diff(name, 'ribs')

    def fibs(self, name: str) -> MapDiff:
        return self.device_map_diff(name, 'fibs')

    def agents(self, name: str) -> MapDiff:
        return self.device_map_diff(name, 'agents', by_identity=True)

    def load_balancers_changed(self, name: str) -> bool:
        return self.device_field_changed(name, 'load_balancers')

    def config_changed(self, name: str) -> bool:
        return self.device_field_changed(name, 'config')

    def interface_changes(self, name: str) -> tuple[tuple[str, bool, bool], ...]:
        """``(interface, config_changed, oper_changed)`` for changed interfaces
        of a device (added and removed interfaces count as both)."""
        return self._memo(f'ifchanges:{name}', lambda: self._interface_changes(name))

    def _interface_changes(self, name: str) -> tuple[tuple[str, bool, bool], ...]:
        o, n = self._device_pair(name)
        diff = self.interfaces(name)
        out = []
        for key in diff.added + diff.removed:
            out.append((key, True, True))
        for key in diff.changed:
            assert o is not None and n is not None
            ov, nv = o.interfaces[key], n.interfaces[key]
            out.append(
                (
                    key,
                    ov.config != nv.config
                    or getattr(ov, 'link', None) != getattr(nv, 'link', None),
                    ov.oper != nv.oper,
                )
            )
        return tuple(out)

    # -- paths --------------------------------------------------------------

    def changed_paths(self, *, bookkeeping: bool = False) -> list[Path]:
        """Coarse changed paths (device-level sections and top-level maps).

        Bookkeeping fields (resolver epochs and outcomes) are left out unless
        ``bookkeeping`` is set; ``is_empty`` counts them.
        """
        out: list[Path] = []
        for k in self.links().keys:
            out.append(('links', k))
        for k in self.demands().keys:
            out.append(('demands', k))
        for k in self.traffic_classes().keys:
            out.append(('traffic_classes', k))
        if self.placement_changed():
            out.append(('placement',))
        if self.transport_changed():
            out.append(('transport',))
        d = self.devices()
        for name in d.added + d.removed:
            out.append(('devices', name))
        for name in d.changed:
            o, n = self._device_pair(name)
            assert o is not None and n is not None
            for f in dataclasses.fields(DeviceState):
                if not bookkeeping and f.name in BOOKKEEPING_FIELDS:
                    continue
                ov, nv = getattr(o, f.name), getattr(n, f.name)
                if ov is nv or ov == nv:
                    continue
                if isinstance(nv, PMap) or isinstance(ov, PMap):
                    for k in diff_pmap(
                        ov if isinstance(ov, PMap) else None,
                        nv if isinstance(nv, PMap) else None,
                        by_identity=(f.name == 'agents'),
                    ).keys:
                        out.append(('devices', name, f.name, str(k)))
                else:
                    out.append(('devices', name, f.name))
        return out

    def is_empty(self) -> bool:
        return not self.changed_paths(bookkeeping=True)


# ---------------------------------------------------------------------------
# Canonicalization and equality helpers
# ---------------------------------------------------------------------------


def canon(old: V, new: V) -> V:
    """Return *old* when *new* equals it by value, else *new*."""
    if old is new or old == new:
        return old
    return new


def _is_ignored_field(name: str) -> bool:
    """Bookkeeping fields: versions, timestamps and epoch counters."""
    return (
        name == 'version'
        or name == 'since'
        or name.endswith('_since')
        or name.endswith('_epoch')
    )


def tree_equal(a: Any, b: Any) -> bool:
    """Value equality ignoring ``version`` and ``since`` fields everywhere."""
    if a is b:
        return True
    if isinstance(a, PMap) and isinstance(b, PMap):
        if len(a) != len(b):
            return False
        for old, new in a._changed_shards(b):
            if old.keys() != new.keys():
                return False
            if not all(tree_equal(old[k], new[k]) for k in old):
                return False
        return True
    if (
        dataclasses.is_dataclass(a)
        and dataclasses.is_dataclass(b)
        and type(a) is type(b)
    ):
        for f in dataclasses.fields(a):
            if _is_ignored_field(f.name):
                continue
            if not tree_equal(getattr(a, f.name), getattr(b, f.name)):
                return False
        return True
    if isinstance(a, tuple) and isinstance(b, tuple):
        return len(a) == len(b) and all(
            tree_equal(x, y) for x, y in zip(a, b, strict=True)
        )
    return bool(a == b)
