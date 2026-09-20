"""The state tree: immutable records, owning maps, paths, deltas.

All simulated state lives in one immutable tree (``NetworkState``), in the
style of FBOSS ``SwitchState``: records are frozen slotted dataclasses,
maps are ``PMap`` (an owning dict copied on set), an update produces a new
root plus a ``StateDelta`` computed identity-first. Every committed leaf is
transitively immutable; ``validate_immutable`` checks that in debug mode.
"""

from __future__ import annotations

import dataclasses
import os
from dataclasses import dataclass, field
from typing import (
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


class PMap(Generic[K, V]):
    """Immutable mapping that owns an unexposed backing dict.

    ``set``/``remove``/``update`` return a new ``PMap`` sharing nothing
    mutable with the old one; ``set`` returns ``self`` when the value is
    the identical object (canonicalization). Content equality; not
    hashable (tree nodes are not keys). Iteration follows insertion order
    of the backing dict; use ``sorted_items`` for a deterministic order.
    """

    __slots__ = ('_d',)

    def __init__(
        self, items: Mapping[K, V] | Iterable[tuple[K, V]] | None = None
    ) -> None:
        self._d: dict[K, V] = dict(items) if items is not None else {}

    @classmethod
    def _wrap(cls, d: dict[K, V]) -> PMap[K, V]:
        obj = cls.__new__(cls)
        obj._d = d
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

    def keys(self):
        return self._d.keys()

    def values(self):
        return self._d.values()

    def items(self):
        return self._d.items()

    def sorted_items(self) -> list[tuple[K, V]]:
        return sorted(self._d.items(), key=lambda kv: kv[0])  # type: ignore[arg-type, return-value]

    def __eq__(self, other: object) -> bool:
        if isinstance(other, PMap):
            return self._d == other._d
        return NotImplemented

    __hash__ = None  # type: ignore[assignment]

    def __repr__(self) -> str:
        return f'PMap({self._d!r})'

    def set(self, key: K, value: V) -> PMap[K, V]:
        if key in self._d and self._d[key] is value:
            return self
        d = dict(self._d)
        d[key] = value
        return self._wrap(d)

    def remove(self, key: K) -> PMap[K, V]:
        if key not in self._d:
            return self
        d = dict(self._d)
        del d[key]
        return self._wrap(d)

    def update(self, items: Mapping[K, V] | Iterable[tuple[K, V]]) -> PMap[K, V]:
        """Apply several sets with one copy; returns ``self`` if nothing changed."""
        pairs: list[tuple[K, V]]
        if isinstance(items, Mapping):
            pairs = list(cast(Mapping[K, V], items).items())
        else:
            pairs = list(items)
        if all(k in self._d and self._d[k] is v for k, v in pairs):
            return self
        d = dict(self._d)
        for k, v in pairs:
            d[k] = v
        return self._wrap(d)

    def builder(self) -> PMapBuilder[K, V]:
        return PMapBuilder(self)


class PMapBuilder(Generic[K, V]):
    """Mutable staging area for batch edits; ``build()`` copies the map once."""

    __slots__ = ('_base', '_d')

    def __init__(self, base: PMap[K, V]) -> None:
        self._base = base
        self._d: dict[K, V] | None = None

    def _dict(self) -> dict[K, V]:
        if self._d is None:
            self._d = dict(self._base._d)
        return self._d

    def __getitem__(self, key: K) -> V:
        return (self._d if self._d is not None else self._base._d)[key]

    def get(self, key: K, default: Any = None) -> V | Any:
        return (self._d if self._d is not None else self._base._d).get(key, default)

    def __contains__(self, key: object) -> bool:
        return key in (self._d if self._d is not None else self._base._d)

    def set(self, key: K, value: V) -> None:
        current = self._d if self._d is not None else self._base._d
        if key in current and current[key] is value:
            return
        self._dict()[key] = value

    def remove(self, key: K) -> None:
        current = self._d if self._d is not None else self._base._d
        if key in current:
            del self._dict()[key]

    def build(self) -> PMap[K, V]:
        """Publish the staged map. The builder detaches from the published
        dict, so later edits copy again and never reach a snapshot."""
        if self._d is None:
            return self._base
        built = PMap._wrap(self._d)
        self._base, self._d = built, None
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

_LEAF_TYPES = (int, float, str, bytes, bool, type(None), FloatArray)
_FORBIDDEN = (list, dict, set, bytearray, memoryview)


def validate_immutable(obj: Any, path: str = 'root') -> None:
    """Raise ``TypeError`` if *obj* is not transitively immutable.

    Allowed: ints, floats, strs, bytes, bools, None, ``FloatArray``,
    tuples, frozensets, ``PMap``, enums, and frozen dataclasses whose
    fields are all allowed.
    """
    stack: list[tuple[Any, str]] = [(obj, path)]
    seen: set[int] = set()
    while stack:
        o, p = stack.pop()
        if isinstance(o, _LEAF_TYPES):
            continue
        if isinstance(o, _FORBIDDEN):
            raise TypeError(f'mutable {type(o).__name__} at {p}')
        if id(o) in seen:
            continue
        seen.add(id(o))
        if isinstance(o, PMap):
            for k, v in o.items():
                stack.append((v, f'{p}[{k!r}]'))
            continue
        if isinstance(o, (tuple, frozenset)):
            for i, v in enumerate(o):
                stack.append((v, f'{p}[{i}]'))
            continue
        if dataclasses.is_dataclass(o) and not isinstance(o, type):
            params = getattr(o, '__dataclass_params__', None)
            if params is None or not params.frozen:
                raise TypeError(f'non-frozen dataclass {type(o).__name__} at {p}')
            for f in dataclasses.fields(o):
                stack.append((getattr(o, f.name), f'{p}.{f.name}'))
            continue
        if hasattr(o, '__dict__') or hasattr(o, '__slots__'):
            # Enums and other immutable-by-convention singletons are allowed
            # only if they declare themselves so.
            if getattr(type(o), '__netsim_immutable__', False) or _is_enum(o):
                continue
            raise TypeError(f'unrecognized object {type(o).__name__} at {p}')


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
    srv6_sids: Any = None
    srv6_policies: Any = None
    agents: PMap[str, Any] = field(default_factory=empty_pmap)
    nht: Any = None


@record
class NetworkState:
    version: int = 0
    devices: PMap[str, DeviceState] = field(default_factory=empty_pmap)
    links: PMap[str, Any] = field(default_factory=empty_pmap)
    demands: PMap[str, Any] = field(default_factory=empty_pmap)
    traffic_classes: PMap[str, Any] = field(default_factory=empty_pmap)
    placement: Any = None
    transport: Any = None
    allocators: Allocators = field(default_factory=Allocators)


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


BOOKKEEPING_FIELDS = frozenset({'resolver_input_epoch', 'resolver_outcomes'})


def diff_pmap(
    old: PMap | None, new: PMap | None, *, by_identity: bool = False
) -> MapDiff:
    """Keys added, removed and changed between two maps.

    Identity first: an entry whose value is the same object is unchanged
    without comparison; otherwise values are compared by content unless
    ``by_identity`` (used for opaque agent state).
    """
    if old is new:
        return MapDiff((), (), ())  # fresh: no shared sentinel on this path
    if old is None:
        old = PMap()
    if new is None:
        new = PMap()
    od, nd = old._d, new._d
    added = tuple(sorted(k for k in nd if k not in od))
    removed = tuple(sorted(k for k in od if k not in nd))
    changed = []
    for k, v in nd.items():
        if k in od:
            ov = od[k]
            if ov is v:
                continue
            if by_identity or ov != v:
                changed.append(k)
    return MapDiff(added, removed, tuple(sorted(changed)))


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
        if a.keys() != b.keys():
            return False
        return all(tree_equal(a[k], b[k]) for k in a)
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
