"""Shared resource types: Store, FilterStore, PriorityStore, Resource,
PriorityResource, PreemptiveResource, and Container."""

from __future__ import annotations

import bisect
from collections import deque
from heapq import heappop, heappush
from typing import Any, Callable, Iterator, NamedTuple, Protocol

from netsim.core import Environment, Event, Process, SimTime


class RequestQueue(Protocol):
    """Container protocol for pending put/get requests.

    Satisfied by ``collections.deque`` (FIFO) and ``SortedQueue``
    (priority order). The trigger loop scans by index and removes the head
    with ``popleft()``; ``del q[i]`` is used only for non-head removal.
    """

    def append(self, item: Any, /) -> None: ...
    def remove(self, item: Any, /) -> None: ...
    def popleft(self) -> Any: ...
    def __len__(self) -> int: ...
    def __iter__(self) -> Iterator[Any]: ...
    def __getitem__(self, index: int, /) -> Any: ...
    def __delitem__(self, index: int, /) -> None: ...


# ---------------------------------------------------------------------------
# Base Put / Get events
# ---------------------------------------------------------------------------


class Put(Event):
    """Request to put something into a resource.  Acts as a context manager."""

    __slots__ = ('resource', 'proc')

    def __init__(self, resource: BaseResource) -> None:
        super().__init__(resource._env)
        self.resource = resource
        self.proc = self.env.active_process

        resource.put_queue.append(self)
        self.callbacks.append(resource._trigger_get)  # type: ignore[union-attr]
        resource._trigger_put(None)

    def __enter__(self) -> Put:
        return self

    def __exit__(self, *args: Any) -> None:
        self.cancel()

    def cancel(self) -> None:
        """Cancel this put request if it has not yet been triggered."""
        if not self.triggered:
            self.resource.put_queue.remove(self)


class Get(Event):
    """Request to get something from a resource.  Acts as a context manager."""

    __slots__ = ('resource', 'proc')

    def __init__(self, resource: BaseResource) -> None:
        super().__init__(resource._env)
        self.resource = resource
        self.proc = self.env.active_process

        resource.get_queue.append(self)
        self.callbacks.append(resource._trigger_put)  # type: ignore[union-attr]
        resource._trigger_get(None)

    def __enter__(self) -> Get:
        return self

    def __exit__(self, *args: Any) -> None:
        self.cancel()

    def cancel(self) -> None:
        """Cancel this get request if it has not yet been triggered."""
        if not self.triggered:
            self.resource.get_queue.remove(self)


# ---------------------------------------------------------------------------
# BaseResource
# ---------------------------------------------------------------------------


class BaseResource:
    """Abstract base for shared resources.

    Subclasses implement ``_do_put()`` and ``_do_get()`` to define
    when put/get requests are satisfied.

    ``_do_put()`` / ``_do_get()`` return a truthy value to let the trigger
    loop continue with the next queued request, or a falsy value to stop
    after the current one (whether or not it was satisfied).

    Resources carry a ``__dict__`` so users can attach attributes to them;
    only the Event types use ``__slots__``.
    """

    PutQueue: Callable[[], RequestQueue] = deque
    """Factory for the pending-put queue (see ``RequestQueue``)."""
    GetQueue: Callable[[], RequestQueue] = deque
    """Factory for the pending-get queue (see ``RequestQueue``)."""

    def __init__(self, env: Environment, capacity: float | int) -> None:
        self._env = env
        self._capacity = capacity
        self.put_queue: RequestQueue = self.PutQueue()
        self.get_queue: RequestQueue = self.GetQueue()

    @property
    def capacity(self) -> float | int:
        return self._capacity

    def put(self, *args: Any, **kwargs: Any) -> Put:
        raise NotImplementedError

    def get(self, *args: Any, **kwargs: Any) -> Get:
        raise NotImplementedError

    def _do_put(self, event: Put) -> bool | None:
        raise NotImplementedError

    def _do_get(self, event: Get) -> bool | None:
        raise NotImplementedError

    def _trigger_put(self, get_event: Get | None) -> None:
        """Try to satisfy pending put requests via ``_do_put()``."""
        queue = self.put_queue
        idx = 0
        while idx < len(queue):
            put_event = queue[idx]
            proceed = self._do_put(put_event)
            if not put_event.triggered:
                idx += 1
            elif idx == 0:
                if queue.popleft() is not put_event:
                    raise RuntimeError('Put queue invariant violated')
            else:
                if queue[idx] is not put_event:
                    raise RuntimeError('Put queue invariant violated')
                del queue[idx]
            if not proceed:
                break

    def _trigger_get(self, put_event: Put | None) -> None:
        """Try to satisfy pending get requests via ``_do_get()``."""
        queue = self.get_queue
        idx = 0
        while idx < len(queue):
            get_event = queue[idx]
            proceed = self._do_get(get_event)
            if not get_event.triggered:
                idx += 1
            elif idx == 0:
                if queue.popleft() is not get_event:
                    raise RuntimeError('Get queue invariant violated')
            else:
                if queue[idx] is not get_event:
                    raise RuntimeError('Get queue invariant violated')
                del queue[idx]
            if not proceed:
                break


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------


class StorePut(Put):
    """Put *item* into a Store."""

    __slots__ = ('item',)

    def __init__(self, store: Store, item: Any) -> None:
        self.item = item
        super().__init__(store)


class StoreGet(Get):
    """Get an item from a Store."""

    __slots__ = ()


class Store(BaseResource):
    """FIFO store with optional *capacity* (default unlimited)."""

    def __init__(
        self,
        env: Environment,
        capacity: float | int = float('inf'),
    ) -> None:
        if capacity <= 0:
            raise ValueError('"capacity" must be > 0.')
        super().__init__(env, capacity)
        self.items: deque[Any] = deque()

    def put(self, item: Any) -> StorePut:  # type: ignore[override]
        return StorePut(self, item)

    def get(self) -> StoreGet:  # type: ignore[override]
        return StoreGet(self)

    def _do_put(self, event: StorePut) -> bool | None:  # type: ignore[override]
        if len(self.items) < self._capacity:
            self.items.append(event.item)
            event.succeed()
        return None

    def _do_get(self, event: StoreGet) -> bool | None:  # type: ignore[override]
        if self.items:
            event.succeed(self.items.popleft())
        return None


# ---------------------------------------------------------------------------
# FilterStore
# ---------------------------------------------------------------------------


class FilterStoreGet(StoreGet):
    """Get an item matching *filter* from a FilterStore."""

    __slots__ = ('filter',)

    def __init__(
        self,
        resource: FilterStore,
        filter: Callable[[Any], bool] = lambda item: True,
    ) -> None:
        self.filter = filter
        super().__init__(resource)


class FilterStore(Store):
    """Store supporting filtered get requests.

    Get requests may not be served in FIFO order. A later request whose
    filter matches an available item is served before an earlier request
    whose filter does not match.
    """

    def get(  # type: ignore[override]
        self,
        filter: Callable[[Any], bool] = lambda item: True,
    ) -> FilterStoreGet:
        return FilterStoreGet(self, filter)

    def _do_get(  # type: ignore[override]
        self,
        event: FilterStoreGet,
    ) -> bool | None:
        for item in self.items:
            if event.filter(item):
                self.items.remove(item)
                event.succeed(item)
                break
        return True


# ---------------------------------------------------------------------------
# PriorityStore / PriorityItem
# ---------------------------------------------------------------------------


class PriorityItem(NamedTuple):
    """Pairs a priority with an arbitrary item.

    All comparisons use *priority* only, so unorderable items can be
    stored in a PriorityStore.
    """

    priority: Any
    item: Any

    def __eq__(self, other: object) -> bool:  # type: ignore[override]
        if not isinstance(other, PriorityItem):
            return NotImplemented
        return self.priority == other.priority

    def __lt__(self, other: PriorityItem) -> bool:  # type: ignore[override]
        return self.priority < other.priority

    def __le__(self, other: PriorityItem) -> bool:  # type: ignore[override]
        return self.priority <= other.priority

    def __gt__(self, other: PriorityItem) -> bool:  # type: ignore[override]
        return self.priority > other.priority

    def __ge__(self, other: PriorityItem) -> bool:  # type: ignore[override]
        return self.priority >= other.priority

    def __hash__(self) -> int:  # type: ignore[override]
        return hash(self.priority)


class PriorityStore(Store):
    """Store that retrieves items in priority order (smallest first).

    Uses a heap internally. Items must be comparable, or wrapped in
    PriorityItem.
    """

    def __init__(
        self,
        env: Environment,
        capacity: float | int = float('inf'),
    ) -> None:
        super().__init__(env, capacity)
        self.items: list[Any] = []  # type: ignore[assignment]  # heap

    def _do_put(self, event: StorePut) -> bool | None:  # type: ignore[override]
        if len(self.items) < self._capacity:
            heappush(self.items, event.item)
            event.succeed()
        return None

    def _do_get(self, event: StoreGet) -> bool | None:  # type: ignore[override]
        if self.items:
            event.succeed(heappop(self.items))
        return None


# ---------------------------------------------------------------------------
# Resource (mutex-like)
# ---------------------------------------------------------------------------


class Request(Put):
    """Request usage of a Resource."""

    __slots__ = ('usage_since',)

    resource: Resource

    def __init__(self, resource: Resource) -> None:
        self.usage_since: SimTime | None = None
        super().__init__(resource)

    def __exit__(self, *args: Any) -> None:
        super().__exit__(*args)
        # Auto-release on context manager exit (unless GeneratorExit).
        # Only release if the request was actually granted.
        if args[0] is not GeneratorExit and self.triggered:
            self.resource.release(self)


class Release(Get):
    """Release usage of a Resource."""

    __slots__ = ('request',)

    def __init__(self, resource: Resource, request: Request) -> None:
        self.request = request
        super().__init__(resource)


class PriorityRequest(Request):
    """Request with a *priority* and optional *preempt* flag."""

    __slots__ = ('priority', 'preempt', 'time', 'key')

    def __init__(
        self,
        resource: Resource,
        priority: int = 0,
        preempt: bool = True,
    ) -> None:
        self.priority = priority
        self.preempt = preempt
        self.time = resource._env.now
        self.key = (self.priority, self.time, not self.preempt)
        super().__init__(resource)


class SortedQueue(list):
    """List that maintains sorted order by each item's ``key`` attribute.

    Uses ``bisect.insort()``: O(log n) comparisons, O(n) insertion. Head
    removal is O(n) as well; a list is required so bisect can index it.
    """

    __slots__ = ('maxlen',)

    def __init__(self, maxlen: int | None = None) -> None:
        super().__init__()
        self.maxlen = maxlen

    def append(self, item: Any) -> None:
        if self.maxlen is not None and len(self) >= self.maxlen:
            raise RuntimeError('Cannot append event. Queue is full.')
        bisect.insort(self, item, key=lambda e: e.key)

    def popleft(self) -> Any:
        """Remove and return the head (deque-compatible; O(n) on a list)."""
        return self.pop(0)


class Preempted:
    """Cause of a preemption interrupt."""

    __slots__ = ('by', 'usage_since', 'resource')

    def __init__(
        self,
        by: Process | None,
        usage_since: SimTime | None,
        resource: Resource,
    ) -> None:
        self.by = by
        self.usage_since = usage_since
        self.resource = resource


class Resource(BaseResource):
    """Mutual-exclusion resource with *capacity* usage slots."""

    def __init__(self, env: Environment, capacity: int = 1) -> None:
        if capacity <= 0:
            raise ValueError('"capacity" must be > 0.')
        super().__init__(env, capacity)
        self.users: list[Request] = []
        self.queue = self.put_queue

    @property
    def count(self) -> int:
        """Number of current users."""
        return len(self.users)

    def request(self) -> Request:
        return Request(self)

    def release(self, request: Request) -> Release:
        return Release(self, request)

    def _do_put(self, event: Request) -> None:  # type: ignore[override]
        if len(self.users) < self.capacity:
            self.users.append(event)
            event.usage_since = self._env.now
            event.succeed()

    def _do_get(self, event: Release) -> None:  # type: ignore[override]
        try:
            self.users.remove(event.request)
        except ValueError:
            pass
        event.succeed()


class PriorityResource(Resource):
    """Resource with priority-ordered request queue."""

    PutQueue = SortedQueue

    def request(  # type: ignore[override]
        self,
        priority: int = 0,
        preempt: bool = True,
    ) -> PriorityRequest:
        return PriorityRequest(self, priority, preempt)


class PreemptiveResource(PriorityResource):
    """PriorityResource where higher-priority requests preempt lower-priority
    users via Interrupt.

    Only users whose request was made from within a process can be
    preempted (there is nothing to interrupt otherwise); a request made
    outside any process holds its slot until released.
    """

    users: list[PriorityRequest]  # type: ignore[assignment]

    def _do_put(self, event: PriorityRequest) -> None:  # type: ignore[override]
        if len(self.users) >= self.capacity and event.preempt:
            candidates = [u for u in self.users if u.proc is not None]
            if candidates:
                preempt = max(candidates, key=lambda u: u.key)
                if preempt.key > event.key:
                    self.users.remove(preempt)
                    preempt.proc.interrupt(  # type: ignore[union-attr]
                        Preempted(
                            by=event.proc,
                            usage_since=preempt.usage_since,
                            resource=self,
                        )
                    )
        return super()._do_put(event)


# ---------------------------------------------------------------------------
# Container
# ---------------------------------------------------------------------------


class ContainerPut(Put):
    """Put *amount* into a Container."""

    __slots__ = ('amount',)

    def __init__(self, container: Container, amount: float | int) -> None:
        if amount <= 0:
            raise ValueError(f'amount(={amount}) must be > 0.')
        self.amount = amount
        super().__init__(container)


class ContainerGet(Get):
    """Get *amount* from a Container."""

    __slots__ = ('amount',)

    def __init__(self, container: Container, amount: float | int) -> None:
        if amount <= 0:
            raise ValueError(f'amount(={amount}) must be > 0.')
        self.amount = amount
        super().__init__(container)


class Container(BaseResource):
    """Resource for homogeneous matter (continuous or discrete).

    The *level* tracks the current amount.
    """

    def __init__(
        self,
        env: Environment,
        capacity: float | int = float('inf'),
        init: float | int = 0,
    ) -> None:
        if capacity <= 0:
            raise ValueError('"capacity" must be > 0.')
        if init < 0:
            raise ValueError('"init" must be >= 0.')
        if init > capacity:
            raise ValueError('"init" must be <= "capacity".')
        super().__init__(env, capacity)
        self._level = init

    @property
    def level(self) -> float | int:
        """Current amount in the container."""
        return self._level

    def put(self, amount: float | int) -> ContainerPut:  # type: ignore[override]
        return ContainerPut(self, amount)

    def get(self, amount: float | int) -> ContainerGet:  # type: ignore[override]
        return ContainerGet(self, amount)

    def _do_put(self, event: ContainerPut) -> bool | None:  # type: ignore[override]
        if self._capacity - self._level >= event.amount:
            self._level += event.amount
            event.succeed()
            return True
        return None

    def _do_get(self, event: ContainerGet) -> bool | None:  # type: ignore[override]
        if self._level >= event.amount:
            self._level -= event.amount
            event.succeed()
            return True
        return None
