"""Observation: settled deliveries and stats (the log lives in ``timeline``).

Deltas are dispatched to internal hooks synchronously at commit; public
subscribers are served by **separate events per delivery** released at
``SETTLED`` after the last round of the timestamp, each carrying the
triggering delta and the ``settled_root`` captured just before delivery.
"""

from __future__ import annotations

from typing import Any, Callable

from netsim import core
from netsim.model.state import NetworkState, StateDelta

SETTLED = core.DEFERRED + 9

Subscriber = Callable[[float, Any, StateDelta, NetworkState], None]


class _Delivery(core.Event):
    __slots__ = ('payload',)

    def __init__(
        self,
        env: core.Environment,
        callback: Callable[[core.Event], None],
        payload: Any,
    ) -> None:
        self.env = env
        self.callbacks: list[core.EventCallback] | None = [callback]
        self._triggered = True
        self._value: Any = payload
        self._ok = True
        self._defused = False
        self.payload = payload
        env.schedule(self, SETTLED, 0)


class EventBus:
    def __init__(self, env: core.Environment, network: Any) -> None:
        self.env = env
        self.network = network
        self._subscribers: tuple[Subscriber, ...] = ()
        self._waiters: list[tuple[core.Event, Callable[[StateDelta], bool] | None]] = []
        self._queued: list[tuple[float, Any, StateDelta]] = []

    def subscribe(self, callback: Subscriber) -> Callable[[], None]:
        self._subscribers = self._subscribers + (callback,)

        def unsubscribe() -> None:
            self._subscribers = tuple(s for s in self._subscribers if s is not callback)

        return unsubscribe

    def next(self) -> core.Event:
        ev = core.Event(self.env)
        self._waiters.append((ev, None))
        return ev

    def wait_for(self, predicate: Callable[[StateDelta], bool]) -> core.Event:
        ev = core.Event(self.env)
        self._waiters.append((ev, predicate))
        return ev

    def enqueue(self, time: float, origin: Any, delta: StateDelta) -> None:
        self._queued.append((time, origin, delta))

    def release(self, time: float) -> None:
        """Called at the end of the last round of *time*: one SETTLED
        event per (delivery, subscriber)."""
        queued, self._queued = self._queued, []
        for t, origin, delta in queued:
            for sub in self._subscribers:
                _Delivery(
                    self.env,
                    lambda ev, sub=sub, t=t, origin=origin, delta=delta: sub(
                        t, origin, delta, self.network.state
                    ),
                    None,
                )
            remaining = []
            for ev, pred in self._waiters:
                if pred is None or pred(delta):
                    _Delivery(
                        self.env,
                        lambda e, ev=ev, delta=delta: _succeed_settled(
                            ev, delta, self.network.state
                        ),
                        None,
                    )
                else:
                    remaining.append((ev, pred))
            self._waiters = remaining


def _succeed_settled(ev: core.Event, delta: StateDelta, root: NetworkState) -> None:
    if not ev.triggered:
        ev.succeed((delta, root))


class Stats:
    def __init__(self) -> None:
        self.counters: dict[str, list[tuple[float, float]]] = {}

    def add(self, name: str, time: float, value: float = 1.0) -> None:
        self.counters.setdefault(name, []).append((time, value))
