"""Core discrete-event simulation primitives.

Environment, Event, Timeout, Process, and condition types (AllOf, AnyOf).
"""

from __future__ import annotations

from heapq import heappop, heappush
from itertools import count
from typing import Any, Callable, Generator, Iterable, Iterator

from netsim.exceptions import EmptySchedule, Interrupt

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

URGENT: int = 0
"""Priority for interrupts and process initialization events."""

NORMAL: int = 1
"""Default event priority."""

Infinity: float = float('inf')

_PENDING: object = object()
"""Sentinel for an event whose value has not yet been set."""

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

EventPriority = int
SimTime = int | float
EventCallback = Callable[['Event'], None]
ProcessGenerator = Generator['Event', Any, Any]

# ---------------------------------------------------------------------------
# Event
# ---------------------------------------------------------------------------


class Event:
    """An event that may happen at some point in time.

    Pending until triggered via ``succeed()``, ``fail()``, or ``trigger()``.
    Once triggered it is scheduled for processing by the environment.
    After processing (all callbacks invoked), ``callbacks`` is set to ``None``.
    """

    __slots__ = ('env', 'callbacks', '_value', '_ok', '_defused')

    def __init__(self, env: Environment) -> None:
        self.env = env
        self.callbacks: list[EventCallback] | None = []
        self._value: Any = _PENDING
        self._ok: bool = False
        self._defused: bool = False

    def __repr__(self) -> str:
        return f'<{self._desc()} object at {id(self):#x}>'

    def _desc(self) -> str:
        return f'{type(self).__name__}()'

    # -- State queries ------------------------------------------------------

    @property
    def triggered(self) -> bool:
        """True once the event has been triggered."""
        return self._value is not _PENDING

    @property
    def processed(self) -> bool:
        """True once callbacks have been invoked."""
        return self.callbacks is None

    @property
    def ok(self) -> bool:
        """True if the event succeeded."""
        return self._ok

    @property
    def defused(self) -> bool:
        """True if a failed event's exception has been handled."""
        return self._defused

    @defused.setter
    def defused(self, value: bool) -> None:
        self._defused = value

    @property
    def value(self) -> Any:
        """The event's value.

        Raises:
            AttributeError: If the event is still pending.
        """
        if self._value is _PENDING:
            raise AttributeError(f'Value of {self} is not yet available')
        return self._value

    # -- Triggering ---------------------------------------------------------

    def succeed(self, value: Any = None) -> Event:
        """Mark as successful and schedule for processing.

        Args:
            value: The value to attach to this event.

        Raises:
            RuntimeError: If already triggered.
        """
        if self._value is not _PENDING:
            raise RuntimeError(f'{self} has already been triggered')
        self._ok = True
        self._value = value
        self.env.schedule(self)
        return self

    def fail(self, exception: BaseException) -> Event:
        """Mark as failed and schedule for processing.

        Args:
            exception: The exception to attach.

        Raises:
            TypeError: If *exception* is not a BaseException.
            RuntimeError: If already triggered.
        """
        if self._value is not _PENDING:
            raise RuntimeError(f'{self} has already been triggered')
        if not isinstance(exception, BaseException):
            raise TypeError(f'{exception} is not an exception.')
        self._ok = False
        self._value = exception
        self.env.schedule(self)
        return self

    def trigger(self, event: Event) -> None:
        """Copy ok/value from *event* and schedule. Used for chaining.

        Args:
            event: Source event to copy state from.

        Raises:
            RuntimeError: If already triggered.
        """
        if self._value is not _PENDING:
            raise RuntimeError(f'{self} has already been triggered')
        self._ok = event._ok
        self._value = event._value
        self.env.schedule(self)

    # -- Composition --------------------------------------------------------

    def __and__(self, other: Event) -> Condition:
        return Condition(self.env, Condition.all_events, [self, other])

    def __or__(self, other: Event) -> Condition:
        return Condition(self.env, Condition.any_events, [self, other])


# ---------------------------------------------------------------------------
# Timeout
# ---------------------------------------------------------------------------


class Timeout(Event):
    """Event that auto-triggers after *delay* time units.

    Args:
        env: The simulation environment.
        delay: Non-negative delay before triggering.
        value: Value attached to the event on trigger.
    """

    __slots__ = ('_delay',)

    def __init__(
        self,
        env: Environment,
        delay: SimTime,
        value: Any = None,
    ) -> None:
        if delay < 0:
            raise ValueError(f'Negative delay {delay}')
        # Inline Event.__init__ for performance.
        self.env = env
        self.callbacks: list[EventCallback] | None = []
        self._value = value
        self._delay = delay
        self._ok = True
        self._defused = False
        env.schedule(self, NORMAL, delay)

    def _desc(self) -> str:
        return f'Timeout({self._delay})'


# ---------------------------------------------------------------------------
# Process internals
# ---------------------------------------------------------------------------


class _Initialize(Event):
    """Schedules the first ``_resume`` call for a Process."""

    __slots__ = ()

    def __init__(self, env: Environment, process: Process) -> None:
        self.env = env
        self.callbacks: list[EventCallback] | None = [process._resume]
        self._value: Any = None
        self._ok = True
        self._defused = False
        env.schedule(self, URGENT)


class _Interruption(Event):
    """Schedules an Interrupt exception on a process."""

    __slots__ = ('process',)

    def __init__(self, process: Process, cause: Any) -> None:
        self.env = process.env
        self.callbacks: list[EventCallback] | None = [self._interrupt]
        self._value: Any = Interrupt(cause)
        self._ok = False
        self._defused = True

        if process._value is not _PENDING:
            raise RuntimeError(f'{process} has terminated and cannot be interrupted.')
        if process is self.env.active_process:
            raise RuntimeError('A process is not allowed to interrupt itself.')

        self.process = process
        self.env.schedule(self, URGENT)

    def _interrupt(self, event: Event) -> None:
        if self.process._value is not _PENDING:
            return  # Process already dead (concurrent interrupts).
        # Remove the process from the target event's callbacks.
        target_cbs = self.process._target.callbacks
        if target_cbs is not None:
            target_cbs.remove(self.process._resume)
        self.process._resume(self)


# ---------------------------------------------------------------------------
# Process
# ---------------------------------------------------------------------------


class Process(Event):
    """Generator-based simulation process.

    A process is an Event. It triggers when the generator terminates,
    so ``yield process`` waits for completion and returns the generator's
    return value.

    Args:
        env: The simulation environment.
        generator: A generator yielding Event instances.
        name: Optional name (defaults to generator function name).
    """

    __slots__ = ('_generator', '_target', '_name')

    def __init__(
        self,
        env: Environment,
        generator: ProcessGenerator,
        name: str | None = None,
    ) -> None:
        if not hasattr(generator, 'throw'):
            raise ValueError(f'{generator} is not a generator.')

        self.env = env
        self.callbacks: list[EventCallback] | None = []
        self._value: Any = _PENDING
        self._ok: bool = False
        self._defused: bool = False

        self._generator = generator
        self._name = name
        self._target: Event = _Initialize(env, self)

    def _desc(self) -> str:
        return f'Process({self.name})'

    @property
    def name(self) -> str:
        """Process name. Explicit if provided, else the generator function name."""
        if self._name is not None:
            return self._name
        if self._generator is not None:
            return self._generator.__name__  # type: ignore[attr-defined]
        return f'Process@{id(self):#x}'

    @property
    def target(self) -> Event:
        """The event the process is currently waiting for."""
        return self._target

    @property
    def is_alive(self) -> bool:
        """True until the generator exits."""
        return self._value is _PENDING

    def interrupt(self, cause: Any = None) -> None:
        """Interrupt this process optionally providing a *cause*.

        Raises:
            RuntimeError: If the process has terminated or interrupts itself.
        """
        _Interruption(self, cause)

    def _resume(self, event: Event) -> None:
        """Resume the generator with *event*'s value or exception.

        Loops eagerly when the yielded event is already processed
        (``callbacks is None``), sending its value back immediately.
        """
        self.env._active_proc = self

        gen = self._generator
        assert gen is not None  # Alive processes always have a generator.

        while True:
            # Advance the generator.
            try:
                if event._ok:
                    event = gen.send(event._value)
                else:
                    event._defused = True
                    event = gen.throw(_copy_exception(event._value))
            except StopIteration as e:
                # Generator returned — trigger this Process event.
                event = None  # type: ignore[assignment]
                self._ok = True
                self._value = e.args[0] if e.args else None
                self._generator = None  # type: ignore[assignment]
                self.env.schedule(self)
                break
            except BaseException as e:
                # Generator raised — fail this Process event.
                event = None  # type: ignore[assignment]
                self._ok = False
                if e.__traceback__ is not None:
                    e.__traceback__ = e.__traceback__.tb_next
                self._value = e
                self._generator = None  # type: ignore[assignment]
                self.env.schedule(self)
                break

            # The generator yielded an event to wait on.
            try:
                if event.callbacks is not None:
                    # Event not yet processed — register for callback.
                    event.callbacks.append(self._resume)
                    break
                # Event already processed — loop back and send its value.
            except AttributeError:
                if hasattr(event, 'callbacks'):
                    raise
                descr = _describe_frame(gen.gi_frame)  # type: ignore[attr-defined]
                raise RuntimeError(f'\n{descr}Invalid yield value "{event}"') from None

        self._target = event  # type: ignore[assignment]
        self.env._active_proc = None


# ---------------------------------------------------------------------------
# Conditions
# ---------------------------------------------------------------------------


class ConditionValue:
    """Dict-like result of a Condition.

    Access individual event values via ``result[event]``.
    Supports ``keys()``, ``values()``, ``items()``, ``todict()``.
    """

    __slots__ = ('events',)

    def __init__(self) -> None:
        self.events: list[Event] = []

    def __getitem__(self, key: Event) -> Any:
        if key not in self.events:
            raise KeyError(str(key))
        return key._value

    def __contains__(self, key: Event) -> bool:
        return key in self.events

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ConditionValue):
            return self.events == other.events
        if isinstance(other, dict):
            return self.todict() == other
        return NotImplemented

    def __repr__(self) -> str:
        return f'<ConditionValue {self.todict()}>'

    def __iter__(self) -> Iterator[Event]:
        return self.keys()

    def keys(self) -> Iterator[Event]:
        return (e for e in self.events)

    def values(self) -> Iterator[Any]:
        return (e._value for e in self.events)

    def items(self) -> Iterator[tuple[Event, Any]]:
        return ((e, e._value) for e in self.events)

    def todict(self) -> dict[Event, Any]:
        return {e: e._value for e in self.events}


class Condition(Event):
    """Composite event that triggers when ``evaluate(events, count)`` is True.

    Value is a ConditionValue of events that triggered before the condition
    was processed. Fails immediately if any sub-event fails.

    Args:
        env: The simulation environment.
        evaluate: Function ``(events, count) -> bool``.
        events: Sub-events to monitor.
    """

    __slots__ = ('_evaluate', '_events', '_count')

    def __init__(
        self,
        env: Environment,
        evaluate: Callable[[tuple[Event, ...], int], bool],
        events: Iterable[Event],
    ) -> None:
        super().__init__(env)
        self._evaluate = evaluate
        self._events = tuple(events)
        self._count = 0

        if not self._events:
            self.succeed(ConditionValue())
            return

        for event in self._events:
            if self.env != event.env:
                raise ValueError(
                    'It is not allowed to mix events from different environments'
                )

        for event in self._events:
            if event.callbacks is None:
                self._check(event)
            else:
                event.callbacks.append(self._check)

        assert isinstance(self.callbacks, list)
        self.callbacks.append(self._build_value)

    def _check(self, event: Event) -> None:
        if self._value is not _PENDING:
            return
        self._count += 1
        if not event._ok:
            event._defused = True
            self.fail(event._value)
        elif self._evaluate(self._events, self._count):
            self.succeed()

    def _build_value(self, event: Event) -> None:
        self._remove_check_callbacks()
        if event._ok:
            self._value = ConditionValue()
            self._populate_value(self._value)

    def _populate_value(self, value: ConditionValue) -> None:
        for event in self._events:
            if isinstance(event, Condition):
                if event.triggered:
                    event._populate_value(value)
            elif event.callbacks is None:
                value.events.append(event)

    def _remove_check_callbacks(self) -> None:
        for event in self._events:
            if event.callbacks and self._check in event.callbacks:
                event.callbacks.remove(self._check)
            if isinstance(event, Condition):
                event._remove_check_callbacks()

    @staticmethod
    def all_events(events: tuple[Event, ...], count: int) -> bool:
        return len(events) == count

    @staticmethod
    def any_events(events: tuple[Event, ...], count: int) -> bool:
        return count > 0 or len(events) == 0


class AllOf(Condition):
    """Triggered when all *events* have been triggered."""

    def __init__(self, env: Environment, events: Iterable[Event]) -> None:
        super().__init__(env, Condition.all_events, events)


class AnyOf(Condition):
    """Triggered when any of *events* has been triggered."""

    def __init__(self, env: Environment, events: Iterable[Event]) -> None:
        super().__init__(env, Condition.any_events, events)


# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------


class StopSimulation(Exception):
    """Raised internally by the *until* callback to stop ``Environment.run()``."""

    @classmethod
    def callback(cls, event: Event) -> None:
        if event.ok:
            raise cls(event.value)
        raise event._value


class Environment:
    """Simulation clock and event scheduler.

    Events are stored as ``(time, priority, eid, event)`` tuples in a
    heap for O(log n) scheduling.
    """

    __slots__ = ('_now', '_queue', '_eid', '_active_proc')

    def __init__(self, initial_time: SimTime = 0) -> None:
        self._now: SimTime = initial_time
        self._queue: list[tuple[SimTime, EventPriority, int, Event]] = []
        self._eid = count()
        self._active_proc: Process | None = None

    @property
    def now(self) -> SimTime:
        """Current simulation time."""
        return self._now

    @property
    def active_process(self) -> Process | None:
        """The currently active process, or ``None``."""
        return self._active_proc

    # -- Factory methods ----------------------------------------------------

    def process(
        self,
        generator: ProcessGenerator,
        name: str | None = None,
    ) -> Process:
        """Create and return a new Process."""
        return Process(self, generator, name=name)

    def timeout(
        self,
        delay: SimTime = 0,
        value: Any = None,
    ) -> Timeout:
        """Return a Timeout that triggers after *delay*."""
        return Timeout(self, delay, value)

    def event(self) -> Event:
        """Return a new pending Event."""
        return Event(self)

    def all_of(self, events: Iterable[Event]) -> AllOf:
        """Return an AllOf condition for *events*."""
        return AllOf(self, events)

    def any_of(self, events: Iterable[Event]) -> AnyOf:
        """Return an AnyOf condition for *events*."""
        return AnyOf(self, events)

    # -- Scheduling ---------------------------------------------------------

    def schedule(
        self,
        event: Event,
        priority: EventPriority = NORMAL,
        delay: SimTime = 0,
    ) -> None:
        """Schedule *event* at ``now + delay`` with *priority*."""
        heappush(
            self._queue,
            (self._now + delay, priority, next(self._eid), event),
        )

    def peek(self) -> SimTime:
        """Time of the next scheduled event, or ``Infinity``."""
        try:
            return self._queue[0][0]
        except IndexError:
            return Infinity

    def step(self) -> None:
        """Process the next event.

        Raises:
            EmptySchedule: If no events remain.
        """
        try:
            self._now, _, _, event = heappop(self._queue)
        except IndexError:
            raise EmptySchedule from None

        callbacks, event.callbacks = event.callbacks, None  # type: ignore[assignment]
        for callback in callbacks:  # type: ignore[union-attr]
            callback(event)

        if not event._ok and not event._defused:
            raise _copy_exception(event._value)

    def run(self, until: SimTime | Event | None = None) -> Any:
        """Run the simulation.

        Args:
            until: Stop condition. ``None`` runs until no events remain.
                A number runs until simulation time reaches that value.
                An Event runs until that event is processed.

        Returns:
            The *until* event's value, or None.
        """
        if until is not None:
            if not isinstance(until, Event):
                at: SimTime = until
                if at <= self.now:
                    raise ValueError(f'until ({at}) must be > current simulation time')
                until = Event(self)
                until._ok = True
                until._value = None
                self.schedule(until, URGENT, at - self.now)
            elif until.callbacks is None:
                return until.value

            assert until.callbacks is not None
            until.callbacks.append(StopSimulation.callback)

        try:
            while True:
                self.step()
        except StopSimulation as exc:
            return exc.args[0]
        except EmptySchedule:
            if until is not None:
                assert not until.triggered
                raise RuntimeError(
                    f'No scheduled events left but "until" event was not '
                    f'triggered: {until}'
                ) from None
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _copy_exception(exc: BaseException) -> BaseException:
    """Create a copy of *exc* with a fresh traceback.

    Falls back to the original exception if re-construction fails
    (e.g. custom ``__init__`` signature).
    """
    try:
        copy = type(exc)(*exc.args)
    except Exception:
        copy = exc  # Can't reconstruct; reuse original.
    copy.__cause__ = exc
    return copy


def _describe_frame(frame: Any) -> str:
    """Format source location for error messages."""
    if frame is None:
        return ''
    filename = frame.f_code.co_filename
    name = frame.f_code.co_name
    lineno = frame.f_lineno
    try:
        with open(filename) as f:
            for no, line in enumerate(f):
                if no + 1 == lineno:
                    return (
                        f'  File "{filename}", line {lineno}, in {name}\n'
                        f'    {line.strip()}\n'
                    )
    except OSError:
        pass
    return f'  File "{filename}", line {lineno}, in {name}\n'
