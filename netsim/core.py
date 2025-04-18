from __future__ import annotations

from abc import ABC, abstractmethod
import time
from enum import IntEnum
from copy import deepcopy
from collections import deque
from heapq import heappop, heappush
from typing import (
    Deque,
    Generator,
    Iterator,
    List,
    Union,
    Optional,
    Dict,
    Callable,
    Any,
)
import logging
from netsim.common import (
    EventID,
    EventPriority,
    ProcessID,
    ProcessName,
    ResourceID,
    ResourceName,
    SimTime,
    TimeInterval,
)
from netsim.stat import ProcessStat, QueueStat, ResourceStat, SimStat


LOG_FMT = "%(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
logger = logging.getLogger(__name__)


class EventStatus(IntEnum):
    """
    A given Event can be in one of the following states:

    - CREATED: newly created event
    - PLANNED: event's time is set (planned), but not yet scheduled
    - SCHEDULED: event is placed in the event queue and can be triggered
    - TRIGGERED: the event is triggered and will be processed
    - PROCESSED: event happened and is removed from the queue
    """

    CREATED = 1
    PLANNED = 2
    SCHEDULED = 3
    TRIGGERED = 4
    PROCESSED = 5


class ProcessStatus(IntEnum):
    """
    A given Process can be in one of the following states:

    - CREATED: newly created process
    - RUNNING: process is running
    - STOPPED: process ended and cannot resume
    """

    CREATED = 1
    RUNNING = 2
    STOPPED = 3


class Process:
    """
    Represents a simulation process.

    Attributes:
        ctx: The simulation context.
        proc_id: The unique ID for this process in the context.
        name: Optional name of the process.
        status: Current status of the process.
        stat: Statistics object for the process.
    """

    def __init__(self, ctx: SimContext, coro: Coro, name: Optional[ProcessName] = None):
        """
        Initializes a new Process.

        Args:
            ctx: The simulation context.
            coro: The coroutine that defines this process's behavior.
            name: Optional name of the process.
        """
        self.ctx: SimContext = ctx
        self.proc_id: ProcessID = ctx.get_next_process_id()
        self.name: str = name if name else f"{type(self).__name__}_{self.proc_id}"
        self._coro: Coro = coro
        self._subscribers: List[Process] = [self]
        self._timeout: Optional[EventID] = None
        self.ctx.add_process(self)
        self.status: ProcessStatus = ProcessStatus.CREATED
        self.stat: ProcessStat = ProcessStat(ctx)
        self.stat_callbacks: List[StatCallback] = []
        self.tick_callbacks: List[TickCallback] = []

    def __repr__(self) -> str:
        return f"{self.name}(proc_id={self.proc_id}, coro={self._coro})"

    def extend_name(self, extension: str, prepend: bool = False) -> None:
        """
        Extend the process name with the given string extension.

        Args:
            extension: String to be appended or prepended.
            prepend: If True, prepend; otherwise, append.
        """
        if prepend:
            self.name = extension + self.name
        else:
            self.name = self.name + extension

    def in_timeout(self) -> bool:
        """
        Check if the process is currently waiting for a timeout event.

        Returns:
            True if waiting for a timeout event; False otherwise.
        """
        return self._timeout is not None

    def is_stopped(self) -> bool:
        """
        Check if the process status is STOPPED.

        Returns:
            True if STOPPED; False otherwise.
        """
        return self.status == ProcessStatus.STOPPED

    def _set_active(self) -> None:
        """
        Mark this process as the active one in the context.
        """
        self._timeout = None
        self.ctx.set_active_process(self)

    def start(self) -> None:
        """
        Start the process by advancing its coroutine until it yields an event or stops.
        """
        self.status = ProcessStatus.RUNNING
        self._set_active()
        try:
            event: Optional[Event] = next(self._coro)
        except StopIteration:
            self.status = ProcessStatus.STOPPED
            return  # No events, process stops immediately

        if event is not None:
            event.process = self
            self.stat.event_generated(event)
            for subscriber in self._subscribers:
                event.subscribe(subscriber)
            if event.is_planned and not event.is_scheduled:
                self.ctx.schedule_event(event)

    def resume(self, event: Event) -> None:
        """
        Resume the process coroutine, sending the event's value into it.

        Args:
            event: The event causing the process to resume.

        Raises:
            RuntimeError: If attempting to resume a non-running process or if the event
                does not match the one this process is waiting for.
        """
        if self.status != ProcessStatus.RUNNING:
            raise RuntimeError(f"Can't resume the process {self} that is not running!")

        # Only the timeout event that originally put the process to sleep can wake it
        if self._timeout is None or self._timeout == event.event_id:
            self._set_active()
            try:
                next_event: Optional[Event] = self._coro.send(event.value)
                if next_event is not None:
                    next_event.process = self
                    self.stat.event_generated(next_event)
                    for subscriber in self._subscribers:
                        next_event.subscribe(subscriber)
                    if next_event.is_planned and not next_event.is_scheduled:
                        self.ctx.schedule_event(next_event)
            except StopIteration:
                self.status = ProcessStatus.STOPPED
        else:
            raise RuntimeError(
                f"Wrong {event} to resume the process in timeout {self}!"
            )

    def subscribe(self, process: Process) -> None:
        """
        Add a process to the subscribers of the current process.

        Args:
            process: Process to subscribe.
        """
        self._subscribers.append(process)

    def timeout(self, delay: SimTime) -> Timeout:
        """
        Create a timeout event for this process to wait on.

        Args:
            delay: Time interval to wait.

        Returns:
            A Timeout event.
        """
        event = Timeout(self.ctx, delay=delay, process=self)
        self._timeout = event.event_id
        return event

    def exec_stat_callbacks(self) -> None:
        """
        Execute any registered statistic callbacks.
        """
        for stat_callback in self.stat_callbacks:
            stat_callback()

    def add_stat_callback(self, callback: StatCallback) -> None:
        """
        Register a callback function to be called when statistic data is collected.
        """
        self.stat_callbacks.append(callback)

    def exec_tick_callbacks(self) -> None:
        """
        Execute any registered tick callbacks.
        """
        for tick_callback in self.tick_callbacks:
            tick_callback()

    def add_tick_callback(self, callback: TickCallback) -> None:
        """
        Register a callback function to be called on simulation ticks.
        """
        self.tick_callbacks.append(callback)


class StatCollector(Process):
    """
    A dedicated process to collect (and optionally reset) simulation statistics at intervals.
    """

    def __init__(
        self,
        ctx: SimContext,
        stat_interval: SimTime,
        stat_container: SimStat,
        name: Optional[ProcessName] = None,
    ):
        """
        Initialize a new StatCollector.

        Args:
            ctx: Simulation context.
            stat_interval: Interval (time) between statistic collections.
            stat_container: The SimStat object where stats are accumulated.
            name: Optional name for this collector process.
        """
        self._stat_interval = stat_interval
        self._stat_container = stat_container
        super().__init__(ctx, self._collection_trigger(), name=name)
        self.add_stat_callback(self.stat.advance_time)

    def _collect(self, reset: bool) -> None:
        """
        Iterate over all processes and resources in the context and collect stats.

        Args:
            reset: If True, reset stats after collection; otherwise, accumulate.
        """
        self._stat_container.advance_time()

        for process in self.ctx.get_process_iter():
            process.stat.update_stat()
            interval: TimeInterval = (
                self._stat_container.prev_timestamp,
                self._stat_container.cur_timestamp,
            )
            self._stat_container.process_stat_samples.setdefault(process.name, {})[
                interval
            ] = deepcopy(process.stat.cur_stat_frame)
            if reset:
                process.stat.reset_stat()

        for resource in self.ctx.get_resource_iter():
            resource.stat.update_stat()
            interval: TimeInterval = (
                self._stat_container.prev_timestamp,
                self._stat_container.cur_timestamp,
            )
            self._stat_container.resource_stat_samples.setdefault(resource.name, {})[
                interval
            ] = deepcopy(resource.stat.cur_stat_frame)
            if reset:
                resource.stat.reset_stat()

    def _collection_trigger(self) -> Generator[Optional[Event], None, None]:
        """
        Coroutine that yields events to trigger periodic collection.

        Yields:
            CollectStat events if a stat interval is set; otherwise stops immediately.
        """
        if self._stat_interval:
            while True:
                yield CollectStat(self.ctx, delay=self._stat_interval, process=self)
                self._collect(reset=True)
        else:
            yield

    def collect_now(self) -> None:
        """
        Manual statistic collection without resetting.
        """
        self._collect(reset=False)


class Resource(ABC):
    """
    Abstract base class for a resource entity in the simulation.
    """

    def __init__(
        self,
        ctx: SimContext,
        capacity: Optional[int] = None,
        name: Optional[ResourceName] = None,
    ):
        """
        Initialize a new Resource.

        Args:
            ctx: Simulation context.
            capacity: Optional capacity limit of the resource.
            name: Resource name.
        """
        self.ctx = ctx
        self._capacity: Optional[int] = capacity
        self.res_id: ResourceID = ctx.get_next_resource_id()
        self.name: str = name if name else f"{type(self).__name__}_{self.res_id}"
        self._put_queue: Deque[Put] = deque()
        self._get_queue: Deque[Get] = deque()
        self.stat = ResourceStat(ctx)
        self.stat_callbacks: List[StatCallback] = []
        self.tick_callbacks: List[TickCallback] = []
        self.ctx.add_resource(self)

    def __repr__(self) -> str:
        return f"{self.name}(res_id={self.res_id}, capacity={self._capacity})"

    def extend_name(self, extension: str, prepend: bool = False) -> None:
        """
        Extend the resource name with the given string extension.
        """
        if prepend:
            self.name = extension + self.name
        else:
            self.name = self.name + extension

    @abstractmethod
    def _put_admission_check(self, event: Put) -> bool:
        """
        Define when a put event can be admitted (e.g., depends on capacity).
        """
        raise NotImplementedError("Subclasses must implement _put_admission_check.")

    @abstractmethod
    def _get_admission_check(self, event: Get) -> bool:
        """
        Define when a get event can be admitted (e.g., depends on availability).
        """
        raise NotImplementedError("Subclasses must implement _get_admission_check.")

    def add_put(self, event: Put) -> None:
        """
        Register a new put request on this resource.
        """
        self._put_queue.append(event)
        self._trigger_put(event)

    def add_get(self, event: Get) -> None:
        """
        Register a new get request on this resource.
        """
        self._get_queue.append(event)
        self._trigger_get(event)

    def _trigger_get(self, event: Event) -> None:
        """
        Attempt to admit any pending get events that can be satisfied.
        """
        for _ in range(len(self._get_queue)):
            get_event = self._get_queue.popleft()
            if self._get_admission_check(get_event):
                get_event.add_callback(self._trigger_put)
                self.ctx.schedule_event(get_event, self.ctx.now)
                get_event.trigger()
                break
            self._get_queue.append(get_event)

    def _trigger_put(self, event: Event) -> None:
        """
        Attempt to admit any pending put events that can be satisfied.
        """
        for _ in range(len(self._put_queue)):
            put_event = self._put_queue.popleft()
            if self._put_admission_check(put_event):
                put_event.add_callback(self._trigger_get)
                self.ctx.schedule_event(put_event, self.ctx.now)
                put_event.trigger()
                break
            self._put_queue.append(put_event)

    def request(self) -> Get:
        """
        Syntactic sugar for resource get.
        """
        raise NotImplementedError("Subclasses must implement request.")

    def release(self) -> Put:
        """
        Syntactic sugar for resource put.
        """
        raise NotImplementedError("Subclasses must implement release.")

    @abstractmethod
    def put(self, *args: Any, **kwargs: Any) -> Put:
        """
        Create and return a put event.
        """
        raise NotImplementedError("Subclasses must implement put.")

    @abstractmethod
    def get(self, *args: Any, **kwargs: Any) -> Get:
        """
        Create and return a get event.
        """
        raise NotImplementedError("Subclasses must implement get.")

    @abstractmethod
    def _put_callback(self, event: Put) -> None:
        """
        Callback executed when a put event is admitted.
        """
        raise NotImplementedError("Subclasses must implement _put_callback.")

    @abstractmethod
    def _get_callback(self, event: Get) -> None:
        """
        Callback executed when a get event is admitted.
        """
        raise NotImplementedError("Subclasses must implement _get_callback.")

    def exec_stat_callbacks(self) -> None:
        """
        Execute registered callbacks related to statistics.
        """
        for stat_callback in self.stat_callbacks:
            stat_callback()

    def add_stat_callback(self, callback: StatCallback) -> None:
        """
        Register a callback function to be called when statistic data is collected.
        """
        self.stat_callbacks.append(callback)

    def exec_tick_callbacks(self) -> None:
        """
        Execute registered callbacks on simulation tick.
        """
        for tick_callback in self.tick_callbacks:
            tick_callback()

    def add_tick_callback(self, callback: TickCallback) -> None:
        """
        Register a callback function to be called on simulation ticks.
        """
        self.tick_callbacks.append(callback)


class QueueFIFO(Resource):
    """
    A FIFO queue resource, supporting put (enqueuing) and get (dequeuing) events.
    """

    def __init__(
        self,
        ctx: SimContext,
        capacity: Optional[int] = None,
        name: Optional[ResourceName] = None,
    ):
        super().__init__(ctx, capacity, name)
        self._queue: Deque[Any] = deque()
        self.stat = QueueStat(ctx)

    def __len__(self) -> int:
        return len(self._queue)

    def _put_admission_check(self, event: PutQueueFIFO) -> bool:
        """
        Check if the queue can accept a new item.
        """
        if event.process.in_timeout():
            return False
        if self._capacity is None:
            return True
        return len(self._queue) < self._capacity

    def _get_admission_check(self, event: GetQueueFIFO) -> bool:
        """
        Check if there is an item available to get.
        """
        if event.process.in_timeout():
            return False
        return bool(self._queue)

    def put(self, item: Any) -> PutQueueFIFO:
        """
        Create and return a PutQueueFIFO event.
        """
        put_event = PutQueueFIFO(
            self.ctx, self, process=self.ctx.active_process, item=item
        )
        self.stat.put_requested(put_event)
        return put_event

    def get(self) -> GetQueueFIFO:
        """
        Create and return a GetQueueFIFO event.
        """
        get_event = GetQueueFIFO(self.ctx, self, process=self.ctx.active_process)
        self.stat.get_requested(get_event)
        return get_event

    def _put_callback(self, event: PutQueueFIFO) -> None:
        """
        Enqueue the item and update stats.
        """
        self._queue.append(event.item)
        self.stat.put_processed(event)

    def _get_callback(self, event: GetQueueFIFO) -> None:
        """
        Dequeue the item and update stats.
        """
        event.item = self._queue.popleft()
        self.stat.get_processed(event)


class Event:
    """
    An event in the simulation.
    """

    def __init__(
        self,
        ctx: SimContext,
        time: Optional[SimTime] = None,
        func: Optional[Callable[[Event], Any]] = None,
        priority: EventPriority = 10,
        process: Optional[Process] = None,
        auto_trigger: bool = False,
    ):
        self.ctx: SimContext = ctx
        self._func: Optional[Callable[[Event], Any]] = func
        self._callbacks: List[EventCallback] = []
        self._value: Any = None
        self.time: Optional[SimTime] = time
        self.priority: EventPriority = priority
        self.event_id: EventID = ctx.get_next_event_id()
        self.process: Optional[Process] = process
        self.status: EventStatus = (
            EventStatus.CREATED if self.time is None else EventStatus.PLANNED
        )
        self._auto_trigger: bool = auto_trigger

    def __hash__(self) -> EventID:
        return self.event_id

    def __eq__(self, other: Event) -> bool:
        return self.event_id == other.event_id

    def __lt__(self, other: Event) -> bool:
        if self.time < other.time:
            return True
        if self.time == other.time:
            if self.priority < other.priority:
                return True
            if self.priority == other.priority:
                return self.event_id < other.event_id
        return False

    def __repr__(self) -> str:
        type_name = type(self).__name__
        process_name = self.process.name if self.process else None
        return f"{type_name}(time={self.time}, event_id={self.event_id}, proc={process_name})"

    @property
    def value(self) -> Any:
        """
        The computed value of the event if func is set, otherwise the stored value.
        """
        if self._func is not None:
            self._value = self._func(self)
        return self._value

    @property
    def is_planned(self) -> bool:
        return self.status >= EventStatus.PLANNED

    @property
    def is_scheduled(self) -> bool:
        return self.status >= EventStatus.SCHEDULED

    @property
    def is_triggered(self) -> bool:
        return self.status >= EventStatus.TRIGGERED

    def plan(self, time: SimTime) -> None:
        if self.status < EventStatus.PLANNED:
            if self.time is None:
                if time is None:
                    raise RuntimeError(f"Cannot plan {self}. No time set!")
                self.time = time
            self.status = EventStatus.PLANNED
        else:
            raise RuntimeError(f"Cannot plan {self}. It has already been planned!")

    def schedule(self, time: Optional[SimTime] = None) -> None:
        if not self.is_planned:
            self.plan(time)
        elif self.status == EventStatus.SCHEDULED:
            raise RuntimeError(f"{self} has already been scheduled!")

        self.status = EventStatus.SCHEDULED
        self.ctx.add_event(self)
        if self._auto_trigger:
            self.trigger()

    def trigger(self) -> None:
        if self.status != EventStatus.SCHEDULED:
            raise RuntimeError(f"{self} cannot be triggered as it wasn't scheduled!")
        self.status = EventStatus.TRIGGERED

    def subscribe(self, proc: Process) -> None:
        self.add_callback(proc.resume)

    def add_callback(self, callback: EventCallback) -> None:
        self._callbacks.append(callback)

    def run(self) -> None:
        if self.status < EventStatus.TRIGGERED:
            raise RuntimeError(f"Cannot run event {self}. It was not triggered!")
        if self.process:
            self.process.stat.event_exec(self)

        for callback in self._callbacks:
            callback(self)

        self.status = EventStatus.PROCESSED


class Timeout(Event):
    """
    An event that automatically triggers after a given delay from now.
    """

    def __init__(
        self,
        ctx: SimContext,
        delay: SimTime,
        priority: EventPriority = 11,
        process: Optional[Process] = None,
    ):
        self._delay = delay
        super().__init__(ctx, ctx.now + delay, priority=priority, process=process)
        # Timeout retains auto_trigger
        self._auto_trigger = True

    def __repr__(self) -> str:
        type_name = type(self).__name__
        process_name = self.process.name if self.process else None
        return (
            f"{type_name}(time={self.time}, event_id={self.event_id}, "
            f"proc={process_name}, delay={self._delay})"
        )


class StopSim(Timeout):
    """
    Event that stops the simulation after the specified delay.
    """

    def __init__(
        self,
        ctx: SimContext,
        delay: SimTime,
        priority: EventPriority = 2,
        process: Optional[Process] = None,
    ):
        super().__init__(ctx, delay, priority=priority, process=process)
        self._auto_trigger = True
        self._callbacks.append(self.ctx.stop)

    def __repr__(self) -> str:
        type_name = type(self).__name__
        process_name = self.process.name if self.process else None
        return (
            f"{type_name}(time={self.time}, event_id={self.event_id}, "
            f"proc={process_name}, delay={self._delay})"
        )


class CollectStat(Timeout):
    """
    Periodic statistic collection event.
    """

    def __init__(
        self,
        ctx: SimContext,
        delay: SimTime,
        priority: EventPriority = 1,
        process: Optional[Process] = None,
    ):
        super().__init__(ctx, delay, priority=priority, process=process)
        self._auto_trigger = True

    def __repr__(self) -> str:
        type_name = type(self).__name__
        process_name = self.process.name if self.process else None
        return (
            f"{type_name}(time={self.time}, event_id={self.event_id}, "
            f"proc={process_name}, delay={self._delay})"
        )


class Put(Event):
    """
    Base put event that attempts to put data into a resource.
    """

    def __init__(
        self,
        ctx: SimContext,
        resource: Resource,
        process: Process,
        priority: EventPriority = 10,
    ):
        """
        Initialize a base Put event.
        """
        super().__init__(ctx, priority=priority, process=process)
        self._resource = resource
        self._callbacks.append(self._resource._put_callback)
        self._resource.add_put(self)


class PutQueueFIFO(Put):
    """
    Put event for QueueFIFO resources.
    """

    def __init__(
        self,
        ctx: SimContext,
        resource: QueueFIFO,
        process: Process,
        item: Any,
    ):
        super().__init__(ctx, resource, process)
        self.item: Any = item


class Get(Event):
    """
    Base get event that attempts to retrieve data from a resource.
    """

    def __init__(
        self,
        ctx: SimContext,
        resource: Resource,
        process: Process,
        priority: EventPriority = 9,
    ):
        super().__init__(ctx, priority=priority, process=process)
        self._resource = resource
        self._callbacks.append(self._resource._get_callback)
        self._resource.add_get(self)


class GetQueueFIFO(Get):
    """
    Get event for QueueFIFO resources.
    """

    def __init__(
        self,
        ctx: SimContext,
        resource: QueueFIFO,
        process: Process,
    ):
        super().__init__(ctx, resource, process)
        self.item: Any = None
        self._func = lambda event: event.item


class SimContext:
    """
    Maintains the global state of the simulation, including event queue,
    time, processes, and resources.
    """

    def __init__(self, starttime: SimTime = 0):
        self.now: SimTime = starttime
        self.active_process: Optional[Process] = None
        self._event_queue: List[Event] = []
        self._procs: Dict[ProcessID, Process] = {}
        self._resources: Dict[ResourceID, Resource] = {}
        self._stopped: bool = False
        self._nextevent_id: EventID = 0
        self._next_process_id: ProcessID = 1  # 0 is reserved for the simulator
        self._next_resource_id: ResourceID = 0
        self.stat = None

    def __deepcopy__(self, memo: Dict[Any, Any]) -> None:
        # Prevent copying references that must remain unique.
        return None

    def get_next_event_id(self) -> EventID:
        nextevent_id = self._nextevent_id
        self._nextevent_id += 1
        return nextevent_id

    def get_next_process_id(self) -> ProcessID:
        next_process_id = self._next_process_id
        self._next_process_id += 1
        return next_process_id

    def get_next_resource_id(self) -> ResourceID:
        next_resource_id = self._next_resource_id
        self._next_resource_id += 1
        return next_resource_id

    def is_stopped(self) -> bool:
        return self._stopped

    def get_event(self) -> Optional[Event]:
        if self._event_queue:
            return heappop(self._event_queue)
        return None

    def add_event(self, event: Event) -> None:
        if not event.is_scheduled:
            raise RuntimeError(f"Cannot add {event}. The event has not been scheduled.")
        if event.time is not None and event.time < self.now:
            raise RuntimeError(
                f"Cannot add {event}. The event.time {event.time} is in the past. Now is {self.now}"
            )
        heappush(self._event_queue, event)

    def schedule_event(self, event: Event, time: Optional[SimTime] = None) -> None:
        if event.time is None:
            if time is None:
                raise RuntimeError(f"Cannot schedule {event}. No time set!")
        else:
            time = event.time
        if time < self.now:
            raise RuntimeError(
                f"Cannot schedule {event} into the past ({time}). Now is {self.now}."
            )
        event.schedule(time)

    def stop(self, event: Optional[Event] = None) -> None:
        _ = event
        self._stopped = True

    def advance_simtime(self, newtime: SimTime) -> bool:
        if newtime > self.now:
            self.now = newtime
            return True
        return False

    def create_process(self, coro: Coro, name: Optional[ProcessName]) -> Process:
        return Process(ctx=self, coro=coro, name=name)

    def add_process(self, process: Process) -> None:
        self._procs[process.proc_id] = process

    def add_resource(self, resource: Resource) -> None:
        self._resources[resource.res_id] = resource

    def get_process_iter(self) -> Iterator[Process]:
        return iter(self._procs.values())

    def get_resource_iter(self) -> Iterator[Resource]:
        return iter(self._resources.values())

    def set_active_process(self, process: Process) -> None:
        self.active_process = process

    def exec_all_stat_callbacks(self) -> None:
        for proc in self._procs.values():
            proc.exec_stat_callbacks()
        for resource in self._resources.values():
            resource.exec_stat_callbacks()

    def exec_all_tick_callbacks(self) -> None:
        for proc in self._procs.values():
            proc.exec_tick_callbacks()
        for resource in self._resources.values():
            resource.exec_tick_callbacks()


class Simulator:
    """
    Main driver that handles simulation execution and optional stat collection intervals.
    """

    def __init__(
        self, ctx: Optional[SimContext] = None, stat_interval: Optional[float] = None
    ):
        self.ctx: SimContext = ctx if ctx is not None else SimContext()
        self.event_counter = 0
        self.stat: SimStat = SimStat(self.ctx)
        self._stat_interval: Optional[float] = stat_interval
        self.stat_collectors: List[StatCollector] = []
        self.add_stat_collector(
            StatCollector(
                self.ctx, stat_interval=stat_interval, stat_container=self.stat
            )
        )

    @property
    def avg_event_rate(self) -> float:
        if self.ctx.now == 0:
            return 0.0
        return self.event_counter / self.ctx.now

    @property
    def now(self) -> SimTime:
        return self.ctx.now

    def add_stat_collector(self, stat_collector: StatCollector) -> None:
        self.stat_collectors.append(stat_collector)

    def run(self, until_time: Optional[SimTime] = None) -> None:
        if until_time is not None:
            self.ctx.schedule_event(StopSim(self.ctx, delay=until_time))
        self._run()

    def _run(self) -> None:
        started_at = time.time()

        # Start all processes
        for proc in self.ctx.get_process_iter():
            proc.start()

        # Main event loop
        while (event := self.ctx.get_event()) and not self.ctx.is_stopped():
            if self.ctx.advance_simtime(event.time):
                self.ctx.exec_all_stat_callbacks()
                self.ctx.exec_all_tick_callbacks()
            if event.is_triggered:
                event.run()
                self.event_counter += 1

        # If no stat interval was set, do one final collection
        if not self._stat_interval:
            for stat_collector in self.stat_collectors:
                stat_collector.collect_now()

        logger.info(
            "Simulation ended at %s, it took %s wall clock seconds. Executed %s events.",
            self.ctx.now,
            time.time() - started_at,
            self.event_counter,
        )


StatCallback = Callable[[], None]
TickCallback = Callable[[], None]
EventCallback = Callable[[Event], None]
Coro = Generator[Optional[Event], Any, Optional[Event]]
