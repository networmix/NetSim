# pylint: disable=too-many-instance-attributes,too-many-lines
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
from netsim.simulator.sim_common import (
    EventID,
    EventPriority,
    ProcessID,
    ProcessName,
    ResourceID,
    ResourceName,
    SimTime,
    TimeInterval,
)

from netsim.simulator.simstat import ProcessStat, QueueStat, ResourceStat, SimStat


LOG_FMT = "%(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
logger = logging.getLogger(__name__)


class EventStatus(IntEnum):
    """
    A given Event can be in the following states:
        CREATED - newly created event
        PLANNED - this event can happen if it gets scheduled and triggered
        SCHEDULED - event is placed into the event queue and can happen if triggered
        TRIGGERED - the event will happen
        PROCESSED - event happened, has been removed from the queue and processed
    """

    CREATED = 1
    PLANNED = 2
    SCHEDULED = 3
    TRIGGERED = 4
    PROCESSED = 5


class ProcessStatus(IntEnum):
    """
    A given Process can be in one of the following states:
        CREATED - newly created process
        RUNNING - process has started and can run
        STOPPED - process is stopped and can't be resumed
    """

    CREATED = 1
    RUNNING = 2
    STOPPED = 3


class Process:
    def __init__(self, ctx: SimContext, coro: Coro, name: Optional[ProcessName] = None):
        self._ctx: SimContext = ctx
        self.proc_id: ProcessID = ctx.get_next_process_id()
        self.name = name if name else f"{type(self).__name__}_{self.proc_id}"
        self._coro: Coro = coro
        self._subscribers: List[Process] = [self]
        self._timeout: Optional[EventID] = None
        self._ctx.add_process(self)
        self.status: ProcessStatus = ProcessStatus.CREATED
        self.stat: ProcessStat = ProcessStat(ctx)
        self.stat_callbacks: List[StatCallback] = []
        self.tick_callbacks: List[TickCallback] = []

    def __repr__(self) -> str:
        return f"{self.name}(proc_id={self.proc_id}, coro={self._coro})"

    def extend_name(self, extension: str, prepend: bool = False) -> None:
        if prepend:
            self.name = extension + self.name
        else:
            self.name = self.name + extension

    def in_timeout(self) -> bool:
        return self._timeout is not None

    def is_stopped(self) -> bool:
        return self.status == ProcessStatus.STOPPED

    def _set_active(self) -> None:
        self._timeout = None
        self._ctx.set_active_process(self)

    def start(self, *args: List[Any], **kwargs: Dict[str, Any]) -> None:
        _, _ = args, kwargs
        self.status = ProcessStatus.RUNNING
        self._set_active()
        event: Optional[Event] = next(self._coro)
        if event is not None:
            event.process = self
            self.stat.event_generated(event)
            for subscriber in self._subscribers:
                event.subscribe(subscriber)
            if event.is_planned() and not event.is_scheduled():
                self._ctx.schedule_event(event)

    def resume(self, event: Event, *args: List[Any], **kwargs: Dict[str, Any]) -> None:
        _, _ = args, kwargs
        if self.status != ProcessStatus.RUNNING:
            raise RuntimeError(f"Can't resume the process {self} that is not running!")

        if not self._timeout or self._timeout == event.event_id:
            # If I'm in timeout, only expiration of this timeout can wake me up
            self._set_active()
            try:
                next_event: Optional[Event] = self._coro.send(event.value)
                if next_event is not None:
                    next_event.process = self
                    self.stat.event_generated(event)
                    for subscriber in self._subscribers:
                        next_event.subscribe(subscriber)
                    if next_event.is_planned() and not next_event.is_scheduled():
                        self._ctx.schedule_event(next_event)
            except StopIteration:
                self.status = ProcessStatus.STOPPED

        else:
            raise RuntimeError(
                f"Wrong {event} to resume the process in timeout {self}!"
            )

    def subscribe(
        self, process: Process, *args: List[Any], **kwargs: Dict[str, Any]
    ) -> None:
        _, _ = args, kwargs
        self._subscribers.append(process)

    def timeout(self, delay: SimTime) -> Timeout:
        event = Timeout(self._ctx, delay=delay, process=self)
        self._timeout = event.event_id
        return event

    def exec_stat_callbacks(self) -> None:
        for stat_callback in self.stat_callbacks:
            stat_callback()

    def add_stat_callback(self, callback: StatCallback) -> None:
        self.stat_callbacks.append(callback)

    def exec_tick_callbacks(self) -> None:
        for tick_callback in self.tick_callbacks:
            tick_callback()

    def add_tick_callback(self, callback: TickCallback) -> None:
        self.tick_callbacks.append(callback)


class StatCollector(Process):
    def __init__(
        self,
        ctx: SimContext,
        stat_interval: SimTime,
        stat_container: SimStat,
        name: Optional[ProcessName] = None,
    ):
        self._stat_interval = stat_interval
        self._stat_container = stat_container
        super().__init__(ctx, self._collection_trigger(), name=name)
        self.add_stat_callback(self.stat.advance_time)

    def _collect(self, reset: bool) -> None:
        self._stat_container.advance_time()

        for process in self._ctx.get_process_iter():
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

        for resource in self._ctx.get_resource_iter():
            resource.stat.update_stat()
            interval: TimeInterval = (
                self._stat_container.prev_timestamp,
                self._stat_container.cur_timestamp,
            )
            self._stat_container.process_stat_samples.setdefault(resource.name, {})[
                interval
            ] = deepcopy(resource.stat.cur_stat_frame)
            if reset:
                resource.stat.reset_stat()

    def _collection_trigger(self) -> Event:
        if self._stat_interval:
            while True:
                yield CollectStat(self._ctx, delay=self._stat_interval, process=self)
                self._collect(reset=True)
        else:
            yield

    def collect_now(self) -> None:
        self._collect(
            reset=False,
        )


class Resource(ABC):
    def __init__(
        self,
        ctx: SimContext,
        capacity: Optional[int] = None,
        name: Optional[ResourceName] = None,
    ):
        self._ctx = ctx
        self._capacity: Optional[int] = capacity
        self.res_id: ResourceID = ctx.get_next_resource_id()
        self.name = name if name else f"{type(self).__name__}_{self.res_id}"
        self._put_queue: Deque[Put] = deque()
        self._get_queue: Deque[Get] = deque()
        self.stat = ResourceStat(ctx)
        self.stat_callbacks: List[StatCallback] = []
        self.tick_callbacks: List[TickCallback] = []

    def __repr__(self) -> str:
        return f"{self.name}(res_id={self.res_id}, capacity={self._capacity})"

    def extend_name(self, extension: str, prepend: bool = False) -> None:
        if prepend:
            self.name = extension + self.name
        else:
            self.name = self.name + extension

    @abstractmethod
    def _put_admission_check(self, event: Put) -> bool:
        raise NotImplementedError(self)

    @abstractmethod
    def _get_admission_check(self, event: Get) -> bool:
        raise NotImplementedError(self)

    def add_put(self, event: Put) -> None:
        self._put_queue.append(event)
        self._trigger_put(event)

    def _trigger_get(self, event: Event) -> None:
        _ = event

        for _ in range(len(self._get_queue)):
            get_event = self._get_queue.popleft()
            if self._get_admission_check(get_event):
                get_event.add_callback(self._trigger_put)
                self._ctx.schedule_event(get_event, self._ctx.now)
                break
            self._get_queue.append(get_event)

    def add_get(self, event: Get) -> None:
        self._get_queue.append(event)
        self._trigger_get(event)

    def _trigger_put(self, event: Event) -> None:
        _ = event

        for _ in range(len(self._put_queue)):
            put_event = self._put_queue.popleft()
            if self._put_admission_check(put_event):
                put_event.add_callback(self._trigger_get)
                self._ctx.schedule_event(put_event, self._ctx.now)
                break
            self._put_queue.append(put_event)

    def request(self) -> Get:
        raise NotImplementedError(self)

    def release(self) -> Put:
        raise NotImplementedError(self)

    @abstractmethod
    def put(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Put:
        raise NotImplementedError

    @abstractmethod
    def get(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Get:
        raise NotImplementedError

    @abstractmethod
    def _put_callback(self, event: Put) -> None:
        raise NotImplementedError

    @abstractmethod
    def _get_callback(self, event: Get) -> None:
        raise NotImplementedError

    def exec_stat_callbacks(self) -> None:
        for stat_callback in self.stat_callbacks:
            stat_callback()

    def add_stat_callback(self, callback: StatCallback) -> None:
        self.stat_callbacks.append(callback)

    def exec_tick_callbacks(self) -> None:
        for tick_callback in self.stat_callbacks:
            tick_callback()

    def add_tick_callback(self, callback: TickCallback) -> None:
        self.tick_callbacks.append(callback)


class QueueFIFO(Resource):
    def __init__(
        self,
        ctx: SimContext,
        capacity: Optional[int] = None,
        name: Optional[ResourceName] = None,
    ):
        super().__init__(ctx, capacity, name)
        self._queue: Deque[Any] = deque()
        self.stat = QueueStat(ctx)

    def __len__(self):
        return len(self._queue)

    def _put_admission_check(self, event: PutQueueFIFO) -> bool:
        if event.process.in_timeout():
            return False
        if self._capacity is None:
            return True
        if len(self._queue) < self._capacity:
            return True
        return False

    def _get_admission_check(self, event: GetQueueFIFO) -> bool:
        if event.process.in_timeout():
            return False
        if self._queue:
            return True
        return False

    def put(
        self, item: Any, *args: List[Any], **kwargs: Dict[str, Any]
    ) -> PutQueueFIFO:
        put_event = PutQueueFIFO(
            self._ctx, self, process=self._ctx.active_process, item=item
        )
        self.stat.put_requested(put_event)
        return put_event

    def get(self, *args: List[Any], **kwargs: Dict[str, Any]) -> GetQueueFIFO:
        get_event = GetQueueFIFO(self._ctx, self, process=self._ctx.active_process)
        self.stat.get_requested(get_event)
        return get_event

    def _put_callback(self, event: PutQueueFIFO) -> None:
        self._queue.append(event.item)
        self.stat.put_processed(event)

    def _get_callback(self, event: GetQueueFIFO) -> None:
        event.item = self._queue.popleft()
        self.stat.get_processed(event)


class Event:
    def __init__(
        self,
        ctx: SimContext,
        time: Optional[SimTime] = None,
        func: Optional[Callable[[Event], None]] = None,
        priority: EventPriority = 10,
        process: Optional[Process] = None,
        auto_trigger: bool = False,
    ):
        self._ctx: SimContext = ctx
        self._func: Callable[[Event], Any] = func
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
                if self.event_id < other.event_id:
                    return True
        return False

    def __repr__(self) -> str:
        type_name = type(self).__name__
        process_name = self.process.name if self.process else None
        return f"{type_name}(time={self.time}, event_id={self.event_id}, proc={process_name})"

    @property
    def value(self) -> Any:
        if self._func is not None:
            self._value = self._func(self)
        return self._value

    def is_planned(self) -> bool:
        return self.status >= EventStatus.PLANNED

    def is_scheduled(self) -> bool:
        return self.status >= EventStatus.SCHEDULED

    def is_triggered(self) -> bool:
        return self.status >= EventStatus.TRIGGERED

    def plan(self, time: SimTime) -> None:
        """
        A given event instance can be planned only once.
        It is done either by setting time during creation or by calling this method.
        """
        if self.status < EventStatus.PLANNED:
            if self.time is None:
                if time is None:
                    raise RuntimeError(f"Can not plan {self}. No time set!")
                self.time = time
            self.status = EventStatus.PLANNED
        else:
            raise RuntimeError(f"Can not plan {self}. It has been already planned!")

    def schedule(self, time: Optional[SimTime] = None) -> None:
        """
        A given event instance can be scheduled only once. If this event has not been planned yet, the
        parameter 'time' is used to plan it. If a given event has _auto_trigger variable set to True,
        this method will also trigger the event.
        """
        if not self.is_planned():
            self.plan(time)

        elif self.status == EventStatus.SCHEDULED:
            raise RuntimeError(f"{self} has already been scheduled!")

        self.status = EventStatus.SCHEDULED

        self._ctx.add_event(self)
        if self._auto_trigger:
            self.trigger()

    def trigger(self) -> None:
        """
        Event needs to be scheduled to be triggered.
        Event can be triggered multiple times.
        """
        if self.status != EventStatus.SCHEDULED:
            raise RuntimeError(f"{self} can not be triggered as it wasn't scheduled!")
        self.status = EventStatus.TRIGGERED

    def subscribe(self, proc: Process) -> None:
        self.add_callback(callback=proc.resume)

    def add_callback(self, callback: EventCallback) -> None:
        self._callbacks.append(callback)

    def run(self) -> None:
        if self.status < EventStatus.TRIGGERED:
            raise RuntimeError(f"Can not run event {self}. It was not triggered!")
        if self.process:
            self.process.stat.event_exec(self)

        for callback in self._callbacks:
            # Resuming processes that were subscribed to this event
            callback(self)


class Timeout(Event):
    def __init__(
        self,
        ctx: SimContext,
        delay: SimTime,
        priority: EventPriority = 11,
        process: Optional[Process] = None,
    ):
        self._delay = delay
        super().__init__(ctx, ctx.now + delay, priority=priority, process=process)
        self._auto_trigger = True

    def __repr__(self) -> str:
        type_name = type(self).__name__
        process_name = self.process.name if self.process else None
        return f"{type_name}(time={self.time}, event_id={self.event_id}, proc={process_name}, delay={self._delay})"


class StopSim(Timeout):
    def __init__(
        self,
        ctx: SimContext,
        delay: SimTime,
        priority: EventPriority = 2,
        process: Optional[Process] = None,
    ):
        super().__init__(ctx, ctx.now + delay, priority=priority, process=process)
        self._auto_trigger = True
        self._callbacks.append(self._ctx.stop)

    def __repr__(self) -> str:
        type_name = type(self).__name__
        process_name = self.process.name if self.process else None
        return f"{type_name}(time={self.time}, event_id={self.event_id}, proc={process_name}, delay={self._delay})"


class CollectStat(Timeout):
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
        return f"{type_name}(time={self.time}, event_id={self.event_id}, proc={process_name}, delay={self._delay})"


class Put(Event):
    def __init__(
        self,
        ctx: SimContext,
        resource: Resource,
        process: Process,
        priority: EventPriority = 10,
    ):
        super().__init__(ctx, priority=priority, process=process)
        self._auto_trigger = True
        self._resource = resource
        self._callbacks.append(self._resource._put_callback)
        self._resource.add_put(self)


class PutQueueFIFO(Put):
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
    def __init__(
        self,
        ctx: SimContext,
        resource: Resource,
        process: Process,
        priority: EventPriority = 9,
    ):
        super().__init__(ctx, priority=priority, process=process)
        self._auto_trigger = True
        self._resource = resource
        self._callbacks.append(self._resource._get_callback)
        self._resource.add_get(self)


class GetQueueFIFO(Get):
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
    def __init__(self, starttime: SimTime = 0):
        self.now: SimTime = starttime
        self.active_process: Union[Process, None] = None
        self._event_queue: List[Event] = []
        self._procs: Dict[ProcessID, Process] = {}
        self._resources: Dict[ResourceID, Resource] = {}
        self._stopped: bool = False
        self._nextevent_id: EventID = 0
        self._next_process_id: ProcessID = 1  # 0 is reserved for the simulator
        self._next_resource_id: ResourceID = 0
        self.stat = None

    def __deepcopy__(self, memo: Dict[Any, Any]) -> None:
        """
        There is no real point in making a deep copy of the SimContext.
        Moreover, it breaks copying of Stat objects. Hence, making it a no-op.
        """
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

    def get_event(self) -> Union[Event, None]:
        if self._event_queue:
            return heappop(self._event_queue)
        return None

    def add_event(self, event: Event) -> None:
        if not event.is_scheduled:
            raise RuntimeError(
                f"Can not add {event}. The event has not been scheduled."
            )
        if event.time < self.now:
            raise RuntimeError(
                f"Can not add {event}. The event.time is {event.time} is in the past. Now is {self.now}"
            )

        heappush(self._event_queue, event)

    def schedule_event(self, event: Event, time: Optional[SimTime] = None) -> None:
        if event.time is None:
            if time is None:
                raise RuntimeError(f"Can not schedule {event}. No time set!")
        else:
            time = event.time

        if time < self.now:
            raise RuntimeError(
                f"Can not schedule {event} into the past ({time}). Now is {self.now}."
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
        proc = Process(ctx=self, coro=coro, name=name)
        self._procs[proc.proc_id] = proc
        return proc

    def add_process(self, process: Process):
        self._procs[process.proc_id] = process

    def get_process_iter(self) -> Iterator[Process]:
        return iter(self._procs.values())

    def get_resource_iter(self) -> Iterator[Process]:
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
    def __init__(
        self, ctx: Optional[SimContext] = None, stat_interval: Optional[float] = None
    ):
        self._ctx: SimContext = ctx if ctx is not None else SimContext()
        self.event_counter = 0
        self.stat: SimStat = SimStat(self._ctx)
        self._stat_interval: Optional[float] = stat_interval
        self.stat_collectors: List[StatCollector] = []
        self.add_stat_collector(
            StatCollector(
                self._ctx, stat_interval=stat_interval, stat_container=self.stat
            )
        )

    @property
    def avg_event_rate(self) -> float:
        return self.event_counter / self._ctx.now

    @property
    def ctx(self) -> SimContext:
        return self._ctx

    @property
    def now(self) -> SimTime:
        return self._ctx.now

    def add_stat_collector(self, stat_collector: StatCollector) -> None:
        self.stat_collectors.append(stat_collector)

    def run(self, until_time: Optional[SimTime] = None) -> None:
        if until_time is not None:
            self._ctx.schedule_event(StopSim(self._ctx, delay=until_time))
        self._run()

    def _run(self) -> None:
        started_at = time.time()
        for proc in self._ctx.get_process_iter():
            proc.start()
        while (event := self._ctx.get_event()) and not self._ctx.is_stopped():
            if self._ctx.advance_simtime(event.time):
                self._ctx.exec_all_stat_callbacks()
                self._ctx.exec_all_tick_callbacks()
            if event.is_triggered():
                event.run()
                self.event_counter += 1
        if not self._stat_interval:
            for stat_collector in self.stat_collectors:
                stat_collector.collect_now()
        logger.info(
            "Simulation ended at %s, it took %s wall clock seconds. Executed %s events.",
            self._ctx.now,
            time.time() - started_at,
            self.event_counter,
        )


StatCallback = Callable[[None], None]
TickCallback = Callable[[None], None]
EventCallback = Callable[[Event], None]
Coro = Generator[Optional[Event], Any, Optional[Event]]
