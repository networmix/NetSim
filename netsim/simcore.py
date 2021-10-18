from __future__ import annotations

from abc import ABC, abstractmethod
from enum import IntEnum
from collections import deque
from heapq import heappop, heappush
from typing import (
    Deque,
    Iterator,
    List,
    Union,
    Optional,
    Dict,
    Coroutine,
    Callable,
    Any,
)
import logging


LOG_FMT = "%(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
logger = logging.getLogger(__name__)

# defining useful type aliases
SimTime = Union[int, float]
EventPriority = int
EventID = int
ProcessID = int
ResourceID = int
ResourceCapacity = Union[int, float]


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


class Process:
    def __init__(self, ctx: SimContext, coro: Coro):
        self._ctx: SimContext = ctx
        self._proc_id: ProcessID = ctx.get_next_process_id()
        self._coro: Coro = coro
        self._subscribers: List[Process] = [self]
        self._stopped: bool = False
        self._timeout: Optional[EventID] = None
        self._ctx.add_process(self)

    def __repr__(self) -> str:
        type_name = type(self).__name__
        return f"{type_name}(proc_id={self._proc_id}, coro={self._coro})"

    @property
    def proc_id(self) -> ProcessID:
        return self._proc_id

    @property
    def in_timeout(self) -> bool:
        return self._timeout is not None

    def _set_active(self) -> None:
        self._timeout = None
        self._ctx.set_active_process(self)
        logger.debug("%s activated at %s", self, self._ctx.now)

    def start(self, *args: List[Any], **kwargs: Dict[str, Any]) -> None:
        _, _ = args, kwargs
        self._set_active()
        event: Optional[Event] = next(self._coro)
        if event is not None:
            if not isinstance(event, NoOp):
                for subscriber in self._subscribers:
                    event.subscribe(subscriber)
            if event.planned and not event.scheduled:
                self._ctx.schedule_event(event)

    def resume(self, event: Event, *args: List[Any], **kwargs: Dict[str, Any]) -> None:
        _, _ = args, kwargs
        if self._stopped:
            raise RuntimeError(f"Can't resume stopped process {self}!")

        if self._timeout is None or self._timeout == event.event_id:
            self._set_active()
            try:
                next_event: Optional[Event] = self._coro.send(event.value)
                if next_event is not None:
                    if not isinstance(next_event, NoOp):
                        for subscriber in self._subscribers:
                            next_event.subscribe(subscriber)
                    if next_event.planned and not next_event.scheduled:
                        self._ctx.schedule_event(next_event)
            except StopIteration:
                self._stopped = True

        else:
            raise RuntimeError(f"Can't resume process in timeout {self}!")

    def subscribe(
        self, process: Process, *args: List[Any], **kwargs: Dict[str, Any]
    ) -> None:
        _, _ = args, kwargs
        self._subscribers.append(process)

    def timeout(self, delay: SimTime) -> Timeout:
        event = Timeout(self._ctx, delay=delay, process=self)
        self._timeout = event.event_id
        return event

    def noop(self) -> NoOp:
        return NoOp(self._ctx, process=self)


class Resource(ABC):
    def __init__(self, ctx: SimContext, capacity: Optional[int] = None):
        self._ctx = ctx
        self._capacity: Optional[int] = capacity
        self._res_id: ResourceID = ctx.get_next_resource_id()
        self._put_queue: Deque[Put] = deque()
        self._get_queue: Deque[Get] = deque()

    @abstractmethod
    def _put_admission_check(self, event: Put) -> bool:
        raise NotImplementedError(self)

    @abstractmethod
    def _get_admission_check(self, event: Get) -> bool:
        raise NotImplementedError(self)

    @abstractmethod
    def _increase_runtime_len(self, event: Put) -> bool:
        raise NotImplementedError(self)

    @abstractmethod
    def _decrease_runtime_len(self, event: Get) -> bool:
        raise NotImplementedError(self)

    def add_put(self, event: Put) -> None:
        self._put_queue.append(event)
        logger.debug("%s added to put_queue of %s at %s", event, self, self._ctx.now)
        logger.debug("_put_queue of %s at %s: %s", self, self._ctx.now, self._put_queue)
        self._trigger_put(event)

    def _trigger_get(self, event: Event) -> None:
        logger.debug("_trigger_get on %s at %s", self, self._ctx.now)
        triggered: List[Get] = []

        for _ in range(len(self._get_queue)):
            get_event = self._get_queue.popleft()
            if self._get_admission_check(get_event):
                logger.debug(
                    "get_event %s passed admission at %s", get_event, self._ctx.now
                )
                self._increase_runtime_len(event)
                triggered.append(get_event)
                get_event.add_callback(self._trigger_put)
                self._ctx.schedule_event(get_event, self._ctx.now)
            else:
                self._get_queue.append(get_event)
        logger.debug("_get_queue of %s at %s: %s", self, self._ctx.now, self._get_queue)

    def add_get(self, event: Get) -> None:
        self._get_queue.append(event)
        logger.debug("%s added to get_queue of %s at %s", event, self, self._ctx.now)
        logger.debug("_get_queue of %s at %s: %s", self, self._ctx.now, self._get_queue)
        self._trigger_get(event)

    def _trigger_put(self, event: Event) -> None:
        logger.debug("_trigger_put on %s at %s", self, self._ctx.now)
        triggered: List[Put] = []
        for _ in range(len(self._put_queue)):
            put_event = self._put_queue.popleft()
            if self._put_admission_check(put_event):
                logger.debug(
                    "put_event %s passed admission at %s", put_event, self._ctx.now
                )
                self._decrease_runtime_len(event)
                triggered.append(put_event)
                put_event.add_callback(self._trigger_get)
                self._ctx.schedule_event(put_event, self._ctx.now)
            else:
                self._get_queue.append(put_event)
        logger.debug("_put_queue of %s at %s: %s", self, self._ctx.now, self._put_queue)

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


class QueueFIFO(Resource):
    def __init__(self, ctx: SimContext):
        super().__init__(ctx)
        self._queue: Deque[Any] = deque()
        self._queue_runtime_len: int = 0

    def __len__(self):
        return len(self._queue)

    def _increase_runtime_len(self, event: PutQueueFIFO) -> bool:
        self._queue_runtime_len += 1

    def _decrease_runtime_len(self, event: GetQueueFIFO) -> bool:
        self._queue_runtime_len -= 1

    def _put_admission_check(self, event: PutQueueFIFO) -> bool:
        if event.process.in_timeout:
            return False
        if self._capacity is None:
            return True
        if self._queue_runtime_len < self._capacity:
            return True
        return False

    def _get_admission_check(self, event: GetQueueFIFO) -> bool:
        if event.process.in_timeout:
            return False
        if self._queue_runtime_len:
            return True
        return False

    def put(
        self, item: Any, *args: List[Any], **kwargs: Dict[str, Any]
    ) -> PutQueueFIFO:
        return PutQueueFIFO(
            self._ctx, self, process=self._ctx.active_process, item=item
        )

    def get(self, *args: List[Any], **kwargs: Dict[str, Any]) -> GetQueueFIFO:
        return GetQueueFIFO(self._ctx, self, process=self._ctx.active_process)

    def _put_callback(self, event: PutQueueFIFO) -> None:
        self._queue.append(event.item)
        logger.debug("%s was put into %s at %s", event.item, self, self._ctx.now)

    def _get_callback(self, event: GetQueueFIFO) -> None:
        event.item = self._queue.popleft()
        logger.debug("%s was gotten from %s at %s", event.item, self, self._ctx.now)


class Event:
    def __init__(
        self,
        ctx: SimContext,
        time: Optional[SimTime] = None,
        func: Optional[Callable[[Event], None]] = None,
        priority: EventPriority = 10,
        process: Optional[Process] = None,
    ):
        self._time: Optional[SimTime] = time
        self._priority: EventPriority = priority
        self._event_id: EventID = ctx.get_next_event_id()
        self._process: Optional[Process] = process
        self._ctx: SimContext = ctx
        self._func: Callable[[Event], Any] = func
        self._callbacks: List[EventCallback] = []
        self._value: Any = None
        self._status: EventStatus = (
            EventStatus.CREATED if self._time is None else EventStatus.PLANNED
        )
        self._auto_trigger: bool = False
        logger.debug("%s created at %s", self, self._ctx.now)

    def __hash__(self):
        return self._event_id

    def __eq__(self, other):
        return self._event_id == other.event_id

    def __lt__(self, other):
        return (self._time, self._priority, self._event_id) < (
            other.time,
            other.priority,
            other.event_id,
        )

    def __repr__(self) -> str:
        type_name = type(self).__name__
        proc_id = self._process.proc_id if self._process else None
        return f"{type_name}(time={self._time}, event_id={self._event_id}, proc_id={proc_id})"

    @property
    def time(self) -> SimTime:
        return self._time

    @property
    def priority(self) -> EventPriority:
        return self._priority

    @property
    def process(self) -> Process:
        return self._process

    @property
    def event_id(self) -> EventID:
        return self._event_id

    @property
    def value(self) -> Any:
        if self._func is not None:
            self._value = self._func(self)
        return self._value

    @property
    def status(self) -> EventStatus:
        return self._status

    @property
    def planned(self) -> bool:
        return self._status >= EventStatus.PLANNED

    @property
    def scheduled(self) -> bool:
        return self._status >= EventStatus.SCHEDULED

    @property
    def triggered(self) -> bool:
        return self._status >= EventStatus.TRIGGERED

    def plan(self, time: SimTime) -> None:
        """
        A given event instance can be planned only once.
        It is done either by setting time during creation or by calling this method.
        """
        if not self.planned:
            if self._time is None:
                if time is None:
                    raise RuntimeError(f"Can not plan {self}. No time set!")
                self._time = time
            self._status = EventStatus.PLANNED
        else:
            raise RuntimeError(f"Can not plan {self}. It has been already planned!")

    def schedule(self, time: Optional[SimTime] = None) -> None:
        """
        A given event instance can be scheduled only once. If this event has not been planned yet, the
        parameter 'time' is used to plan it. If a given event has _auto_trigger variable set to True,
        this method will also trigger the event.
        """
        if not self.planned:
            self.plan(time)

        elif self.scheduled:
            raise RuntimeError(f"{self} has already been scheduled!")

        self._status = EventStatus.SCHEDULED

        self._ctx.add_event(self)
        logger.debug("%s was scheduled", self)
        if self._auto_trigger:
            self.trigger()

    def trigger(self) -> None:
        """
        Event needs to be scheduled to be triggered.
        Event can be triggered multiple times.
        """
        if not self.scheduled:
            raise RuntimeError(f"{self} can not be triggered as it wasn't scheduled!")
        if self._status == EventStatus.SCHEDULED:
            self._status = EventStatus.TRIGGERED
            logger.debug("%s was triggered", self)

    def subscribe(self, proc: Process) -> None:
        self.add_callback(callback=proc.resume)
        logger.debug("%s subscribed to %s at %s", proc, self, self._ctx.now)

    def add_callback(self, callback: EventCallback) -> None:
        self._callbacks.append(callback)

    def run(self) -> None:
        if not self.scheduled:
            raise RuntimeError(f"Can not run event {self}. It was not scheduled!")
        logger.debug("Executing %s at %s", self, self._ctx.now)
        for callback in self._callbacks:
            callback(self)


EventCallback = Callable[[Event], None]
Coro = Coroutine[Optional[Event], Any, Optional[Event]]


class Timeout(Event):
    def __init__(
        self,
        ctx: SimContext,
        delay: SimTime,
        priority: EventPriority = 10,
        process: Optional[Process] = None,
    ):
        self._delay = delay
        super().__init__(ctx, ctx.now + delay, priority=priority, process=process)
        self._auto_trigger = True

    def __repr__(self) -> str:
        type_name = type(self).__name__
        proc_id = self._process.proc_id if self._process else None
        return f"{type_name}(time={self._time}, event_id={self._event_id}, proc_id={proc_id}, delay={self._delay})"


class NoOp(Event):
    def __init__(
        self,
        ctx: SimContext,
        process: Optional[Process] = None,
    ):
        super().__init__(ctx, ctx.now, priority=255, process=process)
        self._auto_trigger = True

    def __repr__(self) -> str:
        type_name = type(self).__name__
        proc_id = self._process.proc_id if self._process else None
        return f"{type_name}(time={self._time}, event_id={self._event_id}, proc_id={proc_id})"


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
    def __init__(self, start_time: SimTime = 0):
        self._cur_simtime: SimTime = start_time
        self._cur_active_process: Union[Process, None] = None
        self._event_queue: List[Event] = []
        self._procs: Dict[ProcessID, Process] = {}
        self._resources: Dict[ResourceID, Resource] = {}
        self._stopped: bool = False
        self.__next_event_id: EventID = 0
        self.__next_process_id: ProcessID = 1  # 0 is reserved for the simulator
        self.__next_resource_id: ResourceID = 0

    @property
    def _next_event_id(self) -> EventID:
        next_event_id = self.__next_event_id
        self.__next_event_id += 1
        return next_event_id

    @property
    def _next_process_id(self) -> ProcessID:
        next_process_id = self.__next_process_id
        self.__next_process_id += 1
        return next_process_id

    @property
    def _next_resource_id(self) -> ResourceID:
        next_resource_id = self.__next_resource_id
        self.__next_resource_id += 1
        return next_resource_id

    @property
    def now(self) -> SimTime:
        return self._cur_simtime

    @property
    def stopped(self) -> bool:
        return self._stopped

    @property
    def active_process(self) -> Process:
        return self._cur_active_process

    def get_next_event_id(self) -> EventID:
        return self._next_event_id

    def get_next_process_id(self) -> ProcessID:
        return self._next_process_id

    def get_next_resource_id(self) -> ResourceID:
        return self._next_resource_id

    def get_event(self) -> Union[Event, None]:
        if self._event_queue:
            return heappop(self._event_queue)
        return None

    def add_event(self, event: Event) -> None:
        if not event.scheduled:
            raise RuntimeError(
                f"Can not add {event}. The event has not been scheduled."
            )
        if event.time < self.now:
            raise RuntimeError(
                f"Can not add {event}. The event.time is {event.time} is in the past. Now is {self.now}"
            )

        heappush(self._event_queue, event)
        logger.debug(
            "Added %s into the event queue.\nState of the event queue: %s",
            event,
            self._event_queue,
        )

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

    def stop(self) -> None:
        self._stopped = True

    def advance_simtime(self, new_time: SimTime) -> None:
        if new_time > self._cur_simtime:
            self._cur_simtime = new_time
            logger.debug("\nAdvancing time to %s", new_time)

    def create_process(self, coro: Coro) -> Process:
        proc = Process(ctx=self, coro=coro)
        self._procs[proc.proc_id] = proc
        return proc

    def add_process(self, process: Process):
        self._procs[process.proc_id] = process

    def get_process_iter(self) -> Iterator[Process]:
        return iter(self._procs.values())

    def set_active_process(self, process: Process) -> None:
        self._cur_active_process = process


class Simulator:
    def __init__(self, ctx: Optional[SimContext] = None):
        self._ctx: SimContext = ctx if ctx is not None else SimContext()
        self._event_counter = 0

    @property
    def event_counter(self) -> int:
        return self._event_counter

    @property
    def ctx(self) -> SimContext:
        return self._ctx

    def run(self, until_time: Optional[SimTime] = None) -> None:
        self._run(until_time)

    def _run(self, until_time: Optional[SimTime] = None) -> None:
        for proc in self._ctx.get_process_iter():
            proc.start()
        while (event := self._ctx.get_event()) and not self._ctx.stopped:
            logger.debug("Dequeued %s\nEvent queue: %s", event, self._ctx._event_queue)
            self._ctx.advance_simtime(event.time)
            if until_time is not None and self._ctx.now >= until_time:
                break
            self._event_counter += 1
            event.run()
        logger.info("Simulation ended at %s", self._ctx.now)
