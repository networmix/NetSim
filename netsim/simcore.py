from __future__ import annotations

from abc import ABC, abstractmethod
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


class Process:
    def __init__(self, ctx: SimContext, coro: Coro):
        self._ctx: SimContext = ctx
        self._proc_id: ProcessID = ctx.get_next_process_id()
        self._coro: Coro = coro
        self._subscribers: List[Process] = [self]
        self._stopped: bool = False
        self._ctx.add_process(self)

    def __repr__(self) -> str:
        type_name = type(self).__name__
        return f"{type_name}(proc_id={self._proc_id}, coro={self._coro})"

    @property
    def proc_id(self) -> ProcessID:
        return self._proc_id

    def _set_active(self) -> None:
        self._ctx.set_active_process(self)
        logger.debug("%s activated at %d", self, self._ctx.now)

    def start(self) -> None:
        self._set_active()
        event = next(self._coro)

        for subscriber in self._subscribers:
            event.subscribe(subscriber)
        if event.triggered:
            self._ctx.add_event(event)

    def resume(self, event: Event) -> None:
        if not self._stopped:
            self._set_active()
            try:
                next_event = self._coro.send(event.value)
                if not isinstance(next_event, NoOp):
                    for subscriber in self._subscribers:
                        next_event.subscribe(subscriber)
                if next_event.triggered:
                    self._ctx.add_event(next_event)
            except StopIteration:
                self._stopped = True

    def subscribe(self, process: Process) -> None:
        self._subscribers.append(process)

    def timeout(self, delay: SimTime) -> Timeout:
        return Timeout(self._ctx, delay=delay, process=self)

    def noop(self) -> NoOp:
        return NoOp(self._ctx, process=self)


class Resource(ABC):
    def __init__(self, ctx: SimContext):
        self._ctx = ctx
        self._res_id: ResourceID = ctx.get_next_resource_id()
        self._put_queue: Deque[Put] = deque()
        self._get_queue: Deque[Get] = deque()

    @abstractmethod
    def _put_admission_check(self, event: Put) -> bool:
        raise NotImplementedError(self)

    @abstractmethod
    def _get_admission_check(self, event: Get) -> bool:
        raise NotImplementedError(self)

    def add_put(self, event: Put) -> None:
        if self._put_admission_check(event):
            event.add_callback(self._trigger_get)
            self._ctx.schedule_event(event, self._ctx.now)
        else:
            self._put_queue.append(event)
        logger.debug("%s added to %s at %d", event, self, self._ctx.now)

    def _trigger_get(self, _: Event) -> None:
        triggered: List[Get] = []
        for get_event in self._get_queue:
            if self._get_admission_check(get_event):
                triggered.append(get_event)
                get_event.add_callback(self._trigger_put)
                self._ctx.schedule_event(get_event, self._ctx.now)
                break
        map(self._put_queue.remove, triggered)

    def add_get(self, event: Get) -> None:
        if self._get_admission_check(event):
            event.add_callback(self._trigger_put)
            self._ctx.schedule_event(event, self._ctx.now)
        else:
            self._get_queue.append(event)
        logger.debug("%s added to %s at %d", event, self, self._ctx.now)

    def _trigger_put(self, _: Event) -> None:
        triggered: List[Put] = []
        for put_event in self._put_queue:
            if self._put_admission_check(put_event):
                triggered.append(put_event)
                put_event.add_callback(self._trigger_get)
                self._ctx.schedule_event(put_event, self._ctx.now)
                break
        map(self._put_queue.remove, triggered)

    def request(self) -> Get:
        raise NotImplementedError(self)

    def release(self) -> Put:
        raise NotImplementedError(self)


class QueueFIFO(Resource):
    def __init__(self, ctx: SimContext, capacity: int = 0):
        super().__init__(ctx)
        self._capacity: int = capacity
        self._queue: Deque[Any] = deque()

    def _put_admission_check(self, event: PutQueueFIFO) -> bool:
        if self._capacity <= 0:
            return True
        if len(self._queue) < self._capacity:
            return True
        return False

    def _get_admission_check(self, event: GetQueueFIFO) -> bool:
        if self._queue:
            return True
        return False

    def put(self, item: Any) -> PutQueueFIFO:
        return PutQueueFIFO(
            self._ctx, self, process=self._ctx.active_process, item=item
        )

    def get(self) -> GetQueueFIFO:
        return GetQueueFIFO(self._ctx, self, process=self._ctx.active_process)

    def _put(self, event: PutQueueFIFO) -> None:
        self._queue.append(event.item)
        logger.debug("%s added to %s at %d", event.item, self, self._ctx.now)

    def _get(self, event: GetQueueFIFO) -> None:
        event.item = self._queue.popleft()
        logger.debug("%s popped from %s at %d", event.item, self, self._ctx.now)


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
        logger.debug("%s created at %d", self, self._ctx.now)

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
    def triggered(self) -> bool:
        return self._time is not None

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

    def subscribe(self, proc: Process) -> None:
        self.add_callback(callback=proc.resume)
        logger.debug("%s subscribed to %s at %d", proc, self, self._ctx.now)

    def set_time(self, time: SimTime) -> None:
        self._time = time

    def add_callback(self, callback: EventCallback) -> None:
        self._callbacks.append(callback)

    def run(self) -> None:
        logger.debug("Executing %s at %s", self, self._ctx.now)
        for callback in self._callbacks:
            callback(self)


EventCallback = Callable[[Event], None]
Coro = Coroutine[Event, Any, None]


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
        self._resource = resource
        self._callbacks.append(self._resource._put)
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
        self._resource = resource
        self._callbacks.append(self._resource._get)
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
        if not event.triggered or event.time < self.now:
            raise RuntimeError(
                f"Can not add {event}. The event.time is {event.time}. Now is {self}"
            )
        heappush(self._event_queue, event)
        logger.debug("Added %s", event)

    def schedule_event(self, event: Event, time: SimTime) -> None:
        if time < self.now:
            raise RuntimeError(
                f"Can not schedule {event}. The event.time is {event.time}. Now is {self}"
            )
        event.set_time(time)
        heappush(self._event_queue, event)
        logger.debug("Scheduled %s", event)

    def stop(self) -> None:
        self._stopped = True

    def advance_simtime(self, new_time: SimTime) -> None:
        if new_time > self._cur_simtime:
            self._cur_simtime = new_time
            logger.debug("\nAdvancing time to %d", new_time)

    def create_process(self, coro: Coro) -> Process:
        proc = Process(ctx=self, coro=coro)
        self._procs[proc.proc_id] = proc
        return proc

    def add_process(self, process: Process):
        self._procs[process.proc_id] = process

    def get_process_iter(self) -> Iterator:
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

    def run(self, until_time: Optional[SimTime] = None) -> None:
        for proc in self._ctx.get_process_iter():
            proc.start()
        while (event := self._ctx.get_event()) and not self._ctx.stopped:
            logger.debug("Dequeued %s", event)
            self._ctx.advance_simtime(event.time)
            if until_time is not None and self._ctx.now >= until_time:
                break
            self._event_counter += 1
            event.run()
        logger.info("Simulation ended at %d", self._ctx.now)
