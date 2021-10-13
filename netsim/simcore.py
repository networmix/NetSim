from __future__ import annotations

from heapq import heappop, heappush
from typing import Iterator, List, Union, Optional, Type, Dict, Coroutine, Callable, Any
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
    def __init__(self, ctx: SimContext, coro: Coroutine):
        self._ctx: SimContext = ctx
        self._proc_id: ProcessID = ctx.get_next_process_id()
        self._coro: Coroutine = coro
        self._subscribers: List[Process] = [self]
        self._stopped: bool = False
        self._ctx.add_process(self)

    @property
    def proc_id(self) -> ProcessID:
        return self._proc_id

    def _set_active(self) -> None:
        self._ctx.set_active_process(self)

    def start(self) -> None:
        self._set_active()
        event = next(self._coro)
        event.set_proc_id(self)
        for subscriber in self._subscribers:
            event.subscribe(subscriber)
        self._ctx.add_event(event)

    def resume(self, event: Event) -> None:
        if not self._stopped:
            self._set_active()
            try:
                next_event = self._coro.send(event)
                next_event.set_proc_id(self)
                for subscriber in self._subscribers:
                    next_event.subscribe(subscriber)
                self._ctx.add_event(next_event)
            except StopIteration:
                self._stopped = True

    def subscribe(self, process: Process) -> None:
        self._subscribers.append(process)


class Resource:
    def __init__(self, ctx: SimContext):
        self._ctx = ctx
        self._res_id: ResourceID = ctx.get_next_resource_id()
        self._put_queue: List[Put] = []
        self._get_queue: List[Get] = []

    def put(self) -> None:
        heappush(self._put_queue, Put(self._ctx, self))

    def get(self) -> None:
        heappush(self._get_queue, Get(self._ctx, self))

    def _put(self, event: Put) -> None:
        pass

    def _get(self, event: Get) -> None:
        pass

    def _do_put(self, event: Put) -> None:
        raise NotImplementedError(self)

    def _do_get(self, event: Get) -> None:
        raise NotImplementedError(self)


class Event:
    def __init__(
        self,
        ctx: SimContext,
        time: SimTime,
        func: Optional[Callable[[Event], None]] = None,
        priority: EventPriority = 10,
        proc_id: ProcessID = 0,
    ):
        self._time: SimTime = time
        self._priority: EventPriority = priority
        self._event_id: EventID = ctx.get_next_event_id()
        self._proc_id: ProcessID = proc_id
        self._ctx: SimContext = ctx
        self._func: Callable[[Event], None] = func
        self._callbacks: List[EventCallback] = []

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
        return f"{type(self).__name__}(time={self._time}, event_id={self._event_id}, proc_id={self._proc_id})"

    @property
    def time(self) -> SimTime:
        return self._time

    @property
    def priority(self) -> EventPriority:
        return self._priority

    @property
    def event_id(self) -> EventID:
        return self._event_id

    def set_proc_id(self, proc: Process) -> None:
        self._proc_id = proc.proc_id

    def subscribe(self, proc: Process) -> None:
        self.add_callback(callback=proc.resume)

    def add_callback(self, callback: EventCallback) -> None:
        self._callbacks.append(callback)

    def run(self) -> None:
        logger.debug("Executing %s at %s", self, self._ctx.now)
        if self._func is not None:
            ret = self._func(self)
            logger.debug(ret)
        for callback in self._callbacks:
            callback(self)


EventCallback = Callable[[Event], None]


class Timeout(Event):
    def __init__(
        self,
        ctx: SimContext,
        delay: SimTime,
        priority: EventPriority = 10,
        proc_id: ProcessID = 0,
    ):
        self._delay = delay
        super().__init__(ctx, ctx.now + delay, priority=priority, proc_id=proc_id)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(time={self._time}, event_id={self._event_id}, proc_id={self._proc_id}, delay={self._delay})"


class Put(Event):
    def __init__(
        self,
        ctx: SimContext,
        resource: Resource,
        priority: EventPriority = 10,
        proc_id: ProcessID = 0,
    ):
        super().__init__(ctx, ctx.now, priority=priority, proc_id=proc_id)
        self._resource = resource
        self._callbacks.append(resource._put)


class Get(Event):
    def __init__(
        self,
        ctx: SimContext,
        resource: Resource,
        priority: EventPriority = 10,
        proc_id: ProcessID = 0,
    ):
        super().__init__(ctx, ctx.now, priority=priority, proc_id=proc_id)
        self._resource = resource
        self._callbacks.append(resource._get)


class SimContext:
    def __init__(self, start_time: SimTime = 0):
        self._cur_simtime: SimTime = start_time
        self._cur_active_process: ProcessID = 0
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
        heappush(self._event_queue, event)

    def stop(self) -> None:
        self._stopped = True

    def advance_simtime(self, new_time: SimTime) -> None:
        self._cur_simtime = new_time

    def create_process(self, coro: Coroutine) -> Process:
        proc = Process(ctx=self, coro=coro)
        self._procs[proc.proc_id] = proc
        return proc

    def add_process(self, process: Process):
        self._procs[process.proc_id] = process

    def get_process_iter(self) -> Iterator:
        return iter(self._procs.values())

    def get_active_process(self) -> Process:
        return self._procs[self._cur_active_process]

    def set_active_process(self, process: Process) -> None:
        self._cur_active_process = process.proc_id

    def timeout(self, delay: SimTime) -> Timeout:
        return Timeout(self, delay=delay, proc_id=self._cur_active_process)


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
