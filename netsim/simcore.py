from __future__ import annotations

from heapq import heappop, heappush
from typing import Iterator, List, Union, Optional, Type, Dict, Coroutine, Callable
import logging


LOG_FMT = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(level=logging.DEBUG, format=LOG_FMT)
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

    def start(self) -> None:
        event = next(self._coro)
        event.set_proc_id(self)
        for subscriber in self._subscribers:
            event.subscribe(subscriber)
        self._ctx.add_event(event)

    def resume(self, event: Event) -> None:
        if not self._stopped:
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
    def __init__(self, ctx: SimContext, capacity: ResourceCapacity):
        self._ctx = ctx
        self._res_id: ResourceID = ctx.get_next_resource_id()
        self._capacity = capacity
        self._put_queue: List[Event] = []
        self._get_queue: List[Event] = []


class Event:
    def __init__(
        self,
        ctx: SimContext,
        time: SimTime,
        func: Optional[Callable] = None,
        priority: EventPriority = 10,
        proc_id: ProcessID = 0,
    ):
        self._time: SimTime = time
        self._priority: EventPriority = priority
        self._event_id: EventID = ctx.get_next_event_id()
        self._proc_id: ProcessID = proc_id
        self._ctx: SimContext = ctx
        self._func: Callable = func
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
        self._callbacks.append(proc.resume)

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


class SimContext:
    def __init__(self, start_time: SimTime = 0):
        self._cur_simtime: SimTime = start_time
        self._eventq: List[Event] = []
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
        if self._eventq:
            return heappop(self._eventq)
        return None

    def add_event(self, event: Event) -> None:
        heappush(self._eventq, event)

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

    def timeout(self, delay: SimTime, process: Process) -> None:
        event = Timeout(self, delay=delay, proc_id=process.proc_id)
        event.subscribe(process)
        self.add_event(event)


class Simulator:
    def __init__(self, ctx: Optional[SimContext] = None):
        self._ctx: SimContext = ctx if ctx is not None else SimContext()

    def run(self) -> None:
        for proc in self._ctx.get_process_iter():
            proc.start()
        while (event := self._ctx.get_event()) and not self._ctx.stopped:
            logger.debug("Dequeued %s", event)
            self._ctx.advance_simtime(event.time)
            event.run()
