from __future__ import annotations

from abc import ABC, abstractmethod
import random
from dataclasses import dataclass
import logging
from typing import Callable, Dict, List, Optional, Set, Type, Union, Generator, Any

from schema import Schema

from netsim.netsim_common import (
    InterfaceBW,
    NetSimObjectID,
    NetSimObjectName,
    PacketAction,
    PacketFlowID,
    PacketID,
    PacketSize,
)
from netsim.netsim_stat import PacketQueueStat, PacketStat
from netsim.sim_common import SimTime

from netsim.simcore import (
    Coro,
    Process,
    QueueFIFO,
    SimContext,
)

from netsim.simstat import Stat
from netsim.stat_base import DistrBuilder, DistrFunc


LOG_FMT = "%(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
logger = logging.getLogger(__name__)

SEED = 0
random.seed(SEED)

QUEUE_ADMISSION_RED_PARAMS = Schema(
    [
        {
            "admission_policy": lambda s: s == "red",
            "wq": float,
            "minth": int,
            "maxth": int,
            "maxp": float,
            "s": float,
        }
    ]
)


QUEUE_ADMISSION_TAILDROP_PARAMS = Schema(
    [
        {
            "admission_policy": lambda s: s == "taildrop",
        }
    ]
)


@dataclass
class QueueState:
    ...


@dataclass
class QueueStateRED(QueueState):
    admission_policy: str  # name of the policy
    wq: float  # queue weight
    minth: float  # min threshold
    maxth: float  # max threshold
    maxp: float  # maximum value for pb
    s: float  #  transmission time for a small packet
    avg: float = 0  # smoothed average queue size for RED
    q_time: float = 0  # start of the queue idle time
    count: int = -1  # packets since last marked packet


class NetSimObject(ABC):
    _next_netsim_id: NetSimObjectID = 0

    def __init__(
        self,
        ctx: SimContext,
        name: Optional[NetSimObjectName] = None,
        stat_type: Optional[Type[Stat]] = None,
    ):
        self._ctx: SimContext = ctx
        self.ns_id, NetSimObject._next_netsim_id = (
            NetSimObject._next_netsim_id,
            NetSimObject._next_netsim_id + 1,
        )
        self.name: NetSimObjectName = (
            name if name else f"{type(self).__name__}_{self.ns_id}"
        )
        self.process: Process = ctx.create_process(self.run(), self.name)
        self.process.extend_name("_proc")
        self.stat: Stat = Stat(self._ctx) if not stat_type else stat_type(self._ctx)

    def __repr__(self) -> str:
        return f"{self.name}(process={self.process.name})"

    def extend_name(self, extension: str, prepend: bool = False) -> None:
        if prepend:
            self.name = extension + self.name
        else:
            self.name = self.name + extension

    @abstractmethod
    def run(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Coro:
        raise NotImplementedError(self)


class Packet:
    _next_packet_id: PacketID = 0

    def __init__(
        self,
        ctx: SimContext,
        size: PacketSize,
        flow_id: PacketFlowID = 0,
    ):
        self._ctx = ctx
        self.packet_id, Packet._next_packet_id = (
            Packet._next_packet_id,
            Packet._next_packet_id + 1,
        )
        self.flow_id = flow_id
        self.size = size
        self.generated_timestamp = ctx.now
        self.touched_timestamp: Optional[SimTime] = None
        self.touched_by: Optional[NetSimObjectName] = None
        self.last_action: Optional[PacketAction] = None

    def __repr__(self) -> str:
        type_name = type(self).__name__
        return f"{type_name}(packet_id={self.packet_id}, flow_id={self.flow_id}, size={self.size}, timestamp={self.generated_timestamp})"

    @classmethod
    def reset_packet_id(cls):
        cls.next_packet_id: PacketID = 0

    def touched(self, ns_obj: NetSimObject, action: PacketAction) -> None:
        self.touched_timestamp = self._ctx.now
        self.touched_by = ns_obj.name
        self.last_action = action

    def todict(self) -> Dict[str, Any]:
        return {
            field: getattr(self, field)
            for field in vars(self)
            if not field.startswith("_")
        }


class Sender(NetSimObject):
    def __init__(
        self,
        ctx: SimContext,
        name: Optional[NetSimObjectName] = None,
        stat_type: Optional[Type[Stat]] = None,
    ):
        super().__init__(ctx, name=name, stat_type=stat_type or PacketStat)
        self.stat: PacketStat = self.stat
        self._subscribers: List[Receiver] = []
        self.process.add_stat_callback(self.stat.advance_time)

    def subscribe(
        self, receiver: Receiver, *args: List[Any], **kwargs: Dict[str, Any]
    ) -> None:
        _, _ = args, kwargs
        self._subscribers.append(receiver)
        receiver.add_subscription(self)

    def _packet_sent(self, packet: Packet) -> None:
        packet.touched(self, action=PacketAction.SENT)
        self.stat.packet_sent(packet)

    def _packet_received(self, packet: Packet) -> None:
        packet.touched(self, action=PacketAction.RECEIVED)
        self.stat.packet_received(packet)

    def _packet_dropped(self, packet: Packet) -> None:
        packet.touched(self, action=PacketAction.DROPPED)
        self.stat.packet_dropped(packet)


class Receiver(NetSimObject):
    def __init__(
        self,
        ctx: SimContext,
        name: Optional[NetSimObjectName] = None,
        stat_type: Optional[Type[Stat]] = None,
    ):
        super().__init__(ctx, name=name, stat_type=stat_type or PacketStat)
        self.stat: PacketStat = self.stat
        self.process.add_stat_callback(self.stat.advance_time)
        self._subscriptions: Set[Union[Sender, SenderReceiver]] = set()

    @abstractmethod
    def put(
        self,
        item: Any,
        source: NetSimObject,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ) -> None:
        raise NotImplementedError(self)

    def add_subscription(self, ns_obj: Union[Sender, SenderReceiver]) -> None:
        self._subscriptions.add(ns_obj)

    def get_subscriptions(self) -> Set[Union[Sender, SenderReceiver]]:
        return self._subscriptions

    def _packet_received(self, packet: Packet) -> None:
        packet.touched(self, action=PacketAction.RECEIVED)
        self.stat.packet_received(packet)

    def _packet_dropped(self, packet: Packet) -> None:
        packet.touched(self, action=PacketAction.DROPPED)
        self.stat.packet_dropped(packet)


class SenderReceiver(NetSimObject):
    def __init__(
        self,
        ctx: SimContext,
        name: Optional[NetSimObjectName] = None,
        stat_type: Optional[Type[Stat]] = None,
    ):
        super().__init__(ctx, name=name, stat_type=stat_type or PacketStat)
        self.stat: PacketStat = self.stat
        self._subscribers: List[Receiver] = []
        self._subscriptions: Set[Union[Sender, SenderReceiver]] = set()
        self.process.add_stat_callback(self.stat.advance_time)

    @abstractmethod
    def put(
        self,
        item: Any,
        source: NetSimObject,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ) -> None:
        raise NotImplementedError(self)

    def subscribe(
        self, receiver: Receiver, *args: List[Any], **kwargs: Dict[str, Any]
    ) -> None:
        _, _ = args, kwargs
        self._subscribers.append(receiver)
        receiver.add_subscription(self)

    def add_subscription(self, ns_obj: Union[Sender, SenderReceiver]) -> None:
        self._subscriptions.add(ns_obj)

    def get_subscriptions(self) -> Set[Union[Sender, SenderReceiver]]:
        return self._subscriptions

    def _packet_sent(self, packet: Packet) -> None:
        packet.touched(self, action=PacketAction.SENT)
        self.stat.packet_sent(packet)

    def _packet_received(self, packet: Packet) -> None:
        packet.touched(self, action=PacketAction.RECEIVED)
        self.stat.packet_received(packet)

    def _packet_dropped(self, packet: Packet) -> None:
        packet.touched(self, action=PacketAction.DROPPED)
        self.stat.packet_dropped(packet)


@dataclass
class PacketSourceProfile:
    arrival_time_distr: DistrFunc
    arrival_distr_params: Dict[str, Any]
    size_distr: DistrFunc
    size_distr_params: Dict[str, Any]
    flow_distr: DistrFunc
    flow_distr_params: Dict[str, Any]
    initial_delay: SimTime = 0

    @classmethod
    def from_dict(cls, params: Dict[str, Any]):
        arrival_time_distr = DistrFunc[params["arrival_time_distr"]]
        arrival_distr_params = params["arrival_distr_params"]

        size_distr = DistrFunc[params["size_distr"]]
        size_distr_params = params["size_distr_params"]

        flow_distr = DistrFunc[params["flow_distr"]]
        flow_distr_params = params["flow_distr_params"]

        initial_delay = params["initial_delay"] if "initial_delay" in params else 0

        return cls(
            arrival_time_distr,
            arrival_distr_params,
            size_distr,
            size_distr_params,
            flow_distr,
            flow_distr_params,
            initial_delay,
        )


class PacketSource(Sender):
    def __init__(
        self,
        ctx: SimContext,
        arrival_func: Generator,
        size_func: Generator,
        flow_func: Optional[Generator] = None,
        initial_delay: SimTime = 0,
        name: Optional[NetSimObjectName] = None,
    ):
        super().__init__(ctx, name=name)
        self._arrival_func: Generator = arrival_func
        self._size_func: Generator = size_func
        self._flow_func: Optional[Generator] = flow_func
        self._initial_delay: SimTime = initial_delay

    @classmethod
    def create(
        cls, ctx: SimContext, name: NetSimObjectName, profile: PacketSourceProfile
    ) -> PacketSource:
        arrival_func = DistrBuilder.create(
            profile.arrival_time_distr, profile.arrival_distr_params
        )
        size_func = DistrBuilder.create(profile.size_distr, profile.size_distr_params)
        flow_func = DistrBuilder.create(profile.flow_distr, profile.flow_distr_params)

        return cls(ctx, arrival_func, size_func, flow_func, profile.initial_delay, name)

    def run(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Coro:
        yield self.process.timeout(self._initial_delay)
        while True:
            arrival = next(self._arrival_func, None)
            if arrival is None:
                return
            yield self.process.timeout(arrival)
            size = next(self._size_func, None)
            flow = next(self._flow_func, 0) if self._flow_func else 0

            if size is None:
                return
            packet = Packet(self._ctx, size, flow_id=flow)
            self._packet_sent(packet)
            for subscriber in self._subscribers:
                subscriber.put(packet, self)


class PacketSink(Receiver):
    def put(
        self,
        item: Packet,
        source: Union[Sender, SenderReceiver],
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ) -> None:
        self._packet_received(item)

    def run(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Coro:
        yield


class PacketQueue(SenderReceiver):
    def __init__(
        self,
        ctx: SimContext,
        queue_len_limit: Optional[int] = None,
        name: Optional[NetSimObjectName] = None,
        admission_params: Optional[Dict[str, Any]] = None,
        service_func: Optional[Generator] = None,
    ):
        super().__init__(ctx, name=name, stat_type=PacketQueueStat)
        self.stat: PacketQueueStat = self.stat
        self._queue = QueueFIFO(ctx, name=self.name)
        self._queue_len_limit: Optional[int] = queue_len_limit
        self._queue.extend_name("_QueueFIFO")
        self._admission_params: Optional[Dict[str, Any]] = admission_params
        self._queue_state: Optional[QueueState] = None
        self._admission_func: Optional[Callable[[PacketQueue, Packet], bool]] = None
        self._service_func: Optional[Generator] = service_func
        self._packet_get_callbacks: List[Callable[[PacketQueue, Packet], None]] = []

        if not admission_params and queue_len_limit is None:
            self._admission_func: Callable[
                [PacketQueue, Packet], bool
            ] = self._queue_admission_all

        elif not admission_params and queue_len_limit is not None:
            self._admission_func: Callable[
                [PacketQueue, Packet], bool
            ] = self._queue_admission_tail_drop

        elif QUEUE_ADMISSION_TAILDROP_PARAMS.is_valid([admission_params]):
            self._admission_func: Callable[
                [PacketQueue, Packet], bool
            ] = self._queue_admission_tail_drop

        elif QUEUE_ADMISSION_RED_PARAMS.is_valid([admission_params]):
            self._admission_func: Callable[
                [PacketQueue, Packet], bool
            ] = self._queue_admission_red
            self._queue_state = QueueStateRED(**admission_params)

            def q_time_callback(pq: PacketQueue, p: Packet) -> None:
                if not len(pq._queue) > 0:
                    pq._queue_state.q_time = pq._ctx.now

            self._packet_get_callbacks.append(q_time_callback)

        else:
            raise RuntimeError(f"Unknown admission_params: {admission_params}")

    def run(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Coro:
        while True:
            packet: Packet = yield self._queue.get()
            if packet:
                self._packet_get(packet)
                if self._service_func:
                    service_delay = next(self._service_func, None)
                    yield self.process.timeout(service_delay)
                for subscriber in self._subscribers:
                    subscriber.put(packet, self)
                self._packet_sent(packet)
            else:
                raise RuntimeError(f"{packet}")

    def put(
        self,
        item: Packet,
        source: Union[Sender, SenderReceiver],
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ) -> None:
        self._packet_received(item)
        if self._admission_func(packet=item):
            self._queue.put(item)
            self._packet_put(item)
        else:
            self._packet_dropped(item)

    def _queue_admission_tail_drop(self, packet: Packet) -> bool:
        _ = packet
        if not self._queue_len_limit or len(self._queue) < self._queue_len_limit:
            return True
        return False

    def _queue_admission_red(self, packet: Packet) -> bool:
        _ = packet
        self._queue_state: QueueStateRED

        if len(self._queue):
            # queue is nonempty
            self._queue_state.avg = (
                1 - self._queue_state.wq
            ) * self._queue_state.avg + self._queue_state.wq * len(self._queue)
        else:
            # queue is empty
            m = (self._ctx.now - self._queue_state.q_time) / self._queue_state.s
            self._queue_state.avg *= (1 - self._queue_state.wq) ** m

        if self._queue_len_limit and not len(self._queue) < self._queue_len_limit:
            # "hard" drop
            self._queue_state.count = 0
            return False

        if self._queue_state.minth <= self._queue_state.avg < self._queue_state.maxth:
            self._queue_state.count += 1
            pb = (
                self._queue_state.maxp
                * (self._queue_state.avg - self._queue_state.minth)
                / (self._queue_state.maxth - self._queue_state.minth)
            )

            if self._queue_state.count * pb < 1:
                pa = pb / (1 - self._queue_state.count * pb)
            else:
                pa = 1.0

            if random.random() < pa:
                self._queue_state.count = 0
                return False

        elif self._queue_state.maxth <= self._queue_state.avg:
            self._queue_state.count = 0
            return False

        else:
            self._queue_state.count = -1
        return True

    def _queue_admission_all(self, packet: Packet) -> bool:
        _ = packet
        return True

    def _packet_put(self, packet: Packet) -> None:
        self.stat.packet_put(packet)

    def _packet_get(self, packet: Packet) -> None:
        for callback in self._packet_get_callbacks:
            callback(self, packet)
        self.stat.packet_get(packet)

    def _packet_dropped(self, packet: Packet) -> None:
        self.stat.packet_dropped(packet)
        self.stat.packet_dropped(packet)


class PacketInterfaceRx(PacketQueue):
    def __init__(
        self,
        ctx: SimContext,
        propagation_delay: Optional[SimTime] = None,
        transmission_len_limit: Optional[int] = None,
        name: Optional[NetSimObjectName] = None,
    ):
        super().__init__(ctx, name=name)
        self._propagation_delay: Optional[SimTime] = propagation_delay
        self._transmission_len_limit: Optional[int] = transmission_len_limit

    def run(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Coro:
        while True:
            packet: Packet = yield self._queue.get()
            if packet:
                if self._propagation_delay:
                    yield self.process.timeout(self._propagation_delay)
                self._packet_get(packet)
                for subscriber in self._subscribers:
                    subscriber.put(packet, self)
                self._packet_sent(packet)
            else:
                raise RuntimeError(
                    f"Resumed {self} without packet at {self._ctx.now}",
                )

    def put(
        self,
        item: Packet,
        source: Union[Sender, SenderReceiver],
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ) -> None:
        # this is effectively a transmission media

        if self._queue_admission_tail_drop(packet=item):
            self._queue.put(item)
            self._packet_put(item)
            self._packet_received(item)
        else:
            self._packet_dropped(item)

    def _queue_admission_tail_drop(self, packet: Packet):
        _ = packet
        if (
            not self._transmission_len_limit
            or len(self._queue) < self._transmission_len_limit
        ):
            return True
        return False


class PacketInterfaceTx(PacketQueue):
    def __init__(
        self,
        ctx: SimContext,
        bw: InterfaceBW,
        queue_len_limit: Optional[int] = None,
        admission_params: Optional[Dict[str, Any]] = None,
        name: Optional[NetSimObjectName] = None,
    ):
        super().__init__(
            ctx,
            name=name,
            queue_len_limit=queue_len_limit,
            admission_params=admission_params,
        )
        self._bw: InterfaceBW = bw
        self._busy: bool = False

    def run(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Coro:
        while True:
            packet: Packet = yield self._queue.get()
            if packet:
                self._busy = True
                self._packet_get(packet)
                yield self.process.timeout(packet.size * 8 / self._bw)
                for subscriber in self._subscribers:
                    subscriber.put(packet, self)
                self._packet_sent(packet)
                self._busy = False
            else:
                raise RuntimeError(
                    f"Resumed {self} without packet at {self._ctx.now}",
                )
