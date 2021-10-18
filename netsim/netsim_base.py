from __future__ import annotations

from abc import ABC, abstractmethod
import logging
from typing import DefaultDict, Dict, List, Optional, Union, Generator, Any
from collections import defaultdict
from dataclasses import dataclass, field

from netsim.simcore import SimTime, Coro, Process, QueueFIFO, SimContext


LOG_FMT = "%(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
logger = logging.getLogger(__name__)


# defining useful type aliases
PacketID = int
PacketSize = Union[int, float]  # in bytes
PacketAddress = int
PacketFlowID = int
NetSimObjectID = int
NetSimObjectName = str
InterfaceBW = float  # in bits per second


@dataclass
class PacketStat:  # pylint: disable=too-many-instance-attributes
    _ctx: SimContext = field(repr=False)
    total_sent_pkts: int = 0
    total_received_pkts: int = 0
    total_dropped_pkts: int = 0
    total_sent_bytes: PacketSize = 0
    total_received_bytes: PacketSize = 0
    total_dropped_bytes: PacketSize = 0
    received_pkts_hist: DefaultDict[SimTime, int] = field(
        repr=False, default_factory=lambda: defaultdict(int)
    )
    sent_pkts_hist: DefaultDict[SimTime, int] = field(
        repr=False, default_factory=lambda: defaultdict(int)
    )
    dropped_pkts_hist: DefaultDict[SimTime, int] = field(
        repr=False, default_factory=lambda: defaultdict(int)
    )
    received_size_hist: DefaultDict[SimTime, PacketSize] = field(
        repr=False, default_factory=lambda: defaultdict(int)
    )
    sent_size_hist: DefaultDict[SimTime, PacketSize] = field(
        repr=False, default_factory=lambda: defaultdict(int)
    )
    dropped_size_hist: DefaultDict[SimTime, PacketSize] = field(
        repr=False, default_factory=lambda: defaultdict(int)
    )

    def packet_sent(self, packet: Packet):
        self.total_sent_pkts += 1
        self.sent_pkts_hist[self._ctx.now] += 1
        self.total_sent_bytes += packet.size
        self.sent_size_hist[self._ctx.now] += packet.size

    def packet_received(self, packet: Packet):
        self.total_received_pkts += 1
        self.received_pkts_hist[self._ctx.now] += 1
        self.total_received_bytes += packet.size
        self.received_size_hist[self._ctx.now] += packet.size

    def packet_dropped(self, packet: Packet):
        self.total_dropped_pkts += 1
        self.dropped_pkts_hist[self._ctx.now] += 1
        self.total_dropped_bytes += packet.size
        self.dropped_size_hist[self._ctx.now] += packet.size


@dataclass
class PacketQueueStat:  # pylint: disable=too-many-instance-attributes
    _ctx: SimContext = field(repr=False)
    _queue: QueueFIFO = field(repr=False)
    total_sent_pkts: int = 0
    total_received_pkts: int = 0
    total_dropped_pkts: int = 0
    total_sent_bytes: PacketSize = 0
    total_received_bytes: PacketSize = 0
    total_dropped_bytes: PacketSize = 0
    received_pkts_hist: DefaultDict[SimTime, int] = field(
        repr=False, default_factory=lambda: defaultdict(int)
    )
    sent_pkts_hist: DefaultDict[SimTime, int] = field(
        repr=False, default_factory=lambda: defaultdict(int)
    )
    dropped_pkts_hist: DefaultDict[SimTime, int] = field(
        repr=False, default_factory=lambda: defaultdict(int)
    )
    received_size_hist: DefaultDict[SimTime, PacketSize] = field(
        repr=False, default_factory=lambda: defaultdict(int)
    )
    sent_size_hist: DefaultDict[SimTime, PacketSize] = field(
        repr=False, default_factory=lambda: defaultdict(int)
    )
    dropped_size_hist: DefaultDict[SimTime, PacketSize] = field(
        repr=False, default_factory=lambda: defaultdict(int)
    )
    queue_len_hist: DefaultDict[SimTime, int] = field(
        repr=False, default_factory=lambda: defaultdict(int)
    )
    queue_size_hist: DefaultDict[SimTime, PacketSize] = field(
        repr=False, default_factory=lambda: defaultdict(int)
    )

    def packet_get(self, packet: Packet):
        self.total_sent_pkts += 1
        self.sent_pkts_hist[self._ctx.now] += 1
        self.total_sent_bytes += packet.size
        self.sent_size_hist[self._ctx.now] += packet.size
        self.queue_len_hist[self._ctx.now] = len(self._queue) - 1

    def packet_put(self, packet: Packet):
        self.total_received_pkts += 1
        self.received_pkts_hist[self._ctx.now] += 1
        self.total_received_bytes += packet.size
        self.received_size_hist[self._ctx.now] += packet.size
        self.queue_len_hist[self._ctx.now] = len(self._queue) + 1

    def packet_dropped(self, packet: Packet):
        self.total_dropped_pkts += 1
        self.dropped_pkts_hist[self._ctx.now] += 1
        self.total_dropped_bytes += packet.size
        self.dropped_size_hist[self._ctx.now] += packet.size


class NetSimObject(ABC):
    _next_netsim_id: NetSimObjectID = 0

    def __init__(self, ctx: SimContext):
        self._ctx: SimContext = ctx
        self._name: NetSimObjectName = ""
        self._id, NetSimObject._next_netsim_id = (
            NetSimObject._next_netsim_id,
            NetSimObject._next_netsim_id + 1,
        )
        self._process: Process = ctx.create_process(self.run())

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        if not self._name:
            self._name = name

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
        self.ctx = ctx
        self.packet_id, Packet._next_packet_id = (
            Packet._next_packet_id,
            Packet._next_packet_id + 1,
        )
        self.flow_id = flow_id
        self.size = size
        self.timestamp = ctx.now

    def __repr__(self) -> str:
        type_name = type(self).__name__
        return f"{type_name}(packet_id={self.packet_id}, flow_id={self.flow_id}, size={self.size}, timestamp={self.timestamp})"

    @classmethod
    def reset_packet_id(cls):
        cls.next_packet_id: PacketID = 0


class Sender(NetSimObject):
    def __init__(
        self,
        ctx: SimContext,
    ):
        super().__init__(ctx)
        self._subscribers: List[Receiver] = []
        self.stat: PacketStat = PacketStat(ctx)

    def subscribe(self, receiver: Receiver, *args: List[Any], **kwargs: Dict[str, Any]):
        _, _ = args, kwargs
        self._subscribers.append(receiver)


class Receiver(NetSimObject):
    def __init__(
        self,
        ctx: SimContext,
    ):
        super().__init__(ctx)
        self.stat: PacketStat = PacketStat(ctx)

    @abstractmethod
    def put(self, item: Any, *args: List[Any], **kwargs: Dict[str, Any]):
        raise NotImplementedError(self)


class SenderReceiver(NetSimObject):
    def __init__(
        self,
        ctx: SimContext,
    ):
        super().__init__(ctx)
        self._subscribers: List[Receiver] = []
        self.stat: PacketStat = PacketStat(ctx)

    @abstractmethod
    def put(self, item: Any, *args: List[Any], **kwargs: Dict[str, Any]):
        raise NotImplementedError(self)

    def subscribe(self, receiver: Receiver, *args: List[Any], **kwargs: Dict[str, Any]):
        _, _ = args, kwargs
        self._subscribers.append(receiver)


class PacketSource(Sender):
    def __init__(
        self,
        ctx: SimContext,
        arrival_func: Generator,
        size_func: Generator,
        flow_func: Optional[Generator] = None,
        initial_delay: SimTime = 0,
    ):
        super().__init__(ctx)
        self._arrival_func: Generator = arrival_func
        self._size_func: Generator = size_func
        self._flow_func: Optional[Generator] = flow_func
        self._initial_delay: SimTime = initial_delay
        self._subscribers: List = []

    def run(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Coro:
        yield self._process.timeout(self._initial_delay)
        while True:
            arrival = next(self._arrival_func, None)
            if arrival is None:
                return
            yield self._process.timeout(arrival)
            size = next(self._size_func, None)
            flow = next(self._flow_func, 0) if self._flow_func else 0

            if size is None:
                return
            packet = Packet(self._ctx, size, flow_id=flow)
            self.stat.packet_sent(packet)
            logger.debug(
                "Packet created by %s_%s at %s",
                type(self).__name__,
                self._process.proc_id,
                self._ctx.now,
            )
            for subscriber in self._subscribers:
                subscriber.put(packet)


class PacketSink(Receiver):
    def put(self, item: Packet, *args: List[Any], **kwargs: Dict[str, Any]):
        self.stat.packet_received(item)
        logger.debug(
            "Packet received by %s_%s at %s",
            type(self).__name__,
            self._process.proc_id,
            self._ctx.now,
        )

    def run(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Coro:
        yield self._process.noop()


class PacketQueue(SenderReceiver):
    def __init__(self, ctx: SimContext):
        super().__init__(ctx)
        self._queue = QueueFIFO(ctx)
        self.queue_stat: PacketQueueStat = PacketQueueStat(ctx, self._queue)

    def run(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Coro:
        while True:
            packet: Packet = yield self._queue.get()
            if packet:
                for subscriber in self._subscribers:
                    subscriber.put(packet)
                self.queue_stat.packet_get(packet)
                self.stat.packet_sent(packet)
                logger.debug(
                    "Packet sent by %s_%s at %s",
                    type(self).__name__,
                    self._process.proc_id,
                    self._ctx.now,
                )
            else:
                raise RuntimeError(f"{packet}")

    def put(self, item: Packet, *args: List[Any], **kwargs: Dict[str, Any]):
        self._queue.put(item)
        self.queue_stat.packet_put(item)
        self.stat.packet_received(item)
        logger.debug(
            "Packet received by %s_%s at %s",
            type(self).__name__,
            self._process.proc_id,
            self._ctx.now,
        )


class PacketInterfaceRx(Receiver):
    pass


class PacketInterfaceTx(PacketQueue):
    def __init__(
        self, ctx: SimContext, bw: InterfaceBW, queue_len_limit: Optional[int] = None
    ):
        super().__init__(ctx)
        self._bw: InterfaceBW = bw
        self.queue_stat = PacketQueueStat(ctx, self._queue)
        self._queue_len_limit: Optional[int] = queue_len_limit
        self._busy: bool = False

    def run(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Coro:
        while True:
            packet: Packet = yield self._queue.get()
            if packet:
                self._awaiting_packet = False
                self._busy = True
                self.queue_stat.packet_get(packet)
                logger.debug(
                    "Packet serialization started by %s_%s at %s",
                    type(self).__name__,
                    self._process.proc_id,
                    self._ctx.now,
                )
                yield self._process.timeout(packet.size * 8 / self._bw)
                for subscriber in self._subscribers:
                    subscriber.put(packet)
                self.stat.packet_sent(packet)
                self._busy = False
                logger.debug(
                    "Packet serialization finished by %s_%s at %s",
                    type(self).__name__,
                    self._process.proc_id,
                    self._ctx.now,
                )
            else:
                raise RuntimeError(
                    f"Resumed {type(self).__name__}_{self._process.proc_id} without packet at {self._ctx.now}",
                )

    def put(self, item: Packet, *args: List[Any], **kwargs: Dict[str, Any]):
        if self._queue_admission_tail_drop(packet=item):
            self._queue.put(item)
            self.queue_stat.packet_put(item)
            self.stat.packet_received(item)
            logger.debug(
                "Packet received by %s_%s at %s",
                type(self).__name__,
                self._process.proc_id,
                self._ctx.now,
            )
        else:
            self.stat.packet_dropped(item)
            self.queue_stat.packet_dropped(item)
            logger.debug(
                "Packet dropped by %s_%s at %s",
                type(self).__name__,
                self._process.proc_id,
                self._ctx.now,
            )

    def _queue_admission_tail_drop(self, packet: Packet):
        _ = packet
        if not self._queue_len_limit or len(self._queue) < self._queue_len_limit:
            return True
        return False
