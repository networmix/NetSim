from __future__ import annotations

from abc import ABC, abstractmethod
import logging
from typing import Coroutine, DefaultDict, List, Optional, Union, Generator, Any
from collections import defaultdict
from dataclasses import dataclass, field

from netsim.simcore import Process, SimContext, SimTime, Resource, Coro


LOG_FMT = "%(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
logger = logging.getLogger(__name__)


# defining useful type aliases
PacketSize = Union[int, float]
PacketSrcDst = int
PacketFlowID = int


@dataclass
class PacketStat:
    _ctx: SimContext = field(repr=False)
    total_sent_pkts: int = 0
    total_received_pkts: int = 0
    total_sent_size: PacketSize = 0
    total_received_size: PacketSize = 0
    received_pkts_hist: DefaultDict[SimTime, int] = field(
        repr=False, default_factory=lambda: defaultdict(int)
    )
    sent_pkts_hist: DefaultDict[SimTime, int] = field(
        repr=False, default_factory=lambda: defaultdict(int)
    )
    received_size_hist: DefaultDict[SimTime, PacketSize] = field(
        repr=False, default_factory=lambda: defaultdict(int)
    )
    sent_size_hist: DefaultDict[SimTime, PacketSize] = field(
        repr=False, default_factory=lambda: defaultdict(int)
    )

    def packet_sent(self, packet: Packet):
        self.total_sent_pkts += 1
        self.sent_pkts_hist[self._ctx.now] += 1
        self.total_sent_size += packet.size
        self.sent_size_hist[self._ctx.now] += packet.size

    def packet_received(self, packet: Packet):
        self.total_received_pkts += 1
        self.received_pkts_hist[self._ctx.now] += 1
        self.total_received_size += packet.size
        self.received_size_hist[self._ctx.now] += packet.size


class Packet:
    def __init__(
        self,
        ctx: SimContext,
        size: PacketSize,
        flow_id: PacketFlowID = 0,
    ):
        self.ctx = ctx
        self.flow_id = flow_id
        self.size = size
        self.timestamp = ctx.now

    def __repr__(self) -> str:
        type_name = type(self).__name__
        return f"{type_name}(flow_id={self.flow_id} size={self.size} timestamp={self.timestamp})"


class Sender(ABC):
    def __init__(
        self,
        ctx: SimContext,
    ):
        self._env: SimContext = ctx
        self._process: Process = ctx.create_process(self.run())
        self._subscribers: List[Receiver] = []
        self.stat: PacketStat = PacketStat(ctx)

    @abstractmethod
    def run(self) -> Coro:
        raise NotImplementedError(self)

    def subscribe(self, receiver: Receiver):
        self._subscribers.append(receiver)


class Receiver(ABC):
    def __init__(
        self,
        ctx: SimContext,
    ):
        self._env: SimContext = ctx
        self._process: Process = ctx.create_process(self.run())
        self.stat: PacketStat = PacketStat(ctx)

    @abstractmethod
    def run(self) -> Coro:
        raise NotImplementedError(self)

    @abstractmethod
    def put(self, item: Any):
        raise NotImplementedError(self)


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

    def run(self) -> Coroutine:
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
            packet = Packet(self._env, size, flow_id=flow)
            self.stat.packet_sent(packet)
            logger.debug(
                "Packet created by %s_%s at %s",
                type(self).__name__,
                self._process.proc_id,
                self._env.now,
            )
            for subscriber in self._subscribers:
                subscriber.put(packet)


class PacketSink(Receiver):
    def put(self, item: Packet):
        self.stat.packet_received(item)
        logger.debug(
            "Packet received by %s_%s at %s",
            type(self).__name__,
            self._process.proc_id,
            self._env.now,
        )

    def run(self):
        yield self._process.noop()


class Queue:
    def __init__(self, env, bw, gap=0, propagation_delay=0):
        self.env = env
        self.queue = Resource(env)
        self.subscribers = []
        self.propagation_delay = propagation_delay
        self.arrival = {}
        self.pkt_count = 0
        self.bw = bw
        self.gap = gap
        self.process = env.process(self.run())
        self.max_q = 0

    def run(self):
        while True:
            p = yield self.queue.get()
            if p:
                yield self.env.timeout((self.gap + p.size) / self.bw)
                for subscriber in self.subscribers:
                    subscriber.put(p)

    # def put(self, packet):
    #     self.arrival[self.env.now + self.propagation_delay - packet.ts] = packet
    #     self.pkt_count += 1
    #     self.queue.put(packet)
    #     self.max_q = max(len(self.queue.items), self.max_q)
