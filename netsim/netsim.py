import logging
from typing import Optional, Union, Generator

from netsim.simcore import SimContext, SimTime, Resource


LOG_FMT = "%(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
logger = logging.getLogger(__name__)


# defining useful type aliases
PacketSize = Union[int, float]
PacketSrcDst = int
PacketFlowID = int


class Packet:
    def __init__(
        self,
        ctx: SimContext,
        size: PacketSize,
        src: PacketSrcDst = 0,
        dst: PacketSrcDst = 0,
        flow_id: PacketFlowID = 0,
    ):
        self.ctx = ctx
        self.src = src
        self.dst = dst
        self.flow_id = flow_id
        self.size = size
        self.ts = ctx.now

    def repr(self) -> str:
        return f"{self.src} {self.dst} {self.flow_id} {self.size}"


class PacketSource:
    def __init__(
        self,
        ctx: SimContext,
        arrival_func: Generator,
        size_func: Generator,
        flow_func: Optional[Generator] = None,
        initial_delay: SimTime = 0,
    ):
        self.env = ctx
        self.arrival_func = arrival_func
        self.size_func = size_func
        self.flow_func = flow_func
        self.process = ctx.create_process(self.run())
        self.initial_delay = initial_delay
        self.subscribers = []
        self._packets_sourced = 0

    @property
    def packets_sourced(self) -> int:
        return self._packets_sourced

    def run(self):
        yield self.env.timeout(self.initial_delay)
        while True:
            arrival = next(self.arrival_func, None)
            if arrival is None:
                return
            yield self.env.timeout(arrival)
            size = next(self.size_func, None)
            flow = next(self.flow_func, 0) if self.flow_func else 0

            if size is None:
                return
            p = Packet(self.env, size, flow_id=flow)
            self._packets_sourced += 1
            logger.debug("Packet created at %s", self.env.now)
            for subscriber in self.subscribers:
                subscriber.put(p)


class PacketSink:
    def __init__(self, ctx: SimContext):
        self.env = ctx
        self.arrival = {}
        self.pkt_count = 0

    def put(self, packet):
        self.arrival[self.env.now] = packet
        self.pkt_count += 1


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
