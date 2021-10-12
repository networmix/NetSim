from typing import Union

from netsim.simcore import SimContext, Event, Simulator, Process


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
        self, ctx: SimContext, arrival_func, size_func, initial_delay=0, gap=0, bw=1
    ):
        self.env = ctx
        self.arrival_func = arrival_func
        self.size_func = size_func
        self.process = ctx.create_process(self.run())
        self.initial_delay = initial_delay
        self.subscribers = []
        self.packets_sent = 0
        self.gap = 0
        self.bw = bw

    def run(self):
        yield self.env.timeout(self.initial_delay)
        while True:
            size = next(self.size_func)
            p = Packet(self.env, size)
            yield self.env.timeout(self.arrival_func() + (self.gap + size) / self.bw)
            self.packets_sent += 1
            for subscriber in self.subscribers:
                subscriber.put(p)


class PacketSink:
    def __init__(self, ctx: SimContext, propagation_delay=0):
        self.env = ctx
        self.propagation_delay = propagation_delay
        self.arrival = {}
        self.pkt_count = 0

    def put(self, packet):
        self.arrival[self.env.now + self.propagation_delay - packet.ts] = packet
        self.pkt_count += 1


class Queue:
    def __init__(self, env, bw, gap=0, propagation_delay=0):
        self.env = env
        self.queue = simpy.Store(env)
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

    def put(self, packet):
        self.arrival[self.env.now + self.propagation_delay - packet.ts] = packet
        self.pkt_count += 1
        self.queue.put(packet)
        self.max_q = max(len(self.queue.items), self.max_q)
