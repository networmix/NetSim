from netsim.simcore import QueueFIFO, SimContext, SimTime, Simulator
from netsim.netsim import (
    PacketQueue,
    PacketSink,
    PacketSource,
    PacketSize,
    Packet,
    NetSim,
)


def test_packet_source_1():
    sim = NetSim()

    def arrival_gen() -> SimTime:
        while True:
            yield 1

    def size_gen() -> PacketSize:
        while True:
            yield 1

    source = PacketSource(sim.ctx, arrival_gen(), size_gen())
    sim.run(until_time=10)
    assert sim.ctx.now == 10
    assert sim.event_counter == 10

    assert source.stat.total_sent_pkts == 9


def test_packet_sink_1():
    sim = NetSim()

    def arrival_gen() -> SimTime:
        while True:
            yield 1

    def size_gen() -> PacketSize:
        while True:
            yield 1

    source = PacketSource(sim.ctx, arrival_gen(), size_gen())
    sink = PacketSink(sim.ctx)
    source.subscribe(sink)

    sim.run(until_time=10)
    assert sim.ctx.now == 10
    assert sim.event_counter == 11

    assert source.stat.total_sent_pkts == 9
    assert sink.stat.total_received_pkts == 9


def test_packet_queue_1():
    sim = NetSim()

    def arrival_gen() -> SimTime:
        while True:
            yield 1

    def size_gen() -> PacketSize:
        while True:
            yield 1

    source = PacketSource(sim.ctx, arrival_gen(), size_gen())
    queue = PacketQueue(sim.ctx)
    source.subscribe(queue)
    sim.run(until_time=10)
    assert sim.ctx.now == 10
    assert sim.event_counter == 28

    assert source.stat.total_sent_pkts == 9
    assert queue.stat.total_received_pkts == 9
    assert queue.stat.total_sent_pkts == 9
