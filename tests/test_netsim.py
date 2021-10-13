from netsim.simcore import SimContext, SimTime, Simulator
from netsim.netsim import PacketSink, PacketSource, PacketSize


def test_packet_source_1():
    ctx = SimContext()
    sim = Simulator(ctx)

    def arrival_gen() -> SimTime:
        while True:
            yield 1

    def size_gen() -> PacketSize:
        while True:
            yield 1

    source = PacketSource(ctx, arrival_gen(), size_gen())
    sim.run(until_time=10)
    assert ctx.now == 10
    assert sim.event_counter == 10

    assert source.stat.total_sent_pkts == 9


def test_packet_sink_1():
    ctx = SimContext()
    sim = Simulator(ctx)

    def arrival_gen() -> SimTime:
        while True:
            yield 1

    def size_gen() -> PacketSize:
        while True:
            yield 1

    source = PacketSource(ctx, arrival_gen(), size_gen())
    sink = PacketSink(ctx)
    source.subscribe(sink)

    sim.run(until_time=10)
    assert ctx.now == 10
    assert sim.event_counter == 11

    assert source.stat.total_sent_pkts == 9
    assert sink.stat.total_received_pkts == 9
