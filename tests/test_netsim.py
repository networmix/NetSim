from netsim.simcore import SimContext, SimTime, Simulator
from netsim.netsim import PacketSource, PacketSize


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

    assert source.packets_sourced == 9
