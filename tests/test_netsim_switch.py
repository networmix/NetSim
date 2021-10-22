# pylint: disable=protected-access,invalid-name
from netsim.simcore import SimTime
from netsim.netsim_base import (
    PacketSink,
    PacketSource,
    PacketSize,
)
from netsim.netsim_switch import PacketProcessor
from netsim.netsim import NetSim


def test_packet_processor_1():
    sim = NetSim()

    def arrival_gen() -> SimTime:
        yield 0
        while True:
            yield 1

    def size_gen() -> PacketSize:
        while True:
            yield 1500

    source = PacketSource(sim.ctx, arrival_gen(), size_gen())
    packet_processor = PacketProcessor(sim.ctx)
    sink = PacketSink(sim.ctx)

    source.subscribe(packet_processor)
    packet_processor.subscribe(sink)

    sim.run(until_time=10)
    assert sim.ctx.now == 10
    assert sim.event_counter == 33

    assert source.stat.total_sent_pkts == 10
    assert packet_processor.stat.total_received_pkts == 10
    assert packet_processor.stat.total_sent_pkts == 0
    assert packet_processor.stat.total_dropped_pkts == 10


def test_packet_processor_2():
    sim = NetSim()

    def arrival_gen() -> SimTime:
        yield 0
        while True:
            yield 1

    def size_gen() -> PacketSize:
        while True:
            yield 1500

    source = PacketSource(sim.ctx, arrival_gen(), size_gen())
    packet_processor = PacketProcessor(sim.ctx)
    sink = PacketSink(sim.ctx)

    source.subscribe(packet_processor)
    packet_processor.subscribe(sink)
    packet_processor.add_rx_interface(source)
    packet_processor.add_tx_interface(sink)

    sim.run(until_time=10)
    assert sim.ctx.now == 10
    assert sim.event_counter == 33

    assert source.stat.total_sent_pkts == 10
    assert packet_processor.stat.total_received_pkts == 10
    assert packet_processor.stat.total_sent_pkts == 10
    assert packet_processor.stat.total_dropped_pkts == 0
