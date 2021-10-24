# pylint: disable=protected-access,invalid-name
from netsim.simcore import SimTime
from netsim.netsim_base import (
    PacketSink,
    PacketSource,
    PacketSize,
)
from netsim.netsim_switch import PacketProcessor, PacketSwitch
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

    packet_processor.add_interface_rx(source)

    sim.run(until_time=10)
    assert sim.ctx.now == 10
    assert sim.event_counter == 32

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

    packet_processor.add_interface_rx(source)
    packet_processor.add_interface_tx(sink)

    sim.run(until_time=10)
    assert sim.ctx.now == 10
    assert sim.event_counter == 32

    assert source.stat.total_sent_pkts == 10
    assert packet_processor.stat.total_received_pkts == 10
    assert packet_processor.stat.total_sent_pkts == 10
    assert packet_processor.stat.total_dropped_pkts == 0


def test_packet_switch_1():
    sim = NetSim()

    def arrival_gen() -> SimTime:
        yield 0
        while True:
            yield 1

    def size_gen() -> PacketSize:
        while True:
            yield 1500

    source = PacketSource(sim.ctx, arrival_gen(), size_gen())
    packet_switch = PacketSwitch(sim.ctx)
    sink = PacketSink(sim.ctx)

    rx = packet_switch.create_interface_rx(0, 0.0)
    source.subscribe(rx)
    tx = packet_switch.create_interface_tx(1, 2 ** 20)
    tx.subscribe(sink)
    packet_processor = packet_switch.create_packet_processor()

    sim.run(until_time=10)

    assert sim.ctx.now == 10
    assert sim.event_counter == 82

    assert source.stat.total_sent_pkts == 10

    assert rx.stat.total_received_pkts == 10
    assert rx.stat.total_sent_pkts == 10
    assert rx.stat.total_dropped_pkts == 0

    assert packet_processor.stat.total_received_pkts == 10
    assert packet_processor.stat.total_sent_pkts == 10
    assert packet_processor.stat.total_dropped_pkts == 0

    assert tx.stat.total_received_pkts == 10
    assert tx.stat.total_sent_pkts == 10
    assert tx.stat.total_dropped_pkts == 0
    assert sink.stat.total_received_pkts == 10
