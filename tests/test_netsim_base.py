# pylint: disable=protected-access,invalid-name
from netsim.simcore import SimTime
from netsim.netsim_base import (
    PacketInterfaceTx,
    PacketQueue,
    PacketSink,
    PacketSource,
    PacketSize,
)
from netsim.netsim import NetSim


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
    assert sim.event_counter == 11

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
    assert sim.event_counter == 12

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
    assert sim.event_counter == 29

    assert source.stat.total_sent_pkts == 9
    assert queue.stat.total_received_pkts == 9
    assert queue.stat.total_sent_pkts == 9


def test_packet_interface_tx_1():
    sim = NetSim()

    def arrival_gen() -> SimTime:
        while True:
            yield 1

    def size_gen() -> PacketSize:
        while True:
            yield 1500

    source = PacketSource(sim.ctx, arrival_gen(), size_gen())
    interface_tx = PacketInterfaceTx(sim.ctx, bw=64000)
    source.subscribe(interface_tx)
    sim.run(until_time=10)
    assert sim.ctx.now == 10
    assert sim.event_counter == 38

    assert source.stat.total_sent_pkts == 9
    assert interface_tx.stat.total_received_pkts == 9
    assert interface_tx.stat.total_sent_pkts == 9


def test_packet_interface_tx_2():
    sim = NetSim()

    def arrival_gen() -> SimTime:
        while True:
            yield 1

    def size_gen() -> PacketSize:
        while True:
            yield 1500

    source = PacketSource(sim.ctx, arrival_gen(), size_gen())
    interface_tx = PacketInterfaceTx(sim.ctx, bw=9600, queue_len_limit=2)
    source.subscribe(interface_tx)
    sim.run(until_time=10)
    assert sim.ctx.now == 10
    assert sim.event_counter == 35

    assert source.stat.total_sent_pkts == 9
    assert interface_tx.stat.total_received_pkts == 9
    assert interface_tx.stat.total_sent_pkts == 7
