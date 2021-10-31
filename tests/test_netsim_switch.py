# pylint: disable=protected-access,invalid-name
from netsim.simcore import SimTime
from netsim.netsim_base import (
    PacketSink,
    PacketSource,
    PacketSize,
)
from netsim.netsim_switch import PacketProcessor, PacketSwitch
from netsim.netsim_simulator import NetSim


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

    assert source.stat.cur_stat_frame.todict() == {
        "avg_drop_rate_bps": 0.0,
        "avg_drop_rate_pps": 0.0,
        "avg_receive_rate_bps": 0.0,
        "avg_receive_rate_pps": 0.0,
        "avg_send_rate_bps": 12000.0,
        "avg_send_rate_pps": 1.0,
        "duration": 10,
        "last_state_change_timestamp": 9,
        "timestamp": 10,
        "total_dropped_bytes": 0,
        "total_dropped_pkts": 0,
        "total_received_bytes": 0,
        "total_received_pkts": 0,
        "total_sent_bytes": 15000,
        "total_sent_pkts": 10,
    }

    assert packet_processor.stat.cur_stat_frame.todict() == {
        "avg_drop_rate_bps": 12000.0,
        "avg_drop_rate_pps": 1.0,
        "avg_receive_rate_bps": 12000.0,
        "avg_receive_rate_pps": 1.0,
        "avg_send_rate_bps": 0.0,
        "avg_send_rate_pps": 0.0,
        "duration": 10,
        "last_state_change_timestamp": 9,
        "timestamp": 10,
        "total_dropped_bytes": 15000,
        "total_dropped_pkts": 10,
        "total_received_bytes": 15000,
        "total_received_pkts": 10,
        "total_sent_bytes": 0,
        "total_sent_pkts": 0,
    }


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

    assert packet_processor.stat.cur_stat_frame.todict() == {
        "avg_drop_rate_bps": 0.0,
        "avg_drop_rate_pps": 0.0,
        "avg_receive_rate_bps": 12000.0,
        "avg_receive_rate_pps": 1.0,
        "avg_send_rate_bps": 12000.0,
        "avg_send_rate_pps": 1.0,
        "duration": 10,
        "last_state_change_timestamp": 9,
        "timestamp": 10,
        "total_dropped_bytes": 0,
        "total_dropped_pkts": 0,
        "total_received_bytes": 15000,
        "total_received_pkts": 10,
        "total_sent_bytes": 15000,
        "total_sent_pkts": 10,
    }


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
    packet_switch.create_packet_processor()

    sim.run(until_time=10)

    assert sim.ctx.now == 10
    assert sim.event_counter == 82

    assert packet_switch.stat.cur_stat_frame.todict() == {
        "avg_drop_rate_bps": 0.0,
        "avg_drop_rate_pps": 0.0,
        "avg_receive_rate_bps": 12000.0,
        "avg_receive_rate_pps": 1.0,
        "avg_send_rate_bps": 12000.0,
        "avg_send_rate_pps": 1.0,
        "duration": 10.0,
        "last_state_change_timestamp": 9.011444091796875,
        "packet_processors": {
            "PacketProcessor": {
                "avg_drop_rate_bps": 0.0,
                "avg_drop_rate_pps": 0.0,
                "avg_receive_rate_bps": 12000.0,
                "avg_receive_rate_pps": 1.0,
                "avg_send_rate_bps": 12000.0,
                "avg_send_rate_pps": 1.0,
                "duration": 10.0,
                "last_state_change_timestamp": 9,
                "timestamp": 10,
                "total_dropped_bytes": 0,
                "total_dropped_pkts": 0,
                "total_received_bytes": 15000,
                "total_received_pkts": 10,
                "total_sent_bytes": 15000,
                "total_sent_pkts": 10,
            }
        },
        "rx_interface_queues": {
            0: {
                "avg_get_processed_rate": 0,
                "avg_get_requested_rate": 0,
                "avg_put_processed_rate": 0,
                "avg_put_requested_rate": 0,
                "avg_queue_len": 0,
                "cur_queue_len": 0,
                "duration": 0,
                "integral_queue_sum": 0,
                "last_state_change_timestamp": 0,
                "max_queue_len": 0,
                "timestamp": 0,
                "total_get_processed_count": 10,
                "total_get_requested_count": 11,
                "total_put_processed_count": 10,
                "total_put_requested_count": 10,
            }
        },
        "rx_interfaces": {
            0: {
                "avg_drop_rate_bps": 0.0,
                "avg_drop_rate_pps": 0.0,
                "avg_receive_rate_bps": 12000.0,
                "avg_receive_rate_pps": 1.0,
                "avg_send_rate_bps": 12000.0,
                "avg_send_rate_pps": 1.0,
                "duration": 10.0,
                "last_state_change_timestamp": 9,
                "timestamp": 10,
                "total_dropped_bytes": 0,
                "total_dropped_pkts": 0,
                "total_received_bytes": 15000,
                "total_received_pkts": 10,
                "total_sent_bytes": 15000,
                "total_sent_pkts": 10,
            }
        },
        "timestamp": 10,
        "total_dropped_bytes": 0,
        "total_dropped_pkts": 0,
        "total_received_bytes": 15000,
        "total_received_pkts": 10,
        "total_sent_bytes": 15000,
        "total_sent_pkts": 10,
        "tx_interface_queues": {
            1: {
                "avg_get_processed_rate": 0,
                "avg_get_requested_rate": 0,
                "avg_put_processed_rate": 0,
                "avg_put_requested_rate": 0,
                "avg_queue_len": 0,
                "cur_queue_len": 0,
                "duration": 0,
                "integral_queue_sum": 0,
                "last_state_change_timestamp": 0,
                "max_queue_len": 0,
                "timestamp": 0,
                "total_get_processed_count": 10,
                "total_get_requested_count": 11,
                "total_put_processed_count": 10,
                "total_put_requested_count": 10,
            }
        },
        "tx_interfaces": {
            1: {
                "avg_drop_rate_bps": 0.0,
                "avg_drop_rate_pps": 0.0,
                "avg_receive_rate_bps": 12000.0,
                "avg_receive_rate_pps": 1.0,
                "avg_send_rate_bps": 12000.0,
                "avg_send_rate_pps": 1.0,
                "duration": 10.0,
                "last_state_change_timestamp": 9.011444091796875,
                "timestamp": 10,
                "total_dropped_bytes": 0,
                "total_dropped_pkts": 0,
                "total_received_bytes": 15000,
                "total_received_pkts": 10,
                "total_sent_bytes": 15000,
                "total_sent_pkts": 10,
            }
        },
    }
