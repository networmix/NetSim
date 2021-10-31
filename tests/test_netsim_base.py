# pylint: disable=protected-access,invalid-name
from netsim.simcore import SimTime
from netsim.netsim_base import (
    PacketInterfaceTx,
    PacketQueue,
    PacketSink,
    PacketSource,
    PacketSize,
)
from netsim.netsim_simulator import NetSim


def test_packet_source_1():
    sim = NetSim()

    def arrival_gen() -> SimTime:
        yield 0
        while True:
            yield 1

    def size_gen() -> PacketSize:
        while True:
            yield 1000

    source = PacketSource(sim.ctx, arrival_gen(), size_gen(), initial_delay=0)
    sim.run(until_time=1)
    assert sim.ctx.now == 1
    assert sim.event_counter == 3  # initial_delay, arrival, stop_sim

    assert source.stat.cur_stat_frame.todict() == {
        "avg_drop_rate_bps": 0.0,
        "avg_drop_rate_pps": 0.0,
        "avg_receive_rate_bps": 0.0,
        "avg_receive_rate_pps": 0.0,
        "avg_send_rate_bps": 8000.0,
        "avg_send_rate_pps": 1.0,
        "dropped_pkts_hist": {},
        "dropped_size_hist": {},
        "duration": 1,
        "last_state_change_timestamp": 0,
        "received_pkts_hist": {},
        "received_size_hist": {},
        "sent_pkts_hist": {0: 1},
        "sent_size_hist": {0: 1000},
        "timestamp": 1,
        "total_dropped_bytes": 0,
        "total_dropped_pkts": 0,
        "total_received_bytes": 0,
        "total_received_pkts": 0,
        "total_sent_bytes": 1000,
        "total_sent_pkts": 1,
    }


def test_packet_source_2():
    sim = NetSim()

    def arrival_gen() -> SimTime:
        yield 0
        while True:
            yield 1

    def size_gen() -> PacketSize:
        while True:
            yield 1000

    source = PacketSource(sim.ctx, arrival_gen(), size_gen(), initial_delay=0)
    sim.run(until_time=3)
    assert sim.ctx.now == 3
    assert sim.event_counter == 5  # initial_delay, arrival, arrival, arrival, stop_sim

    assert source.stat.todict() == {
        "cur_interval_duration": 3,
        "cur_stat_frame": {
            "avg_drop_rate_bps": 0.0,
            "avg_drop_rate_pps": 0.0,
            "avg_receive_rate_bps": 0.0,
            "avg_receive_rate_pps": 0.0,
            "avg_send_rate_bps": 8000.0,
            "avg_send_rate_pps": 1.0,
            "dropped_pkts_hist": {},
            "dropped_size_hist": {},
            "duration": 3,
            "last_state_change_timestamp": 2,
            "received_pkts_hist": {},
            "received_size_hist": {},
            "sent_pkts_hist": {0: 1, 1: 1, 2: 1},
            "sent_size_hist": {0: 1000, 1: 1000, 2: 1000},
            "timestamp": 3,
            "total_dropped_bytes": 0,
            "total_dropped_pkts": 0,
            "total_received_bytes": 0,
            "total_received_pkts": 0,
            "total_sent_bytes": 3000,
            "total_sent_pkts": 3,
        },
        "cur_timestamp": 3,
        "last_state_change_timestamp": 2,
        "prev_stat_frame": {
            "avg_drop_rate_bps": 0.0,
            "avg_drop_rate_pps": 0.0,
            "avg_receive_rate_bps": 0.0,
            "avg_receive_rate_pps": 0.0,
            "avg_send_rate_bps": 12000.0,
            "avg_send_rate_pps": 1.5,
            "dropped_pkts_hist": {},
            "dropped_size_hist": {},
            "duration": 2,
            "last_state_change_timestamp": 2,
            "received_pkts_hist": {},
            "received_size_hist": {},
            "sent_pkts_hist": {0: 1, 1: 1, 2: 1},
            "sent_size_hist": {0: 1000, 1: 1000, 2: 1000},
            "timestamp": 2,
            "total_dropped_bytes": 0,
            "total_dropped_pkts": 0,
            "total_received_bytes": 0,
            "total_received_pkts": 0,
            "total_sent_bytes": 3000,
            "total_sent_pkts": 3,
        },
        "prev_timestamp": 2,
        "start_interval_timestamp": 0,
    }


def test_packet_sink_1():
    sim = NetSim()

    def arrival_gen() -> SimTime:
        yield 0
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

    assert source.stat.cur_stat_frame.todict() == {
        "avg_drop_rate_bps": 0.0,
        "avg_drop_rate_pps": 0.0,
        "avg_receive_rate_bps": 0.0,
        "avg_receive_rate_pps": 0.0,
        "avg_send_rate_bps": 8.0,
        "avg_send_rate_pps": 1.0,
        "dropped_pkts_hist": {},
        "dropped_size_hist": {},
        "duration": 10,
        "last_state_change_timestamp": 9,
        "received_pkts_hist": {},
        "received_size_hist": {},
        "sent_pkts_hist": {0: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6: 1, 7: 1, 8: 1, 9: 1},
        "sent_size_hist": {0: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6: 1, 7: 1, 8: 1, 9: 1},
        "timestamp": 10,
        "total_dropped_bytes": 0,
        "total_dropped_pkts": 0,
        "total_received_bytes": 0,
        "total_received_pkts": 0,
        "total_sent_bytes": 10,
        "total_sent_pkts": 10,
    }
    assert sink.stat.cur_stat_frame.todict() == {
        "avg_drop_rate_bps": 0.0,
        "avg_drop_rate_pps": 0.0,
        "avg_receive_rate_bps": 8.0,
        "avg_receive_rate_pps": 1.0,
        "avg_send_rate_bps": 0.0,
        "avg_send_rate_pps": 0.0,
        "dropped_pkts_hist": {},
        "dropped_size_hist": {},
        "duration": 10,
        "last_state_change_timestamp": 9,
        "received_pkts_hist": {
            0: 1,
            1: 1,
            2: 1,
            3: 1,
            4: 1,
            5: 1,
            6: 1,
            7: 1,
            8: 1,
            9: 1,
        },
        "received_size_hist": {
            0: 1,
            1: 1,
            2: 1,
            3: 1,
            4: 1,
            5: 1,
            6: 1,
            7: 1,
            8: 1,
            9: 1,
        },
        "sent_pkts_hist": {},
        "sent_size_hist": {},
        "timestamp": 10,
        "total_dropped_bytes": 0,
        "total_dropped_pkts": 0,
        "total_received_bytes": 10,
        "total_received_pkts": 10,
        "total_sent_bytes": 0,
        "total_sent_pkts": 0,
    }


def test_packet_queue_1():
    sim = NetSim()

    def arrival_gen() -> SimTime:
        yield 0
        while True:
            yield 1

    def size_gen() -> PacketSize:
        while True:
            yield 1000

    source = PacketSource(sim.ctx, arrival_gen(), size_gen())
    queue = PacketQueue(sim.ctx)
    source.subscribe(queue)
    sim.run(until_time=10)

    assert sim.ctx.now == 10
    assert sim.event_counter == 32

    import pprint

    pprint.pprint(queue.queue_stat.todict())

    assert queue.stat.todict() == {
        "cur_interval_duration": 10,
        "cur_stat_frame": {
            "avg_drop_rate_bps": 0.0,
            "avg_drop_rate_pps": 0.0,
            "avg_receive_rate_bps": 8000.0,
            "avg_receive_rate_pps": 1.0,
            "avg_send_rate_bps": 8000.0,
            "avg_send_rate_pps": 1.0,
            "dropped_pkts_hist": {},
            "dropped_size_hist": {},
            "duration": 10,
            "last_state_change_timestamp": 9,
            "received_pkts_hist": {
                0: 1,
                1: 1,
                2: 1,
                3: 1,
                4: 1,
                5: 1,
                6: 1,
                7: 1,
                8: 1,
                9: 1,
            },
            "received_size_hist": {
                0: 1000,
                1: 1000,
                2: 1000,
                3: 1000,
                4: 1000,
                5: 1000,
                6: 1000,
                7: 1000,
                8: 1000,
                9: 1000,
            },
            "sent_pkts_hist": {
                0: 1,
                1: 1,
                2: 1,
                3: 1,
                4: 1,
                5: 1,
                6: 1,
                7: 1,
                8: 1,
                9: 1,
            },
            "sent_size_hist": {
                0: 1000,
                1: 1000,
                2: 1000,
                3: 1000,
                4: 1000,
                5: 1000,
                6: 1000,
                7: 1000,
                8: 1000,
                9: 1000,
            },
            "timestamp": 10,
            "total_dropped_bytes": 0,
            "total_dropped_pkts": 0,
            "total_received_bytes": 10000,
            "total_received_pkts": 10,
            "total_sent_bytes": 10000,
            "total_sent_pkts": 10,
        },
        "cur_timestamp": 10,
        "last_state_change_timestamp": 9,
        "prev_stat_frame": {
            "avg_drop_rate_bps": 0.0,
            "avg_drop_rate_pps": 0.0,
            "avg_receive_rate_bps": 8888.888888888889,
            "avg_receive_rate_pps": 1.1111111111111112,
            "avg_send_rate_bps": 8888.888888888889,
            "avg_send_rate_pps": 1.1111111111111112,
            "dropped_pkts_hist": {},
            "dropped_size_hist": {},
            "duration": 9,
            "last_state_change_timestamp": 9,
            "received_pkts_hist": {
                0: 1,
                1: 1,
                2: 1,
                3: 1,
                4: 1,
                5: 1,
                6: 1,
                7: 1,
                8: 1,
                9: 1,
            },
            "received_size_hist": {
                0: 1000,
                1: 1000,
                2: 1000,
                3: 1000,
                4: 1000,
                5: 1000,
                6: 1000,
                7: 1000,
                8: 1000,
                9: 1000,
            },
            "sent_pkts_hist": {
                0: 1,
                1: 1,
                2: 1,
                3: 1,
                4: 1,
                5: 1,
                6: 1,
                7: 1,
                8: 1,
                9: 1,
            },
            "sent_size_hist": {
                0: 1000,
                1: 1000,
                2: 1000,
                3: 1000,
                4: 1000,
                5: 1000,
                6: 1000,
                7: 1000,
                8: 1000,
                9: 1000,
            },
            "timestamp": 9,
            "total_dropped_bytes": 0,
            "total_dropped_pkts": 0,
            "total_received_bytes": 10000,
            "total_received_pkts": 10,
            "total_sent_bytes": 10000,
            "total_sent_pkts": 10,
        },
        "prev_timestamp": 9,
        "start_interval_timestamp": 0,
    }

    assert queue.queue_stat.todict() == {
        "cur_interval_duration": 10,
        "cur_stat_frame": {
            "avg_get_rate_pps": 1.0,
            "avg_put_rate_pps": 1.0,
            "avg_queue_len": 0.0,
            "avg_wait_time": 0.0,
            "cur_queue_len": 0,
            "duration": 10,
            "integral_queue_sum": 0,
            "integral_wait_time_sum": 0,
            "last_state_change_timestamp": 9,
            "max_queue_len": 0,
            "max_wait_time": 0,
            "timestamp": 10,
            "total_dropped_bytes": 0,
            "total_dropped_pkts": 0,
            "total_get_bytes": 10000,
            "total_get_pkts": 10,
            "total_put_bytes": 10000,
            "total_put_pkts": 10,
        },
        "cur_timestamp": 10,
        "last_state_change_timestamp": 9,
        "prev_stat_frame": {
            "avg_get_rate_pps": 1.1111111111111112,
            "avg_put_rate_pps": 1.1111111111111112,
            "avg_queue_len": 0.0,
            "avg_wait_time": 0.0,
            "cur_queue_len": 0,
            "duration": 9,
            "integral_queue_sum": 0,
            "integral_wait_time_sum": 0,
            "last_state_change_timestamp": 9,
            "max_queue_len": 0,
            "max_wait_time": 0,
            "timestamp": 9,
            "total_dropped_bytes": 0,
            "total_dropped_pkts": 0,
            "total_get_bytes": 10000,
            "total_get_pkts": 10,
            "total_put_bytes": 10000,
            "total_put_pkts": 10,
        },
        "prev_timestamp": 9,
        "start_interval_timestamp": 0,
    }


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

    import pprint

    pprint.pprint(interface_tx.stat.todict())
    assert interface_tx.stat.todict() == {
        "cur_interval_duration": 10.0,
        "cur_stat_frame": {
            "avg_drop_rate_bps": 0.0,
            "avg_drop_rate_pps": 0.0,
            "avg_receive_rate_bps": 10800.0,
            "avg_receive_rate_pps": 0.9,
            "avg_send_rate_bps": 10800.0,
            "avg_send_rate_pps": 0.9,
            "dropped_pkts_hist": {},
            "dropped_size_hist": {},
            "duration": 10.0,
            "last_state_change_timestamp": 9.1875,
            "received_pkts_hist": {
                1: 1,
                2: 1,
                3: 1,
                4: 1,
                5: 1,
                6: 1,
                7: 1,
                8: 1,
                9: 1,
            },
            "received_size_hist": {
                1: 1500,
                2: 1500,
                3: 1500,
                4: 1500,
                5: 1500,
                6: 1500,
                7: 1500,
                8: 1500,
                9: 1500,
            },
            "sent_pkts_hist": {
                1.1875: 1,
                2.1875: 1,
                3.1875: 1,
                4.1875: 1,
                5.1875: 1,
                6.1875: 1,
                7.1875: 1,
                8.1875: 1,
                9.1875: 1,
            },
            "sent_size_hist": {
                1.1875: 1500,
                2.1875: 1500,
                3.1875: 1500,
                4.1875: 1500,
                5.1875: 1500,
                6.1875: 1500,
                7.1875: 1500,
                8.1875: 1500,
                9.1875: 1500,
            },
            "timestamp": 10,
            "total_dropped_bytes": 0,
            "total_dropped_pkts": 0,
            "total_received_bytes": 13500,
            "total_received_pkts": 9,
            "total_sent_bytes": 13500,
            "total_sent_pkts": 9,
        },
        "cur_timestamp": 10,
        "last_state_change_timestamp": 9.1875,
        "prev_stat_frame": {
            "avg_drop_rate_bps": 0.0,
            "avg_drop_rate_pps": 0.0,
            "avg_receive_rate_bps": 11755.102040816326,
            "avg_receive_rate_pps": 0.9795918367346939,
            "avg_send_rate_bps": 11755.102040816326,
            "avg_send_rate_pps": 0.9795918367346939,
            "dropped_pkts_hist": {},
            "dropped_size_hist": {},
            "duration": 9.1875,
            "last_state_change_timestamp": 9.1875,
            "received_pkts_hist": {
                1: 1,
                2: 1,
                3: 1,
                4: 1,
                5: 1,
                6: 1,
                7: 1,
                8: 1,
                9: 1,
            },
            "received_size_hist": {
                1: 1500,
                2: 1500,
                3: 1500,
                4: 1500,
                5: 1500,
                6: 1500,
                7: 1500,
                8: 1500,
                9: 1500,
            },
            "sent_pkts_hist": {
                1.1875: 1,
                2.1875: 1,
                3.1875: 1,
                4.1875: 1,
                5.1875: 1,
                6.1875: 1,
                7.1875: 1,
                8.1875: 1,
                9.1875: 1,
            },
            "sent_size_hist": {
                1.1875: 1500,
                2.1875: 1500,
                3.1875: 1500,
                4.1875: 1500,
                5.1875: 1500,
                6.1875: 1500,
                7.1875: 1500,
                8.1875: 1500,
                9.1875: 1500,
            },
            "timestamp": 9.1875,
            "total_dropped_bytes": 0,
            "total_dropped_pkts": 0,
            "total_received_bytes": 13500,
            "total_received_pkts": 9,
            "total_sent_bytes": 13500,
            "total_sent_pkts": 9,
        },
        "prev_timestamp": 9.1875,
        "start_interval_timestamp": 0,
    }


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

    assert interface_tx.stat.todict() == {
        "cur_interval_duration": 10.0,
        "cur_stat_frame": {
            "avg_drop_rate_bps": 0.0,
            "avg_drop_rate_pps": 0.0,
            "avg_receive_rate_bps": 10800.0,
            "avg_receive_rate_pps": 0.9,
            "avg_send_rate_bps": 8400.0,
            "avg_send_rate_pps": 0.7,
            "dropped_pkts_hist": {},
            "dropped_size_hist": {},
            "duration": 10.0,
            "last_state_change_timestamp": 9.75,
            "received_pkts_hist": {
                1: 1,
                2: 1,
                3: 1,
                4: 1,
                5: 1,
                6.0: 1,
                7.0: 1,
                8.0: 1,
                9.0: 1,
            },
            "received_size_hist": {
                1: 1500,
                2: 1500,
                3: 1500,
                4: 1500,
                5: 1500,
                6.0: 1500,
                7.0: 1500,
                8.0: 1500,
                9.0: 1500,
            },
            "sent_pkts_hist": {
                2.25: 1,
                3.5: 1,
                4.75: 1,
                6.0: 1,
                7.25: 1,
                8.5: 1,
                9.75: 1,
            },
            "sent_size_hist": {
                2.25: 1500,
                3.5: 1500,
                4.75: 1500,
                6.0: 1500,
                7.25: 1500,
                8.5: 1500,
                9.75: 1500,
            },
            "timestamp": 10,
            "total_dropped_bytes": 0,
            "total_dropped_pkts": 0,
            "total_received_bytes": 13500,
            "total_received_pkts": 9,
            "total_sent_bytes": 10500,
            "total_sent_pkts": 7,
        },
        "cur_timestamp": 10,
        "last_state_change_timestamp": 9.75,
        "prev_stat_frame": {
            "avg_drop_rate_bps": 0.0,
            "avg_drop_rate_pps": 0.0,
            "avg_receive_rate_bps": 11076.923076923076,
            "avg_receive_rate_pps": 0.9230769230769231,
            "avg_send_rate_bps": 8615.384615384615,
            "avg_send_rate_pps": 0.717948717948718,
            "dropped_pkts_hist": {},
            "dropped_size_hist": {},
            "duration": 9.75,
            "last_state_change_timestamp": 9.75,
            "received_pkts_hist": {
                1: 1,
                2: 1,
                3: 1,
                4: 1,
                5: 1,
                6.0: 1,
                7.0: 1,
                8.0: 1,
                9.0: 1,
            },
            "received_size_hist": {
                1: 1500,
                2: 1500,
                3: 1500,
                4: 1500,
                5: 1500,
                6.0: 1500,
                7.0: 1500,
                8.0: 1500,
                9.0: 1500,
            },
            "sent_pkts_hist": {
                2.25: 1,
                3.5: 1,
                4.75: 1,
                6.0: 1,
                7.25: 1,
                8.5: 1,
                9.75: 1,
            },
            "sent_size_hist": {
                2.25: 1500,
                3.5: 1500,
                4.75: 1500,
                6.0: 1500,
                7.25: 1500,
                8.5: 1500,
                9.75: 1500,
            },
            "timestamp": 9.75,
            "total_dropped_bytes": 0,
            "total_dropped_pkts": 0,
            "total_received_bytes": 13500,
            "total_received_pkts": 9,
            "total_sent_bytes": 10500,
            "total_sent_pkts": 7,
        },
        "prev_timestamp": 9.75,
        "start_interval_timestamp": 0,
    }
