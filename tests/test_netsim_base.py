# pylint: disable=protected-access,invalid-name
import pprint

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
    pprint.pprint(source.stat.cur_stat_frame.todict())

    assert source.stat.cur_stat_frame.todict() == {
        "avg_drop_rate_bps": 0.0,
        "avg_drop_rate_pps": 0.0,
        "avg_latency_at_arrival": 0,
        "avg_latency_at_departure": 0.0,
        "avg_latency_at_drop": 0,
        "avg_receive_rate_bps": 0.0,
        "avg_receive_rate_pps": 0.0,
        "avg_send_rate_bps": 8000.0,
        "avg_send_rate_pps": 1.0,
        "duration": 1,
        "last_state_change_timestamp": 0,
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
    pprint.pprint(source.stat.todict())

    assert source.stat.todict() == {
        "cur_interval_duration": 3,
        "cur_stat_frame": {
            "avg_drop_rate_bps": 0.0,
            "avg_drop_rate_pps": 0.0,
            "avg_latency_at_arrival": 0,
            "avg_latency_at_departure": 0.0,
            "avg_latency_at_drop": 0,
            "avg_receive_rate_bps": 0.0,
            "avg_receive_rate_pps": 0.0,
            "avg_send_rate_bps": 8000.0,
            "avg_send_rate_pps": 1.0,
            "duration": 3,
            "last_state_change_timestamp": 2,
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
            "avg_latency_at_arrival": 0,
            "avg_latency_at_departure": 0.0,
            "avg_latency_at_drop": 0,
            "avg_receive_rate_bps": 0.0,
            "avg_receive_rate_pps": 0.0,
            "avg_send_rate_bps": 12000.0,
            "avg_send_rate_pps": 1.5,
            "duration": 2,
            "last_state_change_timestamp": 2,
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

    pprint.pprint(sink.stat.cur_stat_frame.todict())

    assert source.stat.cur_stat_frame.todict() == {
        "avg_drop_rate_bps": 0.0,
        "avg_drop_rate_pps": 0.0,
        "avg_latency_at_arrival": 0,
        "avg_latency_at_departure": 0.0,
        "avg_latency_at_drop": 0,
        "avg_receive_rate_bps": 0.0,
        "avg_receive_rate_pps": 0.0,
        "avg_send_rate_bps": 8.0,
        "avg_send_rate_pps": 1.0,
        "duration": 10,
        "last_state_change_timestamp": 9,
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
        "avg_latency_at_arrival": 0.0,
        "avg_latency_at_departure": 0,
        "avg_latency_at_drop": 0,
        "avg_receive_rate_bps": 8.0,
        "avg_receive_rate_pps": 1.0,
        "avg_send_rate_bps": 0.0,
        "avg_send_rate_pps": 0.0,
        "duration": 10,
        "last_state_change_timestamp": 9,
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

    pprint.pprint(queue.stat.todict())

    assert queue.stat.todict() == {
        "cur_interval_duration": 10,
        "cur_stat_frame": {
            "avg_drop_rate_bps": 0.0,
            "avg_drop_rate_pps": 0.0,
            "avg_get_rate_pps": 1.0,
            "avg_latency_at_arrival": 0.0,
            "avg_latency_at_departure": 0.0,
            "avg_latency_at_drop": 0,
            "avg_put_rate_pps": 1.0,
            "avg_queue_len": 0.0,
            "avg_receive_rate_bps": 8000.0,
            "avg_receive_rate_pps": 1.0,
            "avg_send_rate_bps": 8000.0,
            "avg_send_rate_pps": 1.0,
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
            "avg_get_rate_pps": 1.1111111111111112,
            "avg_latency_at_arrival": 0.0,
            "avg_latency_at_departure": 0.0,
            "avg_latency_at_drop": 0,
            "avg_put_rate_pps": 1.1111111111111112,
            "avg_queue_len": 0.0,
            "avg_receive_rate_bps": 8888.888888888889,
            "avg_receive_rate_pps": 1.1111111111111112,
            "avg_send_rate_bps": 8888.888888888889,
            "avg_send_rate_pps": 1.1111111111111112,
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
            "total_received_bytes": 10000,
            "total_received_pkts": 10,
            "total_sent_bytes": 10000,
            "total_sent_pkts": 10,
        },
        "prev_timestamp": 9,
        "start_interval_timestamp": 0,
    }


def test_packet_queue_admission_taildrop_2():
    sim = NetSim()

    ADMISSION_PARAMS = {
        "admission_policy": "taildrop",
    }
    MAX_QUEUE_LEN = 10

    def arrival_gen() -> SimTime:
        yield 0
        while True:
            for idx in range(0, 10):
                yield 1 / 2 ** idx

    def size_gen() -> PacketSize:
        while True:
            yield 1000

    def service_gen() -> SimTime:
        while True:
            yield 0.25

    source = PacketSource(sim.ctx, arrival_gen(), size_gen())
    queue = PacketQueue(
        sim.ctx,
        queue_len_limit=MAX_QUEUE_LEN,
        admission_params=ADMISSION_PARAMS,
        service_func=service_gen(),
    )
    source.subscribe(queue)
    sim.run(until_time=10)

    assert sim.ctx.now == 10

    pprint.pprint(queue.stat.todict())

    assert queue.stat.todict() == {
        "cur_interval_duration": 10.0,
        "cur_stat_frame": {
            "avg_drop_rate_bps": 8000.0,
            "avg_drop_rate_pps": 1.0,
            "avg_get_rate_pps": 3.6,
            "avg_latency_at_arrival": 0.0,
            "avg_latency_at_departure": 1.2932477678571428,
            "avg_latency_at_drop": 0.0,
            "avg_put_rate_pps": 4.6,
            "avg_queue_len": 4.4390625,
            "avg_receive_rate_bps": 40800.0,
            "avg_receive_rate_pps": 5.1,
            "avg_send_rate_bps": 28000.0,
            "avg_send_rate_pps": 3.5,
            "avg_wait_time": 1.0639105902777777,
            "cur_queue_len": 10,
            "duration": 10.0,
            "integral_queue_sum": 44.390625,
            "integral_wait_time_sum": 38.30078125,
            "last_state_change_timestamp": 9.990234375,
            "max_queue_len": 10,
            "max_wait_time": 2.2578125,
            "timestamp": 10,
            "total_dropped_bytes": 10000,
            "total_dropped_pkts": 10,
            "total_get_bytes": 36000,
            "total_get_pkts": 36,
            "total_put_bytes": 46000,
            "total_put_pkts": 46,
            "total_received_bytes": 51000,
            "total_received_pkts": 51,
            "total_sent_bytes": 35000,
            "total_sent_pkts": 35,
        },
        "cur_timestamp": 10,
        "last_state_change_timestamp": 9.990234375,
        "prev_stat_frame": {
            "avg_drop_rate_bps": 8007.820136852395,
            "avg_drop_rate_pps": 1.0009775171065494,
            "avg_get_rate_pps": 3.6035190615835777,
            "avg_latency_at_arrival": 0.0,
            "avg_latency_at_departure": 1.2932477678571428,
            "avg_latency_at_drop": 0.0,
            "avg_put_rate_pps": 4.604496578690127,
            "avg_queue_len": 4.433626588465298,
            "avg_receive_rate_bps": 40839.882697947214,
            "avg_receive_rate_pps": 5.104985337243402,
            "avg_send_rate_bps": 28027.370478983383,
            "avg_send_rate_pps": 3.5034213098729228,
            "avg_wait_time": 1.0639105902777777,
            "cur_queue_len": 10,
            "duration": 9.990234375,
            "integral_queue_sum": 44.29296875,
            "integral_wait_time_sum": 38.30078125,
            "last_state_change_timestamp": 9.990234375,
            "max_queue_len": 10,
            "max_wait_time": 2.2578125,
            "timestamp": 9.990234375,
            "total_dropped_bytes": 10000,
            "total_dropped_pkts": 10,
            "total_get_bytes": 36000,
            "total_get_pkts": 36,
            "total_put_bytes": 46000,
            "total_put_pkts": 46,
            "total_received_bytes": 51000,
            "total_received_pkts": 51,
            "total_sent_bytes": 35000,
            "total_sent_pkts": 35,
        },
        "prev_timestamp": 9.990234375,
        "start_interval_timestamp": 0,
    }


def test_packet_queue_admission_red_1():
    sim = NetSim()

    ADMISSION_PARAMS = {
        "admission_policy": "red",
        "wq": 0.5,
        "minth": 3,
        "maxth": 10,
        "maxp": 0.99,
        "s": 0.1,
    }

    MAX_QUEUE_LEN = 10

    def arrival_gen() -> SimTime:
        yield 0
        while True:
            for idx in range(0, 10):
                yield 1 / 2 ** idx

    def size_gen() -> PacketSize:
        while True:
            yield 1000

    def service_gen() -> SimTime:
        while True:
            yield 0.25

    source = PacketSource(sim.ctx, arrival_gen(), size_gen())
    queue = PacketQueue(
        sim.ctx,
        queue_len_limit=MAX_QUEUE_LEN,
        admission_params=ADMISSION_PARAMS,
        service_func=service_gen(),
    )
    source.subscribe(queue)
    sim.run(until_time=10)

    assert sim.ctx.now == 10

    pprint.pprint(queue.stat.todict())
    print(queue._queue._queue)

    assert queue.stat.todict() == {
        "cur_interval_duration": 10.0,
        "cur_stat_frame": {
            "avg_drop_rate_bps": 14400.0,
            "avg_drop_rate_pps": 1.8,
            "avg_get_rate_pps": 3.6,
            "avg_latency_at_arrival": 0.0,
            "avg_latency_at_departure": 0.9234933035714286,
            "avg_latency_at_drop": 0.0,
            "avg_put_rate_pps": 4.2,
            "avg_queue_len": 2.5361328125,
            "avg_receive_rate_bps": 40800.0,
            "avg_receive_rate_pps": 5.1,
            "avg_send_rate_bps": 28000.0,
            "avg_send_rate_pps": 3.5,
            "avg_wait_time": 0.6758355034722222,
            "cur_queue_len": 6,
            "duration": 10.0,
            "integral_queue_sum": 25.361328125,
            "integral_wait_time_sum": 24.330078125,
            "last_state_change_timestamp": 9.990234375,
            "max_queue_len": 7,
            "max_wait_time": 1.509765625,
            "timestamp": 10,
            "total_dropped_bytes": 18000,
            "total_dropped_pkts": 18,
            "total_get_bytes": 36000,
            "total_get_pkts": 36,
            "total_put_bytes": 42000,
            "total_put_pkts": 42,
            "total_received_bytes": 51000,
            "total_received_pkts": 51,
            "total_sent_bytes": 35000,
            "total_sent_pkts": 35,
        },
        "cur_timestamp": 10,
        "last_state_change_timestamp": 9.990234375,
        "prev_stat_frame": {
            "avg_drop_rate_bps": 14414.07624633431,
            "avg_drop_rate_pps": 1.8017595307917889,
            "avg_get_rate_pps": 3.6035190615835777,
            "avg_latency_at_arrival": 0.0,
            "avg_latency_at_departure": 0.9234933035714286,
            "avg_latency_at_drop": 0.0,
            "avg_put_rate_pps": 4.204105571847507,
            "avg_queue_len": 2.5327468230694037,
            "avg_receive_rate_bps": 40839.882697947214,
            "avg_receive_rate_pps": 5.104985337243402,
            "avg_send_rate_bps": 28027.370478983383,
            "avg_send_rate_pps": 3.5034213098729228,
            "avg_wait_time": 0.6758355034722222,
            "cur_queue_len": 6,
            "duration": 9.990234375,
            "integral_queue_sum": 25.302734375,
            "integral_wait_time_sum": 24.330078125,
            "last_state_change_timestamp": 9.990234375,
            "max_queue_len": 7,
            "max_wait_time": 1.509765625,
            "timestamp": 9.990234375,
            "total_dropped_bytes": 18000,
            "total_dropped_pkts": 18,
            "total_get_bytes": 36000,
            "total_get_pkts": 36,
            "total_put_bytes": 42000,
            "total_put_pkts": 42,
            "total_received_bytes": 51000,
            "total_received_pkts": 51,
            "total_sent_bytes": 35000,
            "total_sent_pkts": 35,
        },
        "prev_timestamp": 9.990234375,
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

    pprint.pprint(interface_tx.stat.todict())
    assert interface_tx.stat.todict() == {
        "cur_interval_duration": 10.0,
        "cur_stat_frame": {
            "avg_drop_rate_bps": 0.0,
            "avg_drop_rate_pps": 0.0,
            "avg_get_rate_pps": 0.9,
            "avg_latency_at_arrival": 0.0,
            "avg_latency_at_departure": 0.1875,
            "avg_latency_at_drop": 0,
            "avg_put_rate_pps": 0.9,
            "avg_queue_len": 0.0,
            "avg_receive_rate_bps": 10800.0,
            "avg_receive_rate_pps": 0.9,
            "avg_send_rate_bps": 10800.0,
            "avg_send_rate_pps": 0.9,
            "avg_wait_time": 0.0,
            "cur_queue_len": 0,
            "duration": 10.0,
            "integral_queue_sum": 0.0,
            "integral_wait_time_sum": 0,
            "last_state_change_timestamp": 9.1875,
            "max_queue_len": 0,
            "max_wait_time": 0,
            "timestamp": 10,
            "total_dropped_bytes": 0,
            "total_dropped_pkts": 0,
            "total_get_bytes": 13500,
            "total_get_pkts": 9,
            "total_put_bytes": 13500,
            "total_put_pkts": 9,
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
            "avg_get_rate_pps": 0.9795918367346939,
            "avg_latency_at_arrival": 0.0,
            "avg_latency_at_departure": 0.1875,
            "avg_latency_at_drop": 0,
            "avg_put_rate_pps": 0.9795918367346939,
            "avg_queue_len": 0.0,
            "avg_receive_rate_bps": 11755.102040816326,
            "avg_receive_rate_pps": 0.9795918367346939,
            "avg_send_rate_bps": 11755.102040816326,
            "avg_send_rate_pps": 0.9795918367346939,
            "avg_wait_time": 0.0,
            "cur_queue_len": 0,
            "duration": 9.1875,
            "integral_queue_sum": 0.0,
            "integral_wait_time_sum": 0,
            "last_state_change_timestamp": 9.1875,
            "max_queue_len": 0,
            "max_wait_time": 0,
            "timestamp": 9.1875,
            "total_dropped_bytes": 0,
            "total_dropped_pkts": 0,
            "total_get_bytes": 13500,
            "total_get_pkts": 9,
            "total_put_bytes": 13500,
            "total_put_pkts": 9,
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

    pprint.pprint(interface_tx.stat.todict())

    assert interface_tx.stat.todict() == {
        "cur_interval_duration": 10.0,
        "cur_stat_frame": {
            "avg_drop_rate_bps": 0.0,
            "avg_drop_rate_pps": 0.0,
            "avg_get_rate_pps": 0.8,
            "avg_latency_at_arrival": 0.0,
            "avg_latency_at_departure": 2.0,
            "avg_latency_at_drop": 0,
            "avg_put_rate_pps": 0.9,
            "avg_queue_len": 0.8,
            "avg_receive_rate_bps": 10800.0,
            "avg_receive_rate_pps": 0.9,
            "avg_send_rate_bps": 8400.0,
            "avg_send_rate_pps": 0.7,
            "avg_wait_time": 0.875,
            "cur_queue_len": 1,
            "duration": 10.0,
            "integral_queue_sum": 8.0,
            "integral_wait_time_sum": 7.0,
            "last_state_change_timestamp": 9.75,
            "max_queue_len": 2,
            "max_wait_time": 1.75,
            "timestamp": 10,
            "total_dropped_bytes": 0,
            "total_dropped_pkts": 0,
            "total_get_bytes": 12000,
            "total_get_pkts": 8,
            "total_put_bytes": 13500,
            "total_put_pkts": 9,
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
            "avg_get_rate_pps": 0.8205128205128205,
            "avg_latency_at_arrival": 0.0,
            "avg_latency_at_departure": 2.0,
            "avg_latency_at_drop": 0,
            "avg_put_rate_pps": 0.9230769230769231,
            "avg_queue_len": 0.7948717948717948,
            "avg_receive_rate_bps": 11076.923076923076,
            "avg_receive_rate_pps": 0.9230769230769231,
            "avg_send_rate_bps": 8615.384615384615,
            "avg_send_rate_pps": 0.717948717948718,
            "avg_wait_time": 0.875,
            "cur_queue_len": 1,
            "duration": 9.75,
            "integral_queue_sum": 7.75,
            "integral_wait_time_sum": 7.0,
            "last_state_change_timestamp": 9.75,
            "max_queue_len": 2,
            "max_wait_time": 1.75,
            "timestamp": 9.75,
            "total_dropped_bytes": 0,
            "total_dropped_pkts": 0,
            "total_get_bytes": 12000,
            "total_get_pkts": 8,
            "total_put_bytes": 13500,
            "total_put_pkts": 9,
            "total_received_bytes": 13500,
            "total_received_pkts": 9,
            "total_sent_bytes": 10500,
            "total_sent_pkts": 7,
        },
        "prev_timestamp": 9.75,
        "start_interval_timestamp": 0,
    }
