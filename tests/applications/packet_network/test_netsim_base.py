from __future__ import annotations

from typing import Generator

from netsim.core import SimTime
from netsim.applications.packet_network.base import (
    PacketInterfaceTx,
    PacketQueue,
    PacketSink,
    PacketSource,
)
from netsim.applications.packet_network.common import PacketSize
from netsim.applications.packet_network.simulator import NetSim

from tests.common import _close


# --------------------------------------------------------------------------- #
# PacketSource
# --------------------------------------------------------------------------- #
def test_source_rate_and_bytes() -> None:
    sim = NetSim()

    def arrival_gen() -> Generator[SimTime, None, None]:
        yield 0
        while True:
            yield 1  # 1 pkt/s

    def size_gen() -> Generator[PacketSize, None, None]:
        while True:
            yield 1000  # 1 KiB

    src = PacketSource(sim.ctx, arrival_gen(), size_gen(), initial_delay=0)
    sim.run(until_time=3)  # packets at t=0,1,2

    sf = src.stat.cur_stat_frame
    assert sf.total_sent_pkts == 3
    _close(sf.total_sent_bytes, 3000)
    _close(sf.avg_send_rate_pps, 1.0)
    assert sf.total_dropped_pkts == 0
    _close(sim.ctx.now, 3)


# --------------------------------------------------------------------------- #
# PacketSink
# --------------------------------------------------------------------------- #
def test_sink_receives_all_packets() -> None:
    sim = NetSim()

    def arrival_gen() -> Generator[SimTime, None, None]:
        yield 0
        while True:
            yield 1

    def size_gen() -> Generator[PacketSize, None, None]:
        while True:
            yield 1  # tiny packet

    src = PacketSource(sim.ctx, arrival_gen(), size_gen())
    sink = PacketSink(sim.ctx)
    src.subscribe(sink)

    sim.run(until_time=10)

    s_sf = src.stat.cur_stat_frame
    k_sf = sink.stat.cur_stat_frame
    assert s_sf.total_sent_pkts == 10
    assert k_sf.total_received_pkts == 10
    assert k_sf.total_dropped_pkts == 0
    _close(k_sf.avg_receive_rate_pps, 1.0)
    # bytes conservation
    _close(k_sf.total_received_bytes, s_sf.total_sent_bytes)


# --------------------------------------------------------------------------- #
# Unlimited PacketQueue
# --------------------------------------------------------------------------- #
def test_fifo_queue_passthrough() -> None:
    sim = NetSim()

    def arrival_gen() -> Generator[SimTime, None, None]:
        yield 0
        while True:
            yield 1

    def size_gen() -> Generator[PacketSize, None, None]:
        while True:
            yield 1000

    src = PacketSource(sim.ctx, arrival_gen(), size_gen())
    q = PacketQueue(sim.ctx)  # no cap
    src.subscribe(q)

    sim.run(until_time=10)

    sf = q.stat.cur_stat_frame
    assert sf.total_put_pkts == sf.total_get_pkts == 10
    assert sf.total_dropped_pkts == 0
    assert sf.cur_queue_len == 0
    _close(sf.avg_queue_len, 0.0, abs_=1e-9)


# --------------------------------------------------------------------------- #
# Tail‑drop queue
# --------------------------------------------------------------------------- #
def test_taildrop_queue_limits_and_drops() -> None:
    MAX_LEN = 2
    sim = NetSim()

    def arrival_gen() -> Generator[SimTime, None, None]:
        yield 0
        while True:
            yield 0.1  # fast

    def size_gen() -> Generator[PacketSize, None, None]:
        while True:
            yield 500

    def service_gen() -> Generator[SimTime, None, None]:
        while True:
            yield 0.3

    src = PacketSource(sim.ctx, arrival_gen(), size_gen())
    q = PacketQueue(
        sim.ctx,
        queue_len_limit=MAX_LEN,
        admission_params={"admission_policy": "taildrop"},
        service_func=service_gen(),
    )
    src.subscribe(q)
    sim.run(until_time=5)

    sf = q.stat.cur_stat_frame
    # cap respected
    assert sf.max_queue_len <= MAX_LEN
    # drops happened
    assert sf.total_dropped_pkts > 0
    # basic conservation
    assert sf.total_received_pkts == sf.total_put_pkts + sf.total_dropped_pkts


# --------------------------------------------------------------------------- #
# RED queue
# --------------------------------------------------------------------------- #
def test_red_queue_average_length_small() -> None:
    MAX_LEN = 10
    sim = NetSim()

    def arrival_gen() -> Generator[SimTime, None, None]:
        yield 0
        while True:
            yield 0.05

    def size_gen() -> Generator[PacketSize, None, None]:
        while True:
            yield 1000

    def service_gen() -> Generator[SimTime, None, None]:
        while True:
            yield 0.15

    src = PacketSource(sim.ctx, arrival_gen(), size_gen())
    q = PacketQueue(
        sim.ctx,
        queue_len_limit=MAX_LEN,
        admission_params={
            "admission_policy": "red",
            "wq": 0.5,
            "minth": 3,
            "maxth": 7,
            "maxp": 0.9,
            "s": 0.1,
        },
        service_func=service_gen(),
    )
    src.subscribe(q)
    sim.run(until_time=5)

    sf = q.stat.cur_stat_frame
    assert sf.max_queue_len <= MAX_LEN
    assert sf.total_dropped_pkts > 0
    assert sf.avg_queue_len < MAX_LEN / 2


# --------------------------------------------------------------------------- #
# PacketInterfaceTx – wide link
# --------------------------------------------------------------------------- #
def test_tx_interface_no_queue_with_high_bw() -> None:
    sim = NetSim()

    def arrival_gen() -> Generator[SimTime, None, None]:
        while True:
            yield 1

    def size_gen() -> Generator[PacketSize, None, None]:
        while True:
            yield 1500

    src = PacketSource(sim.ctx, arrival_gen(), size_gen())
    tx = PacketInterfaceTx(sim.ctx, bw=64_000)  # 0.1875 s / packet
    src.subscribe(tx)

    sim.run(until_time=10)  # 9 pkts forwarded

    sf = tx.stat.cur_stat_frame
    assert sf.total_dropped_pkts == 0
    assert sf.max_queue_len == 0
    _close(sf.total_sent_pkts, 9)
    _close(sf.avg_latency_at_departure, 0.1875, rel=1e-3)


# --------------------------------------------------------------------------- #
# PacketInterfaceTx – narrow link
# --------------------------------------------------------------------------- #
def test_tx_interface_queue_builds_with_low_bw() -> None:
    sim = NetSim()

    def arrival_gen() -> Generator[SimTime, None, None]:
        while True:
            yield 1

    def size_gen() -> Generator[PacketSize, None, None]:
        while True:
            yield 1500

    src = PacketSource(sim.ctx, arrival_gen(), size_gen())
    tx = PacketInterfaceTx(sim.ctx, bw=9_600, queue_len_limit=2)
    src.subscribe(tx)

    sim.run(until_time=10)

    sf = tx.stat.cur_stat_frame
    assert sf.max_queue_len <= 2
    assert sf.avg_wait_time > 0
    assert sf.total_sent_pkts < sf.total_received_pkts
