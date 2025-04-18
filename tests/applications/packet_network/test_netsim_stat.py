from __future__ import annotations

import math

from netsim.core import SimContext
from netsim.applications.packet_network.base import Packet
from netsim.applications.packet_network.stat import PacketStatFrame

from tests.common import _close


def test_packet_statframe_full_flow() -> None:
    ctx = SimContext()  # simulation clock starts at 0

    # two packets of different sizes
    pkt_big = Packet(ctx, 1000)  # generated_timestamp = 0
    pkt_small = Packet(ctx, 500)

    sf = PacketStatFrame()

    # ------------------------------------------------------------------ phase 1
    # advance sim‑time to 5 s, receive both packets
    sf.set_time(timestamp=5, duration=0)  # pretend we're at t=5
    sf.packet_received(pkt_big)  # latency 5
    sf.packet_received(pkt_small)  # latency 5 (avg still 5)
    assert sf.total_received_pkts == 2
    assert sf.total_received_bytes == 1500
    _close(sf.avg_latency_at_arrival, 5.0)

    # send out one packet at the same timestamp
    sf.packet_sent(pkt_big)  # latency 5
    assert sf.total_sent_pkts == 1
    assert sf.total_sent_bytes == 1000
    _close(sf.avg_latency_at_departure, 5.0)

    # drop one packet
    sf.packet_dropped(pkt_small)  # latency 5
    assert sf.total_dropped_pkts == 1
    assert sf.total_dropped_bytes == 500
    _close(sf.avg_latency_at_drop, 5.0)

    # ------------------------------------------------------------------ phase 2
    # advance to t=15 (Δt = 10 s) and compute rates
    sf.set_time(timestamp=15, duration=10)
    sf.update_stat()  # triggers _calc_avg()

    # packets-per‑second
    _close(sf.avg_receive_rate_pps, 2 / 10)
    _close(sf.avg_send_rate_pps, 1 / 10)
    _close(sf.avg_drop_rate_pps, 1 / 10)

    # bits‑per‑second
    _close(sf.avg_receive_rate_bps, 1500 * 8 / 10)
    _close(sf.avg_send_rate_bps, 1000 * 8 / 10)
    _close(sf.avg_drop_rate_bps, 500 * 8 / 10)

    # ------------------------------------------------------------------ phase 3
    # verify update_on_advance() copies timestamp & keeps averages
    prev = PacketStatFrame()
    prev.set_time(timestamp=15, duration=10)
    prev.total_received_pkts = sf.total_received_pkts
    prev.total_sent_pkts = sf.total_sent_pkts
    prev.total_dropped_pkts = sf.total_dropped_pkts

    sf.update_on_advance(prev)

    assert sf.timestamp == 15
    # averages should be unchanged by the no‑op advance
    _close(sf.avg_receive_rate_pps, 2 / 10)
    _close(sf.avg_send_rate_pps, 1 / 10)
