from netsim.simulator.netsim_base import Packet
from netsim.simulator.netsim_stat import PacketStatFrame
from netsim.simulator.simcore import SimContext


def test_statframe_process_1():
    ctx = SimContext()
    packet_statframe = PacketStatFrame()
    packet = Packet(ctx, 1000)

    for _ in range(10):
        packet_statframe.packet_received(packet)
    assert packet_statframe.total_received_pkts == 10
    assert packet_statframe.total_received_bytes == 10000

    for _ in range(10):
        packet_statframe.packet_sent(packet)
    assert packet_statframe.total_sent_pkts == 10
    assert packet_statframe.total_sent_bytes == 10000

    packet_statframe.set_time(100, 100)
    packet_statframe.update_on_advance(packet_statframe)

    assert packet_statframe.avg_receive_rate_pps == 0.1
    assert packet_statframe.avg_receive_rate_bps == 800
    assert packet_statframe.timestamp == 100
    assert packet_statframe.duration == 100
