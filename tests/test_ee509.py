# pylint: disable=protected-access,invalid-name
from netsim.netsim_switch import PacketSwitch
from netsim.simcore import SimTime
from netsim.netsim_base import (
    PacketQueue,
    PacketSink,
    PacketSource,
    PacketSize,
)
from netsim.netsim import NetSim
from netsim.netgraph.graph import MultiDiGraph


def test_ee509_scenario_1():
    sim = NetSim()

    def arrival_gen() -> SimTime:
        yield 0
        for _ in range(99):
            yield 0.1

    def size_gen() -> PacketSize:
        while True:
            yield 1000

    s_attr = {"arrival_func": arrival_gen(), "size_func": size_gen()}
    d_attr = {}
    sw1_attr = {}
    sw1_tx_attr = {"tx": {"bw": 16000, "queue_len_limit": 1}}
    sw1_rx_attr = {"rx": {"propagation_delay": 0.0}}

    graph = MultiDiGraph()
    graph.add_node("S", ns_type="PacketSource", ns_attr=s_attr)
    graph.add_node("SW1", ns_type="PacketSwitch", ns_attr=sw1_attr)
    graph.add_node("D", ns_type="PacketSink", ns_attr=d_attr)

    graph.add_edge("S", "SW1", ns_attr=sw1_rx_attr)
    graph.add_edge("SW1", "D", ns_attr=sw1_tx_attr)

    sim.load_graph(graph)
    sim.run()

    sw1: PacketSwitch = sim.get_ns_obj("SW1")

    print(sw1.stat)
    assert sw1.stat.total_sent_pkts == 21
    assert sw1.stat.total_received_pkts == 100
    assert sw1.stat.total_dropped_pkts == 79
    assert sw1.stat.total_sent_bytes == 21000
    assert sw1.stat.total_received_bytes == 100000
    assert sw1.stat.total_dropped_bytes == 79000
    assert sw1.stat.avg_send_rate_pps == 2.0
    assert sw1.stat.avg_receive_rate_pps == 9.523809523809524
    assert sw1.stat.avg_drop_rate_pps == 7.523809523809524
    assert sw1.stat.avg_send_rate_bps == 16000.0
    assert sw1.stat.avg_receive_rate_bps == 76190.47619047618
    assert sw1.stat.avg_drop_rate_bps == 60190.47619047619
