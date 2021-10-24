# pylint: disable=protected-access,invalid-name
from netsim.netsim_switch import PacketSwitch
from netsim.simcore import SimTime
from netsim.netsim_base import (
    PacketSize,
)
from netsim.netsim import NetSim
from netsim.netgraph.graph import MultiDiGraph
from .test_data.ee509_data import SCENARIO_1_STAT


def test_ee509_scenario_1():
    sim = NetSim()

    def arrival_gen() -> SimTime:
        yield 0
        while True:
            yield 1

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
    sim.run(until_time=10)

    sw1: PacketSwitch = sim.get_ns_obj("SW1")

    assert sw1.stat.total_sent_pkts == 10
    assert sw1.stat.total_received_pkts == 10
    assert sw1.stat.total_dropped_pkts == 0
    assert sw1.stat.total_sent_bytes == 10000
    assert sw1.stat.total_received_bytes == 10000
    assert sw1.stat.total_dropped_bytes == 0

    import pprint

    pprint.pprint(sim.nstat.todict())
    assert sim.nstat.todict() == SCENARIO_1_STAT
