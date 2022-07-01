# pylint: disable=protected-access,invalid-name
from ngraph.graph import MultiDiGraph

from netsim.des.core import SimTime
from netsim.applications.packet_network.switch import PacketSwitch
from netsim.applications.packet_network.base import (
    PacketSize,
)
from netsim.applications.packet_network.simulator import NetSim

from .test_data.ee509_data import SCENARIO_1_STAT


def test_ee509_scenario_1():
    sim = NetSim(stat_interval=10)

    s_attr = {
        "flow_distr": "Constant",
        "size_distr": "Constant",
        "arrival_time_distr": "Constant",
        "flow_distr_params": {"constant": 1},
        "size_distr_params": {"constant": 1000},
        "arrival_distr_params": {"constant": 1, "first": 0},
    }
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
    sim.run(until_time=1000)

    import pprint

    pprint.pprint(sim.nstat.todict())
    assert sim.nstat.todict() == SCENARIO_1_STAT
