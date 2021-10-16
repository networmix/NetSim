from netsim.simcore import SimTime
from netsim.netsim_base import (
    PacketQueue,
    PacketSink,
    PacketSource,
    PacketSize,
)
from netsim.netsim import NetSim
from netsim.netgraph.graph import MultiDiGraph


def test_netsim_1():
    sim = NetSim()

    def arrival_gen() -> SimTime:
        while True:
            yield 1

    def size_gen() -> PacketSize:
        while True:
            yield 1

    s_attr = {"arrival_func": arrival_gen, "size_func": size_gen}
    d_attr = {}

    graph = MultiDiGraph()
    graph.add_node("S", ns_type="PacketSource", ns_attr=s_attr)
    graph.add_node("D", ns_type="PacketSink", ns_attr=d_attr)

    sim.load_graph(graph)
