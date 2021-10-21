# pylint: disable=protected-access,invalid-name
from netsim.simcore import SimTime
from netsim.netsim_base import (
    PacketQueue,
    PacketSink,
    PacketSource,
    PacketSize,
)
from netsim.netsim import NetSim
from netsim.netgraph.graph import MultiDiGraph


def test_netsim_load_1():
    sim = NetSim()

    def arrival_gen() -> SimTime:
        while True:
            yield 1

    def size_gen() -> PacketSize:
        while True:
            yield 1

    graph = MultiDiGraph()
    graph.add_node(
        "S",
        ns_type="PacketSource",
        ns_attr={"arrival_func": arrival_gen(), "size_func": size_gen()},
    )
    graph.add_node("D", ns_type="PacketSink", ns_attr={})
    graph.add_edge("S", "D", ns_attr={})

    sim.load_graph(graph)


def test_netsim_run_1():
    sim = NetSim()

    def arrival_gen() -> SimTime:
        while True:
            yield 1

    def size_gen() -> PacketSize:
        while True:
            yield 1

    s_attr = {"arrival_func": arrival_gen(), "size_func": size_gen()}
    d_attr = {}

    graph = MultiDiGraph()
    graph.add_node("S", ns_type="PacketSource", ns_attr=s_attr)
    graph.add_node("D", ns_type="PacketSink", ns_attr=d_attr)
    graph.add_edge("S", "D", ns_attr={})

    sim.load_graph(graph)
    sim.run(until_time=10)


def test_netsim_run_2():
    sim = NetSim()

    def arrival_gen() -> SimTime:
        for _ in range(10):
            yield 1

    def size_gen() -> PacketSize:
        while True:
            yield 1000

    s_attr = {"arrival_func": arrival_gen(), "size_func": size_gen()}
    d_attr = {}
    r1_tx_attr = {"bw": 8000, "queue_len_limit": 10}
    r2_rx_attr = {"propagation_delay": 0.5}

    graph = MultiDiGraph()
    graph.add_node("S", ns_type="PacketSource", ns_attr=s_attr)
    graph.add_node("R1_TX", ns_type="PacketInterfaceTx", ns_attr=r1_tx_attr)
    graph.add_node("R2_RX", ns_type="PacketInterfaceRx", ns_attr=r2_rx_attr)
    graph.add_node("D", ns_type="PacketSink", ns_attr=d_attr)

    graph.add_edge("S", "R1_TX", ns_attr={})
    graph.add_edge("R1_TX", "R2_RX", ns_attr={})
    graph.add_edge("R2_RX", "D", ns_attr={})

    sim.load_graph(graph)
    sim.run()

    r1_tx = sim.get("R1_TX")
    r2_rx = sim.get("R2_RX")

    print(r1_tx.stat)
    print(r2_rx.stat)
    assert r1_tx.stat.total_sent_pkts == 10
    assert r1_tx.stat.total_received_pkts == 10
    assert r1_tx.stat.total_dropped_pkts == 0
    assert r1_tx.stat.total_sent_bytes == 10000
    assert r1_tx.stat.total_received_bytes == 10000
    assert r1_tx.stat.total_dropped_bytes == 0
    # assert r1_tx.stat.avg_send_rate_pps == 0.9090909090909091
    # assert r1_tx.stat.avg_receive_rate_pps == 0.9090909090909091
    # assert r1_tx.stat.avg_drop_rate_pps == 0

    assert r2_rx.stat.total_sent_pkts == 10
    assert r2_rx.stat.total_received_pkts == 10
    assert r2_rx.stat.total_dropped_pkts == 0
    assert r2_rx.stat.total_sent_bytes == 10000
    assert r2_rx.stat.total_received_bytes == 10000
    assert r2_rx.stat.total_dropped_bytes == 0
    # assert r2_rx.stat.avg_send_rate_pps == 0.9090909090909091
    # assert r2_rx.stat.avg_receive_rate_pps == 0.9090909090909091
    # assert r2_rx.stat.avg_drop_rate_pps == 0
