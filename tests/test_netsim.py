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


def test_netsim_scenario_1():
    sim = NetSim()

    def arrival_gen() -> SimTime:
        yield 0
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


def test_netsim_scenario_2():
    sim = NetSim()

    def arrival_gen() -> SimTime:
        yield 0
        for _ in range(2):
            yield 1

    def size_gen() -> PacketSize:
        while True:
            yield 1000

    s_attr = {"arrival_func": arrival_gen(), "size_func": size_gen()}
    d_attr = {}
    r1_tx_attr = {"bw": 8000, "queue_len_limit": 10}
    r2_rx_attr = {"propagation_delay": 0.0}

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
    assert r1_tx.stat.total_sent_pkts == 3
    assert r1_tx.stat.total_received_pkts == 3
    assert r1_tx.stat.total_dropped_pkts == 0
    assert r1_tx.stat.total_sent_bytes == 3000
    assert r1_tx.stat.total_received_bytes == 3000
    assert r1_tx.stat.total_dropped_bytes == 0
    assert r1_tx.stat.avg_send_rate_pps == 1
    assert r1_tx.stat.avg_receive_rate_pps == 1
    assert r1_tx.stat.avg_drop_rate_pps == 0

    assert r2_rx.stat.total_sent_pkts == 3
    assert r2_rx.stat.total_received_pkts == 3
    assert r2_rx.stat.total_dropped_pkts == 0
    assert r2_rx.stat.total_sent_bytes == 3000
    assert r2_rx.stat.total_received_bytes == 3000
    assert r2_rx.stat.total_dropped_bytes == 0
    assert r2_rx.stat.avg_send_rate_pps == 1
    assert r2_rx.stat.avg_receive_rate_pps == 1
    assert r2_rx.stat.avg_drop_rate_pps == 0


def test_netsim_scenario_3():
    sim = NetSim()

    def arrival_gen() -> SimTime:
        yield 0
        for _ in range(9):
            yield 1

    def size_gen() -> PacketSize:
        while True:
            yield 1000

    s_attr = {"arrival_func": arrival_gen(), "size_func": size_gen()}
    d_attr = {}
    r1_tx_attr = {"bw": 16000, "queue_len_limit": 10}
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
    assert r1_tx.stat.avg_send_rate_pps == 1
    assert r1_tx.stat.avg_receive_rate_pps == 1
    assert r1_tx.stat.avg_drop_rate_pps == 0

    assert r2_rx.stat.total_sent_pkts == 10
    assert r2_rx.stat.total_received_pkts == 10
    assert r2_rx.stat.total_dropped_pkts == 0
    assert r2_rx.stat.total_sent_bytes == 10000
    assert r2_rx.stat.total_received_bytes == 10000
    assert r2_rx.stat.total_dropped_bytes == 0
    assert r2_rx.stat.avg_send_rate_pps == 1
    assert r2_rx.stat.avg_receive_rate_pps == 1
    assert r2_rx.stat.avg_drop_rate_pps == 0


def test_netsim_scenario_4():
    sim = NetSim()

    def arrival_gen() -> SimTime:
        yield 0
        for _ in range(9):
            yield 0.1

    def size_gen() -> PacketSize:
        while True:
            yield 1000

    s_attr = {"arrival_func": arrival_gen(), "size_func": size_gen()}
    d_attr = {}
    r1_tx_attr = {"bw": 16000, "queue_len_limit": 2}
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
    assert r1_tx.stat.total_sent_pkts == 4
    assert r1_tx.stat.total_received_pkts == 10
    assert r1_tx.stat.total_dropped_pkts == 6
    assert r1_tx.stat.total_sent_bytes == 4000
    assert r1_tx.stat.total_received_bytes == 10000
    assert r1_tx.stat.total_dropped_bytes == 6000
    assert r1_tx.stat.avg_send_rate_pps == 1.6
    assert r1_tx.stat.avg_receive_rate_pps == 4
    assert r1_tx.stat.avg_drop_rate_pps == 2.4

    assert r2_rx.stat.total_sent_pkts == 4
    assert r2_rx.stat.total_received_pkts == 4
    assert r2_rx.stat.total_dropped_pkts == 0
    assert r2_rx.stat.total_sent_bytes == 4000
    assert r2_rx.stat.total_received_bytes == 4000
    assert r2_rx.stat.total_dropped_bytes == 0
    assert r2_rx.stat.avg_send_rate_pps == 1.6
    assert r2_rx.stat.avg_receive_rate_pps == 1.6
    assert r2_rx.stat.avg_drop_rate_pps == 0


def test_netsim_scenario_5():
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
    r1_tx_attr = {"bw": 16000, "queue_len_limit": 1}
    r2_rx_attr = {"propagation_delay": 0.0}

    graph = MultiDiGraph()
    graph.add_node("S", ns_type="PacketSource", ns_attr=s_attr)
    graph.add_node("R1_TX", ns_type="PacketInterfaceTx", ns_attr=r1_tx_attr)
    graph.add_node("R2_RX", ns_type="PacketInterfaceRx", ns_attr=r2_rx_attr)
    graph.add_node("D", ns_type="PacketSink", ns_attr=d_attr)

    graph.add_edge("S", "R1_TX", ns_attr={})
    graph.add_edge("R1_TX", "R2_RX", ns_attr={})
    graph.add_edge("R2_RX", "D", ns_attr={})

    sim.load_graph(graph)
    sim.run(until_time=10)

    r1_tx = sim.get("R1_TX")
    r2_rx = sim.get("R2_RX")

    print(r1_tx.stat)
    print(r2_rx.stat)
    assert r1_tx.stat.total_sent_pkts == 19
    assert r1_tx.stat.total_received_pkts == 100
    assert r1_tx.stat.total_dropped_pkts == 79
    assert r1_tx.stat.total_sent_bytes == 19000
    assert r1_tx.stat.total_received_bytes == 100000
    assert r1_tx.stat.total_dropped_bytes == 79000
    assert r1_tx.stat.avg_send_rate_pps == 1.9
    assert r1_tx.stat.avg_receive_rate_pps == 10
    assert r1_tx.stat.avg_drop_rate_pps == 7.9

    assert r2_rx.stat.total_sent_pkts == 19
    assert r2_rx.stat.total_received_pkts == 19
    assert r2_rx.stat.total_dropped_pkts == 0
    assert r2_rx.stat.total_sent_bytes == 19000
    assert r2_rx.stat.total_received_bytes == 19000
    assert r2_rx.stat.total_dropped_bytes == 0
    assert r2_rx.stat.avg_send_rate_pps == 1.9
    assert r2_rx.stat.avg_receive_rate_pps == 1.9
    assert r2_rx.stat.avg_drop_rate_pps == 0


def test_netsim_scenario_6():
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

    sw1: PacketSwitch = sim.get("SW1")

    print(sw1.switch_stat)
    assert sw1.switch_stat.total_sent_pkts == 21
    assert sw1.switch_stat.total_received_pkts == 100
    assert sw1.switch_stat.total_dropped_pkts == 79
    assert sw1.switch_stat.total_sent_bytes == 21000
    assert sw1.switch_stat.total_received_bytes == 100000
    assert sw1.switch_stat.total_dropped_bytes == 79000
    assert sw1.switch_stat.avg_send_rate_pps == 2.0
    assert sw1.switch_stat.avg_receive_rate_pps == 9.523809523809524
    assert sw1.switch_stat.avg_drop_rate_pps == 7.523809523809524
    assert sw1.switch_stat.avg_send_rate_bps == 16000.0
    assert sw1.switch_stat.avg_receive_rate_bps == 76190.47619047618
    assert sw1.switch_stat.avg_drop_rate_bps == 60190.47619047619
