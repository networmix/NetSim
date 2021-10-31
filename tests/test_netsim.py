# pylint: disable=protected-access,invalid-name
from netsim.netsim_switch import PacketSwitch
from netsim.simcore import SimTime
from netsim.netsim_base import (
    PacketSink,
    PacketSize,
)
from netsim.netsim_simulator import NetSim
from netsim.netgraph.graph import MultiDiGraph
from .test_data.netsim_data import (
    SCENARIO_1_STAT,
    SCENARIO_2_3_STAT,
    SCENARIO_6_STAT,
)


def test_netsim_load_1():
    sim = NetSim()

    graph = MultiDiGraph()
    graph.add_node(
        "S",
        ns_type="PacketSource",
        ns_attr={
            "flow_distr": "Constant",
            "size_distr": "Constant",
            "arrival_time_distr": "Constant",
            "flow_distr_params": {"constant": 1},
            "size_distr_params": {"constant": 1000},
            "arrival_distr_params": {"constant": 1, "first": 0, "count": 3},
        },
    )

    graph.add_node("D", ns_type="PacketSink", ns_attr={})
    graph.add_edge("S", "D", ns_attr={})

    sim.load_graph(graph)

    assert sim.get_ns_obj("S")
    assert sim.get_ns_obj("D")


def test_netsim_scenario_1():
    sim = NetSim()

    s_attr = {
        "flow_distr": "Constant",
        "size_distr": "Constant",
        "arrival_time_distr": "Constant",
        "flow_distr_params": {"constant": 1},
        "size_distr_params": {"constant": 1000},
        "arrival_distr_params": {"constant": 1, "first": 0, "count": 10},
    }
    d_attr = {}

    graph = MultiDiGraph()
    graph.add_node("S", ns_type="PacketSource", ns_attr=s_attr)
    graph.add_node("D", ns_type="PacketSink", ns_attr=d_attr)
    graph.add_edge("S", "D", ns_attr={})

    sim.load_graph(graph)
    sim.run()

    assert sim.nstat.todict() == SCENARIO_1_STAT


def test_netsim_scenario_2():
    sim = NetSim()

    s_attr = {
        "flow_distr": "Constant",
        "size_distr": "Constant",
        "arrival_time_distr": "Constant",
        "flow_distr_params": {"constant": 1},
        "size_distr_params": {"constant": 1000},
        "arrival_distr_params": {"constant": 1, "first": 0},
    }
    d_attr = {}

    graph = MultiDiGraph()
    graph.add_node("S", ns_type="PacketSource", ns_attr=s_attr)
    graph.add_node("D", ns_type="PacketSink", ns_attr=d_attr)
    graph.add_edge("S", "D", ns_attr={})

    sim.load_graph(graph)
    sim.run(until_time=10)

    assert sim.nstat.todict() == SCENARIO_2_3_STAT


def test_netsim_scenario_3():
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

    graph = MultiDiGraph()
    graph.add_node("S", ns_type="PacketSource", ns_attr=s_attr)
    graph.add_node("D", ns_type="PacketSink", ns_attr=d_attr)
    graph.add_edge("S", "D", ns_attr={})

    sim.load_graph(graph)
    sim.run(until_time=10)

    assert sim.nstat.todict() == SCENARIO_2_3_STAT


def test_netsim_scenario_4():
    sim = NetSim()

    s_attr = {
        "flow_distr": "Constant",
        "size_distr": "Constant",
        "arrival_time_distr": "Constant",
        "flow_distr_params": {"constant": 1},
        "size_distr_params": {"constant": 1000},
        "arrival_distr_params": {"constant": 0.1, "first": 0, "count": 10},
    }
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

    r1_tx = sim.get_ns_obj("R1_TX")
    r2_rx = sim.get_ns_obj("R2_RX")

    assert r1_tx.stat.cur_stat_frame.todict() == {
        "timestamp": 2.5,
        "duration": 2.5,
        "last_state_change_timestamp": 2.0,
        "total_sent_pkts": 4,
        "total_received_pkts": 10,
        "total_dropped_pkts": 6,
        "total_sent_bytes": 4000,
        "total_received_bytes": 10000,
        "total_dropped_bytes": 6000,
        "avg_send_rate_pps": 1.6,
        "avg_receive_rate_pps": 4.0,
        "avg_drop_rate_pps": 2.4,
        "avg_send_rate_bps": 12800.0,
        "avg_receive_rate_bps": 32000.0,
        "avg_drop_rate_bps": 19200.0,
    }

    assert r2_rx.stat.cur_stat_frame.todict() == {
        "timestamp": 2.5,
        "duration": 2.5,
        "last_state_change_timestamp": 2.5,
        "total_sent_pkts": 4,
        "total_received_pkts": 4,
        "total_dropped_pkts": 0,
        "total_sent_bytes": 4000,
        "total_received_bytes": 4000,
        "total_dropped_bytes": 0,
        "avg_send_rate_pps": 1.6,
        "avg_receive_rate_pps": 1.6,
        "avg_drop_rate_pps": 0.0,
        "avg_send_rate_bps": 12800.0,
        "avg_receive_rate_bps": 12800.0,
        "avg_drop_rate_bps": 0.0,
    }


def test_netsim_scenario_6():
    sim = NetSim(stat_interval=2)

    s_attr = {
        "flow_distr": "Constant",
        "size_distr": "Constant",
        "arrival_time_distr": "Constant",
        "flow_distr_params": {"constant": 1},
        "size_distr_params": {"constant": 1000},
        "arrival_distr_params": {"constant": 0.1},
    }
    d_attr = {}
    sw_attr = {}
    link_attr = {
        "tx": {"bw": 16000, "queue_len_limit": 1},
        "rx": {"propagation_delay": 0.0},
    }

    graph = MultiDiGraph()
    graph.add_node("S", ns_type="PacketSource", ns_attr=s_attr)
    graph.add_node("SW1", ns_type="PacketSwitch", ns_attr=sw_attr)
    graph.add_node("SW2", ns_type="PacketSwitch", ns_attr=sw_attr)
    graph.add_node("D", ns_type="PacketSink", ns_attr=d_attr)

    graph.add_edge("S", "SW1", ns_attr=link_attr)
    graph.add_edge("SW1", "SW2", ns_attr=link_attr)
    graph.add_edge("SW2", "D", ns_attr=link_attr)

    sim.load_graph(graph)
    # sim.enable_stat_trace()
    sim.run(until_time=4)

    # sw1.stat.get_stat_tracer().seek(0)
    # print(sw1.stat.get_stat_tracer().read())
    assert sim.nstat.todict() == SCENARIO_6_STAT
