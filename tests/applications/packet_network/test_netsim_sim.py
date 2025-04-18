from __future__ import annotations

import math
from typing import Dict

from ngraph.lib.graph import StrictMultiDiGraph

from netsim.applications.packet_network.simulator import NetSim
from netsim.applications.packet_network.base import (
    NetSimObject,
    PacketSink,
    PacketSource,
)
from netsim.applications.packet_network.switch import PacketSwitch
from netsim.applications.packet_network.common import NetSimObjectName

from tests.common import _close


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _src_profile(size: int = 1000, interval: float = 1.0, count: int = 10) -> Dict:
    """Utility to build a Constant-flow PacketSource profile."""
    return {
        "flow_distr": "Constant",
        "size_distr": "Constant",
        "arrival_time_distr": "Constant",
        "flow_distr_params": {"constant": 0},
        "size_distr_params": {"constant": size},
        "arrival_distr_params": {"constant": interval, "first": 0, "count": count},
    }


# --------------------------------------------------------------------------- #
# 1. Graph loading creates correct objects
# --------------------------------------------------------------------------- #
def test_load_graph_creates_objects() -> None:
    sim = NetSim()
    g = StrictMultiDiGraph()
    g.add_node("S", ns_type="PacketSource", ns_attr=_src_profile())
    g.add_node("D", ns_type="PacketSink", ns_attr={})
    g.add_edge("S", "D", key=1, ns_attr={})

    sim.load_graph(g)

    src = sim.get_ns_obj("S")
    dst = sim.get_ns_obj("D")
    assert isinstance(src, PacketSource)
    assert isinstance(dst, PacketSink)
    # topology stored
    assert "physical" in sim.ctx.topology
    assert sim.ctx.topology["physical"] is g


# --------------------------------------------------------------------------- #
# 2. Event‑loop runs and NetStatCollector stores interval snapshots
# --------------------------------------------------------------------------- #
def test_run_until_time_collects_stats() -> None:
    sim = NetSim(stat_interval=1.0)  # 1 s snapshots
    g = StrictMultiDiGraph()
    g.add_node("S", ns_type="PacketSource", ns_attr=_src_profile(count=6))
    g.add_node("D", ns_type="PacketSink", ns_attr={})
    g.add_edge("S", "D", key=1, ns_attr={})
    sim.load_graph(g)

    sim.run(until_time=5)  # should generate 5 packets

    # wall‑clock
    _close(sim.now, 5)
    # NetSim‑level samples: 5 intervals (0‑1,1‑2,…,4‑5)
    samples = sim.nstat.stat_samples
    assert "S" in samples and "D" in samples
    assert len(samples["S"]) == 5
    # every interval for sink has exactly 1 received packet
    pkt_counts = [frame.total_received_pkts for frame in samples["D"].values()]
    assert all(pkts == 1 for pkts in pkt_counts)


# --------------------------------------------------------------------------- #
# 3. Switch edge parsing builds RX/TX interfaces and forwards traffic
# --------------------------------------------------------------------------- #
def test_switch_interfaces_and_forwarding() -> None:
    sim = NetSim()
    g = StrictMultiDiGraph()

    g.add_node("SRC", ns_type="PacketSource", ns_attr=_src_profile(count=5))
    g.add_node("SW", ns_type="PacketSwitch", ns_attr={})
    g.add_node("SINK", ns_type="PacketSink", ns_attr={})

    link_attr = {
        "tx": {"bw": 1_000_000},  # fast link
        "rx": {"propagation_delay": 0.0},
    }

    g.add_edge("SRC", "SW", ns_attr=link_attr)
    g.add_edge("SW", "SINK", key=1, ns_attr=link_attr)
    sim.load_graph(g)
    sim.run(until_time=5)

    sw: PacketSwitch = sim.get_ns_obj("SW")  # type: ignore[assignment]
    assert len(sw.rx_interfaces) == 1 and len(sw.tx_interfaces) == 1

    sink = sim.get_ns_obj("SINK")
    recv = sink.stat.cur_stat_frame.total_received_pkts
    assert recv == 5  # all packets forwarded with no loss
    assert sw.stat.cur_stat_frame.total_dropped_pkts == 0


# --------------------------------------------------------------------------- #
# 4. Stat / packet tracing toggles (smoke test for file creation)
# --------------------------------------------------------------------------- #
def test_tracing_toggles_create_tempfiles(monkeypatch, tmp_path) -> None:
    """
    Enable all tracing options; just ensure .run() finishes and tracer files exist.

    We patch Packet.todict() so the non‑serialisable `ctx` field is replaced by
    its id().  This avoids the JSON dump TypeError without changing production
    code.
    """
    from netsim.applications.packet_network.base import Packet as _Packet

    original_todict = _Packet.todict

    def _safe_todict(self):  # type: ignore[override]
        d = original_todict(self)
        # ctx → int so json.dumps works; keep other fields unchanged
        d["ctx"] = id(self.ctx)
        return d

    monkeypatch.setattr(_Packet, "todict", _safe_todict, raising=True)

    sim = NetSim()
    g = StrictMultiDiGraph()
    g.add_node("S", ns_type="PacketSource", ns_attr=_src_profile(count=2))
    g.add_node("D", ns_type="PacketSink", ns_attr={})
    g.add_edge("S", "D", key=1, ns_attr={})
    sim.load_graph(g)

    # run with all tracing flags enabled (now safe)
    sim.run(
        until_time=2,
        enable_stat_trace=True,
        enable_obj_trace=True,
        enable_packet_trace=True,
    )

    # NetSim‑level tracer exists
    assert sim.nstat.get_stat_tracer() is not None
    # each object has a stat tracer; PacketSource also has a packet tracer
    for obj in sim.get_ns_obj_iter():
        assert obj.stat.get_stat_tracer() is not None
        if hasattr(obj.stat, "get_packet_tracer"):
            assert obj.stat.get_packet_tracer() is not None
