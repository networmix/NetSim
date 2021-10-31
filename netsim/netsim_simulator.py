from __future__ import annotations

import logging
from copy import deepcopy
from typing import Any, Dict, Iterator, Optional, Type, Union
from netsim.netsim_common import NetSimObjectName
from netsim.netsim_stat import NetSimStat
from netsim.netsim_switch import PacketSwitch
from netsim.sim_common import SimTime, TimeInterval

from netsim.simcore import (
    Event,
    SimContext,
    Simulator,
    StatCollector,
)
from netsim.netsim_base import (
    Packet,
    PacketInterfaceRx,
    PacketInterfaceTx,
    PacketSource,
    PacketSink,
    PacketQueue,
    NetSimObject,
    PacketSourceProfile,
    Receiver,
    Sender,
    SenderReceiver,
)
from netsim.netgraph.graph import MultiDiGraph


LOG_FMT = "%(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
logger = logging.getLogger(__name__)


NetSimID = int
NS_TYPE_MAP: Dict[str, Type[NetSimObject]] = {
    "PacketSource": PacketSource,
    "PacketSink": PacketSink,
    "PacketQueue": PacketQueue,
    "PacketInterfaceRx": PacketInterfaceRx,
    "PacketInterfaceTx": PacketInterfaceTx,
    "PacketSwitch": PacketSwitch,
}


class NetStatCollector(StatCollector):
    def __init__(
        self,
        ctx: SimContext,
        nsim: NetSim,
        stat_interval: SimTime,
        stat_container: NetSimStat,
    ):
        super().__init__(ctx, stat_interval, stat_container)
        self._nsim = nsim
        self._stat_container: NetSimStat = self._stat_container

    def _collect(self, reset: bool) -> Event:
        self._stat_container.advance_time()
        for ns_obj in self._nsim.get_ns_obj_iter():
            ns_obj.stat.update_stat()
            interval: TimeInterval = (
                self._stat_container.prev_timestamp,
                self._stat_container.cur_timestamp,
            )

            self._stat_container.stat_samples.setdefault(ns_obj.name, {})[
                interval
            ] = deepcopy(ns_obj.stat.cur_stat_frame)
            if reset:
                ns_obj.stat.reset_stat()


class NetSim(Simulator):
    def __init__(
        self,
        ctx: Optional[SimContext] = None,
        stat_interval: Optional[float] = None,
    ):
        super().__init__(ctx, stat_interval=stat_interval)
        Packet.reset_packet_id()
        self._ns: Dict[NetSimObjectName, NetSimObject] = {}
        self.nstat: NetSimStat = NetSimStat(self._ctx)
        self.add_stat_collector(
            NetStatCollector(
                self._ctx,
                nsim=self,
                stat_interval=stat_interval,
                stat_container=self.nstat,
            )
        )

    def get_ns_obj(self, ns_obj_name: NetSimObjectName) -> NetSimObject:
        return self._ns[ns_obj_name]

    def get_ns_obj_iter(self) -> Iterator[NetSimObject]:
        return iter(self._ns.values())

    def enable_stat_trace(self) -> None:
        for ns_obj in self._ns.values():
            ns_obj.stat.enable_stat_trace(prefix=ns_obj.name)

    def _parse_graph_nodes(self, graph: MultiDiGraph) -> None:
        for node, node_attr in graph.get_nodes().items():
            ns_type = NS_TYPE_MAP[node_attr["ns_type"]]

            if issubclass(ns_type, PacketSource):
                profile = PacketSourceProfile.from_dict(node_attr["ns_attr"])
                ns_node_obj: PacketSource = ns_type.create(
                    self.ctx, name=node, profile=profile
                )

            else:
                ns_attr = node_attr["ns_attr"]
                ns_node_obj: NetSimObject = ns_type(self.ctx, name=node, **ns_attr)
            self._ns[node] = ns_node_obj

    def _parse_graph_edges(self, graph: MultiDiGraph) -> None:
        for src_node, dst_node, edge_id, edge_attr in graph.get_edges().values():
            src_ns_obj: Union[Sender, SenderReceiver] = self._ns[src_node]
            dst_ns_obj: Union[Receiver, SenderReceiver] = self._ns[dst_node]
            ns_edge_attr: Dict[str, Any] = edge_attr["ns_attr"]
            ns_edge_attr_rx: Dict[str, Any] = ns_edge_attr.get("rx", {})
            ns_edge_attr_tx: Dict[str, Any] = ns_edge_attr.get("tx", {})

            if isinstance(src_ns_obj, PacketSwitch):
                src_ns_obj = src_ns_obj.create_interface_tx(
                    f"to_{dst_node}", **ns_edge_attr_tx
                )

            if isinstance(dst_ns_obj, PacketSwitch):
                dst_ns_obj = dst_ns_obj.create_interface_rx(
                    f"from_{src_node}", **ns_edge_attr_rx
                )

            src_ns_obj.subscribe(dst_ns_obj)

    def _postprocess_ns_obj(self) -> None:
        for ns_node_obj in self._ns.values():
            if isinstance(ns_node_obj, PacketSwitch):
                ns_node_obj.create_packet_processor()

    def load_graph(self, graph: MultiDiGraph) -> None:
        self._parse_graph_nodes(graph)
        self._parse_graph_edges(graph)
        self._postprocess_ns_obj()
