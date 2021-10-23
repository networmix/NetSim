from __future__ import annotations

import logging
from copy import deepcopy
from typing import Any, Dict, Iterable, Iterator, Optional, Union
from netsim.netsim_switch import PacketSwitch

from netsim.simcore import (
    CollectStat,
    Event,
    Process,
    SimContext,
    Simulator,
    SimTime,
    StatCollector,
    TimeInterval,
)
from netsim.netsim_base import (
    NetSimStat,
    Packet,
    PacketInterfaceRx,
    PacketInterfaceTx,
    PacketSource,
    PacketSink,
    PacketQueue,
    NetSimObject,
    NetSimObjectName,
    Receiver,
    Sender,
    SenderReceiver,
)
from netsim.netgraph.graph import MultiDiGraph


LOG_FMT = "%(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
logger = logging.getLogger(__name__)


NetSimID = int
NS_TYPE_MAP: Dict[str, NetSimObject] = {
    "PacketSource": PacketSource,
    "PacketSink": PacketSink,
    "PacketQueue": PacketQueue,
    "PacketInterfaceRx": PacketInterfaceRx,
    "PacketInterfaceTx": PacketInterfaceTx,
    "PacketSwitch": PacketSwitch,
}


class NetStatCollector(Process):
    def __init__(
        self,
        ctx: SimContext,
        nsim: NetSim,
        stat_interval: SimTime,
        stat_container: NetSimStat,
    ):
        self._stat_interval = stat_interval
        self._stat_container = stat_container
        self._nsim = nsim
        super().__init__(ctx, self._collection_trigger())

    def _collection_trigger(self) -> Event:
        while True:
            yield CollectStat(
                self._ctx, stat_interval=self._stat_interval, process=self
            )
            self._stat_container.update_stat()
            for ns_obj in self._nsim.get_ns_obj_iter():
                ns_obj.stat.update_stat()
                interval: TimeInterval = (
                    self._stat_container.prev_timestamp,
                    self._stat_container.cur_timestamp,
                )

                self._stat_container.stat_samples.setdefault(ns_obj.name, {})[
                    interval
                ] = deepcopy(ns_obj.stat)
                ns_obj.stat.reset_stat()


class NetSim(Simulator):
    def __init__(
        self,
        ctx: Optional[SimContext] = None,
        stat_interval: Optional[float] = None,
        nstat_interval: Optional[float] = None,
    ):
        super().__init__(ctx, stat_interval=stat_interval)
        Packet.reset_packet_id()
        self._ns: Dict[NetSimObjectName, NetSimObject] = {}
        self.nstat: NetSimStat = NetSimStat(self._ctx)
        self._nstat_collector = (
            NetStatCollector(
                self._ctx,
                nsim=self,
                stat_interval=nstat_interval,
                stat_container=self.nstat,
            )
            if nstat_interval
            else None
        )

    def get_ns_obj(self, ns_obj_name: NetSimObjectName) -> NetSimObject:
        return self._ns[ns_obj_name]

    def get_ns_obj_iter(self) -> Iterator[NetSimObject]:
        return iter(self._ns.values())

    def load_graph(self, graph: MultiDiGraph):
        for node, node_attr in graph.get_nodes().items():
            ns_type = NS_TYPE_MAP[node_attr["ns_type"]]
            ns_attr = node_attr["ns_attr"]
            ns_node_obj: NetSimObject = ns_type(self.ctx, **ns_attr)
            ns_node_obj.name = node
            self._ns[node] = ns_node_obj

        for src_node, dst_node, edge_id, edge_attr in graph.get_edges().values():
            src_ns_obj: Union[Sender, SenderReceiver] = self._ns[src_node]
            dst_ns_obj: Union[Receiver, SenderReceiver] = self._ns[dst_node]
            ns_edge_attr: Dict[str, Any] = edge_attr["ns_attr"]
            ns_edge_attr_rx: Dict[str, Any] = ns_edge_attr.get("rx", {})
            ns_edge_attr_tx: Dict[str, Any] = ns_edge_attr.get("tx", {})

            if isinstance(src_ns_obj, PacketSwitch):
                src_ns_obj = src_ns_obj.create_interface_tx(
                    f"{dst_node}%{edge_id}", **ns_edge_attr_tx
                )

            if isinstance(dst_ns_obj, PacketSwitch):
                dst_ns_obj = dst_ns_obj.create_interface_rx(
                    f"{src_node}%{edge_id}", **ns_edge_attr_rx
                )

            src_ns_obj.subscribe(dst_ns_obj)

        for ns_node_obj in self._ns.values():
            if isinstance(ns_node_obj, PacketSwitch):
                ns_node_obj.create_packet_processor()

    def run(self, until_time: Optional[SimTime] = None) -> None:
        super().run(until_time)
