from __future__ import annotations

import logging
from copy import deepcopy
from typing import Any, Dict, Iterator, Optional, Type, Union

from ngraph.lib.graph import StrictMultiDiGraph

from netsim.common import SimTime, TimeInterval
from netsim.core import (
    Event,
    Simulator,
    StatCollector,
)

from netsim.applications.packet_network.common import NetSimObjectName
from netsim.applications.packet_network.stat import NetSimStat
from netsim.applications.packet_network.switch import PacketSwitch
from netsim.applications.packet_network.base import (
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
    NetSimContext,
)


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
    """
    Specialized StatCollector for NetSim, collecting stats from each NetSimObject at specified intervals.
    """

    def __init__(
        self,
        ctx: NetSimContext,
        nsim: NetSim,
        stat_interval: SimTime,
        stat_container: NetSimStat,
    ):
        super().__init__(ctx, stat_interval, stat_container)
        self._nsim = nsim
        self._stat_container: NetSimStat = self._stat_container

    def _collect(self, reset: bool) -> Event:
        """
        Override StatCollector's _collect method to handle NetSim's objects specifically.
        """
        self._stat_container.advance_time()
        stat_samples: Dict[NetSimObjectName, Dict[str, Union[int, float]]] = {}

        for ns_obj in self._nsim.get_ns_obj_iter():
            ns_obj.stat.update_stat()
            interval: TimeInterval = (
                self._stat_container.prev_timestamp,
                self._stat_container.cur_timestamp,
            )
            cur_stat_frame_copy = deepcopy(ns_obj.stat.cur_stat_frame)
            self._stat_container.stat_samples.setdefault(ns_obj.name, {})[
                interval
            ] = cur_stat_frame_copy
            stat_samples[ns_obj.name] = cur_stat_frame_copy.todict()
            if reset:
                ns_obj.stat.reset_stat()

        if self._stat_container.get_stat_tracer() and stat_samples:
            self._stat_container.dump_stat_samples({str(interval): stat_samples})


class NetSim(Simulator):
    """
    Specialized simulator for building and running network simulations.
    """

    def __init__(
        self,
        ctx: Optional[NetSimContext] = None,
        stat_interval: Optional[float] = None,
    ):
        self.ctx: NetSimContext = ctx if ctx is not None else NetSimContext()
        super().__init__(self.ctx, stat_interval=stat_interval)
        Packet.reset_packet_id()
        self._ns: Dict[NetSimObjectName, NetSimObject] = {}
        self.nstat: NetSimStat = NetSimStat(self.ctx)

        self.add_stat_collector(
            NetStatCollector(
                self.ctx,
                nsim=self,
                stat_interval=stat_interval,
                stat_container=self.nstat,
            )
        )

    def get_ns_obj(self, ns_obj_name: NetSimObjectName) -> NetSimObject:
        """
        Retrieve a specific NetSimObject by name.
        """
        return self._ns[ns_obj_name]

    def get_ns_obj_iter(self) -> Iterator[NetSimObject]:
        """
        Iterator over all NetSimObjects in the simulation.
        """
        return iter(self._ns.values())

    def enable_stat_trace(self) -> None:
        """
        Enable JSON line tracing for the overall NetSim statistics (e.g., NetSimStat).
        """
        self.nstat.enable_stat_trace(prefix="NetSim")

    def enable_obj_trace(self) -> None:
        """
        Enable JSON line tracing for each object's own statistics.
        """
        for ns_obj in self._ns.values():
            ns_obj.stat.enable_stat_trace(prefix=ns_obj.name)

    def enable_packet_trace(self) -> None:
        """
        Enable JSON line tracing for each object's packets.
        """
        for ns_obj in self._ns.values():
            if getattr(ns_obj.stat, "enable_packet_trace", None):
                ns_obj.stat.enable_packet_trace(prefix=ns_obj.name)

    def _parse_graph_nodes(self, graph: StrictMultiDiGraph) -> None:
        """
        Parse the nodes of the input graph, creating NetSimObjects accordingly.
        """
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

    def _parse_graph_edges(self, graph: StrictMultiDiGraph) -> None:
        """
        Parse edges of the input graph, hooking up subscriptions or creating interfaces for PacketSwitch objects.
        """
        for src_node, dst_node, edge_id, edge_attr in graph.get_edges().values():
            src_ns_obj: Union[Sender, SenderReceiver] = self._ns[src_node]
            dst_ns_obj: Union[Receiver, SenderReceiver] = self._ns[dst_node]
            ns_edge_attr: Dict[str, Any] = edge_attr["ns_attr"]
            ns_edge_attr_rx: Dict[str, Any] = ns_edge_attr.get("rx", {})
            ns_edge_attr_tx: Dict[str, Any] = ns_edge_attr.get("tx", {})

            if isinstance(src_ns_obj, PacketSwitch):
                src_ns_obj = src_ns_obj.create_interface_tx(
                    f"{src_node}:{dst_node}:{edge_id}", **ns_edge_attr_tx
                )

            if isinstance(dst_ns_obj, PacketSwitch):
                dst_ns_obj = dst_ns_obj.create_interface_rx(
                    f"{src_node}:{dst_node}:{edge_id}", **ns_edge_attr_rx
                )

            src_ns_obj.subscribe(dst_ns_obj)

    def _postprocess_ns_obj(self) -> None:
        """
        After creating and linking all NetSimObjects, some (like PacketSwitch) need a final setup step.
        """
        for ns_node_obj in self._ns.values():
            if isinstance(ns_node_obj, PacketSwitch):
                ns_node_obj.create_packet_processor()

    def load_graph(self, graph: StrictMultiDiGraph) -> None:
        """
        Load a topology graph into the simulation, creating NetSimObjects and hooking them up.
        """
        self._parse_graph_nodes(graph)
        self._parse_graph_edges(graph)
        self._postprocess_ns_obj()
        self.ctx.add_topology("physical", graph)

    def run(
        self,
        until_time: Optional[SimTime] = None,
        enable_stat_trace: bool = False,
        enable_obj_trace: bool = False,
        enable_packet_trace: bool = False,
    ) -> None:
        """
        Run the simulation until a specified time or until no more events are available.

        Args:
            until_time: If provided, schedule a StopSim event at this time.
            enable_stat_trace: Whether to enable continuous JSON-line stats tracing for the NetSim container.
            enable_obj_trace: Whether to enable continuous JSON-line stats tracing for each object.
            enable_packet_trace: Whether to enable continuous JSON-line stats tracing for packet events.
        """
        if enable_stat_trace:
            self.enable_stat_trace()
        if enable_obj_trace:
            self.enable_obj_trace()
        if enable_packet_trace:
            self.enable_packet_trace()
        super().run(until_time=until_time)
