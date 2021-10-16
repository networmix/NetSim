from __future__ import annotations

import logging
from typing import Dict, Optional

from netsim.simcore import Simulator, SimTime
from netsim.netsim_base import (
    Packet,
    PacketSource,
    PacketSink,
    QueueFIFO,
    NetSimObject,
    NetSimObjectName,
)
from netsim.netgraph.graph import MultiDiGraph


LOG_FMT = "%(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
logger = logging.getLogger(__name__)


NetSimID = int
NS_TYPE_MAP: Dict[str, NetSimObject] = {
    "PacketSource": PacketSource,
    "PacketSink": PacketSink,
    "QueueFIFO": QueueFIFO,
}


class NetSim(Simulator):
    def __init__(self):
        super().__init__()
        Packet.reset_packet_id()
        self._ns: Dict[NetSimObjectName, NetSimObject] = {}

    def load_graph(self, graph: MultiDiGraph):
        for node, node_attr in graph.get_nodes().items():
            ns_type = NS_TYPE_MAP[node_attr["ns_type"]]
            ns_attr = node_attr["ns_attr"]
            self._ns[node] = ns_type(self.ctx, **ns_attr)

    def run(self, until_time: Optional[SimTime] = None) -> None:
        self._run(until_time)
