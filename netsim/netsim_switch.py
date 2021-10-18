from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from netsim.simcore import Simulator, SimTime, SimContext
from netsim.netsim_base import (
    Packet,
    PacketSource,
    PacketSink,
    QueueFIFO,
    NetSimObject,
    NetSimObjectName,
    SenderReceiver,
)


# InterfaceID = int


# class PacketSwitch(SenderReceiver):
#     def __init__(self, ctx: SimContext):
#         super().__init__(ctx)
#         self._ports: Dict[InterfaceID, PacketInterface] = {}
