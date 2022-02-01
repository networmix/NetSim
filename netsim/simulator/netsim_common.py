from enum import IntEnum
from typing import Union


PacketID = int
PacketSize = Union[int, float]  # in bytes
PacketAddress = int
PacketFlowID = int
NetSimObjectID = int
NetSimObjectName = str
InterfaceBW = float  # in bits per second
RatePPS = float  # in packets per second
RateBPS = float  # in bits per second


class PacketAction(IntEnum):
    RECEIVED = 1
    SENT = 2
    DROPPED = 3
