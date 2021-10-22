from __future__ import annotations

import logging
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Union,
)

from netsim.simcore import Coro, Simulator, SimTime, SimContext
from netsim.netsim_base import (
    Packet,
    PacketInterfaceRx,
    PacketInterfaceTx,
    PacketQueue,
    Receiver,
    Sender,
    SenderReceiver,
)

LOG_FMT = "%(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
logger = logging.getLogger(__name__)


InterfaceName = str
PacketProcessorName = str
PacketInterfaceIn = Union[PacketInterfaceRx, Sender]
PacketInterfaceOut = Union[PacketInterfaceTx, SenderReceiver, Receiver]


class PacketProcessingInput(NamedTuple):
    packet: Packet
    interface: PacketInterfaceIn


class PacketProcessingOutput(NamedTuple):
    packet: Packet
    interface: PacketInterfaceOut


class PacketProcessor(PacketQueue):
    def __init__(
        self,
        ctx: SimContext,
        processing_delay: Optional[Generator] = None,
    ):
        super().__init__(ctx)
        self._processing_delay: Optional[Generator] = processing_delay
        self._rx_interfaces: List[PacketInterfaceIn] = []
        self._tx_interfaces: List[PacketInterfaceOut] = []

    def add_tx_interface(self, tx_interface: PacketInterfaceOut) -> None:
        self._tx_interfaces.append(tx_interface)

    def add_rx_interface(self, rx_interface: PacketInterfaceIn) -> None:
        self._rx_interfaces.append(rx_interface)

    def process_packet(self, packet: Packet, rx_interface_name: InterfaceName) -> None:
        pass

    def _flow_based_forwarding(
        self, packet_processing_item: PacketProcessingInput
    ) -> Iterable[PacketProcessingOutput]:
        packet, _ = packet_processing_item
        if self._tx_interfaces:
            tx_interface = self._tx_interfaces[
                packet.flow_id % len(self._tx_interfaces)
            ]
            return [PacketProcessingOutput(packet, tx_interface)]
        return []

    def run(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Coro:
        while True:
            packet_processing_item_in: PacketProcessingInput = yield self._queue.get()
            if packet_processing_item_in:
                packet, _ = packet_processing_item_in
                self._busy = True
                self._packet_get(packet)
                logger.debug(
                    "Packet processing started by %s_%s at %s",
                    type(self).__name__,
                    self._process.proc_id,
                    self._ctx.now,
                )
                if self._processing_delay:
                    yield self._process.timeout(self._processing_delay)

                packet_processing_items_out = self._flow_based_forwarding(
                    packet_processing_item_in
                )
                if packet_processing_items_out:
                    for packet_processing_item_out in packet_processing_items_out:
                        packet_processing_item_out.interface.put(
                            packet_processing_item_out.packet, self
                        )
                        logger.debug(
                            "Packet put into %s_%s by %s_%s at %s",
                            type(self).__name__,
                            self._process.proc_id,
                            type(packet_processing_item_out.interface).__name__,
                            packet_processing_item_out.interface._process.proc_id,
                            self._ctx.now,
                        )
                        self._packet_sent(packet_processing_item_out.packet)
                else:
                    self._packet_dropped(packet)
                self._busy = False
                logger.debug(
                    "Packet processing finished by %s_%s at %s",
                    type(self).__name__,
                    self._process.proc_id,
                    self._ctx.now,
                )
            else:
                raise RuntimeError(
                    f"Resumed {type(self).__name__}_{self._process.proc_id} without packet at {self._ctx.now}",
                )

    def put(
        self,
        item: Packet,
        source_obj: Optional[Sender] = None,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        self._packet_received(item)
        packet_processing_item = PacketProcessingInput(item, source_obj)
        self._queue.put(packet_processing_item)
        self._packet_put(item)
        logger.debug(
            "Packet received by %s_%s at %s",
            type(self).__name__,
            self._process.proc_id,
            self._ctx.now,
        )


class PacketSwitch(SenderReceiver):
    def __init__(self, ctx: SimContext):
        super().__init__(ctx)
        self._packet_processors: Dict[PacketProcessorName, PacketProcessor]
        self._rx_interfaces: Dict[InterfaceName, PacketInterfaceRx] = {}
        self._tx_interfaces: Dict[InterfaceName, PacketInterfaceTx] = {}

    def process_packet(self, packet: Packet, rx_interface_name: InterfaceName) -> None:
        pass
