from __future__ import annotations

import logging
from typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Union,
)

from netsim.simcore import Coro, SimTime, SimContext
from netsim.netsim_base import (
    InterfaceBW,
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


InterfaceID = int
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

    def add_interface_tx(self, tx_interface: PacketInterfaceOut) -> None:
        self._tx_interfaces.append(tx_interface)

    def add_interface_rx(self, rx_interface: PacketInterfaceIn) -> None:
        self._rx_interfaces.append(rx_interface)
        rx_interface.subscribe(self)

    def _process_packet(
        self, packet_processing_item: PacketProcessingInput
    ) -> Iterable[PacketProcessingOutput]:
        return self._flow_based_forwarding(packet_processing_item)

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

    def subscribe(
        self, receiver: Receiver, *args: List[Any], **kwargs: Dict[str, Any]
    ) -> None:
        _, _ = args, kwargs
        raise NotImplementedError

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

                packet_processing_items_out = self._process_packet(
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
        source: Union[Sender, SenderReceiver] = None,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        self._packet_received(item)
        packet_processing_item = PacketProcessingInput(item, source)
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
        self._packet_processors: Dict[PacketProcessorName, PacketProcessor] = {}
        self._rx_interfaces: Dict[InterfaceID, PacketInterfaceRx] = {}
        self._tx_interfaces: Dict[InterfaceID, PacketInterfaceTx] = {}
        self._source_to_rx_map: Dict[
            Union[Sender, SenderReceiver], PacketInterfaceRx
        ] = {}

    def create_interface_tx(
        self,
        interface_id: InterfaceID,
        bw: InterfaceBW,
        queue_len_limit: Optional[int] = None,
    ) -> PacketInterfaceTx:
        tx_interface = PacketInterfaceTx(self._ctx, bw, queue_len_limit)
        self._tx_interfaces[interface_id] = tx_interface
        return tx_interface

    def create_interface_rx(
        self,
        interface_id: InterfaceID,
        propagation_delay: Optional[SimTime],
        transmission_len_limit: Optional[int] = None,
    ) -> PacketInterfaceRx:
        rx_interface = PacketInterfaceRx(
            self._ctx, propagation_delay, transmission_len_limit
        )
        self._rx_interfaces[interface_id] = rx_interface
        return rx_interface

    def create_packet_processor(
        self,
        processor_id: Optional[int] = 0,
        processing_delay: Optional[SimTime] = None,
    ) -> PacketProcessor:
        packet_processor = PacketProcessor(self._ctx, processing_delay)
        self._packet_processors[processor_id] = packet_processor

        for interface_id in sorted(self._rx_interfaces):
            packet_processor.add_interface_rx(self._rx_interfaces[interface_id])

        for interface_id in sorted(self._tx_interfaces):
            packet_processor.add_interface_tx(self._tx_interfaces[interface_id])

        self._create_subscription_map()

        return packet_processor

    def subscribe(
        self, receiver: Receiver, *args: List[Any], **kwargs: Dict[str, Any]
    ) -> None:
        _, _ = args, kwargs
        raise NotImplementedError

    def _create_subscription_map(self) -> None:
        for rx_interface in self._rx_interfaces.values():
            subscriptions = rx_interface.get_subscriptions()
            for subscription in subscriptions:
                self._source_to_rx_map[subscription] = rx_interface

    def run(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Coro:
        while True:
            yield self._process.noop()

    def put(
        self,
        item: Packet,
        source: Optional[Union[Sender, SenderReceiver]] = None,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        raise NotImplementedError
