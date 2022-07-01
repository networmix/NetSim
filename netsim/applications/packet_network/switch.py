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
from netsim.des.core import Coro
from netsim.des.common import SimTime

from netsim.applications.packet_network.stat import PacketSwitchStat
from netsim.applications.packet_network.base import (
    InterfaceBW,
    NetSimObjectName,
    Packet,
    PacketInterfaceRx,
    PacketInterfaceTx,
    PacketQueue,
    Receiver,
    Sender,
    SenderReceiver,
    NetSimContext,
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
        ctx: NetSimContext,
        processing_delay: Optional[Generator] = None,
        name: Optional[NetSimObjectName] = None,
    ):
        super().__init__(ctx, name=name)
        self._processing_delay: Optional[Generator] = processing_delay
        self.rx_interfaces: List[PacketInterfaceIn] = []
        self.tx_interfaces: List[PacketInterfaceOut] = []

    def add_interface_tx(self, tx_interface: PacketInterfaceOut) -> None:
        self.tx_interfaces.append(tx_interface)
        tx_interface.extend_name(f"{self.name}_", prepend=True)

    def add_interface_rx(self, rx_interface: PacketInterfaceIn) -> None:
        self.rx_interfaces.append(rx_interface)
        rx_interface.extend_name(f"{self.name}_", prepend=True)
        rx_interface.subscribe(self)

    def _process_packet(
        self, packet_processing_item: PacketProcessingInput
    ) -> Iterable[PacketProcessingOutput]:
        return self._flow_based_forwarding(packet_processing_item)

    def _flow_based_forwarding(
        self, packet_processing_item: PacketProcessingInput
    ) -> Iterable[PacketProcessingOutput]:
        packet, _ = packet_processing_item
        if self.tx_interfaces:
            tx_interface = self.tx_interfaces[
                int(packet.flow_id % len(self.tx_interfaces))
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

                if self._processing_delay:
                    yield self.process.timeout(self._processing_delay)

                packet_processing_items_out = self._process_packet(
                    packet_processing_item_in
                )
                if packet_processing_items_out:
                    for packet_processing_item_out in packet_processing_items_out:
                        packet_processing_item_out.interface.put(
                            packet_processing_item_out.packet, self
                        )

                        self._packet_sent(packet_processing_item_out.packet)
                else:
                    self._packet_dropped(packet)
                self._busy = False

            else:
                raise RuntimeError(
                    f"Resumed {self} without packet at {self.ctx.now}",
                )

    def put(
        self,
        item: Packet,
        source: Union[Sender, SenderReceiver],
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        self._packet_received(item)
        packet_processing_item = PacketProcessingInput(item, source)
        self._queue.put(packet_processing_item)
        self._packet_put(item)


class PacketSwitch(SenderReceiver):
    def __init__(self, ctx: NetSimContext, name: Optional[NetSimObjectName] = None):
        super().__init__(ctx, name=name)
        self.packet_processors: Dict[PacketProcessorName, PacketProcessor] = {}
        self.rx_interfaces: Dict[InterfaceName, PacketInterfaceRx] = {}
        self.tx_interfaces: Dict[InterfaceName, PacketInterfaceTx] = {}
        self.stat: PacketSwitchStat = PacketSwitchStat(ctx)
        self.process.add_stat_callback(self.stat.advance_time)

    def _init_stat(self):
        for processor_name, processor in self.packet_processors.items():
            self.stat._packet_processors[processor_name] = processor.stat

        for rx_interface_name, rx_interface in self.rx_interfaces.items():
            self.stat._rx_interfaces[rx_interface_name] = rx_interface.stat
            self.stat._rx_interface_queues[rx_interface_name] = rx_interface._queue.stat

        for tx_interface_name, tx_interface in self.tx_interfaces.items():
            self.stat._tx_interfaces[tx_interface_name] = tx_interface.stat
            self.stat._tx_interface_queues[tx_interface_name] = tx_interface._queue.stat
        self.stat.fill_stat()

    def create_interface_tx(
        self,
        interface_name: InterfaceName,
        bw: InterfaceBW,
        queue_len_limit: Optional[int] = None,
    ) -> PacketInterfaceTx:
        name = f"{self.name}_interface_TX_{interface_name}"
        tx_interface = PacketInterfaceTx(self.ctx, bw, queue_len_limit, name=name)
        self.tx_interfaces[interface_name] = tx_interface
        return tx_interface

    def create_interface_rx(
        self,
        interface_name: InterfaceName,
        propagation_delay: Optional[SimTime],
        transmission_len_limit: Optional[int] = None,
    ) -> PacketInterfaceRx:
        name = f"{self.name}_interface_RX_{interface_name}"
        rx_interface = PacketInterfaceRx(
            self.ctx, propagation_delay, transmission_len_limit, name=name
        )
        self.rx_interfaces[interface_name] = rx_interface
        return rx_interface

    def create_packet_processor(
        self,
        processor_name: Optional[PacketProcessorName] = None,
        processing_delay: Optional[SimTime] = None,
    ) -> PacketProcessor:
        if not processor_name:
            processor_name = "PacketProcessor"
        if processor_name not in self.packet_processors:
            packet_processor = PacketProcessor(
                self.ctx, processing_delay, name=processor_name
            )
            packet_processor.extend_name(f"{self.name}_", prepend=True)
            self.packet_processors[processor_name] = packet_processor

        for interface_name in sorted(self.rx_interfaces):
            packet_processor.add_interface_rx(self.rx_interfaces[interface_name])

        for interface_name in sorted(self.tx_interfaces):
            packet_processor.add_interface_tx(self.tx_interfaces[interface_name])

        self._init_stat()
        return packet_processor

    def subscribe(
        self, receiver: Receiver, *args: List[Any], **kwargs: Dict[str, Any]
    ) -> None:
        _, _ = args, kwargs
        raise NotImplementedError

    def run(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Coro:
        yield

    def put(
        self,
        item: Packet,
        source: Union[Sender, SenderReceiver],
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ):
        raise NotImplementedError
