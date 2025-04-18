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

from netsim.core import Coro
from netsim.common import SimTime

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
    """
    Represents the input to a PacketProcessor.

    Attributes:
        packet: The Packet being processed.
        interface: The incoming interface or sender from which the packet arrived.
    """

    packet: Packet
    interface: PacketInterfaceIn


class PacketProcessingOutput(NamedTuple):
    """
    Represents the output from a PacketProcessor.

    Attributes:
        packet: The Packet after processing.
        interface: The outgoing interface or receiver to which the packet should be forwarded.
    """

    packet: Packet
    interface: PacketInterfaceOut


class PacketProcessor(PacketQueue):
    """
    A specialized queue that processes packets and forwards them to one of several possible output interfaces.

    Inherits from PacketQueue, but instead of just storing and forwarding the raw
    Packet objects, it expects and produces PacketProcessingInput/PacketProcessingOutput tuples.
    """

    def __init__(
        self,
        ctx: NetSimContext,
        processing_delay: Optional[Generator[float, None, None]] = None,
        name: Optional[NetSimObjectName] = None,
    ) -> None:
        """
        Initialize a PacketProcessor.

        Args:
            ctx: Simulation context.
            processing_delay: A generator yielding processing delays (in time units). Optional.
            name: Optional name override.
        """
        super().__init__(ctx, name=name)
        self._processing_delay: Optional[Generator[float, None, None]] = (
            processing_delay
        )
        self.rx_interfaces: List[PacketInterfaceIn] = []
        self.tx_interfaces: List[PacketInterfaceOut] = []
        self._busy: bool = (
            False  # Track whether the processor is currently processing a packet
        )

    def add_interface_tx(self, tx_interface: PacketInterfaceOut) -> None:
        """
        Attach an outgoing (TX) interface to this processor.

        Args:
            tx_interface: The interface to add.
        """
        self.tx_interfaces.append(tx_interface)
        tx_interface.extend_name(f"{self.name}_", prepend=True)

    def add_interface_rx(self, rx_interface: PacketInterfaceIn) -> None:
        """
        Attach an incoming (RX) interface to this processor.

        Args:
            rx_interface: The interface to add.
        """
        self.rx_interfaces.append(rx_interface)
        rx_interface.extend_name(f"{self.name}_", prepend=True)
        rx_interface.subscribe(self)

    def _process_packet(
        self, packet_processing_item: PacketProcessingInput
    ) -> Iterable[PacketProcessingOutput]:
        """
        Stub function that can be overridden to implement more complex forwarding logic.

        Args:
            packet_processing_item: The packet and the incoming interface.

        Returns:
            An iterable of PacketProcessingOutput specifying how to forward the packet.
        """
        return self._flow_based_forwarding(packet_processing_item)

    def _flow_based_forwarding(
        self, packet_processing_item: PacketProcessingInput
    ) -> Iterable[PacketProcessingOutput]:
        """
        Default flow-based forwarding logic. Chooses the output interface by hashing the packet's flow_id.

        Args:
            packet_processing_item: The packet and the incoming interface.

        Returns:
            A list (iterable) of PacketProcessingOutput, generally containing exactly one output if any TX interfaces exist.
        """
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
        """
        PacketProcessor does not directly subscribe to a receiver. Instead, it uses add_interface_tx.

        Raises:
            NotImplementedError: Always, because PacketProcessor is designed to have TX interfaces attached, not direct subscriptions.
        """
        _, _ = args, kwargs
        raise NotImplementedError(
            "PacketProcessor should use add_interface_tx() to attach outgoing interfaces."
        )

    def run(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Coro:
        """
        Main coroutine for the packet processor. Receives a PacketProcessingInput from the queue, optionally
        waits for a processing delay, then pushes to the relevant output interface(s).

        Yields:
            A timeout for processing_delay if specified.
        """
        while True:
            packet_processing_item_in: PacketProcessingInput = yield self._queue.get()
            if packet_processing_item_in:
                packet, _ = packet_processing_item_in
                self._busy = True
                self._packet_get(packet)

                if self._processing_delay:
                    delay_val = next(self._processing_delay, None)
                    if delay_val is not None:
                        yield self.process.timeout(delay_val)

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
                    # No output interface decided -> drop
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
    ) -> None:
        """
        Upstream senders (RX interfaces or other nodes) call put() to deliver a packet to this processor.
        We wrap the packet in a PacketProcessingInput and queue it.

        Args:
            item: The packet to process.
            source: The node or interface that delivered the packet.
        """
        self._packet_received(item)
        packet_processing_item = PacketProcessingInput(item, source)
        self._queue.put(packet_processing_item)
        self._packet_put(item)


class PacketSwitch(SenderReceiver):
    """
    Represents a network switch that can have multiple RX/TX interfaces and an internal PacketProcessor.
    """

    def __init__(
        self, ctx: NetSimContext, name: Optional[NetSimObjectName] = None
    ) -> None:
        """
        Initialize a PacketSwitch.

        Args:
            ctx: Simulation context.
            name: Optional name override.
        """
        super().__init__(ctx, name=name)
        self.packet_processors: Dict[PacketProcessorName, PacketProcessor] = {}
        self.rx_interfaces: Dict[InterfaceName, PacketInterfaceRx] = {}
        self.tx_interfaces: Dict[InterfaceName, PacketInterfaceTx] = {}
        self.stat: PacketSwitchStat = PacketSwitchStat(ctx)
        self.process.add_stat_callback(self.stat.advance_time)

    def _init_stat(self) -> None:
        """
        After constructing the switch and its sub-objects,
        register them in this switch's PacketSwitchStat for aggregation.
        """
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
        """
        Build a TX interface with the specified bandwidth and optional queue length limit.

        Args:
            interface_name: A unique name to identify this TX interface.
            bw: Bandwidth in bits/sec (or bytes/sec, depending on convention).
            queue_len_limit: Maximum queue length (tail-drop). If None, unlimited.

        Returns:
            The created PacketInterfaceTx instance.
        """
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
        """
        Build an RX interface with optional propagation delay and queue length limit.

        Args:
            interface_name: A unique name to identify this RX interface.
            propagation_delay: Delay applied to each packet as it leaves this RX queue.
            transmission_len_limit: Max queue length for this RX interface. If None, unlimited.

        Returns:
            The created PacketInterfaceRx instance.
        """
        name = f"{self.name}_interface_RX_{interface_name}"
        rx_interface = PacketInterfaceRx(
            self.ctx, propagation_delay, transmission_len_limit, name=name
        )
        self.rx_interfaces[interface_name] = rx_interface
        return rx_interface

    def create_packet_processor(
        self,
        processor_name: Optional[PacketProcessorName] = None,
        processing_delay: Optional[Generator[float, None, None]] = None,
    ) -> PacketProcessor:
        """
        Build a PacketProcessor that connects all current rx_interfaces to its input
        and all current tx_interfaces to its output.

        Args:
            processor_name: An optional name for the processor. Defaults to 'PacketProcessor'.
            processing_delay: Optional generator yielding processing delays for each packet.

        Returns:
            The created or existing PacketProcessor instance, after linking to all known interfaces.
        """
        if not processor_name:
            processor_name = "PacketProcessor"

        if processor_name not in self.packet_processors:
            packet_processor = PacketProcessor(
                self.ctx, processing_delay, name=processor_name
            )
            packet_processor.extend_name(f"{self.name}_", prepend=True)
            self.packet_processors[processor_name] = packet_processor

        for interface_name in sorted(self.rx_interfaces):
            self.packet_processors[processor_name].add_interface_rx(
                self.rx_interfaces[interface_name]
            )

        for interface_name in sorted(self.tx_interfaces):
            self.packet_processors[processor_name].add_interface_tx(
                self.tx_interfaces[interface_name]
            )

        self._init_stat()
        return self.packet_processors[processor_name]

    def subscribe(
        self, receiver: Receiver, *args: List[Any], **kwargs: Dict[str, Any]
    ) -> None:
        """
        PacketSwitch doesn't directly subscribe to a receiver. Instead, it uses interfaces/packet processors.

        Raises:
            NotImplementedError: Always, because PacketSwitch is designed to use separate interfaces.
        """
        _, _ = args, kwargs
        raise NotImplementedError(
            "PacketSwitch should use interfaces and packet processor to connect with receivers."
        )

    def run(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Coro:
        """
        PacketSwitch has no main loop to process; the real logic is in its interfaces and packet processors.

        Yields:
            None (runs indefinitely, but effectively does nothing).
        """
        yield

    def put(
        self,
        item: Packet,
        source: Union[Sender, SenderReceiver],
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ) -> None:
        """
        PacketSwitch doesn't directly handle packet arrival; that's handled by rx_interfaces.

        Raises:
            NotImplementedError: Always, because PacketSwitch receives packets only via its rx_interfaces.
        """
        raise NotImplementedError(
            "PacketSwitch receives packets via rx_interfaces, not direct put()."
        )
