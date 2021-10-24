from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass, field

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

from netsim.simcore import Coro, SimTime, SimContext, Stat
from netsim.netsim_base import (
    InterfaceBW,
    NetSimObjectName,
    Packet,
    PacketInterfaceRx,
    PacketInterfaceTx,
    PacketQueue,
    PacketQueueStat,
    PacketSize,
    PacketStat,
    RateBPS,
    RatePPS,
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


@dataclass
class PacketSwitchStat(Stat):
    prev_timestamp: SimTime = 0
    cur_timestamp: SimTime = 0

    rx_interfaces: Dict[InterfaceName, PacketStat] = field(default_factory=dict)
    tx_interfaces: Dict[InterfaceName, PacketStat] = field(default_factory=dict)
    rx_interface_queues: Dict[InterfaceName, PacketQueueStat] = field(
        default_factory=dict
    )
    tx_interface_queues: Dict[InterfaceName, PacketQueueStat] = field(
        default_factory=dict
    )
    packet_processors: Dict[PacketProcessorName, PacketQueueStat] = field(
        default_factory=dict
    )

    total_sent_pkts: int = 0
    total_received_pkts: int = 0
    total_dropped_pkts: int = 0
    total_sent_bytes: PacketSize = 0
    total_received_bytes: PacketSize = 0
    total_dropped_bytes: PacketSize = 0

    avg_send_rate_pps: RatePPS = 0
    avg_receive_rate_pps: RatePPS = 0
    avg_drop_rate_pps: RatePPS = 0
    avg_send_rate_bps: RateBPS = 0
    avg_receive_rate_bps: RateBPS = 0
    avg_drop_rate_bps: RateBPS = 0

    def _update_timestamp(self) -> None:
        if self._ctx.now != self.cur_timestamp:
            self.prev_timestamp, self.cur_timestamp = self.cur_timestamp, self._ctx.now

    def _update_total(self) -> None:
        self.total_sent_pkts: int = 0
        self.total_received_pkts: int = 0
        self.total_dropped_pkts: int = 0
        self.total_sent_bytes: PacketSize = 0
        self.total_received_bytes: PacketSize = 0
        self.total_dropped_bytes: PacketSize = 0
        for rx_interface in self.rx_interfaces.values():
            self.total_received_pkts += rx_interface.total_received_pkts
            self.total_received_bytes += rx_interface.total_received_bytes
            self.total_dropped_pkts += rx_interface.total_dropped_pkts
            self.total_dropped_bytes += rx_interface.total_dropped_bytes

        for tx_interface in self.tx_interfaces.values():
            self.total_sent_pkts += tx_interface.total_sent_pkts
            self.total_sent_bytes += tx_interface.total_sent_bytes
            self.total_dropped_pkts += tx_interface.total_dropped_pkts
            self.total_dropped_bytes += tx_interface.total_dropped_bytes

        for processor in self.packet_processors.values():
            self.total_dropped_pkts += processor.total_dropped_pkts
            self.total_dropped_bytes += processor.total_dropped_bytes

    def _update_avg(self) -> None:
        if not self.cur_timestamp:
            return
        self.avg_send_rate_pps = self.total_sent_pkts / self.cur_timestamp
        self.avg_send_rate_bps = self.total_sent_bytes * 8 / self.cur_timestamp
        self.avg_receive_rate_pps = self.total_received_pkts / self.cur_timestamp
        self.avg_receive_rate_bps = self.total_received_bytes * 8 / self.cur_timestamp
        self.avg_drop_rate_pps = self.total_dropped_pkts / self.cur_timestamp
        self.avg_drop_rate_bps = self.total_dropped_bytes * 8 / self.cur_timestamp

    def update_stat(self) -> None:
        self._update_timestamp()
        self._update_total()
        self._update_avg()

    def reset_stat(self) -> None:
        self.total_sent_pkts: int = 0
        self.total_received_pkts: int = 0
        self.total_dropped_pkts: int = 0
        self.total_sent_bytes: PacketSize = 0
        self.total_received_bytes: PacketSize = 0
        self.total_dropped_bytes: PacketSize = 0

    def todict(self) -> Dict[str, Any]:
        ret = defaultdict(dict)

        for item_dict_name in [
            "rx_interfaces",
            "tx_interfaces",
            "rx_interface_queues",
            "tx_interface_queues",
            "packet_processors",
        ]:
            for name, stat in getattr(self, item_dict_name).items():
                ret[item_dict_name][name] = stat.todict()
        return dict(ret)


class PacketProcessor(PacketQueue):
    def __init__(
        self,
        ctx: SimContext,
        processing_delay: Optional[Generator] = None,
        name: Optional[NetSimObjectName] = None,
    ):
        super().__init__(ctx, name=name)
        self._processing_delay: Optional[Generator] = processing_delay
        self._rx_interfaces: List[PacketInterfaceIn] = []
        self._tx_interfaces: List[PacketInterfaceOut] = []

    def add_interface_tx(self, tx_interface: PacketInterfaceOut) -> None:
        self._tx_interfaces.append(tx_interface)
        tx_interface.extend_name(f"{self.name}_", prepend=True)

    def add_interface_rx(self, rx_interface: PacketInterfaceIn) -> None:
        self._rx_interfaces.append(rx_interface)
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
                    "Packet processing started by %s at %s",
                    self,
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
                            "Packet put into %s by %s at %s",
                            self,
                            packet_processing_item_out.interface,
                            self._ctx.now,
                        )
                        self._packet_sent(packet_processing_item_out.packet)
                else:
                    self._packet_dropped(packet)
                self._busy = False
                logger.debug(
                    "Packet processing finished by %s at %s",
                    self,
                    self._ctx.now,
                )
            else:
                raise RuntimeError(
                    f"Resumed {self} without packet at {self._ctx.now}",
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
        logger.debug(
            "Packet received by %s at %s",
            self,
            self._ctx.now,
        )


class PacketSwitch(SenderReceiver):
    def __init__(self, ctx: SimContext, name: Optional[NetSimObjectName] = None):
        super().__init__(ctx, name=name)
        self._packet_processors: Dict[PacketProcessorName, PacketProcessor] = {}
        self._rx_interfaces: Dict[InterfaceName, PacketInterfaceRx] = {}
        self._tx_interfaces: Dict[InterfaceName, PacketInterfaceTx] = {}
        self.stat: PacketSwitchStat = PacketSwitchStat(ctx)
        self._process.add_stat_callback(self.stat.update_stat)

    def _init_stat(self):
        for processor_name, processor in self._packet_processors.items():
            self.stat.packet_processors[processor_name] = processor.stat

        for rx_interface_name, rx_interface in self._rx_interfaces.items():
            self.stat.rx_interfaces[rx_interface_name] = rx_interface.stat
            self.stat.rx_interface_queues[rx_interface_name] = rx_interface._queue.stat

        for tx_interface_name, tx_interface in self._tx_interfaces.items():
            self.stat.tx_interfaces[tx_interface_name] = tx_interface.stat
            self.stat.tx_interface_queues[tx_interface_name] = tx_interface._queue.stat

    def create_interface_tx(
        self,
        interface_name: InterfaceName,
        bw: InterfaceBW,
        queue_len_limit: Optional[int] = None,
    ) -> PacketInterfaceTx:
        name = f"{self.name}_interface_TX_{interface_name}"
        tx_interface = PacketInterfaceTx(self._ctx, bw, queue_len_limit, name=name)
        self._tx_interfaces[interface_name] = tx_interface
        return tx_interface

    def create_interface_rx(
        self,
        interface_name: InterfaceName,
        propagation_delay: Optional[SimTime],
        transmission_len_limit: Optional[int] = None,
    ) -> PacketInterfaceRx:
        name = f"{self.name}_interface_RX_{interface_name}"
        rx_interface = PacketInterfaceRx(
            self._ctx, propagation_delay, transmission_len_limit, name=name
        )
        self._rx_interfaces[interface_name] = rx_interface
        return rx_interface

    def create_packet_processor(
        self,
        processor_name: Optional[PacketProcessorName] = None,
        processing_delay: Optional[SimTime] = None,
    ) -> PacketProcessor:
        if not processor_name:
            processor_name = "PacketProcessor"
        if processor_name not in self._packet_processors:
            packet_processor = PacketProcessor(
                self._ctx, processing_delay, name=processor_name
            )
            packet_processor.extend_name(f"{self.name}_", prepend=True)
            self._packet_processors[processor_name] = packet_processor

        for interface_name in sorted(self._rx_interfaces):
            packet_processor.add_interface_rx(self._rx_interfaces[interface_name])

        for interface_name in sorted(self._tx_interfaces):
            packet_processor.add_interface_tx(self._tx_interfaces[interface_name])

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
