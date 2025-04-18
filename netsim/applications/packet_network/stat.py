from __future__ import annotations
from json import dumps
from tempfile import NamedTemporaryFile

from typing import (
    Dict,
    Any,
    TYPE_CHECKING,
    Optional,
)
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass, field, fields

from netsim.common import SimTime
from netsim.stat import (
    Stat,
    StatFrame,
    StatSamples,
)

from netsim.applications.packet_network.common import (
    RateBPS,
    RatePPS,
    NetSimObjectName,
    PacketID,
    PacketSize,
)

if TYPE_CHECKING:
    from netsim.applications.packet_network.base import Packet
    from netsim.applications.packet_network.switch import (
        InterfaceName,
        PacketProcessorName,
    )


@dataclass
class PacketStatFrame(StatFrame):
    """
    Base statistics frame for tracking sent, received, and dropped packet counts and bytes,
    plus average latencies and rates.
    """

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

    avg_latency_at_arrival: SimTime = 0
    avg_latency_at_departure: SimTime = 0
    avg_latency_at_drop: SimTime = 0

    def packet_sent(self, packet: Packet):
        self.total_sent_pkts += 1
        self.total_sent_bytes += packet.size
        if self.total_sent_pkts == 1:
            self.avg_latency_at_departure = self.timestamp - packet.generated_timestamp
        else:
            self.avg_latency_at_departure = (
                self.avg_latency_at_departure * (self.total_sent_pkts - 1)
                + (self.timestamp - packet.generated_timestamp)
            ) / self.total_sent_pkts

    def packet_received(self, packet: Packet):
        self.total_received_pkts += 1
        self.total_received_bytes += packet.size
        if self.total_received_pkts == 1:
            self.avg_latency_at_arrival = self.timestamp - packet.generated_timestamp
        else:
            self.avg_latency_at_arrival = (
                self.avg_latency_at_arrival * (self.total_received_pkts - 1)
                + (self.timestamp - packet.generated_timestamp)
            ) / self.total_received_pkts

    def packet_dropped(self, packet: Packet):
        self.total_dropped_pkts += 1
        self.total_dropped_bytes += packet.size
        if self.total_dropped_pkts == 1:
            self.avg_latency_at_drop = self.timestamp - packet.generated_timestamp
        else:
            self.avg_latency_at_drop = (
                self.avg_latency_at_drop * (self.total_dropped_pkts - 1)
                + (self.timestamp - packet.generated_timestamp)
            ) / self.total_dropped_pkts

    def _calc_avg(self):
        if self.duration > 0:
            self.avg_send_rate_pps = self.total_sent_pkts / self.duration
            self.avg_send_rate_bps = self.total_sent_bytes * 8 / self.duration

            self.avg_receive_rate_pps = self.total_received_pkts / self.duration
            self.avg_receive_rate_bps = self.total_received_bytes * 8 / self.duration

            self.avg_drop_rate_pps = self.total_dropped_pkts / self.duration
            self.avg_drop_rate_bps = self.total_dropped_bytes * 8 / self.duration

    def update_stat(self) -> None:
        self._calc_avg()


@dataclass
class PacketStat(Stat):
    """
    Statistics container for general objects sending/receiving/dropping packets.
    """

    cur_stat_frame: PacketStatFrame = field(default_factory=PacketStatFrame)
    prev_stat_frame: PacketStatFrame = field(default_factory=PacketStatFrame)
    _packet_tracer: Optional[NamedTemporaryFile] = None

    def packet_sent(self, packet: Packet):
        self.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.packet_sent(packet)
        self._dump_packet(packet)

    def packet_received(self, packet: Packet):
        self.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.packet_received(packet)
        self._dump_packet(packet)

    def packet_dropped(self, packet: Packet):
        self.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.packet_dropped(packet)
        self._dump_packet(packet)

    def _dump_packet(self, packet: Packet) -> None:
        if self._packet_tracer:
            self._packet_tracer.write(dumps(packet.todict()) + "\n")

    def enable_packet_trace(self, prefix: Optional[str] = None) -> None:
        self._packet_tracer = NamedTemporaryFile(
            mode="w", encoding="utf8", prefix=prefix
        )

    def get_packet_tracer(self) -> Optional[NamedTemporaryFile]:
        return self._packet_tracer


@dataclass
class PacketQueueStatFrame(PacketStatFrame):
    """
    Extended statistics frame for queue objects, tracking put/get operations,
    queue lengths, and waiting times.
    """

    _wait_tracker: Dict[PacketID, SimTime] = field(default_factory=dict)

    total_get_pkts: int = 0
    total_put_pkts: int = 0

    total_get_bytes: PacketSize = 0
    total_put_bytes: PacketSize = 0

    avg_put_rate_pps: RatePPS = 0
    avg_get_rate_pps: RatePPS = 0

    cur_queue_len: int = 0
    integral_queue_sum: float = 0
    avg_queue_len: float = 0
    max_queue_len: int = 0

    integral_wait_time_sum: SimTime = 0
    avg_wait_time: SimTime = 0
    max_wait_time: SimTime = 0

    def packet_get(self, packet: Packet):
        """
        Called whenever a packet is removed from the queue.
        """
        self.cur_queue_len -= 1
        if self.cur_queue_len < 0:
            raise RuntimeError(f"cur_queue_len can't become negative. {self}")
        self.total_get_pkts += 1
        self.total_get_bytes += packet.size
        wait_time = self.timestamp - self._wait_tracker[packet.packet_id]
        del self._wait_tracker[packet.packet_id]
        self.max_wait_time = max(self.max_wait_time, wait_time)
        self.integral_wait_time_sum += wait_time

    def packet_put(self, packet: Packet):
        """
        Called whenever a packet is inserted into the queue.
        """
        self.total_put_pkts += 1
        self.total_put_bytes += packet.size
        self.cur_queue_len += 1
        self._wait_tracker[packet.packet_id] = self.timestamp

    def _calc_avg(self) -> None:
        # Calculate base rates from PacketStatFrame
        super()._calc_avg()

        if self.duration > 0:
            self.avg_put_rate_pps = self.total_put_pkts / self.duration
            self.avg_get_rate_pps = self.total_get_pkts / self.duration

    def _update_queue_stat(self) -> None:
        """
        Update aggregated queue stats (max queue length, average queue length, average wait time).
        """
        self.max_queue_len = max(self.max_queue_len, self.cur_queue_len)
        if self.duration > 0:
            self.avg_queue_len = self.integral_queue_sum / self.duration
        if self.total_get_pkts:
            self.avg_wait_time = self.integral_wait_time_sum / self.total_get_pkts

    def _update_queue_integral(self, prev_frame: PacketQueueStatFrame) -> None:
        """
        Accumulate the queue length integral over the interval since the last sample.
        """
        self.integral_queue_sum += prev_frame.cur_queue_len * (
            self.timestamp - prev_frame.timestamp
        )

    def update_stat(self) -> None:
        self._update_queue_stat()
        self._calc_avg()

    def update_on_advance(self, prev_frame: PacketQueueStatFrame) -> None:
        self._update_queue_integral(prev_frame)
        self.update_stat()


@dataclass
class PacketQueueStat(PacketStat):
    """
    Statistics object for queue-like entities. Extends PacketStat with queue-specific operations.
    """

    cur_stat_frame: PacketQueueStatFrame = field(default_factory=PacketQueueStatFrame)
    prev_stat_frame: PacketQueueStatFrame = field(default_factory=PacketQueueStatFrame)

    def packet_get(self, packet: Packet):
        self.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.packet_get(packet)

    def packet_put(self, packet: Packet):
        self.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.packet_put(packet)


@dataclass
class PacketSwitchStatFrame(StatFrame):
    """
    Statistics frame specialized for packet switches, holding interface-level stats and totals.
    """

    rx_interfaces: Dict[InterfaceName, PacketStatFrame] = field(default_factory=dict)
    tx_interfaces: Dict[InterfaceName, PacketStatFrame] = field(default_factory=dict)

    rx_interface_queues: Dict[InterfaceName, PacketQueueStatFrame] = field(
        default_factory=dict
    )
    tx_interface_queues: Dict[InterfaceName, PacketQueueStatFrame] = field(
        default_factory=dict
    )

    packet_processors: Dict[PacketProcessorName, PacketQueueStatFrame] = field(
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

    def _update_total(self, switch_stat: PacketSwitchStat) -> None:
        """
        Aggregate totals from all switch interfaces and processors.
        """
        for rx_interface in switch_stat._rx_interfaces.values():
            self.total_received_pkts += rx_interface.cur_stat_frame.total_received_pkts
            self.total_received_bytes += (
                rx_interface.cur_stat_frame.total_received_bytes
            )
            self.total_dropped_pkts += rx_interface.cur_stat_frame.total_dropped_pkts
            self.total_dropped_bytes += rx_interface.cur_stat_frame.total_dropped_bytes
            self.last_state_change_timestamp = max(
                rx_interface.cur_stat_frame.last_state_change_timestamp,
                self.last_state_change_timestamp,
            )

        for tx_interface in switch_stat._tx_interfaces.values():
            self.total_sent_pkts += tx_interface.cur_stat_frame.total_sent_pkts
            self.total_sent_bytes += tx_interface.cur_stat_frame.total_sent_bytes
            self.total_dropped_pkts += tx_interface.cur_stat_frame.total_dropped_pkts
            self.total_dropped_bytes += tx_interface.cur_stat_frame.total_dropped_bytes
            self.last_state_change_timestamp = max(
                tx_interface.cur_stat_frame.last_state_change_timestamp,
                self.last_state_change_timestamp,
            )

        for processor in switch_stat._packet_processors.values():
            self.total_dropped_pkts += processor.cur_stat_frame.total_dropped_pkts
            self.total_dropped_bytes += processor.cur_stat_frame.total_dropped_bytes
            self.last_state_change_timestamp = max(
                processor.cur_stat_frame.last_state_change_timestamp,
                self.last_state_change_timestamp,
            )

    def _update_avg(self) -> None:
        """
        Compute overall average rates for the switch.
        """
        if self.duration > 0:
            self.avg_send_rate_pps = self.total_sent_pkts / self.duration
            self.avg_send_rate_bps = self.total_sent_bytes * 8 / self.duration
            self.avg_receive_rate_pps = self.total_received_pkts / self.duration
            self.avg_receive_rate_bps = self.total_received_bytes * 8 / self.duration
            self.avg_drop_rate_pps = self.total_dropped_pkts / self.duration
            self.avg_drop_rate_bps = self.total_dropped_bytes * 8 / self.duration

    def _fill_frames(self, switch_stat: PacketSwitchStat) -> None:
        """
        Copy the current frames from the switch into this object for final reporting.
        """
        for fld_name in [
            "_rx_interfaces",
            "_tx_interfaces",
            "_rx_interface_queues",
            "_tx_interface_queues",
            "_packet_processors",
        ]:
            for entity_name, entity_stat in getattr(switch_stat, fld_name).items():
                getattr(self, fld_name[1:])[entity_name] = entity_stat.cur_stat_frame

    def fill_stat(self, switch_stat: PacketSwitchStat) -> None:
        """
        Main routine that collects and aggregates all stats from the switch's sub-entities.
        """
        self._update_total(switch_stat)
        if self.duration:
            self._update_avg()
        self._fill_frames(switch_stat)

    def todict(self):
        ret = defaultdict(dict)
        for fld in fields(self):
            if fld.name in [
                "rx_interfaces",
                "tx_interfaces",
                "rx_interface_queues",
                "tx_interface_queues",
                "packet_processors",
            ]:
                for entity_name, entity_stat in getattr(self, fld.name).items():
                    ret[fld.name][entity_name] = entity_stat.todict()
            else:
                ret[fld.name] = deepcopy(getattr(self, fld.name))
        return dict(ret)


@dataclass
class PacketSwitchStat(Stat):
    """
    Statistics container for a PacketSwitch, which aggregates interface and processor stats.
    """

    _rx_interfaces: Dict[InterfaceName, PacketStat] = field(default_factory=dict)
    _tx_interfaces: Dict[InterfaceName, PacketStat] = field(default_factory=dict)
    _rx_interface_queues: Dict[InterfaceName, PacketQueueStat] = field(
        default_factory=dict
    )
    _tx_interface_queues: Dict[InterfaceName, PacketQueueStat] = field(
        default_factory=dict
    )
    _packet_processors: Dict[PacketProcessorName, PacketQueueStat] = field(
        default_factory=dict
    )

    cur_stat_frame: PacketSwitchStatFrame = field(default_factory=PacketSwitchStatFrame)
    prev_stat_frame: PacketSwitchStatFrame = field(
        default_factory=PacketSwitchStatFrame
    )

    def advance_time(self) -> None:
        """
        Overriding advance_time to handle collecting sub-entity stats before final snapshot.
        """
        if self._stat_tracer:
            self.dump_stat_trace()

        self.prev_timestamp, self.cur_timestamp = self.cur_timestamp, self._ctx.now
        self.cur_interval_duration += self.cur_timestamp - self.prev_timestamp

        self.prev_stat_frame, self.cur_stat_frame = (
            self.cur_stat_frame,
            PacketSwitchStatFrame(),
        )

        self.cur_stat_frame.set_time(self.cur_timestamp, self.cur_interval_duration)
        self.fill_stat()

    def fill_stat(self) -> None:
        """
        Populate the current stat frame with aggregated data from sub-entities.
        """
        self.cur_stat_frame.fill_stat(self)


@dataclass
class NetSimStat(Stat):
    """
    Overall statistics container for the entire NetSim environment.
    """

    stat_samples: Dict[NetSimObjectName, StatSamples] = field(default_factory=dict)

    def dump_stat_trace(self) -> None:
        """
        No-op or specialized logic for NetSim-level stat trace. Overridden in real usage if needed.
        """
        ...

    def dump_stat_samples(self, data_dict: Dict[str, Any]) -> None:
        """
        Write the aggregated data samples to the stat tracer if one is enabled.
        """
        self._stat_tracer.write(dumps(data_dict) + "\n")

    def todict(self) -> Dict[str, Any]:
        ret = defaultdict(dict)
        for item_dict_name in ["stat_samples"]:
            for name, stat in getattr(self, item_dict_name).items():
                for interval, statsample in stat.items():
                    ret[item_dict_name].setdefault(name, {})[
                        interval
                    ] = statsample.todict()
        return dict(ret)
