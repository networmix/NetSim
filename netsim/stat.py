from __future__ import annotations
from collections import defaultdict
from copy import deepcopy, copy
from tempfile import NamedTemporaryFile
from json import dumps
from dataclasses import MISSING, dataclass, fields, field
from typing import Any, Dict, TYPE_CHECKING, Optional

from netsim.common import (
    ProcessName,
    ResourceName,
    SimTime,
    TimeInterval,
)

if TYPE_CHECKING:
    from netsim.core import (
        Event,
        Get,
        Put,
        SimContext,
    )


FIELD_PREFIXES_DONT_RESET = ["timestamp", "cur_", "last_state_change_timestamp", "_"]


@dataclass
class StatFrame:
    """
    Holds a snapshot of stat data for a given timestamp.
    """

    timestamp: SimTime = 0
    duration: SimTime = 0
    last_state_change_timestamp: SimTime = 0

    def update_stat(self) -> None:
        """
        Override in subclasses to handle any stat updates for the current frame.
        """
        ...

    def update_on_advance(self, prev_frame: StatFrame) -> None:
        """
        Called when advancing time from prev_frame to this frame. Subclasses
        can override to incorporate data from the previous frame.
        """
        self.update_stat()

    def reset_stat(self) -> None:
        """
        Reset all statistic fields to defaults unless they match prefixes
        in FIELD_PREFIXES_DONT_RESET.
        """
        for fld in fields(self):
            if not any(
                fld.name.startswith(prefix) for prefix in FIELD_PREFIXES_DONT_RESET
            ):
                if fld.default != MISSING:
                    setattr(self, fld.name, fld.default)
                else:
                    setattr(self, fld.name, fld.default_factory())

    def set_time(self, timestamp: SimTime, duration: SimTime) -> None:
        """
        Update timing fields of this stat frame.
        """
        self.timestamp = timestamp
        self.duration = duration

    def todict(self) -> Dict[str, Any]:
        """
        Return a dictionary representation of this stat frame, deeply copying
        internal mutable structures.
        """
        return {
            field.name: (
                deepcopy(getattr(self, field.name))
                if not isinstance(getattr(self, field.name), defaultdict)
                else dict(deepcopy(getattr(self, field.name)))
            )
            for field in fields(self)
            if not field.name.startswith("_")
        }


@dataclass
class Stat:
    """
    Base class responsible for stats of a given Process or Resource.
    Manages a current and a previous frame, time advancement, and optional tracing.
    """

    _ctx: SimContext = field(repr=False)
    _stat_tracer: Optional[NamedTemporaryFile] = None
    cur_stat_frame: StatFrame = field(default_factory=StatFrame)
    prev_stat_frame: StatFrame = field(default_factory=StatFrame)

    prev_timestamp: SimTime = 0
    cur_timestamp: SimTime = 0

    start_interval_timestamp: SimTime = 0
    cur_interval_duration: SimTime = 0

    last_state_change_timestamp: SimTime = 0

    def advance_time(self) -> None:
        """
        Advance statistics to the current simulation time, update the stats,
        and optionally dump to trace.
        """
        if self.cur_stat_frame.duration:
            self.cur_stat_frame.update_stat()

        if self._stat_tracer:
            self.dump_stat_trace()

        self.prev_timestamp, self.cur_timestamp = self.cur_timestamp, self._ctx.now
        self.cur_interval_duration += self.cur_timestamp - self.prev_timestamp

        self.prev_stat_frame = copy(self.cur_stat_frame)
        self.cur_stat_frame.set_time(self.cur_timestamp, self.cur_interval_duration)
        self.cur_stat_frame.update_on_advance(self.prev_stat_frame)

    def reset_stat(self) -> None:
        """
        Reset runtime statistics in the current frame to defaults.
        Used in periodic sample collection.
        """
        self.start_interval_timestamp = self.cur_timestamp
        self.cur_interval_duration = 0
        self.cur_stat_frame.reset_stat()

    def update_stat(self) -> None:
        """
        Trigger a stat update on the current stat frame if time has advanced.
        """
        if self.cur_interval_duration:
            self.cur_stat_frame.update_stat()

    def todict(self) -> Dict[str, Any]:
        """
        Return a dictionary representation of all fields, including nested StatFrame.
        """
        return {
            field.name: (
                deepcopy(getattr(self, field.name))
                if not isinstance(getattr(self, field.name), StatFrame)
                else getattr(self, field.name).todict()
            )
            for field in fields(self)
            if not field.name.startswith("_")
        }

    def dump_stat_trace(self) -> None:
        """
        Write the current stat frame as a JSON line into the tracer file.
        """
        self._stat_tracer.write(dumps(self.cur_stat_frame.todict()) + "\n")
        self._stat_tracer.flush()

    def enable_stat_trace(self, prefix: Optional[str] = None) -> None:
        """
        Enable tracing by creating a NamedTemporaryFile for writing stat snapshots.
        """
        self._stat_tracer = NamedTemporaryFile(mode="w", encoding="utf8", prefix=prefix)

    def get_stat_tracer(self) -> Optional[NamedTemporaryFile]:
        """
        Retrieve the temporary file used for tracing, if any.
        """
        return self._stat_tracer


@dataclass
class ProcessStatFrame(StatFrame):
    """
    StatFrame for a Process that tracks event generation and execution counts.
    """

    total_event_gen_count: int = 0
    total_event_exec_count: int = 0

    avg_event_gen_rate: float = 0
    avg_event_exec_rate: float = 0

    def update_stat(self) -> None:
        """
        Update stats. Typically calculates any rates or averages.
        """
        self._calc_avg()

    def _calc_avg(self) -> None:
        """
        Internal helper to compute rates based on total event counts and duration.
        """
        if self.duration > 0:
            self.avg_event_gen_rate = self.total_event_gen_count / self.duration
            self.avg_event_exec_rate = self.total_event_exec_count / self.duration

    def event_generated(self) -> None:
        """
        Increment the total event generation count.
        """
        self.total_event_gen_count += 1

    def event_exec(self) -> None:
        """
        Increment the total event execution count.
        """
        self.total_event_exec_count += 1


@dataclass
class ProcessStat(Stat):
    """
    Statistics for a Process, which uses ProcessStatFrame as its internal frame.
    """

    cur_stat_frame: ProcessStatFrame = field(default_factory=ProcessStatFrame)
    prev_stat_frame: ProcessStatFrame = field(default_factory=ProcessStatFrame)

    def event_generated(self, event: Event) -> None:
        """
        Record that a new event was generated by this process.
        """
        _ = event
        self.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.event_generated()

    def event_exec(self, event: Event) -> None:
        """
        Record that an event was executed by this process.
        """
        _ = event
        self.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.event_exec()


@dataclass
class ResourceStatFrame(StatFrame):
    """
    StatFrame for a Resource that tracks put/get counts and average rates.
    """

    total_put_requested_count: int = 0
    total_get_requested_count: int = 0
    total_put_processed_count: int = 0
    total_get_processed_count: int = 0

    avg_put_requested_rate: float = 0
    avg_get_requested_rate: float = 0
    avg_put_processed_rate: float = 0
    avg_get_processed_rate: float = 0

    def put_requested(self) -> None:
        """
        Increment the count of put requests.
        """
        self.total_put_requested_count += 1

    def get_requested(self) -> None:
        """
        Increment the count of get requests.
        """
        self.total_get_requested_count += 1

    def put_processed(self) -> None:
        """
        Increment the count of processed put requests.
        """
        self.total_put_processed_count += 1

    def get_processed(self) -> None:
        """
        Increment the count of processed get requests.
        """
        self.total_get_processed_count += 1

    def update_stat(self) -> None:
        """
        Call rate calculation for resource stats.
        """
        self._calc_avg()

    def _calc_avg(self) -> None:
        """
        Calculate average rates for put/get requests and processing.
        """
        if self.duration > 0:
            self.avg_put_requested_rate = self.total_put_requested_count / self.duration
            self.avg_get_requested_rate = self.total_get_requested_count / self.duration
            self.avg_put_processed_rate = self.total_put_processed_count / self.duration
            self.avg_get_processed_rate = self.total_get_processed_count / self.duration


@dataclass
class ResourceStat(Stat):
    """
    Statistics for a Resource, which uses ResourceStatFrame as its internal frame.
    """

    cur_stat_frame: ResourceStatFrame = field(default_factory=ResourceStatFrame)
    prev_stat_frame: ResourceStatFrame = field(default_factory=ResourceStatFrame)

    def put_requested(self, put_event: Put) -> None:
        """
        Record that a put event was requested on this resource.
        """
        _ = put_event
        self.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.put_requested()

    def get_requested(self, get_event: Get) -> None:
        """
        Record that a get event was requested on this resource.
        """
        _ = get_event
        self.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.get_requested()

    def put_processed(self, put_event: Put) -> None:
        """
        Record that a put event was processed on this resource.
        """
        _ = put_event
        self.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.put_processed()

    def get_processed(self, get_event: Get) -> None:
        """
        Record that a get event was processed on this resource.
        """
        _ = get_event
        self.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.get_processed()


@dataclass
class QueueStatFrame(ResourceStatFrame):
    """
    StatFrame for a FIFO Queue resource. Tracks queue length and
    computes average length via integral over time.
    """

    cur_queue_len: int = 0
    integral_queue_sum: float = 0
    avg_queue_len: float = 0
    max_queue_len: int = 0

    def put_processed(self) -> None:
        super().put_processed()
        self.cur_queue_len += 1

    def get_processed(self) -> None:
        super().get_processed()
        self.cur_queue_len -= 1
        if self.cur_queue_len < 0:
            raise RuntimeError(f"cur_queue_len can't become negative. {self}")

    def _update_queue_stat(self) -> None:
        """
        Update average and maximum queue length.
        """
        self.max_queue_len = max(self.max_queue_len, self.cur_queue_len)
        if self.duration > 0:
            self.avg_queue_len = self.integral_queue_sum / self.duration

    def _update_queue_integral(self, prev_frame: QueueStatFrame) -> None:
        """
        Accumulate area under the queue length curve from the previous frame's time
        to this frame's time.
        """
        self.integral_queue_sum += prev_frame.cur_queue_len * (
            self.timestamp - prev_frame.timestamp
        )

    def update_stat(self) -> None:
        """
        Update stats for the queue frame, then run resource average calculations.
        """
        self._update_queue_stat()
        self._calc_avg()

    def update_on_advance(self, prev_frame: QueueStatFrame) -> None:
        """
        Called when time advances, so we accumulate the integral portion and
        then do the usual update.
        """
        self._update_queue_integral(prev_frame)
        self.update_stat()


@dataclass
class QueueStat(ResourceStat):
    """
    Statistics for a FIFO queue resource, which uses QueueStatFrame as its frame type.
    """

    cur_stat_frame: QueueStatFrame = field(default_factory=QueueStatFrame)
    prev_stat_frame: QueueStatFrame = field(default_factory=QueueStatFrame)


StatSamples = Dict[TimeInterval, StatFrame]


@dataclass
class SimStat(Stat):
    """
    High-level simulation statistic collector that stores process and resource stat snapshots.
    """

    process_stat_samples: Dict[ProcessName, StatSamples] = field(default_factory=dict)
    resource_stat_samples: Dict[ResourceName, StatSamples] = field(default_factory=dict)

    def todict(self) -> Dict[str, Any]:
        """
        Return a dictionary representation that includes this Stat's fields plus
        process and resource stat samples.
        """
        base_dict = super().todict()
        process_dict = {}
        for proc_name, intervals in self.process_stat_samples.items():
            process_dict[proc_name] = {}
            for interval, statsample in intervals.items():
                process_dict[proc_name][interval] = statsample.todict()

        resource_dict = {}
        for res_name, intervals in self.resource_stat_samples.items():
            resource_dict[res_name] = {}
            for interval, statsample in intervals.items():
                resource_dict[res_name][interval] = statsample.todict()

        base_dict["process_stat_samples"] = process_dict
        base_dict["resource_stat_samples"] = resource_dict
        return base_dict
