from __future__ import annotations
from collections import defaultdict
from copy import deepcopy, copy
from tempfile import NamedTemporaryFile
from json import dumps
from dataclasses import MISSING, dataclass, fields, field
from typing import Any, Dict, TYPE_CHECKING, Optional

from netsim.simulator.core.sim_common import (
    ProcessName,
    ResourceName,
    SimTime,
    TimeInterval,
)


if TYPE_CHECKING:
    from netsim.simulator.core.simcore import (
        Event,
        Get,
        Put,
        SimContext,
    )


FIELD_PREFIXES_DONT_RESET = ["timestamp", "cur_", "last_state_change_timestamp", "_"]


@dataclass
class StatFrame:
    """
    This container holds a snapshot of stat data for a given timestamp
    """

    timestamp: SimTime = 0
    duration: SimTime = 0
    last_state_change_timestamp: SimTime = 0

    def update_stat(self) -> None:
        ...

    def update_on_advance(self, prev_frame: StatFrame) -> None:
        self.update_stat()

    def reset_stat(self):
        for fld in fields(self):
            if not any(
                (fld.name.startswith(prefix) for prefix in FIELD_PREFIXES_DONT_RESET)
            ):
                if fld.default != MISSING:
                    setattr(self, fld.name, fld.default)
                else:
                    setattr(self, fld.name, fld.default_factory())

    def set_time(self, timestamp: SimTime, duration: SimTime) -> None:
        self.timestamp = timestamp
        self.duration = duration

    def todict(self) -> Dict[str, Any]:
        return {
            field.name: deepcopy(getattr(self, field.name))
            if not isinstance(getattr(self, field.name), defaultdict)
            else dict(deepcopy(getattr(self, field.name)))
            for field in fields(self)
            if not field.name.startswith("_")
        }


@dataclass
class Stat:
    """
    The block responsible for stats of a given Process/Resource.
    Presents methods to advance time and update counters.
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
        Handle to advance time of the stat container.
        """

        if self.cur_stat_frame.duration:
            # update avg in the frame
            self.cur_stat_frame.update_stat()

        if self._stat_tracer:
            # dump stat frame to a temp file
            self.dump_stat_trace()

        # update timestamps in the stat block
        self.prev_timestamp, self.cur_timestamp = self.cur_timestamp, self._ctx.now
        self.cur_interval_duration += self.cur_timestamp - self.prev_timestamp

        # clone cur frame to prev
        self.prev_stat_frame = copy(self.cur_stat_frame)

        # update timestamps in the cur frame
        self.cur_stat_frame.set_time(self.cur_timestamp, self.cur_interval_duration)

        # cumulative sums and etc
        self.cur_stat_frame.update_on_advance(self.prev_stat_frame)

    def reset_stat(self) -> None:
        """
        Handle to reset runtime statistics in the current frame to the default.
        Used in periodic sample collection after exporting a sample.
        """
        self.start_interval_timestamp = self.cur_timestamp
        self.cur_interval_duration = 0
        self.cur_stat_frame.reset_stat()

    def update_stat(self) -> None:
        """
        Handle to recalculate runtime statistics (avg and relative values)
        """
        if self.cur_interval_duration:
            self.cur_stat_frame.update_stat()

    def todict(self) -> Dict[str, Any]:
        return {
            field.name: deepcopy(getattr(self, field.name))
            if not isinstance(getattr(self, field.name), StatFrame)
            else getattr(self, field.name).todict()
            for field in fields(self)
            if not field.name.startswith("_")
        }

    def dump_stat_trace(self) -> None:
        self._stat_tracer.write(dumps(self.cur_stat_frame.todict()) + "\n")

    def enable_stat_trace(self, prefix: Optional[None]) -> None:
        self._stat_tracer = NamedTemporaryFile(mode="w", encoding="utf8", prefix=prefix)

    def get_stat_tracer(self) -> Optional[NamedTemporaryFile]:
        return self._stat_tracer


@dataclass
class ProcessStatFrame(StatFrame):
    """
    StatFrame for a Process
    """

    total_event_gen_count: int = 0
    total_event_exec_count: int = 0

    avg_event_gen_rate: float = 0
    avg_event_exec_rate: float = 0

    def update_stat(self) -> None:
        self._calc_avg()

    def _calc_avg(self) -> None:
        self.avg_event_gen_rate = self.total_event_gen_count / self.duration
        self.avg_event_exec_rate = self.total_event_exec_count / self.duration

    def event_generated(self) -> None:
        self.total_event_gen_count += 1

    def event_exec(self) -> None:
        self.total_event_exec_count += 1


@dataclass
class ProcessStat(Stat):
    cur_stat_frame: ProcessStatFrame = field(default_factory=ProcessStatFrame)
    prev_stat_frame: ProcessStatFrame = field(default_factory=ProcessStatFrame)

    def event_generated(self, event: Event) -> None:
        self.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.event_generated()

    def event_exec(self, event: Event) -> None:
        self.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.event_exec()


@dataclass
class ResourceStatFrame(StatFrame):
    """
    StatFrame for a Resource
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
        self.total_put_requested_count += 1

    def get_requested(self) -> None:
        self.total_get_requested_count += 1

    def put_processed(self) -> None:
        self.total_put_processed_count += 1

    def get_processed(self) -> None:
        self.total_get_processed_count += 1

    def update_stat(self) -> None:
        self._calc_avg()

    def _calc_avg(self) -> None:
        self.avg_put_requested_rate = self.total_put_requested_count / self.duration
        self.avg_get_requested_rate = self.total_get_requested_count / self.duration
        self.avg_put_processed_rate = self.total_put_processed_count / self.duration
        self.avg_get_processed_rate = self.total_get_processed_count / self.duration


@dataclass
class ResourceStat(Stat):
    cur_stat_frame: ResourceStatFrame = field(default_factory=ResourceStatFrame)
    prev_stat_frame: ResourceStatFrame = field(default_factory=ResourceStatFrame)

    def put_requested(self, put_event: Put) -> None:
        _ = put_event
        self.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.put_requested()

    def get_requested(self, get_event: Get) -> None:
        _ = get_event
        self.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.get_requested()

    def put_processed(self, put_event: Put) -> None:
        _ = put_event
        self.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.put_processed()

    def get_processed(self, get_event: Get) -> None:
        _ = get_event
        self.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.last_state_change_timestamp = self.cur_timestamp
        self.cur_stat_frame.get_processed()


@dataclass
class QueueStatFrame(ResourceStatFrame):
    cur_queue_len: int = 0
    integral_queue_sum: float = 0
    avg_queue_len: float = 0
    max_queue_len: int = 0

    def put_processed(self) -> None:
        self.total_put_processed_count += 1
        self.cur_queue_len += 1

    def get_processed(self) -> None:
        self.total_get_processed_count += 1
        self.cur_queue_len -= 1
        if self.cur_queue_len < 0:
            raise RuntimeError(f"cur_queue_len can't become negative. {self}")

    def _update_queue_stat(self) -> None:
        self.max_queue_len = max(self.max_queue_len, self.cur_queue_len)
        self.avg_queue_len = self.integral_queue_sum / self.duration

    def _update_queue_integral(self, prev_frame: QueueStatFrame) -> None:
        self.integral_queue_sum += prev_frame.cur_queue_len * (
            self.timestamp - prev_frame.timestamp
        )

    def update_stat(self) -> None:
        self._update_queue_stat()
        self._calc_avg()

    def update_on_advance(self, prev_frame: QueueStatFrame) -> None:
        self._update_queue_integral(prev_frame)
        self.update_stat()


@dataclass
class QueueStat(ResourceStat):
    cur_stat_frame: QueueStatFrame = field(default_factory=QueueStatFrame)
    prev_stat_frame: QueueStatFrame = field(default_factory=QueueStatFrame)


@dataclass
class SimStat(Stat):
    process_stat_samples: Dict[ProcessName, StatSamples] = field(default_factory=dict)
    resource_stat_samples: Dict[ResourceName, StatSamples] = field(default_factory=dict)

    def todict(self) -> Dict[str, Any]:
        ret = defaultdict(dict)

        for item_dict_name in ["process_stat_samples", "resource_stat_samples"]:
            for name, stat in getattr(self, item_dict_name).items():
                for interval, statsample in stat.items():
                    ret[item_dict_name].setdefault(name, {})[
                        interval
                    ] = statsample.todict()
        return dict(ret)


StatSamples = Dict[TimeInterval, StatFrame]
