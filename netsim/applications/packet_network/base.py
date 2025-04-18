from __future__ import annotations

from abc import ABC, abstractmethod
import random
from dataclasses import dataclass
import logging
from typing import Callable, Dict, List, Optional, Set, Type, Union, Generator, Any

from ngraph.lib.graph import StrictMultiDiGraph
from schema import Schema

from netsim.applications.packet_network.common import (
    InterfaceBW,
    NetSimObjectID,
    NetSimObjectName,
    PacketAction,
    PacketFlowID,
    PacketID,
    PacketSize,
)
from netsim.applications.packet_network.stat import PacketQueueStat, PacketStat
from netsim.common import SimTime

from netsim.core import (
    Coro,
    Process,
    QueueFIFO,
    SimContext,
)
from netsim.stat import Stat
from netsim.stat_base import DistrBuilder, DistrFunc
from netsim.tracer import Tracer


LOG_FMT = "%(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT)
logger = logging.getLogger(__name__)

SEED = 0
random.seed(SEED)

QUEUE_ADMISSION_RED_PARAMS = Schema(
    [
        {
            "admission_policy": lambda s: s == "red",
            "wq": float,
            "minth": int,
            "maxth": int,
            "maxp": float,
            "s": float,
        }
    ]
)

QUEUE_ADMISSION_TAILDROP_PARAMS = Schema(
    [
        {
            "admission_policy": lambda s: s == "taildrop",
        }
    ]
)


@dataclass
class QueueState:
    """
    Holds common queue state data for advanced queue admission policies.
    (Empty base for possible extensions beyond RED.)
    """


@dataclass
class QueueStateRED(QueueState):
    """
    Tracks additional state needed for RED queue admission policy.

    Attributes:
        admission_policy: Must be 'red'.
        wq: Weight for exponential moving average of queue size.
        minth: Minimum threshold for average queue size.
        maxth: Maximum threshold for average queue size.
        maxp: Maximum probability for packet drop.
        s: Reference small-packet transmission time for idle time calculations.
        avg: Current average queue size (updated each arrival).
        q_time: Start of the queue idle time (for weighted idle).
        count: Packets since last dropped/marked packet.
    """

    admission_policy: str
    wq: float
    minth: float
    maxth: float
    maxp: float
    s: float
    avg: float = 0
    q_time: float = 0
    count: int = -1


class NetSimContext(SimContext):
    """
    Specialized SimContext for network simulations.
    Maintains additional references such as topologies and network state database.
    """

    def __init__(self, starttime: SimTime = 0) -> None:
        """
        Initializes the NetSimContext.

        Args:
            starttime: The simulation start time (default 0).
        """
        super().__init__(starttime=starttime)
        self.topology: Dict[str, StrictMultiDiGraph] = {}
        self.state_db: Dict[NetSimObjectName, Any] = {}

    def add_topology(self, name: str, graph: StrictMultiDiGraph) -> None:
        """
        Register a topology graph object in the context.

        Args:
            name: A unique name for the topology.
            graph: The StrictMultiDiGraph object representing the topology.
        """
        self.topology[name] = graph


class NetSimObject(ABC):
    """
    Base class for network-simulation objects (e.g., hosts, routers).
    Each object typically has an associated process for simulation logic and a statistics object.
    """

    _next_netsim_id: NetSimObjectID = 0

    def __init__(
        self,
        ctx: NetSimContext,
        name: Optional[NetSimObjectName] = None,
        stat_type: Optional[Type[Stat]] = None,
    ) -> None:
        """
        Create a NetSim object with a unique ID, name, and associated process/stat.

        Args:
            ctx: The network simulation context.
            name: Optional override name.
            stat_type: Optional Stat subclass to track custom stats.
        """
        self.ctx: NetSimContext = ctx
        self.ns_id, NetSimObject._next_netsim_id = (
            NetSimObject._next_netsim_id,
            NetSimObject._next_netsim_id + 1,
        )
        self.name: NetSimObjectName = (
            name if name else f"{type(self).__name__}_{self.ns_id}"
        )
        # Create a new process for this object
        self.process: Process = ctx.create_process(self.run(), self.name)
        self.process.extend_name("_proc")
        # Create stats
        self.stat: Stat = Stat(self.ctx) if not stat_type else stat_type(self.ctx)

    def __repr__(self) -> str:
        return f"{self.name}(process={self.process.name})"

    def extend_name(self, extension: str, prepend: bool = False) -> None:
        """
        Extend the name of this NetSim object.

        Args:
            extension: The string to add to the object's name.
            prepend: If True, put extension at start; else at the end.
        """
        if prepend:
            self.name = extension + self.name
        else:
            self.name = self.name + extension

    @abstractmethod
    def run(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Coro:
        """
        A coroutine representing the NetSim object's main behavior.
        Must be implemented in each subclass.
        """
        raise NotImplementedError(self)


class Packet:
    """
    Represents a network packet with size, flow_id, and timestamps for tracking.
    """

    _next_packet_id: PacketID = 0

    def __init__(
        self,
        ctx: NetSimContext,
        size: PacketSize,
        flow_id: PacketFlowID = 0,
    ) -> None:
        """
        Create a packet with a unique ID, size, and optional flow ID.

        Args:
            ctx: The network simulation context.
            size: Packet size in bytes.
            flow_id: Optional flow identifier.
        """
        self.ctx = ctx
        self.packet_id, Packet._next_packet_id = (
            Packet._next_packet_id,
            Packet._next_packet_id + 1,
        )
        self.flow_id = flow_id
        self.size = size
        self.generated_timestamp = ctx.now
        self.touched_timestamp: Optional[SimTime] = None
        self.touched_by: Optional[NetSimObjectName] = None
        self.last_action: Optional[PacketAction] = None

    def __repr__(self) -> str:
        type_name = type(self).__name__
        return (
            f"{type_name}(packet_id={self.packet_id}, flow_id={self.flow_id}, "
            f"size={self.size}, timestamp={self.generated_timestamp})"
        )

    @classmethod
    def reset_packet_id(cls) -> None:
        """
        Reset the global packet_id counter to 0.
        """
        cls._next_packet_id = 0

    def touched(self, ns_obj: NetSimObject, action: PacketAction) -> None:
        """
        Update tracking fields when this packet is processed by a NetSim object.

        Args:
            ns_obj: The object that processed this packet.
            action: What action was taken (SENT, RECEIVED, DROPPED).
        """
        self.touched_timestamp = self.ctx.now
        self.touched_by = ns_obj.name
        self.last_action = action

    def todict(self) -> Dict[str, Any]:
        """
        Convert the packet's public attributes into a JSON-serialisable dict.

        * Private fields (those starting with ``_``) are skipped.
        * ``ctx`` is replaced by a lightweight identifier so it can be JSON-serialized.
        """
        serialised: Dict[str, Any] = {}
        for name, value in vars(self).items():
            if name.startswith("_") or name == "ctx":
                continue
            serialised[name] = value
        return serialised


class Sender(NetSimObject):
    """
    A sending endpoint in the network simulation. It can generate packets and send them to a list of receivers.
    """

    def __init__(
        self,
        ctx: NetSimContext,
        name: Optional[NetSimObjectName] = None,
        stat_type: Optional[Type[Stat]] = None,
    ) -> None:
        """
        Create a Sender object.

        Args:
            ctx: The network simulation context.
            name: Optional override name.
            stat_type: Optional PacketStat-based tracker for sent/received stats.
        """
        super().__init__(ctx, name=name, stat_type=stat_type or PacketStat)
        self.stat: PacketStat = self.stat  # type override
        self._subscribers: List[Receiver] = []
        # Ensure that we advance stats on every stat collection
        self.process.add_stat_callback(self.stat.advance_time)

    def subscribe(
        self, receiver: Receiver, *args: List[Any], **kwargs: Dict[str, Any]
    ) -> None:
        """
        Subscribe a receiver so that this sender can forward packets to it.

        Args:
            receiver: The receiver to subscribe.
        """
        _ = args, kwargs  # unused
        self._subscribers.append(receiver)
        receiver.add_subscription(self)

    def _packet_sent(self, packet: Packet) -> None:
        """
        Mark the packet as sent for stats/logging.
        """
        packet.touched(self, action=PacketAction.SENT)
        self.stat.packet_sent(packet)

    def _packet_received(self, packet: Packet) -> None:
        """
        Mark the packet as received for stats/logging.
        (Typically, a Sender doesn't receive, but this is here for uniformity.)
        """
        packet.touched(self, action=PacketAction.RECEIVED)
        self.stat.packet_received(packet)

    def _packet_dropped(self, packet: Packet) -> None:
        """
        Mark the packet as dropped for stats/logging.
        """
        packet.touched(self, action=PacketAction.DROPPED)
        self.stat.packet_dropped(packet)


class Receiver(NetSimObject):
    """
    A receiving endpoint in the network simulation. It can subscribe to senders
    or intermediate nodes and handle packets.
    """

    def __init__(
        self,
        ctx: NetSimContext,
        name: Optional[NetSimObjectName] = None,
        stat_type: Optional[Type[Stat]] = None,
    ) -> None:
        """
        Create a Receiver object.

        Args:
            ctx: The network simulation context.
            name: Optional override name.
            stat_type: Optional PacketStat-based tracker for sent/received stats.
        """
        super().__init__(ctx, name=name, stat_type=stat_type or PacketStat)
        self.stat: PacketStat = self.stat
        self.process.add_stat_callback(self.stat.advance_time)
        self._subscriptions: Set[Union[Sender, SenderReceiver]] = set()

    @abstractmethod
    def put(
        self,
        item: Any,
        source: NetSimObject,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ) -> None:
        """
        A method for higher-level network objects to deliver a packet to this receiver.
        Must be implemented in subclasses.

        Args:
            item: The incoming packet (or data).
            source: The object that sent/delivered the item.
        """
        raise NotImplementedError(self)

    def add_subscription(self, ns_obj: Union[Sender, "SenderReceiver"]) -> None:
        """
        Track that this receiver is subscribed to a given sender or intermediate node.

        Args:
            ns_obj: The sender or intermediate node subscribed to this receiver.
        """
        self._subscriptions.add(ns_obj)

    def get_subscriptions(self) -> Set[Union[Sender, "SenderReceiver"]]:
        """
        Returns the set of objects publishing packets to this receiver.
        """
        return self._subscriptions

    def _packet_received(self, packet: Packet) -> None:
        """
        Record that a packet was received.
        """
        packet.touched(self, action=PacketAction.RECEIVED)
        self.stat.packet_received(packet)

    def _packet_dropped(self, packet: Packet) -> None:
        """
        Record that a packet was dropped at this receiver.
        """
        packet.touched(self, action=PacketAction.DROPPED)
        self.stat.packet_dropped(packet)


class SenderReceiver(NetSimObject):
    """
    An object that can both receive and send packets, serving as an intermediate node in the network.
    """

    def __init__(
        self,
        ctx: NetSimContext,
        name: Optional[NetSimObjectName] = None,
        stat_type: Optional[Type[Stat]] = None,
    ) -> None:
        """
        Create a SenderReceiver object.

        Args:
            ctx: The network simulation context.
            name: Optional override name.
            stat_type: Optional PacketStat-based tracker for sent/received stats.
        """
        super().__init__(ctx, name=name, stat_type=stat_type or PacketStat)
        self.stat: PacketStat = self.stat
        self._subscribers: List[Receiver] = []
        self._subscriptions: Set[Union[Sender, "SenderReceiver"]] = set()
        self.process.add_stat_callback(self.stat.advance_time)

    @abstractmethod
    def put(
        self,
        item: Any,
        source: NetSimObject,
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ) -> None:
        """
        A method for other network objects to deliver a packet to this node.
        Must be implemented in subclasses.

        Args:
            item: The incoming packet (or data).
            source: The object that sent/delivered the item.
        """
        raise NotImplementedError(self)

    def subscribe(
        self, receiver: Receiver, *args: List[Any], **kwargs: Dict[str, Any]
    ) -> None:
        """
        Subscribe a receiver to this node.

        Args:
            receiver: The receiver to subscribe.
        """
        _ = args, kwargs
        self._subscribers.append(receiver)
        receiver.add_subscription(self)

    def add_subscription(self, ns_obj: Union[Sender, "SenderReceiver"]) -> None:
        """
        Track that this node is receiving data from the given sender or node.
        """
        self._subscriptions.add(ns_obj)

    def get_subscriptions(self) -> Set[Union[Sender, "SenderReceiver"]]:
        """
        Returns the set of objects that feed packets into this node.
        """
        return self._subscriptions

    def _packet_sent(self, packet: Packet) -> None:
        """
        Mark the packet as sent for stats/logging.
        """
        packet.touched(self, action=PacketAction.SENT)
        self.stat.packet_sent(packet)

    def _packet_received(self, packet: Packet) -> None:
        """
        Mark the packet as received for stats/logging.
        """
        packet.touched(self, action=PacketAction.RECEIVED)
        self.stat.packet_received(packet)

    def _packet_dropped(self, packet: Packet) -> None:
        """
        Mark the packet as dropped for stats/logging.
        """
        packet.touched(self, action=PacketAction.DROPPED)
        self.stat.packet_dropped(packet)


@dataclass
class PacketSourceProfile:
    """
    A set of distribution parameters for creating a packet flow (arrival times, sizes, flows).
    """

    arrival_time_distr: DistrFunc
    arrival_distr_params: Dict[str, Any]
    size_distr: DistrFunc
    size_distr_params: Dict[str, Any]
    flow_distr: DistrFunc
    flow_distr_params: Dict[str, Any]
    initial_delay: SimTime = 0

    @classmethod
    def from_dict(cls, params: Dict[str, Any]) -> PacketSourceProfile:
        """
        Build a PacketSourceProfile from a dictionary of params that includes:
          - arrival_time_distr / arrival_distr_params
          - size_distr / size_distr_params
          - flow_distr / flow_distr_params
          - optional initial_delay
        """
        arrival_time_distr = DistrFunc[params["arrival_time_distr"]]
        arrival_distr_params = params["arrival_distr_params"]

        size_distr = DistrFunc[params["size_distr"]]
        size_distr_params = params["size_distr_params"]

        flow_distr = DistrFunc[params["flow_distr"]]
        flow_distr_params = params["flow_distr_params"]

        initial_delay = params.get("initial_delay", 0)

        return cls(
            arrival_time_distr,
            arrival_distr_params,
            size_distr,
            size_distr_params,
            flow_distr,
            flow_distr_params,
            initial_delay,
        )


class PacketSource(Sender):
    """
    A periodic or stochastically-driven packet generator that sends packets to its subscribed receivers.
    """

    def __init__(
        self,
        ctx: NetSimContext,
        arrival_func: Generator[float, None, None],
        size_func: Generator[int, None, None],
        flow_func: Optional[Generator[int, None, None]] = None,
        initial_delay: SimTime = 0,
        name: Optional[NetSimObjectName] = None,
    ) -> None:
        """
        Create a PacketSource with distributions for arrival, size, and optional flow.

        Args:
            ctx: The network simulation context.
            arrival_func: Yields inter-arrival times.
            size_func: Yields packet sizes.
            flow_func: Yields flow IDs, if any.
            initial_delay: Delay before the first packet generation.
            name: Optional override name.
        """
        super().__init__(ctx, name=name)
        self._arrival_func: Generator[float, None, None] = arrival_func
        self._size_func: Generator[int, None, None] = size_func
        self._flow_func: Optional[Generator[int, None, None]] = flow_func
        self._initial_delay: SimTime = initial_delay

    @classmethod
    def create(
        cls, ctx: NetSimContext, name: NetSimObjectName, profile: PacketSourceProfile
    ) -> PacketSource:
        """
        Factory method to create a PacketSource from a PacketSourceProfile.

        Args:
            ctx: The simulation context.
            name: The name for the created source.
            profile: A PacketSourceProfile specifying distributions and parameters.

        Returns:
            The created PacketSource.
        """
        arrival_func = DistrBuilder.create(
            profile.arrival_time_distr, profile.arrival_distr_params
        )
        size_func = DistrBuilder.create(profile.size_distr, profile.size_distr_params)
        flow_func = DistrBuilder.create(profile.flow_distr, profile.flow_distr_params)

        return cls(ctx, arrival_func, size_func, flow_func, profile.initial_delay, name)

    def run(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Coro:
        """
        The main coroutine that repeatedly:
          1) Waits for the arrival time
          2) Generates a packet with a random size, flow_id
          3) Sends it to all subscribers
        """
        yield self.process.timeout(self._initial_delay)
        while True:
            arrival = next(self._arrival_func, None)
            if arrival is None:
                return
            yield self.process.timeout(arrival)

            size = next(self._size_func, None)
            if size is None:
                return

            flow = next(self._flow_func, 0) if self._flow_func else 0

            packet = Packet(self.ctx, size, flow_id=flow)
            self._packet_sent(packet)
            for subscriber in self._subscribers:
                subscriber.put(packet, self)


class PacketSink(Receiver):
    """
    A simple sink that receives packets and does nothing except record stats.
    """

    def put(
        self,
        item: Packet,
        source: Union[Sender, SenderReceiver],
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ) -> None:
        """
        Receive a packet, record its reception in stats, and discard it.

        Args:
            item: The packet received.
            source: The sender or node that forwarded it.
        """
        self._packet_received(item)

    def run(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Coro:
        """
        Main coroutine does nothing but yield control indefinitely.
        """
        while True:
            yield


class PacketQueue(SenderReceiver):
    """
    A queue that buffers packets (FIFO) before sending them to subscribers.
    Supports tail-drop or RED admission control based on admission_params.
    Optionally supports a user-supplied service_func to model service delay.
    """

    def __init__(
        self,
        ctx: NetSimContext,
        queue_len_limit: Optional[int] = None,
        name: Optional[NetSimObjectName] = None,
        admission_params: Optional[Dict[str, Any]] = None,
        service_func: Optional[Generator[float, None, None]] = None,
    ) -> None:
        """
        Create a PacketQueue with optional length limit and admission control.

        Args:
            ctx: The network simulation context.
            queue_len_limit: The max queue size. If None, unlimited.
            name: Optional override name.
            admission_params: Dict describing admission policy (RED or taildrop).
            service_func: Generator that yields service delay times for each packet.
        """
        super().__init__(ctx, name=name, stat_type=PacketQueueStat)
        self.stat: PacketQueueStat = self.stat
        self._queue: QueueFIFO[Packet] = QueueFIFO(ctx, name=self.name)
        self._queue_len_limit: Optional[int] = queue_len_limit
        self._queue.extend_name("_QueueFIFO")
        self._admission_params: Optional[Dict[str, Any]] = admission_params
        self._queue_state: Optional[QueueState] = None
        self._admission_func: Optional[Callable[[PacketQueue, Packet], bool]] = None
        self._service_func: Optional[Generator[float, None, None]] = service_func
        self._packet_get_callbacks: List[Callable[[PacketQueue, Packet], None]] = []

        # Determine admission policy
        if not admission_params and queue_len_limit is None:
            self._admission_func = self._queue_admission_all
        elif not admission_params and queue_len_limit is not None:
            self._admission_func = self._queue_admission_tail_drop
        elif QUEUE_ADMISSION_TAILDROP_PARAMS.is_valid([admission_params]):
            self._admission_func = self._queue_admission_tail_drop
        elif QUEUE_ADMISSION_RED_PARAMS.is_valid([admission_params]):
            self._admission_func = self._queue_admission_red
            self._queue_state = QueueStateRED(**admission_params)

            def q_time_callback(pq: PacketQueue, p: Packet) -> None:
                # If queue becomes empty after removing a packet, record the idle start time
                if len(pq._queue) == 0:
                    pq._queue_state.q_time = pq.ctx.now

            self._packet_get_callbacks.append(q_time_callback)
        else:
            raise RuntimeError(f"Unknown admission_params: {admission_params}")

    def run(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Coro:
        """
        Main coroutine that:
          1) Retrieves a packet from the FIFO queue
          2) Applies optional service delay
          3) Sends packets to subscribers
        """
        while True:
            packet: Packet = yield self._queue.get()
            if packet:
                self._packet_get(packet)
                if self._service_func:
                    service_delay = next(self._service_func, None)
                    if service_delay is not None:
                        yield self.process.timeout(service_delay)

                for subscriber in self._subscribers:
                    subscriber.put(packet, self)
                self._packet_sent(packet)
            else:
                raise RuntimeError(f"No packet retrieved at {self.ctx.now}")

    def put(
        self,
        item: Packet,
        source: Union[Sender, SenderReceiver],
        *args: List[Any],
        **kwargs: Dict[str, Any],
    ) -> None:
        """
        Called by upstream nodes to enqueue a packet, if admission allows.

        Args:
            item: The packet to enqueue.
            source: The object that generated this packet.
        """
        self._packet_received(item)
        if self._admission_func(packet=item):
            self._queue.put(item)
            self._packet_put(item)
        else:
            self._packet_dropped(item)

    def _queue_admission_tail_drop(self, packet: Packet) -> bool:
        """
        Tail-drop admission control. Drop if queue is full.
        """
        return (self._queue_len_limit is None) or (
            len(self._queue) < self._queue_len_limit
        )

    def _queue_admission_red(self, packet: Packet) -> bool:
        """
        Random Early Detection (RED) admission control.
        """
        assert isinstance(self._queue_state, QueueStateRED)

        # Update average queue size
        if len(self._queue) > 0:
            self._queue_state.avg = (
                1 - self._queue_state.wq
            ) * self._queue_state.avg + self._queue_state.wq * len(self._queue)
        else:
            # Weighted idle: Exponential decay of avg if queue is empty
            idle_time = self.ctx.now - self._queue_state.q_time
            if self._queue_state.s > 0:
                m = idle_time / self._queue_state.s
                self._queue_state.avg *= (1 - self._queue_state.wq) ** m
            else:
                logger.warning(
                    "QueueStateRED parameter 's' <= 0. Skipping idle weighting."
                )
                self._queue_state.avg *= 1 - self._queue_state.wq

        # Hard limit if queue is at capacity
        if (
            self._queue_len_limit is not None
            and len(self._queue) >= self._queue_len_limit
        ):
            self._queue_state.count = 0
            return False

        # Evaluate drop probability
        if self._queue_state.minth <= self._queue_state.avg < self._queue_state.maxth:
            self._queue_state.count += 1
            pb = (
                self._queue_state.maxp
                * (self._queue_state.avg - self._queue_state.minth)
                / (self._queue_state.maxth - self._queue_state.minth)
            )
            denom = 1 - (self._queue_state.count * pb)
            pa = pb / denom if denom > 0 else 1.0
            if random.random() < pa:
                self._queue_state.count = 0
                return False

        elif self._queue_state.avg >= self._queue_state.maxth:
            self._queue_state.count = 0
            return False
        else:
            # avg below minth
            self._queue_state.count = -1

        return True

    def _queue_admission_all(self, packet: Packet) -> bool:
        """
        Always accept the incoming packet (no dropping).
        """
        return True

    def _packet_put(self, packet: Packet) -> None:
        """
        Internal hook: called after a packet is successfully admitted and put in the queue.
        """
        self.stat.packet_put(packet)

    def _packet_get(self, packet: Packet) -> None:
        """
        Internal hook: called after a packet is taken from the queue for service.
        """
        for callback in self._packet_get_callbacks:
            callback(self, packet)
        self.stat.packet_get(packet)

    def _packet_dropped(self, packet: Packet) -> None:
        """
        Internal hook: called when a packet is dropped (due to admission control).
        """
        self.stat.packet_dropped(packet)


class PacketInterfaceRx(PacketQueue):
    """
    An RX interface that simulates an incoming link with an optional propagation delay.
    Unlike a general PacketQueue, it always uses tail-drop if _transmission_len_limit is set.
    """

    def __init__(
        self,
        ctx: NetSimContext,
        propagation_delay: Optional[SimTime] = None,
        transmission_len_limit: Optional[int] = None,
        name: Optional[NetSimObjectName] = None,
    ) -> None:
        """
        Create a PacketInterfaceRx.

        Args:
            ctx: The network simulation context.
            propagation_delay: Delay applied to each packet as it leaves the queue.
            transmission_len_limit: If set, max queue size (tail-drop).
            name: Optional override name.
        """
        super().__init__(ctx, name=name)
        self._propagation_delay: Optional[SimTime] = propagation_delay
        self._transmission_len_limit: Optional[int] = transmission_len_limit

    def run(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Coro:
        """
        Main coroutine that:
          1) Dequeues a packet
          2) Waits optional propagation_delay
          3) Sends packet to subscribers
        """
        while True:
            packet: Packet = yield self._queue.get()
            if packet:
                if self._propagation_delay:
                    yield self.process.timeout(self._propagation_delay)
                self._packet_get(packet)
                for subscriber in self._subscribers:
                    subscriber.put(packet, self)
                self._packet_sent(packet)
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
        Enqueue a packet, using tail-drop if the interface is at capacity.

        Args:
            item: The packet to enqueue.
            source: The node sending this packet.
        """
        self._packet_received(item)
        if self._queue_admission_tail_drop(packet=item):
            self._queue.put(item)
            self._packet_put(item)
        else:
            self._packet_dropped(item)

    def _queue_admission_tail_drop(self, packet: Packet) -> bool:
        """
        Tail-drop admission control for the RX interface.
        """
        return (
            self._transmission_len_limit is None
            or len(self._queue) < self._transmission_len_limit
        )


class PacketInterfaceTx(PacketQueue):
    """
    A TX interface that simulates an outgoing link with a specified bandwidth.
    It will transmit one packet at a time, incurring transmission time based on packet size and link bandwidth.
    """

    def __init__(
        self,
        ctx: NetSimContext,
        bw: InterfaceBW,
        queue_len_limit: Optional[int] = None,
        admission_params: Optional[Dict[str, Any]] = None,
        name: Optional[NetSimObjectName] = None,
    ) -> None:
        """
        Create a PacketInterfaceTx.

        Args:
            ctx: The network simulation context.
            bw: Link bandwidth in bytes/sec or bits/sec (depending on usage).
            queue_len_limit: Max queue size. If None, unlimited.
            admission_params: RED or taildrop admission parameters (optional).
            name: Optional override name.
        """
        super().__init__(
            ctx,
            name=name,
            queue_len_limit=queue_len_limit,
            admission_params=admission_params,
        )
        self._bw: InterfaceBW = bw
        self._busy: bool = False  # Track whether currently transmitting a packet

    def run(self, *args: List[Any], **kwargs: Dict[str, Any]) -> Coro:
        """
        Main coroutine that:
          1) Retrieves a packet
          2) Incurs transmission delay = (packet.size * 8) / bw (bits/sec)
          3) Sends packet to subscribers
        """
        while True:
            packet: Packet = yield self._queue.get()
            if packet:
                self._busy = True
                self._packet_get(packet)

                # Prevent division-by-zero if bw <= 0
                if self._bw <= 0:
                    logger.error(
                        f"Bandwidth is non-positive ({self._bw}) in {self.name}. Cannot transmit."
                    )
                    # We could drop or yield infinitely. Let's just drop:
                    self._packet_dropped(packet)
                    self._busy = False
                    continue

                yield self.process.timeout(packet.size * 8 / self._bw)

                for subscriber in self._subscribers:
                    subscriber.put(packet, self)
                self._packet_sent(packet)
                self._busy = False
            else:
                raise RuntimeError(
                    f"Resumed {self} without packet at {self.ctx.now}",
                )
