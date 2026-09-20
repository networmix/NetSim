"""Client identities, profiles and the protocol-agent contract (Gate C).

Every producer of routes is a client (FBOSS ``ClientID`` / ``FibClient``,
zebra route ``type`` + ``instance``): the user's statics, the oracle IGP,
protocol agents, controllers. Identity is ``(name, instance)``; the
profile carries the default distance and RFC 9256 §2.6 protocol origin.

The second half of this module is the **agent contract** of the design's
"Protocol agents and integration" section (revision 16): the records an
agent reads (``AgentContext`` and its projections, inbox entries), the
records it returns (``AgentOutput`` and its operation records) and the
records the tree keeps about it (``AgentNode``, ``RunReceipt``, NHT and
transport state). Everything here is clock-free and immutable; the runtime
(``netsim.runtime.agents``, ``netsim.runtime.transport``) owns inboxes,
timers, queues and publication.

Contract rules (tested in ``tests/model/test_agent_contract.py``):

- an agent never receives a ``Device`` handle, the model ``_View`` or a
  ``NetworkState``: the context carries only the projections below;
- every payload, state and configuration value is transitively immutable
  (``validate_immutable``), so a frozen envelope cannot carry a mutable
  message or RNG;
- an output is validated as a whole before anything is applied: at most one
  ``RouteOp`` per family, ``sync`` exclusive with ``add`` / ``delete``,
  timers with strictly positive delays, message sizes non-negative;
- state and ``srdb_view`` are compared by identity: returning the context's
  own object means "unchanged".
"""

from __future__ import annotations

import dataclasses
from dataclasses import field
from typing import Any, Protocol, runtime_checkable

from netsim.model.state import PMap, empty_pmap, record, validate_immutable


@record
class ClientId:
    name: str
    instance: int = 0

    def __lt__(self, other: ClientId) -> bool:
        return (self.name, self.instance) < (other.name, other.instance)


@record
class ClientProfile:
    client: ClientId
    distance: int
    protocol_origin: int = 30
    """RFC 9256 §2.6: configuration/CLI 30, BGP SR-TE 20, PCEP 10."""
    originator: tuple[int, int] = (0, 0)
    link_state: bool = False
    """This client's route metrics are IGP costs usable by NHT."""


STATIC = ClientId('static', 0)
IGP = ClientId('igp', 0)

STATIC_PROFILE = ClientProfile(STATIC, distance=1)
IGP_PROFILE = ClientProfile(IGP, distance=110)

CONNECTED = ClientId('connected', 0)
LOCAL = ClientId('local', 0)
CONNECTED_PROFILE = ClientProfile(CONNECTED, distance=0)
LOCAL_PROFILE = ClientProfile(LOCAL, distance=0)

SRV6_LOCAL = ClientId('srv6-local', 0)
SRV6_LOCAL_PROFILE = ClientProfile(SRV6_LOCAL, distance=0)


# ---------------------------------------------------------------------------
# Paths and causes
# ---------------------------------------------------------------------------

Path = tuple[str, ...]
"""A device-scoped subscription path, relative to the device node:
``('interfaces', 'eth1', 'oper')``, ``('ribs', '4')``, ``('neighbors',)``,
``('fibs', '6')``, ``('nht',)``, ``('srv6_sids',)``, ``('config',)``.
A prefix subscribes to the subtree; an ancestor change (replacement or
removal) wakes descendant subscriptions and a descendant change wakes
ancestor subscriptions (contract principle 4)."""

# Cause kinds (why an agent run was scheduled).
CAUSE_INIT = 'init'
CAUSE_RESET = 'reset'
CAUSE_SUBSCRIPTION = 'subscription'
CAUSE_INBOX = 'inbox'
CAUSE_TIMER = 'timer'
CAUSE_SESSION = 'session'
CAUSE_NHT = 'nht'
CAUSE_ROUTES = 'routes'
CAUSE_SIDS = 'sids'
CAUSE_RETRY = 'retry'


@record
class Cause:
    kind: str
    key: Any = None
    """Subscription path, timer name, connection id, NHT key or row key."""


# ---------------------------------------------------------------------------
# Agent configuration, node and receipts (tree records)
# ---------------------------------------------------------------------------


def _check_duration(name: str, value: float, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f'{name} must be a number, got {value!r}')
    if value != value or value in (float('inf'), float('-inf')):
        raise ValueError(f'{name} must be finite, got {value!r}')
    if positive and not value > 0:
        raise ValueError(f'{name} must be strictly positive, got {value!r}')
    if value < 0:
        raise ValueError(f'{name} must be non-negative, got {value!r}')
    return float(value)


@record
class AgentConfig:
    """Immutable per-agent configuration; protocol options live in ``params``."""

    run_delay: float = 1e-3
    """Batching latency between a cause and the run (finite, non-negative;
    zero is permitted and guarded by ``max_rounds_per_timestamp``)."""
    processing_delay: float = 1e-3
    """Added to every datagram and message delivery this agent sends. The
    default of one millisecond keeps deliveries strictly in the future even
    over zero-delay links (the engine never delivers a message at the time
    it was sent); a zero value is valid only with positive link delays and
    the transport rejects a send whose delivery would not advance the clock."""
    inbox_limit: int = 10_000
    """Maximum captured plus uncaptured inbox entries; overflow is an explicit
    rejection of the delivery (never silent truncation)."""
    queue_limit: int = 1_000
    """Per-connection queued messages per direction."""
    byte_limit: int = 1 << 20
    """Per-connection queued bytes per direction."""
    listen_ports: tuple[int, ...] = ()
    params: Any = None
    """Protocol-specific immutable options (peers, AS, hold timers ...)."""

    def __post_init__(self) -> None:
        _check_duration('run_delay', self.run_delay)
        _check_duration('processing_delay', self.processing_delay)
        for name in ('inbox_limit', 'queue_limit', 'byte_limit'):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f'{name} must be a positive integer')
        for port in self.listen_ports:
            if not isinstance(port, int) or not 0 <= port <= 65535:
                raise ValueError(f'invalid listen port {port!r}')
        validate_immutable(self.params, 'AgentConfig.params')


@record
class RunReceipt:
    """Identity of one captured agent run.

    ``(device_generation, agent_generation, run_id)`` is the publication
    key: a receipt of a stale generation is invalid, a published receipt is
    consumed exactly once (its inbox prefix and outbox), a rejected receipt
    keeps its captured prefix, pre-run state and RNG for ``retry()``.
    """

    device_generation: int
    agent_generation: int
    run_id: int
    time: float
    status: str = 'PUBLISHED'
    """``PUBLISHED`` | ``REJECTED``."""
    reason: str | None = None
    inbox_consumed: int = 0
    """Length of the captured inbox prefix this run consumed."""
    causes_count: int = 0
    ops_count: int = 0


RECEIPT_PUBLISHED = 'PUBLISHED'
RECEIPT_REJECTED = 'REJECTED'


@record
class AgentNode:
    """A device's agent as the tree sees it (``DeviceState.agents[name]``).

    ``state`` and ``srdb_view`` are opaque and identity-compared; ``rng``
    is the committed ``random.Random`` state (an immutable tuple) or
    ``None`` before the first run; ``initialized`` becomes true once
    ``on_init`` has been published; ``generation`` changes on reset.
    """

    name: str
    generation: int
    client: ClientId
    config: AgentConfig = field(default_factory=AgentConfig)
    state: Any = None
    srdb_view: Any = None
    rng: Any = None
    initialized: bool = False
    receipt: RunReceipt | None = None
    """The last published receipt (``None`` before the first publication)."""
    runs: int = 0
    """Published runs of this generation."""

    def __eq__(self, other: object) -> bool:
        """Opaque values compare by identity, never structurally: a plugin's
        ``__eq__`` is never consulted by the tree, and a fresh equal-valued
        state is a change (identity is the canonicalization contract)."""
        if other is self:
            return True
        if not isinstance(other, AgentNode):
            return NotImplemented
        return (
            self.state is other.state
            and self.srdb_view is other.srdb_view
            and self.name == other.name
            and self.generation == other.generation
            and self.client == other.client
            and self.config == other.config
            and self.rng == other.rng
            and self.initialized == other.initialized
            and self.receipt == other.receipt
            and self.runs == other.runs
        )

    __hash__ = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Context projections (what an agent may see of its device)
# ---------------------------------------------------------------------------


@record
class LinkView:
    """Own attributes of the link behind an interface: never the peer."""

    index: int
    delay: float
    capacity: float


@record
class InterfaceView:
    name: str
    index: int
    kind: int
    """``LOOPBACK`` | ``ETHERNET`` | ``PORT_CHANNEL`` (``interfaces.InterfaceKind``)."""
    admin: int
    oper: int
    """Effective oper state after debounce (RFC 2863 value)."""
    reason: Any = None
    since: float = 0.0
    mtu: int = 1500
    metric: int = 1
    ipv4: tuple[tuple[int, int], ...] = ()
    ipv6: tuple[tuple[int, int], ...] = ()
    unnumbered: bool = False
    link_local: int | None = None
    mac: int | None = None
    l3_usable_v4: bool = False
    l3_usable_v6: bool = False
    aggregate_id: str | None = None
    """Bundle this Ethernet belongs to, if any."""
    members: tuple[str, ...] = ()
    """Active members of a PortChannel."""
    bandwidth: float | None = None
    link: LinkView | None = None
    generation: int = 0
    """Interface incarnation for scoped NHT registrations."""
    config: Any = None
    """Immutable local interface configuration; no carrier or peer state."""


@record
class NeighborView:
    interface: str
    af: int
    address: int
    mac: int
    scope: str | None = None
    """Interface name for link-local scoped entries."""


@record
class RouteView:
    """One RIB row of the device with its programming status."""

    route: Any
    """``routing.Route``."""
    status: str
    """``PENDING`` | ``INSTALLED`` | ``NOT_INSTALLED``."""
    reason: str | None = None
    selected: bool = False
    """The row is in the group the installed FIB entry uses."""


@record
class LookupView:
    """Installed forwarding for one address (``AgentContext.lookup``)."""

    af: int
    address: int
    prefix: tuple[int, int] | None
    adjacencies: tuple[Any, ...] = ()
    """``forwarding.Adjacency`` legs (empty for drops and misses)."""
    action: Any = None
    """Special action (``DROP_*``, ``RECEIVE``) when the entry is not a group."""
    fib_version: int = 0
    processed_epoch: int = 0
    status: str = 'INSTALLED'
    """``INSTALLED`` (the FIB reflects every committed input) or ``PENDING``
    (the resolver has not consumed the current input epoch)."""


@record
class SidResultView:
    request_id: str
    behavior: int
    sid: int | None
    """Allocated SID value or ``None`` when the request failed."""
    length: int | None = None
    adjacency_up: bool = False
    reason: str | None = None


@record
class ConnectionView:
    """The agent-visible state of one of its connections.

    Path reachability (derived by the TRANSPORT kind from global forwarding)
    is deliberately absent: an agent learns about a broken path only through
    session events and timeouts, never ahead of detection.
    """

    id: int
    state: str
    """``LISTEN`` | ``CONNECTING`` | ``ESTABLISHED`` | ``DOWN``."""
    local: Endpoint
    remote: Endpoint | None
    initiator: bool
    reason: str | None = None
    generation: int = 0
    """Local endpoint incarnation; late events of an older one are no-ops."""
    queued_messages: int = 0
    queued_bytes: int = 0


# ---------------------------------------------------------------------------
# Inbox entries (runtime -> agent)
# ---------------------------------------------------------------------------


@record
class Endpoint:
    """A transport endpoint: family, address, scope (interface for
    link-local) and port. Ports are per family and address."""

    af: int
    address: int
    port: int
    scope: str | None = None

    def __post_init__(self) -> None:
        if not 0 <= self.port <= 65535:
            raise ValueError(f'invalid port {self.port}')


@record
class Sender:
    """Scoped identity of a datagram's sender: the receiving interface, the
    source endpoint as observed on the wire. Never a device name."""

    interface: str
    endpoint: Endpoint


@record
class Delivery:
    """A delivered datagram (``interface``) or session message (``connection``)."""

    time: float
    payload: Any
    interface: str | None = None
    sender: Sender | None = None
    connection: int | None = None
    port: int = 0
    seq: int = 0
    """Per-direction sequence number for session messages."""
    generation: int | None = None

    def __post_init__(self) -> None:
        if (self.interface is None) == (self.connection is None):
            raise ValueError('a delivery is either a datagram or a session message')


@record
class TimerFired:
    time: float
    name: str
    generation: int | None = None


# Session event states and reasons.
LISTEN = 'LISTEN'
CONNECTING = 'CONNECTING'
ESTABLISHED = 'ESTABLISHED'
DOWN = 'DOWN'

REFUSED = 'REFUSED'
TIMEOUT = 'TIMEOUT'
CLOSED = 'CLOSED'
ABORTED = 'ABORTED'
RESET = 'RESET'
UNREACHABLE_ENDPOINT = 'UNREACHABLE'
OVERFLOW = 'OVERFLOW'


@record
class SessionEvent:
    time: float
    connection: int
    state: str
    reason: str | None = None
    local: Endpoint | None = None
    remote: Endpoint | None = None
    initiator: bool = False
    generation: int | None = None


@record
class Rejection:
    """An explicit overflow or admission failure reported to the sender
    (a rejected datagram or message is never dropped silently)."""

    time: float
    reason: str
    connection: int | None = None
    interface: str | None = None
    detail: Any = None
    generation: int | None = None


InboxEntry = Delivery | TimerFired | SessionEvent | Rejection


# ---------------------------------------------------------------------------
# SR-DB view advertised by an agent (policy validation with srdb_source)
# ---------------------------------------------------------------------------


@record
class RemoteSid:
    """A claim about a SID learned by protocol, never read from the oracle."""

    sid: int
    length: int
    behavior: int
    flavors: int = 0
    structure: Any = None
    owner: str | None = None
    """Router id or advertised node name of the SID's owner."""
    adjacency_up: bool = True
    peer: str | None = None
    """For adjacency SIDs: the advertised peer identity."""
    interface: str | None = None
    """Advertised interface name for symbolic AdjSeg resolution."""


@record
class SrDbView:
    sids: tuple[RemoteSid, ...] = ()
    locators: tuple[tuple[str, tuple[int, int]], ...] = ()
    """``(owner, prefix)`` pairs."""
    version: int = 0


# ---------------------------------------------------------------------------
# Operation records (agent -> runtime; applied only by publication)
# ---------------------------------------------------------------------------


@record
class RouteOp:
    """One family's routing operation: either ``sync`` (replace every row of
    the client in this family) or ``delete`` then ``add``."""

    af: int
    add: tuple[Any, ...] = ()
    delete: tuple[Any, ...] = ()
    sync: tuple[Any, ...] | None = None

    def __post_init__(self) -> None:
        if self.sync is not None and (self.add or self.delete):
            raise ValueError('RouteOp: sync is exclusive with add/delete')


ADD_POLICY = 'add'
REPLACE_POLICY = 'replace'
DELETE_POLICY = 'delete'
SET_STEERING = 'steer'


@record
class PolicyOp:
    kind: str
    policy: Any = None
    """``srv6.SrPolicy`` for add / replace."""
    key: tuple[int, int] | None = None
    """``(color, endpoint)`` for delete."""
    rules: tuple[Any, ...] = ()
    """``srv6.SteeringRule`` rows for ``SET_STEERING`` (replaces the client's)."""
    locator: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in (ADD_POLICY, REPLACE_POLICY, DELETE_POLICY, SET_STEERING):
            raise ValueError(f'unknown policy op {self.kind!r}')
        if self.kind in (ADD_POLICY, REPLACE_POLICY) and self.policy is None:
            raise ValueError(f'{self.kind} needs a policy')
        if self.kind == DELETE_POLICY and self.key is None:
            raise ValueError('delete needs a key')


REQUEST_SID = 'request'
RELEASE_SID = 'release'


@record
class SidOp:
    kind: str
    request_id: str
    behavior: int = 0
    args: tuple[tuple[str, Any], ...] = ()

    def __post_init__(self) -> None:
        if self.kind not in (REQUEST_SID, RELEASE_SID):
            raise ValueError(f'unknown SID op {self.kind!r}')


@record
class NhtKey:
    """Registration identity (owner, family, address, scope, options)."""

    owner: ClientId
    af: int
    address: int
    interface: str | None = None
    """Scope interface for link-local addresses."""
    interface_generation: int | None = None
    connected_only: bool = False
    resolve_via_default: bool = False

    def __lt__(self, other: NhtKey) -> bool:
        return self._sort_key() < other._sort_key()

    def _sort_key(self) -> tuple:
        return (
            self.owner,
            self.af,
            self.address,
            self.interface or '',
            self.interface_generation or 0,
            self.connected_only,
            self.resolve_via_default,
        )


REGISTER_NHT = 'register'
UNREGISTER_NHT = 'unregister'


@record
class NhtOp:
    kind: str
    key: NhtKey

    def __post_init__(self) -> None:
        if self.kind not in (REGISTER_NHT, UNREGISTER_NHT):
            raise ValueError(f'unknown NHT op {self.kind!r}')


@record
class Datagram:
    """A link-channel send: from this device's ``interface``, to the peer on
    the wire (no address needed on a point-to-point link or bundle).
    ``size`` is the modeled byte count (used for admission and stats)."""

    interface: str
    payload: Any
    port: int = 0
    af: int = 6
    size: int = 0

    def __post_init__(self) -> None:
        if not 0 <= self.port <= 65535:
            raise ValueError(f'invalid port {self.port}')
        if self.size < 0:
            raise ValueError('size must be non-negative')
        validate_immutable(self.payload, 'Datagram.payload')


@record
class Message:
    """A reliable-message send on an established connection."""

    connection: int
    payload: Any
    size: int = 0

    def __post_init__(self) -> None:
        if self.size < 0:
            raise ValueError('size must be non-negative')
        validate_immutable(self.payload, 'Message.payload')


LISTEN_OP = 'listen'
UNLISTEN_OP = 'unlisten'
OPEN_OP = 'open'
CLOSE_OP = 'close'
ABORT_OP = 'abort'


@record
class SessionOp:
    """Session lifecycle request.

    ``listen`` / ``unlisten`` take ``local``; ``open`` takes ``local`` and
    ``remote`` (an active open against a listener; ``timeout`` is the
    no-progress timeout of the connection); ``close`` drains queued messages
    then closes; ``abort`` invalidates every pending, in-flight and queued
    entry of the incarnation.
    """

    kind: str
    local: Endpoint | None = None
    remote: Endpoint | None = None
    connection: int | None = None
    timeout: float = 30.0

    def __post_init__(self) -> None:
        if self.kind not in (LISTEN_OP, UNLISTEN_OP, OPEN_OP, CLOSE_OP, ABORT_OP):
            raise ValueError(f'unknown session op {self.kind!r}')
        if self.kind in (LISTEN_OP, UNLISTEN_OP, OPEN_OP) and self.local is None:
            raise ValueError(f'{self.kind} needs a local endpoint')
        if self.kind == OPEN_OP and self.remote is None:
            raise ValueError('open needs a remote endpoint')
        if self.kind in (CLOSE_OP, ABORT_OP) and self.connection is None:
            raise ValueError(f'{self.kind} needs a connection id')
        _check_duration('timeout', self.timeout, positive=True)


@record
class TimerOp:
    """``delay`` strictly positive arms (re-arms) the timer; ``None`` cancels."""

    name: str
    delay: float | None = None

    def __post_init__(self) -> None:
        if self.delay is not None:
            _check_duration('delay', self.delay, positive=True)


_MUTABLE_TOP = (list, dict, set, bytearray, memoryview)


@record
class AgentOutput:
    """What one run returns. ``state`` and ``srdb_view`` are the new values
    (return the context's objects to keep them); ``stats`` are counter
    increments buffered by the context; everything else is applied only by
    the runtime's publication."""

    state: Any = None
    route_ops: tuple[RouteOp, ...] = ()
    policy_ops: tuple[PolicyOp, ...] = ()
    sid_ops: tuple[SidOp, ...] = ()
    nht_ops: tuple[NhtOp, ...] = ()
    datagrams: tuple[Datagram, ...] = ()
    messages: tuple[Message, ...] = ()
    sessions: tuple[SessionOp, ...] = ()
    timers: tuple[TimerOp, ...] = ()
    srdb_view: Any = None
    stats: tuple[tuple[str, float], ...] = ()

    def __post_init__(self) -> None:
        families = [op.af for op in self.route_ops]
        if len(families) != len(set(families)):
            raise ValueError('at most one RouteOp per address family')
        # The state is opaque and may be large: only a mutable container at
        # the top is rejected here; the runtime validates a newly admitted
        # state transitively once (a state returned by identity is trusted).
        if isinstance(self.state, _MUTABLE_TOP):
            raise TypeError(f'mutable {type(self.state).__name__} as agent state')
        if self.srdb_view is not None and not isinstance(self.srdb_view, SrDbView):
            raise TypeError('srdb_view must be an SrDbView or None')
        for entry in self.stats:
            if (
                not isinstance(entry, tuple)
                or len(entry) != 2
                or not isinstance(entry[0], str)
                or isinstance(entry[1], bool)
                or not isinstance(entry[1], (int, float))
            ):
                raise TypeError(f'stats entries are (name, number): {entry!r}')

    def is_noop(self) -> bool:
        return not (
            self.route_ops
            or self.policy_ops
            or self.sid_ops
            or self.nht_ops
            or self.datagrams
            or self.messages
            or self.sessions
            or self.timers
            or self.stats
        )


# ---------------------------------------------------------------------------
# NHT tree records (C2 computes results in the FIB kind)
# ---------------------------------------------------------------------------


@record
class NhtResult:
    """The answer to one registration: a pure RIB query, never FIB identity."""

    eligible: bool
    input_epoch: int
    """Epoch that produced this semantic answer. A registration may reuse it
    after an equivalent refresh; NhtTable.input_epochs tracks the latest check.
    A direct nht.resolve query always carries its caller's current epoch."""
    via_prefix: tuple[int, int] | None = None
    via_source: ClientId | None = None
    cost: int | None = None
    """IGP cost to the queried next hop, or ``None`` when unavailable."""
    cost_source: ClientId | None = None
    """Provenance of ``cost`` (the client whose metric it is)."""
    legs: tuple[Any, ...] = ()
    """Resolved ``forwarding.Adjacency`` legs of the answer."""
    queries: tuple[tuple[int, int, bool], ...] = ()
    """``(af, address, found)`` lookups performed, failed ones included."""
    reason: str | None = None
    interfaces: tuple[str, ...] = ()
    """Consulted interfaces, including failed adjacency resolution."""


@record
class NhtTable:
    """``DeviceState.nht``: registrations and their current results."""

    registrations: PMap[NhtKey, NhtResult | None] = field(default_factory=empty_pmap)
    version: int = 0
    input_epochs: PMap[int, int] = field(default_factory=empty_pmap)
    """Latest resolver input epoch checked for each registered address family.
    Kept outside NhtResult so epoch-only updates preserve notification identity."""


# ---------------------------------------------------------------------------
# Transport tree records (C3 derives them in the TRANSPORT kind)
# ---------------------------------------------------------------------------


@record
class ConnectionState:
    """One connection as committed in ``NetworkState.transport``.

    Sequence numbers and queues are runtime state; the tree keeps what is
    observable: endpoints, generations, state and path reachability.
    """

    id: int
    a_device: str
    a_agent: str
    a_local: Endpoint
    a_generation: int
    b_device: str | None = None
    b_agent: str | None = None
    b_local: Endpoint | None = None
    b_generation: int = 0
    initiator_a: bool = True
    state: str = CONNECTING
    reason: str | None = None
    timeout: float = 30.0
    a_to_b_reachable: bool = False
    b_to_a_reachable: bool = False
    deps: tuple[str, ...] = ()
    """Devices visited by the previous path derivation (both directions)."""
    a_interface: str | None = None
    b_interface: str | None = None
    a_interface_generation: int = 0
    b_interface_generation: int = 0
    a_to_b_delay: float = 0.0
    b_to_a_delay: float = 0.0
    draining: bool = False
    """Close requested; accepted messages drain before DOWN/CLOSED."""


@record
class Listener:
    device: str
    agent: str
    endpoint: Endpoint
    generation: int
    interface: str | None = None
    interface_generation: int = 0


@record
class TransportState:
    listeners: PMap[Any, Listener] = field(default_factory=empty_pmap)
    """Keyed by ``(device, af, address, scope, port)``."""
    connections: PMap[int, ConnectionState] = field(default_factory=empty_pmap)
    next_connection: int = 1
    version: int = 0


# ---------------------------------------------------------------------------
# The context and the agent protocol
# ---------------------------------------------------------------------------


class AgentStats:
    """Buffered counters; flushed by the runtime on publication."""

    __slots__ = ('_items',)

    def __init__(self) -> None:
        self._items: list[tuple[str, float]] = []

    def add(self, name: str, value: float = 1.0) -> None:
        self._items.append((name, float(value)))

    def drain(self) -> tuple[tuple[str, float], ...]:
        items, self._items = tuple(self._items), []
        return items


class AgentContext(Protocol):
    """Read-only projection an agent runs against (built by the runtime).

    Nothing here is a handle, a view over the whole tree or a runtime
    object; every attribute is a frozen record, a tuple, a ``PMap`` or a
    scalar, except ``rng`` (a private ``random.Random`` seeded from the
    committed state) and ``stats`` (a buffer the runtime drains).
    """

    @property
    def now(self) -> float: ...

    @property
    def device(self) -> str: ...

    @property
    def agent(self) -> str: ...

    @property
    def client(self) -> ClientId: ...

    @property
    def generation(self) -> int: ...

    @property
    def config(self) -> Any:
        """The device's ``DeviceConfig`` (enabled, seed, delays, SRv6 settings)."""
        ...

    @property
    def agent_config(self) -> AgentConfig: ...

    @property
    def router_id(self) -> int: ...

    @property
    def interfaces(self) -> PMap[str, InterfaceView]: ...

    @property
    def neighbors(self) -> tuple[NeighborView, ...]: ...

    def rib_view(self, af: int) -> tuple[RouteView, ...]: ...

    def lookup(self, af: int, address: int, scope: str | None = None) -> LookupView: ...

    @property
    def nht(self) -> PMap[NhtKey, NhtResult | None]: ...

    @property
    def sid_results(self) -> tuple[SidResultView, ...]: ...

    @property
    def srdb_local(self) -> Any:
        """The device's own ``Srv6Sids`` (locators and local SIDs)."""
        ...

    @property
    def policy_states(self) -> PMap[Any, Any]: ...

    @property
    def agent_state(self) -> Any: ...

    @property
    def srdb_view(self) -> Any: ...

    @property
    def inbox(self) -> tuple[InboxEntry, ...]:
        """The captured prefix consumed when this run is published."""
        ...

    @property
    def connections(self) -> PMap[int, ConnectionView]: ...

    @property
    def timers(self) -> PMap[str, float]:
        """Armed timers and their firing times."""
        ...

    @property
    def causes(self) -> tuple[Cause, ...]: ...

    @property
    def rng(self) -> Any: ...

    @property
    def stats(self) -> AgentStats: ...


@runtime_checkable
class DeviceAgent(Protocol):
    """A protocol plugin. ``config`` and ``profile`` are immutable; the
    object carries no evolving state (that lives in ``AgentNode.state``)."""

    @property
    def client(self) -> ClientId: ...

    @property
    def profile(self) -> ClientProfile: ...

    @property
    def config(self) -> AgentConfig: ...

    def subscriptions(self) -> tuple[Path, ...]: ...

    def on_init(self, ctx: AgentContext) -> AgentOutput: ...

    def on_run(self, ctx: AgentContext) -> AgentOutput: ...


def check_agent(agent: Any) -> None:
    """Validate a plugin object against the contract (used at registration)."""
    if not isinstance(agent, DeviceAgent):
        raise TypeError('agent does not implement DeviceAgent')
    if not isinstance(agent.client, ClientId):
        raise TypeError('agent.client must be a ClientId')
    if not isinstance(agent.profile, ClientProfile):
        raise TypeError('agent.profile must be a ClientProfile')
    if agent.profile.client != agent.client:
        raise ValueError('agent.profile.client must equal agent.client')
    if not isinstance(agent.config, AgentConfig):
        raise TypeError('agent.config must be an AgentConfig')
    for path in agent.subscriptions():
        if not isinstance(path, tuple) or not all(isinstance(p, str) for p in path):
            raise TypeError(f'subscription {path!r} is not a tuple of strings')
        if path and path[0] in ('devices', 'links', 'demands', 'placement'):
            raise ValueError(f'subscription {path!r} is not device-scoped')


def check_output(output: Any) -> AgentOutput:
    """Validate a run's return value; raises before anything is applied."""
    if not isinstance(output, AgentOutput):
        raise TypeError('an agent run must return an AgentOutput')
    for rop in output.route_ops:
        for row in rop.add + (rop.sync or ()):
            if row.af != rop.af:
                raise ValueError('RouteOp: row family does not match')
    return output


def unchanged(old: Any, new: Any) -> bool:
    """Identity comparison used for agent state and srdb_view."""
    return old is new


def replace(node: Any, **changes: Any) -> Any:
    return dataclasses.replace(node, **changes)


__all__ = [
    'ABORT_OP',
    'ABORTED',
    'ADD_POLICY',
    'AgentConfig',
    'AgentContext',
    'AgentNode',
    'AgentOutput',
    'AgentStats',
    'CAUSE_INBOX',
    'CAUSE_INIT',
    'CAUSE_NHT',
    'CAUSE_RESET',
    'CAUSE_RETRY',
    'CAUSE_ROUTES',
    'CAUSE_SESSION',
    'CAUSE_SIDS',
    'CAUSE_SUBSCRIPTION',
    'CAUSE_TIMER',
    'CLOSE_OP',
    'CLOSED',
    'CONNECTED',
    'CONNECTED_PROFILE',
    'CONNECTING',
    'Cause',
    'ClientId',
    'ClientProfile',
    'ConnectionState',
    'ConnectionView',
    'DELETE_POLICY',
    'DOWN',
    'Datagram',
    'Delivery',
    'DeviceAgent',
    'ESTABLISHED',
    'Endpoint',
    'IGP',
    'IGP_PROFILE',
    'InboxEntry',
    'InterfaceView',
    'LISTEN',
    'LISTEN_OP',
    'LOCAL',
    'LOCAL_PROFILE',
    'LinkView',
    'Listener',
    'LookupView',
    'Message',
    'NeighborView',
    'NhtKey',
    'NhtOp',
    'NhtResult',
    'NhtTable',
    'OPEN_OP',
    'OVERFLOW',
    'Path',
    'PolicyOp',
    'RECEIPT_PUBLISHED',
    'RECEIPT_REJECTED',
    'REFUSED',
    'REGISTER_NHT',
    'RELEASE_SID',
    'REPLACE_POLICY',
    'REQUEST_SID',
    'RESET',
    'Rejection',
    'RemoteSid',
    'RouteOp',
    'RouteView',
    'RunReceipt',
    'SET_STEERING',
    'SRV6_LOCAL',
    'SRV6_LOCAL_PROFILE',
    'STATIC',
    'STATIC_PROFILE',
    'Sender',
    'SessionEvent',
    'SessionOp',
    'SidOp',
    'SidResultView',
    'SrDbView',
    'TIMEOUT',
    'TimerFired',
    'TimerOp',
    'TransportState',
    'UNLISTEN_OP',
    'UNREACHABLE_ENDPOINT',
    'UNREGISTER_NHT',
    'check_agent',
    'check_output',
    'replace',
    'unchanged',
]
