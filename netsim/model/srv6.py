"""SRv6 data model: the Gate B contract shared by every implementation slice.

Frozen records, constants and pure SR-DB operations live here. Device handles
commit validated allocation and ownership changes; L3 derives adjacency state.
Compression, encapsulation and forwarding use these shared records. Standards: RFC 8402 (SR architecture), RFC 8754 (SRH), RFC 8986
(network programming), RFC 9256 (SR Policy), RFC 9602 (5f00::/16),
RFC 9800 (NEXT-C-SID). The authoritative Gate B behaviour set is the one
in the design (revision 15): H.Encaps and H.Encaps.Red at the headend;
End and End.X with the PSP and USD flavors; End.DT46; NEXT-C-SID for
those three (uN, uA, uDT46). Anything else is rejected at configuration.
"""

from __future__ import annotations

from dataclasses import field, replace
from ipaddress import IPv6Address, IPv6Network, summarize_address_range
from typing import TYPE_CHECKING, Any, Iterable

from netsim.model.contracts import STATIC, ClientId, RemoteSid, SrDbView
from netsim.model.state import PMap, canon, empty_pmap, record

if TYPE_CHECKING:
    from netsim.model.state import DeviceState, NetworkState

# ---------------------------------------------------------------------------
# Behaviours, flavors, headend behaviours (small ints on the hot path)
# ---------------------------------------------------------------------------

END = 1
END_X = 2
END_DT46 = 3
# Post-Gate-C behaviours are named so configuration can reject them by name.
END_DT4 = 4
END_DT6 = 5
END_DX4 = 6
END_DX6 = 7
END_B6_ENCAPS = 8

GATE_B_BEHAVIORS = frozenset({END, END_X, END_DT46})

# Flavors are bit flags (RFC 8986 §4.16, RFC 9800 §4).
PSP = 1
USP = 2
USD = 4
NEXT_CSID = 8
GATE_B_FLAVORS = PSP | USD | NEXT_CSID
"""USP is rejected in Gate B; PSP and USD may be combined with NEXT_CSID."""

# Headend behaviours (RFC 8986 §5).
H_ENCAPS = 1
H_ENCAPS_RED = 2

# Policy fallback when no candidate path is valid.
FALLBACK_IGP = 1
FALLBACK_DROP = 2

# Policy status values.
POLICY_UP = 'UP'
POLICY_DOWN = 'DOWN'

# Protocol origins (RFC 9256 §2.6).
ORIGIN_PCEP = 10
ORIGIN_BGP_SRTE = 20
ORIGIN_CLI = 30

# Steering forms (see the design's action-program STEER step).
STEER_BSID = 'bsid'
STEER_PER_DESTINATION = 'per_destination'
STEER_PER_FLOW = 'per_flow'
STEER_DEMAND = 'demand'


# ---------------------------------------------------------------------------
# SID structure and formats (RFC 9800 §3 and §4)
# ---------------------------------------------------------------------------


@record
class SidStructure:
    """Locator-Block, Locator-Node, Function and Argument lengths in bits.
    ``lbl + lnl + fl + al`` is at most 128; a NEXT-C-SID container holds
    ``lbl`` block bits followed by C-SIDs of ``lnl + fl`` bits each."""

    lbl: int
    lnl: int
    fl: int
    al: int

    def __post_init__(self) -> None:
        for name in ('lbl', 'lnl', 'fl', 'al'):
            v = getattr(self, name)
            if not isinstance(v, int) or v < 0:
                raise ValueError(f'{name} must be a non-negative int')
        if self.total_bits > 128:
            raise ValueError('SID structure exceeds 128 bits')

    @property
    def lnfl(self) -> int:
        return self.lnl + self.fl

    @property
    def total_bits(self) -> int:
        return self.lbl + self.lnl + self.fl + self.al

    @property
    def installed_length(self) -> int:
        """Prefix length of the installed local SID: block, node and function."""
        return self.lbl + self.lnl + self.fl


UNCOMPRESSED = SidStructure(48, 16, 16, 48)
F3216_GIB = SidStructure(32, 16, 0, 80)
"""Bare uN: block ‖ node id (GIB), installed as /48."""
F3216_LIB = SidStructure(32, 0, 16, 80)
"""Bare uA (LIB): block ‖ function, installed as /48."""
F3216_WLIB = SidStructure(32, 0, 32, 64)
"""Wide LIB: block ‖ 32-bit function, installed as /64."""
F3216_TERMINAL = SidStructure(32, 0, 16, 0)
"""Bare terminal uDT46: block ‖ function with no argument, installed as /48."""
F3216_COMPOSITE = SidStructure(32, 16, 16, 64)
"""FRR / SONiC form: block ‖ node ‖ function, installed as /64."""

# Default C-SID ranges inside a block (Cisco/FRR conventions), inclusive.
GIB_RANGE = (0x0001, 0xDFFF)
LIB_RANGE = (0xE000, 0xFFF6)
WLIB_RANGE = (0xFFF7, 0xFFFF)
DEFAULT_BLOCK = (0x5F00 << 112, 32)
"""``5f00::/32`` inside RFC 9602's ``5f00::/16``: (network int, prefix length)."""


# ---------------------------------------------------------------------------
# Locators and local SIDs (per device, one writer: L3 for oper, clients for config)
# ---------------------------------------------------------------------------


@record
class Locator:
    name: str
    prefix: tuple[int, int]
    """Installed locator prefix ``(network, length)``; originated by the oracle."""
    structure: SidStructure = UNCOMPRESSED
    block: tuple[int, int] = DEFAULT_BLOCK
    node_id: int | None = None
    """Node identifier in the GIB (NEXT-C-SID formats) or ``None``."""
    algorithm: int = 0


@record
class LocalSid:
    """One entry of the My-SID table (config plus derived oper)."""

    sid: int
    """The SID value as a 128-bit int (bare or composite form, literally)."""
    length: int
    """Installed prefix length (``structure.installed_length``)."""
    behavior: int
    flavors: int = 0
    structure: SidStructure = UNCOMPRESSED
    owner: ClientId = ClientId('static', 0)
    interface: str | None = None
    """End.X / uA: the bound interface (bundle or Ethernet)."""
    nexthop: int | None = None
    """End.X / uA: optional peer address; ``None`` = interface-only adjacency."""
    request_id: str | None = None
    adjacency_up: bool = True
    """Derived: the bound adjacency is usable; the srv6-local row is installed only then."""


@record
class SidRanges:
    """Inclusive 16-bit GIB, LIB and WLIB ranges for one locator block.

    WLIB selects the high 16 bits of a 32-bit function (NetSim F3216 convention).
    GIB/LIB scope and disjointness follow RFC 9800 §§5.1-5.2.
    Allocation divides each function pool into four disjoint purpose ranges.
    """

    gib: tuple[int, int] = GIB_RANGE
    lib: tuple[int, int] = LIB_RANGE
    wlib: tuple[int, int] = WLIB_RANGE

    def __post_init__(self) -> None:
        ranges = (self.gib, self.lib, self.wlib)
        for lo, hi in ranges:
            if not 1 <= lo <= hi <= 0xFFFF:
                raise ValueError('SID ranges must be nonzero inclusive 16-bit ranges')
        for i, (lo, hi) in enumerate(ranges):
            for other_lo, other_hi in ranges[i + 1 :]:
                if lo <= other_hi and other_lo <= hi:
                    raise ValueError('GIB, LIB and WLIB must be pairwise disjoint')


@record
class SidRequest:
    """Allocation receipt; current oper state is in Srv6Sids.sids[result.sid]."""

    owner: ClientId
    request_id: str
    behavior: int
    args: tuple[tuple[str, Any], ...]
    result: LocalSid


@record
class Srv6Sids:
    """A device's SR-DB local part: locators, local SIDs and the allocator."""

    locators: PMap[str, Locator] = field(default_factory=empty_pmap)
    sids: PMap[int, LocalSid] = field(default_factory=empty_pmap)
    """Keyed by SID value."""
    requests: PMap[str, Any] = field(default_factory=empty_pmap)
    """Client-qualified request_key() strings map to SidRequest allocation results."""
    drop_unknown_local: bool = True
    """Cover the LIB and WLIB ranges with UNREACHABLE rows (``SID_UNKNOWN`` drops)."""
    next_function: PMap[str, int] = field(default_factory=empty_pmap)
    """Cursors keyed by block, locator, node/function widths and purpose.
    Purpose is adjacency, terminal, bsid or client; keys are allocator-private.
    """
    ranges: PMap[tuple[int, int], SidRanges] = field(default_factory=empty_pmap)


# ---------------------------------------------------------------------------
# Segment lists and SR Policies (RFC 9256)
# ---------------------------------------------------------------------------


@record
class LiteralSid:
    address: int
    structure: SidStructure | None = None
    """Compression metadata; unknown or invalid metadata keeps the SID literal."""
    flavors: int | None = None


@record
class AdjSeg:
    device: str
    interface: str


@record
class NodeSeg:
    device: str


@record
class TermSeg:
    device: str
    behavior: int = END_DT46


Segment = LiteralSid | AdjSeg | NodeSeg | TermSeg


@record
class SegmentList:
    segments: tuple[Any, ...]
    weight: int = 1
    name: str | None = None


@record
class CandidatePath:
    preference: int = 100
    segment_lists: tuple[SegmentList, ...] = ()
    name: str | None = None
    protocol_origin: int = ORIGIN_CLI
    originator: tuple[int, int] = (0, 0)
    discriminator: int = 0


@record
class SrPolicy:
    owner: ClientId
    color: int
    endpoint: int
    """IPv6 address of the endpoint (128-bit int)."""
    name: str | None = None
    bsid: int | None = None
    candidate_paths: tuple[CandidatePath, ...] = ()
    fallback: int = FALLBACK_IGP

    @property
    def key(self) -> tuple[int, int]:
        return (self.color, self.endpoint)


@record
class PolicyState:
    """Derived by the FIB kind: validity, selection and programming."""

    active_path: int | None = None
    """Index into ``candidate_paths`` of the selected path, or ``None``."""
    valid_lists: tuple[tuple[int, int, tuple[int, ...]], ...] = ()
    """``(path index, list index, wire entries)`` for every valid list of the active path."""
    status: str = POLICY_DOWN
    basic_valid: tuple[tuple[int, int], ...] = ()
    """RFC 9256 §5.1 validity, including mandatory first-entry resolution.

    Each item is ``(path index, list index)``; structure alone is insufficient.
    """
    reasons: tuple[tuple[int, int, str], ...] = ()
    """Rejections ``(path index, list index, reason)`` from any validation layer."""
    programmed_version: int = 0
    """Last installed IPv6 FIB version, retained while programming is PENDING."""
    strict_valid: tuple[tuple[int, int], ...] = ()
    """Lists that pass the strict profile, independently of profile enablement."""
    first_valid: tuple[tuple[int, int], ...] = ()
    programming: str = 'PENDING'
    dependencies: tuple[tuple[str, str, str, str], ...] = ()
    """(device, query kind, query, result), including negative queries."""
    active_candidate: tuple[int, tuple[int, int], int] | None = None


@record
class SteeringRule:
    id: str
    policy: tuple[int, int]
    """(color, endpoint) of the policy to steer into."""
    dst: tuple[int, int] | None = None
    """Destination prefix ``(network, length)`` or ``None`` for any."""
    af: int | None = None
    dscp: int | None = None
    sport: int | None = None


@record
class Srv6Policies:
    """A device's SR Policies, their derived states and the steering table."""

    policies: PMap[tuple[int, int], SrPolicy] = field(default_factory=empty_pmap)
    states: PMap[tuple[int, int], PolicyState] = field(default_factory=empty_pmap)
    steering: tuple[SteeringRule, ...] = ()
    """Ordered by longest dst prefix, exact DSCP before wildcard, exact sport before wildcard, id."""
    bsids: PMap[int, tuple[int, int]] = field(default_factory=empty_pmap)
    """Binding SID → policy key (one owner per BSID in Gate B)."""
    steering_by_client: PMap[ClientId, tuple[SteeringRule, ...]] = field(
        default_factory=empty_pmap
    )
    """Owned rules; steering is their deterministically ordered projection."""


# ---------------------------------------------------------------------------
# Encapsulation records carried by FIB legs and the action program
# ---------------------------------------------------------------------------


@record
class Srv6Encap:
    """An encapsulation leg: wire entries (already compressed, first entry
    is the outer destination), the headend behaviour and the source."""

    entries: tuple[int, ...]
    behavior: int = H_ENCAPS_RED
    source: int | None = None
    """Outer source address; ``None`` selects the device's ``srv6_source`` or loopback."""
    policy: tuple[int, int] | None = None
    """Policy key when the leg comes from a policy (for POLICY_DOWN accounting)."""


@record
class PolicyRef:
    """Per-destination steering: a next hop that executes the policy's active path."""

    color: int
    endpoint: int


# ---------------------------------------------------------------------------
# Drop and rejection reasons (strings, as in forwarding.py)
# ---------------------------------------------------------------------------

SID_UNKNOWN = 'SID_UNKNOWN'
SRH_MALFORMED = 'SRH_MALFORMED'
SRH_SL_NONZERO = 'SRH_SL_NONZERO'
UPPER_LAYER_NOT_ALLOWED = 'UPPER_LAYER_NOT_ALLOWED'
NESTED_ENCAP_UNSUPPORTED = 'NESTED_ENCAP_UNSUPPORTED'
POLICY_DOWN_DROP = 'POLICY_DOWN'

# Validation rejections (RFC 9256 §5.1 basic validity and the strict profile).
EMPTY_LIST = 'EMPTY_LIST'
ZERO_WEIGHT = 'ZERO_WEIGHT'
FIRST_SID_UNRESOLVABLE = 'FIRST_SID_UNRESOLVABLE'
SYMBOLIC_UNRESOLVABLE = 'SYMBOLIC_UNRESOLVABLE'
TERMINAL_NOT_LAST = 'TERMINAL_NOT_LAST'
ENDPOINT_NO_DECAP = 'ENDPOINT_NO_DECAP'
ENDPOINT_MISMATCH = 'ENDPOINT_MISMATCH'
PATH_UNREACHABLE = 'PATH_UNREACHABLE'
PARTIAL_ECMP = 'PARTIAL_ECMP'
UNSUPPORTED_BEHAVIOR = 'UNSUPPORTED_BEHAVIOR'
UNSUPPORTED_FLAVOR = 'UNSUPPORTED_FLAVOR'


def behavior_name(behavior: int) -> str:
    """Name of a behaviour (branches, not a shared lookup table: this runs on
    the timeline's extraction path)."""
    if behavior == END:
        return 'End'
    if behavior == END_X:
        return 'End.X'
    if behavior == END_DT46:
        return 'End.DT46'
    if behavior == END_DT4:
        return 'End.DT4'
    if behavior == END_DT6:
        return 'End.DT6'
    if behavior == END_DX4:
        return 'End.DX4'
    if behavior == END_DX6:
        return 'End.DX6'
    if behavior == END_B6_ENCAPS:
        return 'End.B6.Encaps'
    return str(behavior)


def check_gate_b(behavior: int, flavors: int) -> str | None:
    """``None`` when the (behaviour, flavors) pair is in the Gate B set, else
    the rejection reason."""
    if behavior not in GATE_B_BEHAVIORS:
        return UNSUPPORTED_BEHAVIOR
    if flavors & ~GATE_B_FLAVORS:
        return UNSUPPORTED_FLAVOR
    if behavior == END_DT46 and flavors & (PSP | USD):
        return UNSUPPORTED_FLAVOR  # a terminal behaviour has no SRH-popping flavor
    return None


__all__ = [
    'END',
    'END_X',
    'END_DT46',
    'END_DT4',
    'END_DT6',
    'END_DX4',
    'END_DX6',
    'END_B6_ENCAPS',
    'GATE_B_BEHAVIORS',
    'PSP',
    'USP',
    'USD',
    'NEXT_CSID',
    'GATE_B_FLAVORS',
    'H_ENCAPS',
    'H_ENCAPS_RED',
    'FALLBACK_IGP',
    'FALLBACK_DROP',
    'POLICY_UP',
    'POLICY_DOWN',
    'ORIGIN_PCEP',
    'ORIGIN_BGP_SRTE',
    'ORIGIN_CLI',
    'SidStructure',
    'UNCOMPRESSED',
    'F3216_GIB',
    'F3216_LIB',
    'F3216_WLIB',
    'F3216_TERMINAL',
    'F3216_COMPOSITE',
    'GIB_RANGE',
    'LIB_RANGE',
    'WLIB_RANGE',
    'DEFAULT_BLOCK',
    'Locator',
    'LocalSid',
    'Srv6Sids',
    'SidRanges',
    'SidRequest',
    'LiteralSid',
    'AdjSeg',
    'NodeSeg',
    'TermSeg',
    'Segment',
    'SegmentList',
    'CandidatePath',
    'SrPolicy',
    'PolicyState',
    'SteeringRule',
    'Srv6Policies',
    'Srv6Encap',
    'PolicyRef',
    'behavior_name',
    'check_gate_b',
]


# ---------------------------------------------------------------------------
# Pure SR-DB operations. Handles commit only fully validated candidate roots.
# ---------------------------------------------------------------------------


def ipv6(value: int | str) -> int:
    return int(IPv6Address(value))


def prefix6(value: str | tuple[int, int]) -> tuple[int, int]:
    net = IPv6Network(value)
    return int(net.network_address), net.prefixlen


def contains(prefix: tuple[int, int], address: int) -> bool:
    return address >> (128 - prefix[1]) == prefix[0] >> (128 - prefix[1])


def is_csid(structure: SidStructure) -> bool:
    return structure in (
        F3216_GIB,
        F3216_LIB,
        F3216_WLIB,
        F3216_TERMINAL,
        F3216_COMPOSITE,
    )


def flavor_names(flavors: int) -> tuple[str, ...]:
    return tuple(
        name
        for flag, name in (
            (PSP, 'PSP'),
            (USP, 'USP'),
            (USD, 'USD'),
            (NEXT_CSID, 'NEXT-C-SID'),
        )
        if flavors & flag
    )


def require_gate_b(behavior: int, flavors: int = 0, **options: Any) -> None:
    if 'vrf' in options or 'table' in options:
        raise ValueError('VRF/table selection is unsupported in Gate B')
    if options:
        raise ValueError(f'unsupported SR options: {", ".join(sorted(options))}')
    reason = check_gate_b(behavior, flavors)
    if reason is not None:
        raise ValueError(f'{reason}: {behavior_name(behavior)} {flavor_names(flavors)}')


def add_locator(
    state: NetworkState,
    device: str,
    name: str,
    prefix: str | tuple[int, int] | None,
    structure: SidStructure,
    block: str | tuple[int, int],
    node_id: int | None,
    ranges: SidRanges,
) -> tuple[NetworkState, Locator]:
    from netsim.model.state import check_name

    check_name(name)
    dev = state.devices[device]
    db = dev.srv6_sids or Srv6Sids()
    block = prefix6(block)
    allocators = state.allocators
    if node_id is None and (existing := db.locators.get(name)) is not None:
        node_id = existing.node_id
    if is_csid(structure):
        if block[1] != structure.lbl:
            raise ValueError('NEXT-C-SID block length must match lbl')
        used = {
            loc.node_id
            for d in state.devices.values()
            if d.srv6_sids is not None
            for loc in d.srv6_sids.locators.values()
            if loc.block == block
        }
        if node_id is None:
            node_id = max(
                ranges.gib[0], allocators.next_srv6_node.get(block, ranges.gib[0])
            )
            while node_id in used and node_id <= ranges.gib[1]:
                node_id += 1
            if node_id > ranges.gib[1]:
                raise ValueError('GIB exhausted')
            allocators = replace(
                allocators,
                next_srv6_node=allocators.next_srv6_node.set(block, node_id + 1),
            )
        if not ranges.gib[0] <= node_id <= ranges.gib[1]:
            raise ValueError('node id outside GIB')
        derived = (block[0] | (node_id << 80), 48)
        if prefix is not None and prefix6(prefix) != derived:
            raise ValueError('locator prefix must equal block and node id')
        loc = Locator(name, derived, structure, block, node_id)
    else:
        if prefix is None or node_id is not None:
            raise ValueError('classic locator needs an explicit prefix and no node id')
        p = prefix6(prefix)
        if p[1] != structure.lbl + structure.lnl or structure.fl == 0:
            raise ValueError(
                'classic locator length must equal lbl + lnl, with function bits'
            )
        # Classic block is the locator block encoded in its explicit prefix.
        block = (p[0] >> (128 - structure.lbl) << (128 - structure.lbl), structure.lbl)
        loc = Locator(name, p, structure, block)
    old = db.locators.get(name)
    if old is not None:
        if old == loc and db.ranges.get(block, SidRanges()) == ranges:
            return state, old
        raise ValueError(f'locator {name!r} exists')
    prior = db.ranges.get(block)
    if prior is not None and prior != ranges:
        raise ValueError('conflicting ranges for locator block')
    db = replace(
        db, locators=db.locators.set(name, loc), ranges=db.ranges.set(block, ranges)
    )
    candidate = replace(
        state,
        devices=state.devices.set(device, replace(dev, srv6_sids=db)),
        allocators=allocators,
    )
    require_valid(candidate)
    return candidate, loc


def select_locator(
    db: Srv6Sids, structure: SidStructure, locator: str | None, sid: int | None = None
) -> Locator:
    choices = [
        loc
        for name, loc in db.locators.sorted_items()
        if (locator is None or name == locator)
        and is_csid(loc.structure) == is_csid(structure)
        and (
            sid is None
            or contains(loc.block if is_csid(structure) else loc.prefix, sid)
        )
    ]
    if len(choices) != 1:
        raise ValueError(
            'select exactly one matching locator (use locator= for multiple blocks)'
        )
    return choices[0]


def function_range(
    structure: SidStructure, ranges: SidRanges, purpose: str
) -> tuple[int, int]:
    """Four disjoint, inclusive pools, in adjacency/terminal/bsid/client order."""
    index = ('adjacency', 'terminal', 'bsid', 'client').index(purpose)
    if not is_csid(structure):
        lo, hi = 1, (1 << structure.fl) - 1
    elif structure.fl == 32:
        lo, hi = ranges.wlib[0] << 16, (ranges.wlib[1] << 16) | 0xFFFF
    else:
        lo, hi = ranges.lib
    size = hi - lo + 1
    return lo + size * index // 4, lo + size * (index + 1) // 4 - 1


def _sid_value(loc: Locator, structure: SidStructure, function: int) -> int:
    base = loc.prefix[0] if structure.lnl else loc.block[0]
    return base | (function << (128 - structure.installed_length))


def _allocate_function(
    db: Srv6Sids,
    loc: Locator,
    structure: SidStructure,
    purpose: str,
    used: set[int],
    index: int | None = None,
) -> tuple[Srv6Sids, int]:
    low, high = function_range(
        structure, db.ranges.get(loc.block, SidRanges()), purpose
    )
    key = f'{loc.block[0]:032x}/{loc.block[1]}:{loc.prefix[0]:032x}:{structure.lnl}:{structure.fl}:{purpose}'
    preferred = low + index if index is not None else None
    if preferred is not None and preferred <= high:
        value = _sid_value(loc, structure, preferred)
        if value in used:
            raise ValueError('SID collision at interface-derived function')
        return db, value
    function = max(low, db.next_function.get(key, low))
    while function <= high and _sid_value(loc, structure, function) in used:
        function += 1
    if function > high:
        raise ValueError(f'{purpose} function pool exhausted')
    return replace(
        db, next_function=db.next_function.set(key, function + 1)
    ), _sid_value(loc, structure, function)


def add_local_sid(
    state: NetworkState,
    device: str,
    behavior: int,
    *,
    structure: SidStructure,
    flavors: int = 0,
    sid: int | str | None = None,
    interface: str | None = None,
    nexthop: int | str | None = None,
    owner: ClientId = STATIC,
    locator: str | None = None,
    request_id: str | None = None,
    **options: Any,
) -> tuple[NetworkState, LocalSid]:
    require_gate_b(behavior, flavors, **options)
    dev = state.devices[device]
    db = dev.srv6_sids or Srv6Sids()
    value = ipv6(sid) if sid is not None else None
    loc = select_locator(db, structure, locator, value)
    if flavors & NEXT_CSID and (not is_csid(structure) or structure.lnfl == 0):
        raise ValueError('NEXT-C-SID requires a supported SID structure')
    if structure == F3216_GIB and behavior != END:
        raise ValueError('GIB SID must use End')
    if structure == F3216_TERMINAL and behavior != END_DT46:
        raise ValueError('terminal structure requires End.DT46')
    if behavior == END_X:
        if interface is None or interface not in dev.interfaces:
            raise ValueError('End.X requires a bound interface')
        from netsim.model.interfaces import LoopbackNode

        if isinstance(dev.interfaces[interface], LoopbackNode):
            raise ValueError('End.X requires an adjacency interface')
    elif interface is not None or nexthop is not None:
        raise ValueError('only End.X accepts interface/nexthop')
    if value is None and behavior == END_X:
        for old in db.sids.values():
            if (
                old.behavior == behavior
                and old.flavors == flavors
                and old.structure == structure
                and old.interface == interface
                and old.nexthop == (ipv6(nexthop) if nexthop is not None else None)
                and old.owner == owner
                and old.request_id == request_id
                and contains(loc.block if is_csid(structure) else loc.prefix, old.sid)
            ):
                return state, old
    used = set(db.sids) | (set(dev.srv6_policies.bsids) if dev.srv6_policies else set())
    if value is None:
        if structure == F3216_GIB:
            value = loc.prefix[0]
        else:
            purpose = (
                'client'
                if request_id is not None or owner != STATIC
                else 'adjacency'
                if behavior == END_X
                else 'terminal'
                if behavior == END_DT46
                else 'client'
            )
            index = (
                dev.interfaces[interface].index
                if purpose == 'adjacency' and interface is not None
                else None
            )
            db, value = _allocate_function(db, loc, structure, purpose, used, index)
    row = LocalSid(
        value,
        structure.installed_length,
        behavior,
        flavors,
        structure,
        owner,
        interface,
        ipv6(nexthop) if nexthop is not None else None,
        request_id,
        behavior != END_X,
    )
    old = db.sids.get(value)
    if old is not None:
        if replace(row, adjacency_up=old.adjacency_up) == old:
            return state, old
        raise ValueError('SID collision')
    if value in used:
        raise ValueError('SID collides with BSID')
    db = replace(db, sids=db.sids.set(value, row))
    candidate = replace(
        state, devices=state.devices.set(device, replace(dev, srv6_sids=db))
    )
    require_valid(candidate)
    return candidate, row


def remove_local_sid(
    state: NetworkState, device: str, sid: int | str, owner: ClientId = STATIC
) -> NetworkState:
    dev = state.devices[device]
    db = dev.srv6_sids
    value = ipv6(sid)
    if db is None or value not in db.sids:
        return state
    row = db.sids[value]
    if row.owner != owner:
        raise ValueError('SID belongs to another owner')
    requests = db.requests
    for key, request in db.requests.sorted_items():
        if request.result.sid == value:
            requests = requests.remove(key)
    new = replace(db, sids=db.sids.remove(value), requests=requests)
    return replace(
        state, devices=state.devices.set(device, replace(dev, srv6_sids=new))
    )


def request_key(client: ClientId, request_id: str) -> str:
    """Collision-free encoding while retaining the contract's string map keys."""
    return f'{len(client.name)}:{client.name}:{client.instance}:{request_id}'


def request_sid(
    state: NetworkState,
    device: str,
    client: ClientId,
    request_id: str,
    behavior: int,
    args: dict[str, Any],
) -> tuple[NetworkState, LocalSid]:
    from netsim.model.state import validate_immutable

    if not request_id:
        raise ValueError('request id must be nonempty')
    if 'owner' in args or 'request_id' in args:
        raise ValueError('request owner and id are supplied by the client')
    frozen_args = tuple(sorted(args.items()))
    validate_immutable(frozen_args)
    key = request_key(client, request_id)
    db = state.devices[device].srv6_sids
    old = db.requests.get(key) if db else None
    if old is not None:
        if old.behavior != behavior or old.args != frozen_args:
            raise ValueError('request id already used with different arguments')
        assert db is not None
        return state, db.sids[old.result.sid]
    candidate, row = add_local_sid(
        state, device, behavior, owner=client, request_id=request_id, **args
    )
    dev = candidate.devices[device]
    assert dev.srv6_sids is not None
    db = replace(
        dev.srv6_sids,
        requests=dev.srv6_sids.requests.set(
            key, SidRequest(client, request_id, behavior, frozen_args, row)
        ),
    )
    return replace(
        candidate, devices=candidate.devices.set(device, replace(dev, srv6_sids=db))
    ), row


def check_policy(policy: SrPolicy) -> None:
    ipv6(policy.endpoint)
    if policy.color < 0 or policy.fallback not in (FALLBACK_IGP, FALLBACK_DROP):
        raise ValueError('invalid policy color/fallback')
    if policy.bsid is not None:
        ipv6(policy.bsid)
    keys = set()
    for path in policy.candidate_paths:
        key = (path.protocol_origin, path.originator, path.discriminator)
        if key in keys:
            raise ValueError('duplicate candidate path key')
        keys.add(key)
        for segment_list in path.segment_lists:
            for segment in segment_list.segments:
                if isinstance(segment, TermSeg):
                    require_gate_b(segment.behavior)
                elif isinstance(segment, LiteralSid):
                    ipv6(segment.address)
                    if (
                        segment.flavors is not None
                        and segment.flavors & ~GATE_B_FLAVORS
                    ):
                        raise ValueError(UNSUPPORTED_FLAVOR)
                elif isinstance(segment, int):
                    ipv6(segment)
                elif not isinstance(segment, (AdjSeg, NodeSeg)):
                    raise ValueError(
                        'unsupported segment (SR-MPLS/nested encapsulation/VRF)'
                    )
    # Empty/zero-weight lists are stored: RFC 9256 §5.1 validity belongs to
    # the resolver so a policy can expose DOWN and its per-list reasons.


def put_policy(
    state: NetworkState,
    device: str,
    client: ClientId,
    policy: SrPolicy,
    *,
    replace_existing: bool = False,
    locator: str | None = None,
) -> NetworkState:
    check_policy(policy)
    if policy.owner != client:
        raise ValueError('policy belongs to another owner')
    dev = state.devices[device]
    table = dev.srv6_policies or Srv6Policies()
    old = table.policies.get(policy.key)
    if old is not None and old.owner != client:
        raise ValueError('policy key belongs to another owner')
    if old is not None and policy.bsid is None:
        policy = replace(policy, bsid=old.bsid)
    if old == policy:
        return state
    if old is not None and not replace_existing:
        raise ValueError('policy exists; use replace')
    # Candidate identities cannot be claimed by two clients on one headend.
    wanted = {
        (p.protocol_origin, p.originator, p.discriminator)
        for p in policy.candidate_paths
    }
    for other in table.policies.values():
        if other.owner != client and any(
            (p.protocol_origin, p.originator, p.discriminator) in wanted
            for p in other.candidate_paths
        ):
            raise ValueError('candidate key belongs to another owner')
    db = dev.srv6_sids
    if policy.bsid is None and db is not None and db.locators:
        choices = [
            loc
            for name, loc in db.locators.sorted_items()
            if locator is None or name == locator
        ]
        if len(choices) != 1:
            raise ValueError('select exactly one BSID locator')
        loc = choices[0]
        structure = F3216_LIB if is_csid(loc.structure) else loc.structure
        db, bsid = _allocate_function(
            db, loc, structure, 'bsid', set(db.sids) | set(table.bsids)
        )
        policy = replace(policy, bsid=bsid)
        dev = replace(dev, srv6_sids=db)
    bsids = table.bsids
    if old is not None and old.bsid is not None:
        bsids = bsids.remove(old.bsid)
    if policy.bsid is not None:
        if policy.bsid in bsids or (
            dev.srv6_sids and policy.bsid in dev.srv6_sids.sids
        ):
            raise ValueError('BSID collision')
        bsids = bsids.set(policy.bsid, policy.key)
    new = replace(table, policies=table.policies.set(policy.key, policy), bsids=bsids)
    candidate = replace(
        state, devices=state.devices.set(device, replace(dev, srv6_policies=new))
    )
    require_valid(candidate)
    return candidate


def delete_policy(
    state: NetworkState, device: str, client: ClientId, key: tuple[int, int]
) -> NetworkState:
    dev = state.devices[device]
    table = dev.srv6_policies
    if table is None or key not in table.policies:
        return state
    old = table.policies[key]
    if old.owner != client:
        raise ValueError('policy belongs to another owner')
    by_client = table.steering_by_client
    for owner, rules in by_client.sorted_items():
        by_client = by_client.set(owner, tuple(r for r in rules if r.policy != key))
    new = replace(
        table,
        policies=table.policies.remove(key),
        states=table.states.remove(key),
        bsids=table.bsids.remove(old.bsid) if old.bsid is not None else table.bsids,
        steering_by_client=by_client,
        steering=_steering(by_client),
    )
    return replace(
        state, devices=state.devices.set(device, replace(dev, srv6_policies=new))
    )


def _steering(
    by_client: PMap[ClientId, tuple[SteeringRule, ...]],
) -> tuple[SteeringRule, ...]:
    rows = [
        (owner, rule) for owner, rules in by_client.sorted_items() for rule in rules
    ]
    rows.sort(
        key=lambda pair: (
            -(pair[1].dst[1] if pair[1].dst else -1),
            pair[1].dscp is None,
            pair[1].sport is None,
            pair[1].id,
            pair[0],
        )
    )
    return tuple(rule for _, rule in rows)


def set_steering(
    state: NetworkState, device: str, client: ClientId, rules: tuple[SteeringRule, ...]
) -> NetworkState:
    dev = state.devices[device]
    table = dev.srv6_policies or Srv6Policies()
    ids = set()
    for rule in rules:
        if not rule.id or rule.id in ids:
            raise ValueError('steering ids must be nonempty and unique per client')
        ids.add(rule.id)
        policy = table.policies.get(rule.policy)
        if policy is None or policy.owner != client:
            raise ValueError('steering policy must belong to the client')
        if rule.af not in (None, 4, 6) or (rule.dst is not None and rule.af is None):
            raise ValueError('steering destination needs an address family')
        if rule.dst is not None:
            from netsim.model.addressing import mask_for

            net, length = rule.dst
            bits = 32 if rule.af == 4 else 128
            if (
                not 0 <= length <= bits
                or not 0 <= net < 1 << bits
                or net & ~mask_for(length, bits)
            ):
                raise ValueError('invalid steering prefix')
        if rule.dscp is not None and not 0 <= rule.dscp <= 63:
            raise ValueError('invalid steering DSCP')
        if rule.sport is not None and not 0 <= rule.sport <= 65535:
            raise ValueError('invalid steering source port')
    by_client = (
        table.steering_by_client.set(client, rules)
        if rules
        else table.steering_by_client.remove(client)
    )
    new = canon(
        table,
        replace(table, steering_by_client=by_client, steering=_steering(by_client)),
    )
    if new is table:
        return state
    return replace(
        state, devices=state.devices.set(device, replace(dev, srv6_policies=new))
    )


def unknown_prefixes(
    db: Srv6Sids, active_prefixes: Iterable[tuple[int, int]] = ()
) -> tuple[tuple[int, int], ...]:
    """Exact LIB/WLIB cover minus prefixes installed as active local SIDs.

    Subtract address intervals before summarizing: a cover must neither tie
    an active SID at the same prefix nor shadow one with a more-specific
    drop. With no exclusions this returns the complete configured ranges.
    """
    if not db.drop_unknown_local:
        return ()
    from ipaddress import collapse_addresses

    intervals = []
    blocks = sorted(
        {loc.block for loc in db.locators.values() if is_csid(loc.structure)}
    )
    for block in blocks:
        ranges = db.ranges.get(block, SidRanges())
        for low, high in (ranges.lib, ranges.wlib):
            intervals.append(
                (block[0] | low << 80, block[0] | high << 80 | ((1 << 80) - 1))
            )
    exclusions = sorted(
        (int(net.network_address), int(net.broadcast_address))
        for net in map(IPv6Network, active_prefixes)
    )
    nets = []
    index = 0
    for start, end in sorted(intervals):
        while index < len(exclusions) and exclusions[index][1] < start:
            index += 1
        while index < len(exclusions) and exclusions[index][0] <= end:
            low, high = exclusions[index]
            if start < low:
                nets.extend(
                    summarize_address_range(IPv6Address(start), IPv6Address(low - 1))
                )
            start = max(start, high + 1)
            if high >= end:
                # This exclusion may also span the next range or block.
                break
            index += 1
        if start <= end:
            nets.extend(summarize_address_range(IPv6Address(start), IPv6Address(end)))
    return tuple(
        (int(net.network_address), net.prefixlen) for net in collapse_addresses(nets)
    )


def validate(state: NetworkState) -> list[str]:
    problems: list[str] = []
    locators = [
        (name, loc)
        for name, dev in state.devices.sorted_items()
        if dev.srv6_sids
        for _, loc in dev.srv6_sids.locators.sorted_items()
    ]
    for name, dev in state.devices.sorted_items():
        try:
            agent_srdb_view(dev)
        except ValueError as exc:
            problems.append(f'{name}: {exc}')
        if not 1 <= dev.config.srv6_hop_limit <= 255:
            problems.append(f'{name}: invalid SRv6 hop limit')
        if dev.config.srv6_source is not None:
            try:
                ipv6(dev.config.srv6_source)
            except ValueError:
                problems.append(f'{name}: invalid SRv6 source')
        db = dev.srv6_sids
        if db is None:
            continue
        for _, loc in db.locators.sorted_items():
            try:
                prefix6(loc.prefix)
                prefix6(loc.block)
                ranges = db.ranges.get(loc.block, SidRanges())
                ranges.__post_init__()
                if is_csid(loc.structure):
                    if (
                        loc.block[1] != 32
                        or loc.node_id is None
                        or not ranges.gib[0] <= loc.node_id <= ranges.gib[1]
                    ):
                        raise ValueError('node id outside GIB or invalid block')
                    if loc.prefix != (loc.block[0] | loc.node_id << 80, 48):
                        raise ValueError(
                            'locator prefix differs from block and node id'
                        )
                elif loc.prefix[1] != loc.structure.lbl + loc.structure.lnl:
                    raise ValueError('invalid classic locator length')
            except ValueError as exc:
                problems.append(f'{name}:{loc.name}: {exc}')
        for value, row in db.sids.sorted_items():
            reason = check_gate_b(row.behavior, row.flavors)
            if reason:
                problems.append(f'{name}: {reason}: {behavior_name(row.behavior)}')
            if (
                row.sid != value
                or not 0 <= value < 1 << 128
                or row.length != row.structure.installed_length
                or not 0 < row.length <= 128
            ):
                problems.append(f'{name}: invalid SID key/length')
                continue
            if value & ((1 << (128 - row.length)) - 1):
                problems.append(f'{name}: local SID must have zero argument')
            try:
                loc = select_locator(db, row.structure, None, value)
                ranges = db.ranges.get(loc.block, SidRanges())
                if is_csid(row.structure):
                    if row.structure == F3216_GIB:
                        if value != loc.prefix[0] or row.behavior != END:
                            raise ValueError(
                                'GIB SID must equal its node prefix and use End'
                            )
                    else:
                        function = value >> (128 - row.length) & (
                            (1 << row.structure.fl) - 1
                        )
                        high = function >> 16 if row.structure.fl == 32 else function
                        pool = ranges.wlib if row.structure.fl == 32 else ranges.lib
                        if not pool[0] <= high <= pool[1]:
                            raise ValueError('local function outside LIB/WLIB')
                        if row.structure.lnl and not contains(loc.prefix, value):
                            raise ValueError('composite SID has the wrong node id')
                    if row.structure == F3216_TERMINAL and row.behavior != END_DT46:
                        raise ValueError('terminal structure requires End.DT46')
                elif not contains(loc.prefix, value):
                    raise ValueError('SID outside classic locator')
                if row.flavors & NEXT_CSID and not is_csid(row.structure):
                    raise ValueError('NEXT-C-SID requires supported structure')
                if row.behavior == END_X and row.interface not in dev.interfaces:
                    raise ValueError('End.X requires a bound interface')
                if row.behavior != END_X and (
                    row.interface is not None or row.nexthop is not None
                ):
                    raise ValueError('only End.X accepts interface/nexthop')
            except ValueError as exc:
                problems.append(f'{name}: {exc}')
            for other, loc in locators:
                if (
                    other != name
                    and is_csid(loc.structure)
                    and row.length <= loc.prefix[1]
                    and contains((value, row.length), loc.prefix[0])
                ):
                    problems.append(f'{name}: local SID covers {other} uN prefix')
    for i, (name, loc) in enumerate(locators):
        for other, remote in locators[i + 1 :]:
            overlap = contains(loc.prefix, remote.prefix[0]) or contains(
                remote.prefix, loc.prefix[0]
            )
            if overlap:
                problems.append(
                    f'{name}/{other}: locators overlap (node ids must be unique)'
                )
            if loc.block == remote.block:
                left, right = (
                    state.devices[name].srv6_sids,
                    state.devices[other].srv6_sids,
                )
                assert left is not None and right is not None
                if left.ranges.get(loc.block, SidRanges()) != right.ranges.get(
                    remote.block, SidRanges()
                ):
                    problems.append(f'{name}/{other}: conflicting ranges for block')
            elif contains(loc.block, remote.block[0]) or contains(
                remote.block, loc.block[0]
            ):
                problems.append(f'{name}/{other}: locator blocks overlap')
        for other, dev in state.devices.sorted_items():
            for iface, node in dev.interfaces.sorted_items():
                if any(contains(loc.block, addr) for addr, _ in node.config.ipv6):
                    problems.append(
                        f'{name}: locator block covers interface address {other}:{iface}'
                    )
    for name, dev in state.devices.sorted_items():
        table = dev.srv6_policies
        if table is None:
            continue
        expected = {}
        for key, policy in table.policies.sorted_items():
            try:
                check_policy(policy)
                if key != policy.key:
                    raise ValueError('policy key mismatch')
                if policy.bsid is not None:
                    if policy.bsid in expected or (
                        dev.srv6_sids and policy.bsid in dev.srv6_sids.sids
                    ):
                        raise ValueError('BSID collision')
                    expected[policy.bsid] = key
            except ValueError as exc:
                problems.append(f'{name}: {exc}')
        if dict(table.bsids.items()) != expected:
            problems.append(f'{name}: BSID index mismatch')
    return problems


def require_valid(state: NetworkState) -> None:
    problems = validate(state)
    if problems:
        raise ValueError('; '.join(problems))


def consumers_affected(old: NetworkState, new: NetworkState) -> set[str]:
    """Invalidate consumers using recorded device dependencies where complete.

    RIB/neighbor lookups (including misses) and loopback/endpoint observations
    name their consulted devices. Changes on other devices cannot alter those
    answers. Keep the global fallback for SID inventory (literal lookup scans
    all owners), links, device configuration/lifecycle, non-loopback interfaces
    (peer/bundle lookup reads both endpoints), and policy inputs. Missing
    validation records also take the fallback. Derived policy/FIB outputs do
    not feed back into validation.
    """
    from netsim.model.interfaces import LoopbackNode
    from netsim.model.state import diff_pmap

    delta = diff_pmap(old.devices, new.devices, by_identity=True)
    global_change = bool(delta.added or delta.removed or old.links != new.links)
    scoped_devices: set[str] = set()
    view_consumers: set[str] = set()
    for name in delta.changed:
        a, b = old.devices[name], new.devices[name]
        if b.config.srdb_source is not None:
            agent_srdb_view(b)  # reject unsupported sources, including on updates
            agent_name = b.config.srdb_source[1]
            old_agent = a.agents.get(agent_name)
            if (
                old_agent is None
                or old_agent.srdb_view is not b.agents[agent_name].srdb_view
                or a.config.srdb_source != b.config.srdb_source
            ):
                if b.srv6_policies is not None:
                    view_consumers.add(name)
        if (
            a.srv6_sids != b.srv6_sids
            or a.config != b.config
            or policy_inputs(a.srv6_policies) != policy_inputs(b.srv6_policies)
        ):
            global_change = True
        if a.interfaces != b.interfaces:
            changes = diff_pmap(a.interfaces, b.interfaces)
            if all(
                isinstance(node, LoopbackNode)
                for interface in changes.keys
                for node in (a.interfaces.get(interface), b.interfaces.get(interface))
                if node is not None
            ):
                scoped_devices.add(name)
            else:
                global_change = True
        if a.neighbors != b.neighbors or a.ribs != b.ribs:
            scoped_devices.add(name)
    if not (global_change or scoped_devices):
        return view_consumers
    consumers = consumer_index(old, new)
    if global_change:
        return set(consumers) | view_consumers
    return view_consumers | {
        name for name in consumers if _uses_devices(new.devices[name], scoped_devices)
    }


def _uses_devices(dev: DeviceState, changed: set[str]) -> bool:
    # Local FIB programs capture device inputs beyond the validation record.
    if dev.name in changed:
        return True
    table = dev.srv6_policies
    if table is None:
        return False
    for key in table.policies:
        result = table.states.get(key)
        if result is None or not result.dependencies:
            return True
        # Keep all recorded kinds, including negative and learned-view queries:
        # this deliberately over-invalidates rather than infer missing scopes.
        if any(device in changed for device, _, _, _ in result.dependencies):
            return True
    return False


def consumer_index(old: NetworkState, new: NetworkState) -> frozenset[str]:
    """Update the root index by changed shards; plain-IP edits never scan the tree."""
    from netsim.model.state import diff_pmap

    delta = diff_pmap(old.devices, new.devices, by_identity=True)
    consumers = set(old.srv6_consumers)
    consumers.difference_update(delta.removed)
    for name in delta.added + delta.changed:
        table = new.devices[name].srv6_policies
        if table is not None and table.policies:
            consumers.add(name)
        else:
            consumers.discard(name)
    return canon(new.srv6_consumers, frozenset(consumers))


def policy_inputs(table: Srv6Policies | None) -> tuple[Any, ...]:
    return (
        (table.policies, table.steering, table.bsids, table.steering_by_client)
        if table
        else ()
    )


@record
class PolicyProgram:
    """Installed policy action; never read live validation on a packet path."""

    lists: tuple[tuple[Srv6Encap, int], ...] = ()
    fallback: int = FALLBACK_DROP
    candidate: tuple[int, tuple[int, int], int] | None = None


def policy_programs(table: Srv6Policies | None) -> PMap[tuple[int, int], PolicyProgram]:
    if table is None:
        return PMap()
    programs = []
    for key, policy in table.policies.sorted_items():
        state = table.states.get(key) or PolicyState()
        lists = tuple(
            (
                Srv6Encap(entries, policy=key),
                policy.candidate_paths[pi].segment_lists[li].weight,
            )
            for pi, li, entries in sorted(
                state.valid_lists,
                key=lambda item: (
                    -policy.candidate_paths[item[0]].segment_lists[item[1]].weight,
                    policy.candidate_paths[item[0]].segment_lists[item[1]].name or '',
                    item[1],
                ),
            )
        )
        programs.append(
            (key, PolicyProgram(lists, policy.fallback, state.active_candidate))
        )
    return PMap(programs)


class _PolicyValidator:
    """One immutable snapshot for all queries; caches are invocation-local."""

    unknown_reason = PATH_UNREACHABLE
    symbolic_reason = SYMBOLIC_UNRESOLVABLE

    def __init__(self, state: NetworkState, head: str) -> None:
        self.state = state
        self.head = head
        self.deps: set[tuple[str, str, str, str]] = set()
        self.queries: dict[tuple[str, int], Any] = {}

    def query(self, device: str, kind: str, key: Any, result: Any) -> None:
        self.deps.add((device, kind, str(key), str(result)))

    def symbolic(self, segment: AdjSeg | NodeSeg | TermSeg) -> LocalSid | None:
        dev = self.state.devices.get(segment.device)
        sids = dev.srv6_sids.sids if dev and dev.srv6_sids else PMap()
        matches = [
            sid
            for sid in sids.values()
            if (
                isinstance(segment, AdjSeg)
                and sid.behavior == END_X
                and sid.interface == segment.interface
                or isinstance(segment, NodeSeg)
                and sid.behavior == END
                or isinstance(segment, TermSeg)
                and sid.behavior == segment.behavior
            )
        ]
        sid = min(matches, key=lambda s: s.sid) if matches else None
        self.query(segment.device, 'symbolic', segment, sid.sid if sid else 'MISSING')
        return sid

    def literal(self, address: int, current: str) -> tuple[str, LocalSid] | None:
        matches = []
        for name, dev in self.state.devices.sorted_items():
            if dev.srv6_sids:
                for sid in dev.srv6_sids.sids.values():
                    if contains((sid.sid, sid.length), address):
                        matches.append((name, sid))
        matches.sort(
            key=lambda pair: (-pair[1].length, pair[0] != current, pair[0], pair[1].sid)
        )
        result = matches[0] if matches else None
        self.query(current, 'sid', address, result[0] if result else 'MISSING')
        return result

    def peer(self, device: str, interface: str) -> str | None:
        from netsim.model.derive import bundle_members, peer_bundle_key, peer_endpoint
        from netsim.model.interfaces import PortChannelNode

        dev = self.state.devices[device]
        node = dev.interfaces.get(interface)
        if isinstance(node, PortChannelNode):
            peers = {
                p[0]
                for member in bundle_members(dev, interface)
                if (p := peer_bundle_key(self.state, device, member.name)) is not None
            }
            result = next(iter(peers)) if len(peers) == 1 else None
        else:
            endpoint = peer_endpoint(self.state, device, interface)
            result = endpoint[0] if endpoint else None
        self.query(device, 'adjacency', interface, result or 'MISSING')
        return result

    def forwarding(self, device: str, address: int):
        from netsim.model.derive import DeviceContext
        from netsim.model.routing import ResolutionPolicy, resolve_underlay_query

        key = device, address
        if key not in self.queries:
            ctx = DeviceContext(self.state, device)
            self.queries[key] = resolve_underlay_query(
                ctx, ctx.dev.config.resolution_policy or ResolutionPolicy(), address
            )
        entry, legs, deps = self.queries[key]
        self.query(device, 'rib', address, entry.prefix if entry else 'MISSING')
        for af, addr in deps.lookups:
            self.query(device, 'recursive', (af, addr), 'QUERIED')
        for prefix in deps.prefixes:
            self.query(device, 'prefix', prefix, 'QUERIED')
        for interface in deps.interfaces:
            self.query(device, 'interface', interface, 'QUERIED')
        return entry, legs

    def reaches(
        self, current: str, owner: str, address: int, sid: LocalSid
    ) -> str | None:
        from netsim.model import forwarding as fw

        # Iterative DFS detects loops without a Python recursion-depth limit.
        pending = [(current, frozenset())]
        good = False
        failures: set[str] = set()
        while pending:
            node, visited = pending.pop()
            self.query(node, 'reach', address, owner)
            if node == owner:
                # Arrival is insufficient: a more-specific route may shadow
                # the local SID. Validate the RIB's selected behavior, never
                # the installed (possibly delayed) FIB snapshot.
                entry, _ = self.forwarding(node, address)
                if entry and entry.action == fw.SRV6_LOCAL and entry.sid == sid:
                    good = True
                else:
                    failures.add(PATH_UNREACHABLE)
                    self.query(
                        node, 'failure', address, entry.action if entry else 'NO_ROUTE'
                    )
                continue
            if node in visited:
                failures.add('LOOP_DETECTED')
                self.query(node, 'rib', address, 'LOOP_DETECTED')
                continue
            entry, legs = self.forwarding(node, address)
            if entry is None or entry.action != fw.FORWARD or not legs:
                failures.add(PATH_UNREACHABLE)
                self.query(
                    node, 'failure', address, entry.action if entry else 'NO_ROUTE'
                )
                continue
            for leg in reversed(legs):
                peer = self.peer(node, leg.interface)
                if peer is None or leg.encap is not None:
                    failures.add(PATH_UNREACHABLE)
                else:
                    pending.append((peer, visited | {node}))
        if good and failures:
            return PARTIAL_ECMP
        return min(failures) if failures else None

    def endpoint_owned(self, owner: str, policy: SrPolicy) -> bool:
        dev = self.state.devices[owner]
        owned = any(
            policy.endpoint == a
            for node in dev.interfaces.values()
            for a, _ in node.config.ipv6
        ) or bool(
            dev.srv6_sids
            and any(
                contains(loc.prefix, policy.endpoint)
                for loc in dev.srv6_sids.locators.values()
            )
        )
        self.query(owner, 'endpoint', policy.endpoint, owned)
        return owned

    def strict(
        self, segments: tuple[Any, ...], wire: tuple[int, ...], policy: SrPolicy
    ) -> str | None:
        # Check the declared owners/behaviors first (including symbolic intent).
        # Route selection below uses actual wire addresses: a compressed DA can
        # select a different more-specific route than its zero-argument SID.
        from netsim.model.srv6_compress import csid_arg, shift_csid

        current = self.head
        pending = list(reversed(segments))
        while pending:
            segment = pending.pop()
            symbolic = isinstance(segment, (AdjSeg, NodeSeg, TermSeg))
            address = (
                segment.address
                if isinstance(segment, LiteralSid)
                else segment
                if isinstance(segment, int)
                else 0
            )
            if symbolic:
                sid = self.symbolic(segment)
                owner = segment.device
            else:
                found = self.literal(address, current)
                owner, sid = found if found else ('', None)
            if sid is None:
                return self.unknown_reason
            if symbolic:
                address = sid.sid
            self.query(owner, 'sid-oper', sid.sid, sid.adjacency_up)
            if (
                sid.flavors & NEXT_CSID
                and sid.behavior != END_DT46
                and csid_arg(address, sid.structure)
            ):
                # A literal can already contain a NEXT-C-SID continuation.
                # It must execute before advancing to the next SRH entry
                # (RFC 9800 §4.1.1 N01-N09), including for endpoint checks.
                pending.append(shift_csid(address, sid.structure)[0])
            last = not pending
            if sid.behavior == END_DT46 and not last:
                return TERMINAL_NOT_LAST
            if last:
                if sid.behavior != END_DT46 and not sid.flavors & USD:
                    return ENDPOINT_NO_DECAP
                if not self.endpoint_owned(owner, policy):
                    return ENDPOINT_MISMATCH
            # RFC 9800 sections 5.1-5.2: LIB scope is the executing node.
            bare = is_csid(sid.structure) and sid.structure.lnl == 0
            if bare:
                if current != owner:
                    self.query(current, 'local-scope', sid.sid, f'WRONG_OWNER:{owner}')
                    return PATH_UNREACHABLE
            current = owner
            if sid.behavior == END_X:
                if not sid.adjacency_up or sid.interface is None:
                    return PATH_UNREACHABLE
                peer = self.peer(owner, sid.interface)
                if peer is None:
                    return PATH_UNREACHABLE
                current = peer
        return self.encoded(wire, policy)

    def encoded(self, wire: tuple[int, ...], policy: SrPolicy) -> str | None:
        """Validate the emitted continuation with the packet behavior interpreter.

        RFC 9800 §4.1.1 extracts/executes Arg before classic SRH processing;
        valid nonzero arguments carry further C-SIDs. In particular, checking
        only a terminal's prefix misses malformed suffixes after that prefix.
        All route queries remain RIB-derived, independently of FIB programming.
        """
        from netsim.model import forwarding as fw
        from netsim.model.packets import IPv4Packet, IPv6Packet, encapsulate

        packet = encapsulate(
            IPv4Packet(0, 0, 17),
            wire,
            behavior=H_ENCAPS_RED,
            source=0,
            hop_limit=255,
            flow_label=0,
            transit=False,
        )
        current = self.head
        seen: set[tuple[str, int, Any]] = set()
        while True:
            key = current, packet.dst, packet.srh
            if key in seen:
                return 'LOOP_DETECTED'
            seen.add(key)
            found = self.literal(packet.dst, current)
            if found is None:
                return self.unknown_reason
            owner, sid = found
            if is_csid(sid.structure) and sid.structure.lnl == 0 and current != owner:
                self.query(current, 'local-scope', packet.dst, f'WRONG_OWNER:{owner}')
                return PATH_UNREACHABLE
            reason = self.reaches(current, owner, packet.dst, sid)
            if reason:
                return reason
            # Reachability is not a hop-limit simulation. Reset the abstract
            # packet at each behavior, as in the FLUID interpreter.
            result = fw.local_sid(replace(packet, hop_limit=255), sid)
            self.query(owner, 'behavior', packet.dst, result.reason or result.action)
            if result.action == fw.DROP:
                return result.reason or PATH_UNREACHABLE
            if result.packet is None:
                return PATH_UNREACHABLE
            if not isinstance(result.packet, IPv6Packet):
                # The synthetic IPv4 inner packet was decapsulated. Its actual
                # endpoint must agree with the declared policy, too.
                return None if self.endpoint_owned(owner, policy) else ENDPOINT_MISMATCH
            packet = result.packet
            current = owner
            if result.action == fw.CROSS_CONNECT:
                peer = self.peer(owner, sid.interface) if sid.interface else None
                if not sid.adjacency_up or peer is None:
                    return PATH_UNREACHABLE
                current = peer


def agent_srdb_view(dev: DeviceState) -> SrDbView | None:
    """Validate the source explicitly. None is the only oracle mode.

    A registered agent that has not advertised a view yet has an empty view.
    Missing agents, malformed views and unsupported modes are errors, never
    requests to use oracle state.
    """
    source = dev.config.srdb_source
    if source is None:
        return None
    if (
        not isinstance(source, tuple)
        or len(source) != 2
        or source[0] != 'agent'
        or not isinstance(source[1], str)
    ):
        raise ValueError(f'unsupported srdb_source: {source!r}')
    agent = dev.agents.get(source[1])
    if agent is None:
        raise ValueError(f'srdb_source refers to missing agent {source[1]!r}')
    view = agent.srdb_view
    if view is None:
        return SrDbView()
    if not isinstance(view, SrDbView):
        raise ValueError('srdb_source agent must advertise SrDbView')
    return view


class _AgentPolicyValidator(_PolicyValidator):
    """Learned ownership/behavior/reachability, not an end-to-end oracle proof.

    Remote state is never accessed. Local first-entry resolution is still
    required separately; subsequent reachability and endpoint ownership use
    advertised locator/host prefixes. Stale claims intentionally stay valid.
    """

    unknown_reason = 'SID_UNKNOWN_IN_VIEW'
    symbolic_reason = 'SID_UNKNOWN_IN_VIEW'

    def __init__(self, state: NetworkState, head: str, view: SrDbView) -> None:
        super().__init__(state, head)
        self.view = view
        self.claims: dict[tuple[str, int, int], RemoteSid] = {}
        self.sids: list[tuple[str, LocalSid]] = []
        for claim in sorted(view.sids, key=lambda c: (c.sid, c.length, c.owner or '')):
            if claim.owner is None:
                continue
            key = claim.owner, claim.sid, claim.length
            if key in self.claims and self.claims[key] != claim:
                raise ValueError('conflicting SID claims in srdb_source view')
            self.claims[key] = claim
            self.sids.append(
                (
                    claim.owner,
                    LocalSid(
                        claim.sid,
                        claim.length,
                        claim.behavior,
                        claim.flavors,
                        claim.structure or UNCOMPRESSED,
                        interface=claim.interface
                        or (
                            f'sid:{claim.sid}/{claim.length}'
                            if claim.behavior == END_X
                            else None
                        ),
                        adjacency_up=claim.adjacency_up,
                    ),
                )
            )
        # The headend's own SR-DB is local knowledge, not remote discovery.
        dev = state.devices[head]
        if dev.srv6_sids is not None:
            self.sids = [(owner, sid) for owner, sid in self.sids if owner != head]
            self.sids.extend((head, sid) for sid in dev.srv6_sids.sids.values())

    def symbolic(self, segment: AdjSeg | NodeSeg | TermSeg) -> LocalSid | None:
        matches = [
            sid
            for owner, sid in self.sids
            if owner == segment.device
            and (
                isinstance(segment, AdjSeg)
                and sid.behavior == END_X
                and sid.interface == segment.interface
                or isinstance(segment, NodeSeg)
                and sid.behavior == END
                or isinstance(segment, TermSeg)
                and sid.behavior == segment.behavior
            )
        ]
        sid = min(matches, key=lambda s: s.sid) if matches else None
        self.query(
            segment.device, 'view-symbolic', segment, sid.sid if sid else 'UNKNOWN'
        )
        return sid

    def literal(self, address: int, current: str) -> tuple[str, LocalSid] | None:
        matches = [
            (owner, sid)
            for owner, sid in self.sids
            if contains((sid.sid, sid.length), address)
        ]
        matches.sort(
            key=lambda pair: (-pair[1].length, pair[0] != current, pair[0], pair[1].sid)
        )
        found = matches[0] if matches else None
        self.query(current, 'view-sid', address, found[0] if found else 'UNKNOWN')
        return found

    def peer(self, device: str, interface: str) -> str | None:
        # Even local adjacencies need an advertised peer identity. The local
        # interface/neighbor resolver provides MACs, not protocol router names.
        peers = {
            claim.peer
            for (owner, _, _), claim in self.claims.items()
            if owner == device
            and claim.behavior == END_X
            and (claim.interface or f'sid:{claim.sid}/{claim.length}') == interface
            and claim.adjacency_up
            and claim.peer is not None
        }
        result = min(peers) if len(peers) == 1 else None
        self.query(device, 'view-adjacency', interface, result or 'UNKNOWN')
        return result

    def reaches(
        self, current: str, owner: str, address: int, sid: LocalSid
    ) -> str | None:
        if not sid.adjacency_up:
            return 'ADJACENCY_DOWN_IN_VIEW'
        if owner == self.head:
            # Only local RIB state may corroborate a local behavior.
            from netsim.model import forwarding as fw

            entry, _ = self.forwarding(self.head, address)
            return (
                None
                if entry and entry.action == fw.SRV6_LOCAL and entry.sid == sid
                else PATH_UNREACHABLE
            )
        known = (
            is_csid(sid.structure)
            and sid.structure.lnl == 0
            and current == owner
            or any(
                node == owner and contains(prefix, address)
                for node, prefix in self.view.locators
            )
        )
        self.query(owner, 'view-locator', address, 'ADVERTISED' if known else 'UNKNOWN')
        return None if known else 'LOCATOR_UNKNOWN_IN_VIEW'

    def endpoint_owned(self, owner: str, policy: SrPolicy) -> bool:
        if owner == self.head:
            return super().endpoint_owned(owner, policy)
        owned = any(
            node == owner and contains(prefix, policy.endpoint)
            for node, prefix in self.view.locators
        )
        self.query(owner, 'view-endpoint', policy.endpoint, owned)
        return owned

    def forwarding(self, device: str, address: int):
        if device != self.head:
            raise ValueError('agent SR validation cannot query a remote RIB')
        return super().forwarding(device, address)


def derive_policy_states(state: NetworkState, device: str) -> Srv6Policies | None:
    """RFC 9256 section 5.1 layers, then NetSim's stronger end-to-end profile.

    Literal metadata is used only for compression, never inferred from the DB.
    Strict results are exposed even when validation is disabled for trap studies.
    """
    from netsim.model.derive import DeviceContext
    from netsim.model.routing import ResolutionPolicy, resolve_first_entry
    from netsim.model.srv6_compress import compress

    dev = state.devices[device]
    view = agent_srdb_view(dev)
    table = dev.srv6_policies
    if table is None:
        return None
    settings = dev.config.resolution_policy or ResolutionPolicy()
    states = table.states.builder()
    for key in table.states:
        if key not in table.policies:
            states.remove(key)
    for key, policy in table.policies.sorted_items():
        validator = (
            _PolicyValidator(state, device)
            if view is None
            else _AgentPolicyValidator(state, device, view)
        )
        basic, first, strict, reasons, valid = [], [], [], [], []
        for pi, path in enumerate(policy.candidate_paths):
            for li, segment_list in enumerate(path.segment_lists):
                reason = (
                    EMPTY_LIST
                    if not segment_list.segments
                    else ZERO_WEIGHT
                    if segment_list.weight <= 0
                    else None
                )
                encoded = []
                for segment in segment_list.segments:
                    if isinstance(segment, (AdjSeg, NodeSeg, TermSeg)):
                        sid = validator.symbolic(segment)
                        if sid is None:
                            reason = reason or validator.symbolic_reason
                        else:
                            encoded.append((sid.sid, sid.structure, sid.flavors))
                    elif isinstance(segment, LiteralSid):
                        encoded.append(
                            (segment.address, segment.structure, segment.flavors or 0)
                        )
                    elif isinstance(segment, int) and 0 <= segment < 1 << 128:
                        encoded.append((segment, None, 0))
                    else:
                        reason = reason or 'SR_MPLS_MIX'
                if reason:
                    reasons.append((pi, li, reason))
                    continue
                try:
                    wire = compress(encoded)
                    reachable, deps = resolve_first_entry(
                        DeviceContext(state, device), settings, Srv6Encap(wire)
                    )
                    for af, address in deps.lookups:
                        validator.query(
                            device, 'first-lookup', (af, address), reachable
                        )
                    for prefix in deps.prefixes:
                        validator.query(device, 'prefix', prefix, 'QUERIED')
                    for interface in deps.interfaces:
                        validator.query(device, 'interface', interface, 'QUERIED')
                    strict_reason = validator.strict(
                        segment_list.segments, wire, policy
                    )
                except ValueError as error:
                    # Packet validation uses ValueError(reason), e.g. an SRH
                    # beyond RFC 8754 §2's Hdr Ext Len limit. Invalid wire
                    # programs reject this list even with strict checks off;
                    # they must not abort publication of unrelated tree edits.
                    reasons.append((pi, li, f'ENCAP_INVALID: {error}'))
                    continue
                if reachable:
                    first.append((pi, li))
                    # RFC 9256 §5.1: first-SID path resolution is mandatory
                    # basic validity, not merely an independent status field.
                    basic.append((pi, li))
                else:
                    reasons.append((pi, li, FIRST_SID_UNRESOLVABLE))
                if strict_reason:
                    reasons.append((pi, li, strict_reason))
                else:
                    strict.append((pi, li))
                if reachable and (
                    strict_reason is None
                    or (view is None and not settings.validate_all_sids)
                ):
                    valid.append((pi, li, wire))
        for pi, li, reason in reasons:
            validator.query(device, 'list-failure', (pi, li), reason)
        old = table.states.get(key)
        fib = dev.fibs.get(6)
        installed = fib.policy_programs.get(key) if fib is not None else None
        installed_candidate = installed.candidate if installed is not None else None
        candidates = {pi for pi, _, _ in valid}

        # RFC 9256 section 2.9: stable tie-breaks, history only when requested.
        def rank(
            pi: int, policy=policy, installed_candidate=installed_candidate
        ) -> tuple:
            p = policy.candidate_paths[pi]
            return (
                -p.preference,
                -p.protocol_origin,
                -(
                    settings.prefer_installed
                    and installed_candidate
                    == (p.protocol_origin, p.originator, p.discriminator)
                ),
                p.originator,
                -p.discriminator,
                pi,
            )

        active = min(candidates, key=rank) if candidates else None
        new = PolicyState(
            active,
            tuple(v for v in valid if v[0] == active),
            POLICY_UP if active is not None else POLICY_DOWN,
            tuple(basic),
            tuple(reasons),
            old.programmed_version if old else 0,
            tuple(strict),
            tuple(first),
            'PENDING',
            tuple(sorted(validator.deps)),
            (
                policy.candidate_paths[active].protocol_origin,
                policy.candidate_paths[active].originator,
                policy.candidate_paths[active].discriminator,
            )
            if active is not None
            else None,
        )
        states.set(key, canon(old, new))
    return canon(table, replace(table, states=states.build()))


def policy_status(
    state: NetworkState, *, capacity_unit: float = 1.0
) -> list[dict[str, Any]]:
    """Detached Study/export rows: validity, programming, and measured delivery.

    Delivery is per demand that actually encountered this policy in placement,
    including at transit/decapsulating nodes. No demand means no observation,
    never an inferred successful delivery. Rates use the caller's capacity unit.
    """
    rows = []
    report = state.placement
    for device, dev in state.devices.sorted_items():
        table = dev.srv6_policies
        if table is None:
            continue
        for key, policy in table.policies.sorted_items():
            result = table.states.get(key) or PolicyState()
            delivery = []
            if report is not None:
                for name, demand in report.demands.sorted_items():
                    for observation in demand.policies:
                        if (
                            observation.device,
                            observation.color,
                            observation.endpoint,
                        ) == (device, *key):
                            delivery.append(
                                {
                                    'demand': name,
                                    'delivered': observation.delivered / capacity_unit,
                                    'drops': [
                                        [r, w, rate / capacity_unit]
                                        for r, w, rate in observation.drops
                                    ],
                                }
                            )
            rows.append(
                {
                    'device': device,
                    'color': policy.color,
                    'endpoint': str(IPv6Address(policy.endpoint)),
                    'name': policy.name,
                    'status': result.status if key in table.states else 'UNCOMPUTED',
                    'basic_valid': bool(result.basic_valid)
                    if key in table.states
                    else None,
                    'basic_valid_lists': [list(k) for k in result.basic_valid],
                    'first_valid': bool(result.first_valid)
                    if key in table.states
                    else None,
                    'first_valid_lists': [list(k) for k in result.first_valid],
                    'strict_valid': bool(result.strict_valid)
                    if key in table.states
                    else None,
                    'strict_valid_lists': [list(k) for k in result.strict_valid],
                    'active_path': result.active_path,
                    'programming': result.programming,
                    'programmed_version': result.programmed_version
                    if key in table.states
                    else None,
                    'reasons': [list(r) for r in result.reasons],
                    'observed_delivery': delivery,
                    'delivered': sum(d['delivered'] for d in delivery)
                    if delivery
                    else None,
                    'delivery_scope': 'placement',
                }
            )
    return rows
