"""SRv6 data model: the Gate B contract shared by every implementation slice.

Everything here is a frozen record or a small int constant; the algorithms
(allocation, compression, encapsulation, validation, forwarding) live in
the modules named in each section and are implemented against these
records. Standards: RFC 8402 (SR architecture), RFC 8754 (SRH), RFC 8986
(network programming), RFC 9256 (SR Policy), RFC 9602 (5f00::/16),
RFC 9800 (NEXT-C-SID). The authoritative Gate B behaviour set is the one
in the design (revision 15): H.Encaps and H.Encaps.Red at the headend;
End and End.X with the PSP and USD flavors; End.DT46; NEXT-C-SID for
those three (uN, uA, uDT46). Anything else is rejected at configuration.
"""

from __future__ import annotations

from dataclasses import field
from typing import Any

from netsim.model.contracts import ClientId
from netsim.model.state import PMap, empty_pmap, record

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
BEHAVIOR_NAMES = {
    END: 'End',
    END_X: 'End.X',
    END_DT46: 'End.DT46',
    END_DT4: 'End.DT4',
    END_DT6: 'End.DT6',
    END_DX4: 'End.DX4',
    END_DX6: 'End.DX6',
    END_B6_ENCAPS: 'End.B6.Encaps',
}

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
class Srv6Sids:
    """A device's SR-DB local part: locators, local SIDs and the allocator."""

    locators: PMap[str, Locator] = field(default_factory=empty_pmap)
    sids: PMap[int, LocalSid] = field(default_factory=empty_pmap)
    """Keyed by SID value."""
    requests: PMap[str, Any] = field(default_factory=empty_pmap)
    """Client SID requests by request id (``SidClient.request_sid``)."""
    drop_unknown_local: bool = True
    """Cover the LIB and WLIB ranges with UNREACHABLE rows (``SID_UNKNOWN`` drops)."""
    next_function: PMap[str, int] = field(default_factory=empty_pmap)
    """Allocation cursors per sub-range name (``adjacency``, ``terminal``, ``bsid``, ``client``)."""


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
    """Lists valid under RFC 9256 §5.1 basic validity: ``(path index, list index)``."""
    reasons: tuple[tuple[int, int, str], ...] = ()
    """Rejections ``(path index, list index, reason)`` from any validation layer."""
    programmed_version: int = 0
    """FIB version that carries this state (PENDING until the FIB run)."""


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
    return BEHAVIOR_NAMES.get(behavior, str(behavior))


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
