"""Pure NEXT-C-SID encoding and destination arithmetic (RFC 9800).

The input and output of ``compress`` are in forwarding order. RFC 9800
§6.2 S01-S16 is NetSim's deterministic encoder; the SRH reverses the
result only at encapsulation. No allocation or reachability is inferred.
"""

from __future__ import annotations

from collections.abc import Sequence

from netsim.model.srv6 import (
    F3216_COMPOSITE,
    F3216_GIB,
    F3216_LIB,
    F3216_TERMINAL,
    F3216_WLIB,
    NEXT_CSID,
    SidStructure,
)


def _check_address(value: int) -> None:
    if not 0 <= value < 1 << 128:
        raise ValueError('SID_VALUE_OUT_OF_RANGE')


def _check_layout(structure: SidStructure) -> None:
    if structure.lbl == 0 or structure.lnfl == 0:
        raise ValueError('SID_STRUCTURE_INVALID')


def csid_arg(da: int, structure: SidStructure) -> int:
    """DA bits after LBL+LNFL, before shifting (RFC 9800 §4.1.1).

    This is the physical suffix, including for NetSim's AL=0 terminal
    structures. It is not the active C-SID and does not test the EOC marker.
    """
    _check_address(da)
    _check_layout(structure)
    return da & ((1 << (128 - structure.installed_length)) - 1)


def shift_csid(da: int, structure: SidStructure) -> tuple[int, int]:
    """Return (shifted DA, original Arg), keeping the Locator-Block.

    RFC 9800 §4.1.1 N05-N06: copy the suffix left by LNFL and zero-fill
    the low LNFL bits. The caller checks Arg and performs TTL/SL behavior.
    """
    arg = csid_arg(da, structure)
    suffix_bits = 128 - structure.lbl
    block = da >> suffix_bits << suffix_bits
    return block | arg << structure.lnfl, arg


def is_end_of_container(da: int, structure: SidStructure) -> bool:
    """Whether the active C-SID is the zero End-of-Container marker.

    A final nonzero C-SID with Arg=0 is still active: use ``csid_arg``
    for the pre-step's decision to continue with classic SRH processing.
    """
    _check_address(da)
    _check_layout(structure)
    return (da >> (128 - structure.installed_length)) & ((1 << structure.lnfl) - 1) == 0


def container_csids(da: int, structure: SidStructure) -> tuple[int, ...]:
    """Display uniform LNFL-width chunks through the first zero marker.

    Mixed-width containers need each active SID's own structure to execute;
    their boundaries cannot be recovered from an address alone.
    """
    _check_address(da)
    _check_layout(structure)
    values = []
    mask = (1 << structure.lnfl) - 1
    for shift in range(128 - structure.installed_length, -1, -structure.lnfl):
        value = da >> shift & mask
        if value == 0:
            break
        values.append(value)
    return tuple(values)


def _known_structure(
    sid: int, structure: SidStructure | None, flavors: int
) -> SidStructure | None:
    if structure is None or structure.lbl == 0 or structure.lnfl == 0:
        return None
    # RFC 9800 §6.1: a shiftable flavored SID must fill all 128 bits.
    # Revision 15 also models terminal uDT46 with AL=0: it is a known
    # S12 tail, never a shiftable series member. Other incomplete flavored
    # metadata is unknown, including during the tail-fit check.
    if flavors & NEXT_CSID and structure.total_bits != 128 and structure.al != 0:
        return None
    # A short tail must have zero padding, otherwise compression loses bits.
    if sid & ((1 << (128 - structure.total_bits)) - 1):
        return None
    if is_end_of_container(sid, structure):
        return None
    return structure


def _compressible(sid: int, structure: SidStructure | None, flavors: int) -> bool:
    return (
        structure is not None
        and bool(flavors & NEXT_CSID)
        and structure.total_bits == 128
        and csid_arg(sid, structure) == 0
    )


def _same_block(
    a: int, a_structure: SidStructure, b: int, b_structure: SidStructure
) -> bool:
    return a_structure.lbl == b_structure.lbl and a >> (128 - a_structure.lbl) == b >> (
        128 - b_structure.lbl
    )


def compress(sids: Sequence[tuple[int, SidStructure | None, int]]) -> tuple[int, ...]:
    """Encode NEXT-C-SID series and at most one terminal tail per container.

    Unknown/invalid metadata stays literal. Known unflavored SIDs can be
    tails under §6.2 S12, but cannot initiate or extend the shiftable series.
    Input SID values must be unsigned 128-bit integers.
    """
    known = []
    for sid, structure, flavors in sids:
        _check_address(sid)
        known.append((sid, _known_structure(sid, structure, flavors), flavors))
    result = []
    i = 0
    while i < len(known):
        container, first, flavors = known[i]
        i += 1
        if not _compressible(container, first, flavors):
            result.append(container)
            continue
        assert first is not None
        remaining = first.al  # §6.2 S01; never assume an F3216 width.
        while i < len(known):
            sid, structure, flavors = known[i]
            if not _compressible(sid, structure, flavors):
                break
            assert structure is not None
            if (
                not _same_block(container, first, sid, structure)
                or structure.lnfl > remaining
            ):
                break  # S06-S08: next outer iteration starts a new container.
            remaining -= structure.lnfl
            csid = sid >> (128 - structure.installed_length) & (
                (1 << structure.lnfl) - 1
            )
            container |= csid << remaining  # S04: most significant free bits.
            i += 1
        if i < len(known):
            sid, structure, _ = known[i]
            if structure is not None and _same_block(container, first, sid, structure):
                tail_bits = structure.lnfl + structure.al
                if tail_bits <= remaining:
                    # S12-S14: copy one entire known tail (including Argument)
                    # and consume it exactly once, even if unused capacity remains.
                    tail = sid >> (128 - structure.total_bits) & ((1 << tail_bits) - 1)
                    container |= tail << (remaining - tail_bits)
                    i += 1
        result.append(container)  # S16
    return tuple(result)


def build_sid(
    block: int,
    structure: SidStructure,
    *,
    node: int = 0,
    function: int = 0,
    argument: int = 0,
) -> int:
    """Build B|N|F|A with zero padding after ``total_bits``.

    ``block`` is an aligned 128-bit network address (e.g. DEFAULT_BLOCK[0]),
    not a right-aligned /32 integer. Allocation/range ownership is G1's job.
    """
    _check_address(block)
    _check_layout(structure)
    if block & ((1 << (128 - structure.lbl)) - 1):
        raise ValueError('SID_BLOCK_NOT_ALIGNED')
    result = block
    offset = structure.lbl
    for value, width in (
        (node, structure.lnl),
        (function, structure.fl),
        (argument, structure.al),
    ):
        if not 0 <= value < 1 << width:
            raise ValueError('SID_FIELD_OUT_OF_RANGE')
        offset += width
        result |= value << (128 - offset)
    return result


def f3216_un(block: int, node_id: int) -> int:
    """Bare uN: B|node; node_id=0 is the reserved EOC marker."""
    if node_id == 0:
        raise ValueError('CSID_ZERO_RESERVED')
    return build_sid(block, F3216_GIB, node=node_id)


def f3216_ua(block: int, function: int, *, wide: bool = False) -> int:
    """Bare uA: B|function (16-bit LIB, or 32-bit WLIB when wide=True)."""
    if function == 0:
        raise ValueError('CSID_ZERO_RESERVED')
    return build_sid(block, F3216_WLIB if wide else F3216_LIB, function=function)


def f3216_udt46(block: int, function: int) -> int:
    """Bare terminal uDT46: B|function, with AL=0 and zero padding."""
    if function == 0:
        raise ValueError('CSID_ZERO_RESERVED')
    return build_sid(block, F3216_TERMINAL, function=function)


def f3216_composite(block: int, node_id: int, function: int) -> int:
    """Composite /64: B|node|function, distinct from the bare /48 SID."""
    if node_id == 0 or function == 0:
        raise ValueError('CSID_ZERO_RESERVED')
    return build_sid(block, F3216_COMPOSITE, node=node_id, function=function)
