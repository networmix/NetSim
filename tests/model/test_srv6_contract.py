"""The Gate B contract records: constructible, frozen, and the behaviour set."""

import dataclasses

import pytest

from netsim.model import srv6
from netsim.model.state import validate_immutable


def test_structures_and_formats():
    assert srv6.UNCOMPRESSED.total_bits == 128
    assert srv6.F3216_GIB.lnfl == 16 and srv6.F3216_GIB.installed_length == 48
    assert srv6.F3216_COMPOSITE.installed_length == 64
    assert srv6.F3216_TERMINAL.al == 0
    with pytest.raises(ValueError):
        srv6.SidStructure(64, 64, 8, 0)
    with pytest.raises(ValueError):
        srv6.SidStructure(-1, 0, 0, 0)


def test_gate_b_behaviour_set():
    assert srv6.check_gate_b(srv6.END, srv6.PSP | srv6.NEXT_CSID) is None
    assert srv6.check_gate_b(srv6.END_X, srv6.USD) is None
    assert srv6.check_gate_b(srv6.END_DT46, srv6.NEXT_CSID) is None
    assert srv6.check_gate_b(srv6.END_B6_ENCAPS, 0) == srv6.UNSUPPORTED_BEHAVIOR
    assert srv6.check_gate_b(srv6.END_DT4, 0) == srv6.UNSUPPORTED_BEHAVIOR
    assert srv6.check_gate_b(srv6.END, srv6.USP) == srv6.UNSUPPORTED_FLAVOR
    assert srv6.check_gate_b(srv6.END_DT46, srv6.PSP) == srv6.UNSUPPORTED_FLAVOR
    assert srv6.behavior_name(srv6.END_X) == 'End.X'


def test_records_are_immutable_tree_leaves():
    loc = srv6.Locator('loc0', (0x5F00 << 112 | 1 << 80, 48), srv6.F3216_GIB, node_id=1)
    sid = srv6.LocalSid(
        loc.prefix[0], 48, srv6.END, srv6.PSP | srv6.NEXT_CSID, srv6.F3216_GIB
    )
    table = srv6.Srv6Sids(
        locators=srv6.Srv6Sids().locators.set('loc0', loc),
        sids=srv6.Srv6Sids().sids.set(sid.sid, sid),
    )
    policy = srv6.SrPolicy(
        srv6.SrPolicy.__dataclass_fields__['owner'].type
        and __import__('netsim.model.contracts', fromlist=['STATIC']).STATIC,
        10,
        0x20010DB8 << 96 | 4,
        candidate_paths=(
            srv6.CandidatePath(
                200,
                (
                    srv6.SegmentList(
                        (srv6.AdjSeg('R1', 'Po1'), srv6.TermSeg('R4')),
                    ),
                ),
            ),
        ),
    )
    policies = srv6.Srv6Policies(
        policies=srv6.Srv6Policies().policies.set(policy.key, policy)
    )
    for obj in (table, policies, srv6.Srv6Encap((1, 2)), srv6.PolicyRef(10, 4)):
        validate_immutable(obj)
    with pytest.raises(dataclasses.FrozenInstanceError):
        sid.behavior = srv6.END_X  # type: ignore[misc]
    assert policy.key == (10, 0x20010DB8 << 96 | 4)


def test_behavior_names_do_not_depend_on_shared_mutable_state():
    assert not hasattr(srv6, 'BEHAVIOR_NAMES')
    assert [
        srv6.behavior_name(b)
        for b in (srv6.END, srv6.END_X, srv6.END_DT46, srv6.END_B6_ENCAPS, 99)
    ] == [
        'End',
        'End.X',
        'End.DT46',
        'End.B6.Encaps',
        '99',
    ]
