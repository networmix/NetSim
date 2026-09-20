"""Hand-checked RFC 9800 §6.2 vectors; no encoder-derived golden values."""

from ipaddress import IPv6Address
from random import Random

import pytest

from netsim.model import packets as pk
from netsim.model import srv6_compress as c
from netsim.model.srv6 import (
    F3216_COMPOSITE,
    F3216_GIB,
    F3216_LIB,
    F3216_TERMINAL,
    F3216_WLIB,
    H_ENCAPS_RED,
    NEXT_CSID,
    PSP,
    USD,
    SidStructure,
)


def ip(text):
    return int(IPv6Address(text))


def sid(text, structure=F3216_LIB, flavors=NEXT_CSID):
    return ip(text), structure, flavors


def encapsulate(entries):
    return pk.encapsulate(
        pk.PacketTemplate(4, 1, 2).to_packet(),
        entries,
        behavior=H_ENCAPS_RED,
        source=99,
        hop_limit=64,
        flow_label=7,
        transit=False,
    )


def test_strict_bare_uas_and_terminal_one_container_no_srh():
    original = (
        sid('5f00:0:e001::'),  # R1.uA -> R2
        sid('5f00:0:e002::'),  # R2.uA -> R4
        sid('5f00:0:e104::', F3216_TERMINAL),  # R4.uDT46
    )
    # B occupies bits 127..96; A1 << 80 | A2 << 64 | D4 << 48.
    # First A1 leaves 80 bits, A2 consumes 16, terminal D4 consumes 16.
    expected = ip('5f00:0:e001:e002:e104::')
    assert c.compress(original) == (expected,)
    outer = encapsulate(c.compress(original))
    assert outer.dst == expected and outer.srh is None and outer.next_header == 4
    assert pk.frame_bytes(outer) == 1074
    # Before R1's own uA shift, Arg = A2 << 64 | D4 << 48 (80 bits).
    shifted, arg = c.shift_csid(outer.dst, F3216_LIB)
    assert arg == 0xE002E104000000000000
    assert shifted == ip('5f00:0:e002:e104::')
    assert c.shift_csid(shifted, F3216_LIB) == (
        ip('5f00:0:e104::'),
        0xE1040000000000000000,
    )


def test_seven_csid_loose_path_and_reduced_srh_boundary():
    original = (
        sid('5f00:0:1::', F3216_GIB),
        sid('5f00:0:e001::'),
        sid('5f00:0:2::', F3216_GIB),
        sid('5f00:0:e002::'),
        sid('5f00:0:3::', F3216_GIB),
        sid('5f00:0:e003::'),
        sid('5f00:0:e104::', F3216_TERMINAL),
    )
    # 32-bit B + six 16-bit C-SIDs = 128. Five fill the first AL=80;
    # the seventh SID cannot fit and remains the second 128-bit entry.
    expected = (ip('5f00:0:1:e001:2:e002:3:e003'), ip('5f00:0:e104::'))
    assert c.compress(original) == expected
    packet = encapsulate(expected)
    assert packet.dst == expected[0]
    assert packet.srh.entries == (expected[1],)
    assert (packet.srh.segments_left, packet.srh.last_entry) == (1, 0)
    assert pk.frame_bytes(packet) == 1098
    da = packet.dst
    # The sixth active C-SID is reached after five successful pre-steps.
    for _, structure, _ in original[:5]:
        da, arg = c.shift_csid(da, structure)
        assert arg != 0
    assert da == ip('5f00:0:e003::')
    assert c.csid_arg(da, F3216_LIB) == 0
    # G3's classic End path then consumes SRH[0]; shifting itself leaves SL alone.
    assert packet.srh.segments_left == 1


def test_mixed_lib_and_wlib_widths():
    original = (
        sid('5f00:0:e001::'),
        sid('5f00:0:fff7:1234::', F3216_WLIB),
        sid('5f00:0:e002::'),
        sid('5f00:0:e104::', F3216_TERMINAL),
    )
    # AL 80 - WLIB 32 - LIB 16 - terminal 16 = 16 zero bits.
    expected = ip('5f00:0:e001:fff7:1234:e002:e104:0')
    assert c.compress(original) == (expected,)
    da, _ = c.shift_csid(expected, F3216_LIB)
    assert da == ip('5f00:0:fff7:1234:e002:e104::')
    da, arg = c.shift_csid(da, F3216_WLIB)
    assert arg == 0xE002E10400000000
    assert da == ip('5f00:0:e002:e104::')
    # Starting with a WLIB gives only 64 argument bits, not a hard-coded 80.
    assert c.compress((original[1], original[0], original[2], original[3])) == (
        ip('5f00:0:fff7:1234:e001:e002:e104:0'),
    )


def test_block_change_starts_new_container():
    # Each pair packs as B | C1<<80 | C2<<64; a different /32 restarts.
    assert c.compress(
        (
            sid('5f00:0:e001::'),
            sid('5f00:0:e002::'),
            sid('5f00:1:e003::'),
            sid('5f00:1:e104::', F3216_TERMINAL),
        )
    ) == (
        ip('5f00:0:e001:e002::'),
        ip('5f00:1:e003:e104::'),
    )


def test_block_lengths_must_match_even_if_leading_bits_do():
    # /32 and /48 metadata identify different blocks, even with zero extension.
    wide_block = SidStructure(48, 0, 16, 64)
    original = (sid('5f00:0:e001::'), sid('5f00:0:0:e002::', wide_block))
    assert c.compress(original) == tuple(item[0] for item in original)


def test_nonzero_argument_is_literal_and_does_not_absorb_following_sid():
    original = (sid('5f00:0:e001::'), sid('5f00:0:e002::1'), sid('5f00:0:e003::'))
    # E002's AL=80 is nonzero, so it cannot join the zero-argument series;
    # its 16+80-bit tail cannot fit in the preceding 80-bit argument either.
    assert c.compress(original) == (
        ip('5f00:0:e001::'),
        ip('5f00:0:e002::1'),
        ip('5f00:0:e003::'),
    )


def test_unknown_literal_breaks_series_and_stays_literal():
    original = (
        sid('5f00:0:e001::'),
        sid('5f00:0:e099::', None),
        sid('5f00:0:e002::'),
        sid('5f00:0:e104::', F3216_TERMINAL),
    )
    # No structure means no bit extraction, even when the address looks F3216.
    assert c.compress(original) == (
        ip('5f00:0:e001::'),
        ip('5f00:0:e099::'),
        ip('5f00:0:e002:e104::'),
    )


def test_exactly_five_csids_fill_the_eighty_argument_bits():
    original = tuple(sid(f'5f00:0:{n:x}::', F3216_GIB) for n in range(1, 8))
    # 80 = 5*16: the sixth SID uses bits 15..0, the seventh starts again.
    assert c.compress(original[:6]) == (ip('5f00:0:1:2:3:4:5:6'),)
    assert c.compress(original) == (ip('5f00:0:1:2:3:4:5:6'), ip('5f00:0:7::'))


@pytest.mark.parametrize('terminal_flavors', [0, NEXT_CSID])
def test_terminal_tail_fits_with_sixteen_bits_but_not_zero_bits(terminal_flavors):
    original = tuple(sid(f'5f00:0:{n:x}::', F3216_GIB) for n in range(1, 7))
    # B|F with LNL=AL=0 identifies the project's terminal tail; the
    # NEXT_CSID flag does not turn it into a shiftable container starter.
    terminal = sid('5f00:0:e104::', F3216_TERMINAL, terminal_flavors)
    # Four appends use 64/80 bits; 16-bit D4 exactly consumes the tail.
    assert c.compress((*original[:5], terminal)) == (ip('5f00:0:1:2:3:4:5:e104'),)
    # Five appends use all 80, so D4 passes through as its own full SID.
    assert c.compress((*original, terminal)) == (
        ip('5f00:0:1:2:3:4:5:6'),
        ip('5f00:0:e104::'),
    )


@pytest.mark.parametrize(
    'address, structure',
    [
        ('5f00:0:2::', SidStructure(32, 16, 0, 0)),
        ('5f00:0:2:e002::', SidStructure(32, 16, 16, 0)),
    ],
)
def test_zero_argument_length_does_not_make_node_metadata_a_terminal(
    address, structure
):
    original = (
        sid('5f00:0:e001::'),
        sid(address, structure),
        sid('5f00:0:e003::'),
    )
    # RFC 9800 §6.1 requires AL=128-LBL-LNFL: these node/composite
    # structures need 80/64 bits, not 0. Although their 16/32-bit prefixes
    # fit E001's 80-bit argument, they are unknown and must remain literal.
    assert c.compress(original) == (
        ip('5f00:0:e001::'),
        ip(address),
        ip('5f00:0:e003::'),
    )


def test_only_one_known_tail_is_consumed_including_its_argument():
    short = SidStructure(32, 0, 16, 16)
    original = (
        sid('5f00:0:e001::'),
        sid('5f00:0:e222:abcd::', short, 0),
        sid('5f00:0:e104::', F3216_TERMINAL),
    )
    # The non-NEXT tail contributes F=E222 then Arg=ABCD (32 bits total).
    # It terminates the container despite 48 unused bits; D4 stays literal.
    assert c.compress(original) == (
        ip('5f00:0:e001:e222:abcd::'),
        ip('5f00:0:e104::'),
    )


def test_terminal_cannot_start_a_shiftable_sequence():
    # AL=0 offers no capacity and must not be silently expanded to 80.
    original = (sid('5f00:0:e104::', F3216_TERMINAL), sid('5f00:0:e001::'))
    assert c.compress(original) == tuple(item[0] for item in original)


@pytest.mark.parametrize(
    'structure',
    [
        SidStructure(0, 16, 0, 112),
        SidStructure(32, 0, 0, 96),
        SidStructure(32, 0, 16, 64),
    ],
)
def test_invalid_flavored_metadata_is_unknown_even_for_terminal_tail(structure):
    literal = sid('5f00:0:e099::', structure)
    original = (sid('5f00:0:e001::'), literal, sid('5f00:0:e002::'))
    assert c.compress(original) == tuple(item[0] for item in original)


def test_nonzero_padding_and_eoc_cannot_be_silently_discarded():
    original = (
        sid('5f00:0:e001::'),
        sid('5f00:0:e104::1', F3216_TERMINAL),
        sid('5f00::', F3216_GIB),
        sid('5f00:0:e002::'),
    )
    assert c.compress(original) == tuple(item[0] for item in original)


def test_plain_sids_stay_literal_and_empty_list_is_empty():
    assert c.compress(()) == ()
    original = (sid('5f00:0:e001::', flavors=0), sid('5f00:0:e002::', flavors=0))
    assert c.compress(original) == tuple(item[0] for item in original)
    assert c.compress(
        (sid('5f00:0:e001::', flavors=NEXT_CSID | PSP | USD), sid('5f00:0:e002::'))
    ) == (ip('5f00:0:e001:e002::'),)


def test_container_display_and_zero_marker():
    da = ip('5f00:0:1:2:3::')
    assert c.container_csids(da, F3216_GIB) == (1, 2, 3)
    assert not c.is_end_of_container(da, F3216_GIB)
    assert c.is_end_of_container(ip('5f00::'), F3216_GIB)
    assert c.container_csids(ip('5f00::'), F3216_GIB) == ()
    # EOC tests the active C-SID, not Arg == 0: the final nonzero SID is active.
    final = ip('5f00:0:3::')
    assert not c.is_end_of_container(final, F3216_GIB)
    assert c.csid_arg(final, F3216_GIB) == 0
    assert c.shift_csid(final, F3216_GIB) == (ip('5f00::'), 0)
    assert c.container_csids(ip('5f00:0:fff7:1234:fff8:abcd::'), F3216_WLIB) == (
        0xFFF71234,
        0xFFF8ABCD,
    )


def test_f3216_builders_preserve_bare_vs_composite():
    block = ip('5f00::')
    assert c.f3216_un(block, 0x1234) == ip('5f00:0:1234::')
    assert c.f3216_ua(block, 0xE012) == ip('5f00:0:e012::')
    assert c.f3216_ua(block, 0xFFF71234, wide=True) == ip('5f00:0:fff7:1234::')
    assert c.f3216_udt46(block, 0xE104) == ip('5f00:0:e104::')
    composite = c.f3216_composite(block, 0x1234, 0xE012)
    assert composite == ip('5f00:0:1234:e012::')
    assert composite != c.f3216_ua(block, 0xE012)
    # Composite LNFL=32, so appending a bare terminal starts at bit 63.
    assert c.compress(
        ((composite, F3216_COMPOSITE, NEXT_CSID), sid('5f00:0:e104::', F3216_TERMINAL))
    ) == (ip('5f00:0:1234:e012:e104::'),)


def test_generic_builder_positions_fields_and_argument_at_the_most_significant_end():
    structure = SidStructure(32, 16, 16, 16)
    assert c.build_sid(
        ip('5f00::'), structure, node=0x1234, function=0xE001, argument=0xABCD
    ) == ip('5f00:0:1234:e001:abcd::')


@pytest.mark.parametrize(
    'call',
    [
        lambda: c.f3216_un(ip('5f00::'), 0),
        lambda: c.f3216_un(ip('5f00::'), 65536),
        lambda: c.f3216_ua(ip('5f00::'), 0),
        lambda: c.f3216_ua(ip('5f00::'), -1),
        lambda: c.f3216_udt46(ip('5f00::'), 65536),
        lambda: c.f3216_composite(ip('5f00::'), 1, 0),
        lambda: c.f3216_un(ip('5f00::1'), 1),
        lambda: c.f3216_un(1 << 128, 1),
        lambda: c.build_sid(ip('5f00::'), F3216_GIB, node=1, argument=1 << 80),
    ],
)
def test_builder_rejects_out_of_range_fields(call):
    with pytest.raises(ValueError):
        call()


@pytest.mark.parametrize(
    'helper', [c.csid_arg, c.shift_csid, c.container_csids, c.is_end_of_container]
)
def test_arithmetic_rejects_invalid_structures_and_addresses(helper):
    for structure in (SidStructure(0, 16, 0, 112), SidStructure(32, 0, 0, 96)):
        with pytest.raises(ValueError):
            helper(ip('5f00::'), structure)
    for value in (-1, 1 << 128):
        with pytest.raises(ValueError):
            helper(value, F3216_LIB)


def test_invalid_sid_values_raise_instead_of_truncating():
    with pytest.raises(ValueError):
        c.compress(((-1, None, 0),))


def test_decompress_by_execution_seeded_property():
    rng = Random(9800)
    # Exercise 16/32-bit widths, multiple blocks, container boundaries and a
    # terminal in 200 independently generated lists. The oracle is the input
    # list: each active prefix must equal its original SID, before any shift.
    for _ in range(200):
        original = []
        for _ in range(rng.randrange(1, 25)):
            structure = rng.choice((F3216_GIB, F3216_LIB, F3216_WLIB, F3216_COMPOSITE))
            block = 0x5F000000 + rng.randrange(2)
            value = rng.randrange(1, 1 << structure.lnfl)
            original.append(
                (block << 96 | value << (96 - structure.lnfl), structure, NEXT_CSID)
            )
        original.append(
            (original[-1][0] >> 96 << 96 | 0xE104 << 80, F3216_TERMINAL, NEXT_CSID)
        )
        encoded = c.compress(original)
        cursor = 0
        da = encoded[cursor]
        for i, (expected, structure, _) in enumerate(original):
            length = structure.installed_length
            assert da >> (128 - length) == expected >> (128 - length)
            if i == len(original) - 1:
                assert da == expected
                break
            arg = c.csid_arg(da, structure)
            if arg:
                da, returned_arg = c.shift_csid(da, structure)
                assert returned_arg == arg
            else:
                cursor += 1
                da = encoded[cursor]
        assert cursor == len(encoded) - 1
