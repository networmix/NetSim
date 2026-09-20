"""B3 acceptance through configured SR-DBs, resolver and packet actions.

Expected addresses are hand-packed RFC 9800 section 6.2 vectors, independent
of the encoder. The runtime companion reuses these configurations for timed
probes and placement. No test installs synthetic srv6-local RIB entries.
"""

from dataclasses import replace
from ipaddress import IPv6Address

import pytest

from netsim.model import forwarding as fw
from netsim.model import srv6 as sr
from netsim.model.contracts import SRV6_LOCAL, STATIC
from netsim.model.hashing import flow_label_for
from netsim.model.igp import oracle_igp
from netsim.model.network import Network
from netsim.model.packets import (
    ORIGINATED,
    TRANSIT,
    FlowKey,
    PacketTemplate,
    encapsulate,
    frame_bytes,
)
from netsim.model.routing import Nexthop
from netsim.model.srv6_compress import compress, csid_arg
from netsim.model.state import validate_immutable
from tests.model.test_forwarding_srv6 import interpreted_trace


def ip(text):
    return int(IPv6Address(text))


def diamond(*, mtu=1500, delay=0, numbered=False):
    net = Network()
    for i in (1, 2, 3, 4, 9):
        dev = net.add_device(f'R{i}')
        dev.add_loopback('lo', ipv4=[f'10.0.0.{i}/32'], ipv6=[f'2001:db8::{i}/128'])
        if i != 9:
            dev.add_locator('loc', structure=sr.F3216_GIB, node_id=i)
    links = {}
    for a, b in ((1, 2), (1, 3), (2, 4), (3, 4), (4, 9)):
        links[a, b] = net.add_p2p(
            net[f'R{a}'],
            f'toR{b}',
            net[f'R{b}'],
            f'toR{a}',
            unnumbered=True,
            ipv6=(f'2001:db8:{a}{b}::1/64', f'2001:db8:{a}{b}::2/64')
            if numbered
            else None,
            mtu=mtu,
            delay=delay,
            speed=1e9,
        )
    net.add_source(oracle_igp)
    return net, links


def ua(net, node, peer, value, *, structure=sr.F3216_LIB, flavors=0, locator='loc'):
    return net[f'R{node}'].add_local_sid(
        sr.END_X,
        sid=value,
        structure=structure,
        flavors=sr.NEXT_CSID | flavors,
        interface=f'toR{peer}',
        locator=locator,
    )


def un(net, node, *, flavors=0):
    return net[f'R{node}'].add_local_sid(
        sr.END,
        structure=sr.F3216_GIB,
        flavors=sr.NEXT_CSID | flavors,
    )


def terminal(net, *, node=4, value='5f00:0:e104::', locator='loc'):
    return net[f'R{node}'].add_local_sid(
        sr.END_DT46,
        sid=value,
        structure=sr.F3216_TERMINAL,
        flavors=sr.NEXT_CSID,
        locator=locator,
    )


def strict_sids(net):
    return (
        ua(net, 1, 2, '5f00:0:e001::'),
        ua(net, 2, 4, '5f00:0:e002::'),
        terminal(net),
    )


def loose_sids(net, *, delay=0, psp=sr.PSP):
    net.add_p2p(net['R2'], 'toR3', net['R3'], 'toR2', unnumbered=True, delay=delay)
    return (
        un(net, 1),
        ua(net, 1, 2, '5f00:0:e001::'),
        un(net, 2),
        ua(net, 2, 3, '5f00:0:e002::'),
        un(net, 3),
        ua(net, 3, 4, '5f00:0:e003::', flavors=psp),
        terminal(net),
    )


def install_path(net, sids, *, compressed=True, destination=4):
    entries = (
        compress(tuple((s.sid, s.structure, s.flavors) for s in sids))
        if compressed
        else tuple(s.sid for s in sids)
    )
    encap = sr.Srv6Encap(entries=entries)
    for prefix in (f'10.0.0.{destination}/32', f'2001:db8::{destination}/128'):
        net['R1'].add_route(prefix, [Nexthop(srv6=encap)])
    net.converge()
    return encap


def inner_template(af=4, *, destination=4, **kw):
    return PacketTemplate(
        af,
        0x0A000001 if af == 4 else ip('2001:db8::1'),
        0x0A000000 + destination if af == 4 else ip(f'2001:db8::{destination}'),
        **kw,
    )


def outer(inner, encap):
    return encapsulate(
        inner,
        encap.entries,
        behavior=encap.behavior,
        source=ip('2001:db8::1'),
        hop_limit=64,
        flow_label=flow_label_for(FlowKey.from_packet(inner), 0),
        transit=False,
    )


@pytest.mark.parametrize('af', [4, 6])
def test_strict_path_from_srdb_through_compiled_and_interpreted_actions(af):
    net, _ = diamond()
    sids = strict_sids(net)
    encap = install_path(net, sids)
    assert encap.entries == (ip('5f00:0:e001:e002:e104::'),)
    packet = inner_template(af).to_packet()
    trace = net.trace('R1', packet)
    assert trace == interpreted_trace(net, packet, encap)
    assert (trace.outcome, trace.path) == (fw.DELIVER, ('R1', 'R2', 'R4'))
    for hop, expected, limit in zip(
        trace.hops[:2],
        ('5f00:0:e002:e104::', '5f00:0:e104::'),
        (63, 62),
        strict=True,
    ):
        assert (hop.packet.dst, hop.packet.hop_limit, hop.packet.srh) == (
            ip(expected),
            limit,
            None,
        )
        assert hop.packet.payload == packet
        assert frame_bytes(hop.packet) == (1074 if af == 4 else 1094)
    assert trace.hops[-1].packet == packet
    for name, sid in zip(('R1', 'R2', 'R4'), sids, strict=True):
        dev = net[name]
        current = dev.node.srv6_sids.sids[sid.sid]
        assert current.adjacency_up
        entry = dev.fib(6).lookup(sid.sid)
        assert entry.action == fw.SRV6_LOCAL and entry.sid == current
        assert entry.prefix == (sid.sid, sid.length)
        assert any(
            row.nexthops[0].behavior == current
            for row in dev.rib(6).rows_of(SRV6_LOCAL)
        )
    fib = net['R1'].fib(af)
    entry = fib.lookup(packet.dst)
    (leg,) = fib.group(entry).adjacencies
    assert (leg.interface, leg.encap) == ('toR2', encap)
    assert entry.depends_on.lookups == ((6, encap.entries[0]),)
    assert 'toR2' in entry.depends_on.interfaces
    before = net.state
    net.converge()
    assert net.state is before
    validate_immutable(net.state)


@pytest.mark.parametrize('compressed', [True, False])
@pytest.mark.parametrize('af', [4, 6])
def test_equivalent_encodings_have_explicit_sizes_and_hop_limits(compressed, af):
    net, _ = diamond()
    sids = strict_sids(net)
    encap = install_path(net, sids, compressed=compressed, destination=9)
    packet = inner_template(af, destination=9, ttl=20, hop_limit=20).to_packet()
    trace = net.trace('R1', packet)
    assert trace == interpreted_trace(net, packet, encap)
    assert (trace.outcome, trace.path) == (fw.DELIVER, ('R1', 'R2', 'R4', 'R9'))
    # Raw three-SID H.Encaps.Red carries two 16-byte entries plus 8-byte base.
    size = (1074 if compressed else 1114) + (20 if af == 6 else 0)
    assert [frame_bytes(h.packet) for h in trace.hops[:2]] == [size, size]
    assert [h.packet.hop_limit for h in trace.hops[:2]] == [63, 62]
    if compressed:
        assert all(h.packet.srh is None for h in trace.hops[:2])
    else:
        assert [h.packet.dst for h in trace.hops[:2]] == [sids[1].sid, sids[2].sid]
        assert [h.packet.srh.segments_left for h in trace.hops[:2]] == [1, 0]
        assert all(
            h.packet.srh.entries == (sids[2].sid, sids[1].sid) for h in trace.hops[:2]
        )
    assert trace.hops[2].packet == trace.hops[3].packet == packet
    assert frame_bytes(trace.hops[2].packet) == (1034 if af == 4 else 1054)


@pytest.mark.parametrize('af', [4, 6])
def test_seven_csids_consume_two_containers_and_pop_at_sixth(af):
    net, _ = diamond()
    sids = loose_sids(net)
    encap = install_path(net, sids)
    assert encap.entries == (ip('5f00:0:1:e001:2:e002:3:e003'), ip('5f00:0:e104::'))
    packet = inner_template(af).to_packet()
    current = outer(packet, encap)
    assert (
        current.dst,
        current.srh.entries,
        current.srh.segments_left,
        current.srh.last_entry,
    ) == (
        encap.entries[0],
        (encap.entries[1],),
        1,
        0,
    )
    # RFC 9800 section 4.1.1 pre-step leaves SL alone for the first five.
    for i, sid in enumerate(sids[:6]):
        assert (csid_arg(current.dst, sid.structure) == 0) == (i == 5)
        step = fw.local_sid(current, sid)
        assert step.action == (fw.RELOOKUP if i % 2 == 0 else fw.CROSS_CONNECT)
        current = step.packet
        assert current.hop_limit == 63 - i
        if i < 5:
            assert current.srh.segments_left == 1
        else:
            assert current.dst == encap.entries[1] and current.srh is None
    trace = net.trace('R1', packet)
    assert trace == interpreted_trace(net, packet, encap)
    assert (trace.outcome, trace.path) == (fw.DELIVER, ('R1', 'R2', 'R3', 'R4'))
    assert [h.packet.hop_limit for h in trace.hops[:3]] == [62, 60, 58]
    assert [h.packet.dst for h in trace.hops[:3]] == [
        ip('5f00:0:2:e002:3:e003::'),
        ip('5f00:0:3:e003::'),
        ip('5f00:0:e104::'),
    ]
    delta = 20 if af == 6 else 0
    assert [frame_bytes(h.packet) for h in trace.hops[:3]] == [
        1098 + delta,
        1098 + delta,
        1074 + delta,
    ]
    assert trace.hops[-1].packet == packet


@pytest.mark.parametrize(
    'numbered',
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.xfail(
                strict=True,
                reason='netsim/model/routing.py:603 _substitute replaces an unnumbered '
                'locator peer adjacency with the remote SID neighbor lookup; encap '
                'route becomes UNRESOLVED and falls back to plain IP (G5 owns routing)',
            ),
        ),
    ],
)
def test_composite_routes_to_node_but_same_bare_function_executes_at_ingress(numbered):
    net, _ = diamond(numbered=numbered)
    local = ua(net, 1, 3, '5f00:0:e002::')
    remote = ua(net, 2, 4, '5f00:0:e002::')
    composite = ua(net, 2, 4, '5f00:0:2:e002::', structure=sr.F3216_COMPOSITE)
    un(net, 2)  # /64 composite must beat this local /48 End.
    last = un(net, 4, flavors=sr.USD)
    assert local.sid == remote.sid != composite.sid
    packet = inner_template().to_packet()
    for first, expected_path, first_da, limits in (
        (local, ('R1', 'R3', 'R4'), '5f00:0:4::', [63, 62]),
        (composite, ('R1', 'R2', 'R4'), '5f00:0:2:e002:4::', [64, 63]),
    ):
        encap = install_path(net, (first, last))
        trace = net.trace('R1', packet)
        assert trace == interpreted_trace(net, packet, encap)
        assert (trace.outcome, trace.path) == (fw.DELIVER, expected_path)
        assert trace.hops[0].packet.dst == ip(first_da)
        assert [h.packet.hop_limit for h in trace.hops[:2]] == limits
        assert trace.hops[-1].packet == packet
    route = net['R1'].fib(6).lookup(composite.sid)
    assert route.prefix == (ip('5f00:0:2::'), 48) and route.action == fw.FORWARD
    assert net['R2'].fib(6).lookup(composite.sid).sid.sid == composite.sid
    assert net['R1'].fib(6).lookup(local.sid).sid.interface == 'toR3'
    assert net['R2'].fib(6).lookup(remote.sid).sid.interface == 'toR4'


def test_end_relookup_and_end_x_cross_connect_are_distinct_continuations():
    net, _ = diamond()
    sids = strict_sids(net)
    own_node = un(net, 1)
    # The shifted R2 function is also local at R1, bound to the wrong branch.
    # End.X must transmit before this local match; End must re-lookup it.
    ua(net, 1, 3, '5f00:0:e002::')
    encap = install_path(net, sids)
    packet = inner_template().to_packet()
    assert fw.local_sid(outer(packet, encap), sids[0]).action == fw.CROSS_CONNECT
    assert net.trace('R1', packet).path == ('R1', 'R2', 'R4')
    encap = install_path(net, (own_node, *sids))
    assert encap.entries == (ip('5f00:0:1:e001:e002:e104::'),)
    assert fw.local_sid(outer(packet, encap), own_node).action == fw.RELOOKUP
    trace = net.trace('R1', packet)
    assert trace == interpreted_trace(net, packet, encap)
    assert (trace.outcome, trace.path) == (fw.DELIVER, ('R1', 'R2', 'R4'))
    assert [h.packet.hop_limit for h in trace.hops[:2]] == [62, 61]
    entry = net['R1'].fib(4).lookup(packet.dst)
    assert set(entry.depends_on.lookups) == {
        (6, encap.entries[0]),
        (6, ip('5f00:0:e001:e002:e104::')),
    }


@pytest.mark.parametrize('wide_first', [False, True])
def test_mixed_widths_use_the_active_srdb_structure(wide_first):
    net, _ = diamond()
    narrow = ('5f00:0:e001::', sr.F3216_LIB)
    wide = ('5f00:0:fff7:1234::', sr.F3216_WLIB)
    a, b = (wide, narrow) if wide_first else (narrow, wide)
    sids = (
        ua(net, 1, 2, a[0], structure=a[1]),
        ua(net, 2, 4, b[0], structure=b[1]),
        terminal(net),
    )
    encap = install_path(net, sids)
    expected = (
        '5f00:0:fff7:1234:e001:e104::' if wide_first else '5f00:0:e001:fff7:1234:e104::'
    )
    assert encap.entries == (ip(expected),)
    trace = net.trace('R1', inner_template().to_packet())
    assert trace.outcome == fw.DELIVER and trace.path == ('R1', 'R2', 'R4')
    shifted = '5f00:0:e001:e104::' if wide_first else '5f00:0:fff7:1234:e104::'
    assert [h.packet.dst for h in trace.hops[:2]] == [ip(shifted), ip('5f00:0:e104::')]
    assert [h.packet.hop_limit for h in trace.hops[:2]] == [63, 62]
    assert all(
        h.packet.srh is None and frame_bytes(h.packet) == 1074 for h in trace.hops[:2]
    )


def test_block_boundary_starts_new_container_and_keeps_local_scope():
    net, _ = diamond()
    for node in (2, 4):
        net[f'R{node}'].add_locator(
            'other', block='5f00:1::/32', structure=sr.F3216_GIB, node_id=node
        )
    sids = (
        ua(net, 1, 2, '5f00:0:e001::'),
        ua(net, 2, 4, '5f00:1:e001::', locator='other', flavors=sr.PSP),
        terminal(net, value='5f00:1:e104::', locator='other'),
    )
    ua(net, 2, 1, '5f00:0:e001::')  # Same function in a different block must not run.
    encap = install_path(net, sids)
    assert encap.entries == (ip('5f00:0:e001::'), ip('5f00:1:e001:e104::'))
    trace = net.trace('R1', inner_template().to_packet())
    assert trace == interpreted_trace(net, inner_template().to_packet(), encap)
    assert (trace.outcome, trace.path) == (fw.DELIVER, ('R1', 'R2', 'R4'))
    first, second = (h.packet for h in trace.hops[:2])
    assert (first.dst, first.hop_limit, first.srh.segments_left) == (
        encap.entries[1],
        63,
        0,
    )
    # PSP only pops on the classic SL transition, never on a nonzero-Arg shift.
    assert (second.dst, second.hop_limit, second.srh.segments_left) == (
        ip('5f00:1:e104::'),
        62,
        0,
    )
    assert frame_bytes(first) == frame_bytes(second) == 1098


@pytest.mark.parametrize('compressed', [True, False])
@pytest.mark.parametrize('flavors', [0, sr.NEXT_CSID])
def test_composite_terminal_retains_node_function_and_ends_srh(compressed, flavors):
    net, _ = diamond(numbered=True)
    sids = (
        ua(net, 1, 2, '5f00:0:e001::'),
        ua(net, 2, 4, '5f00:0:2:e002::', structure=sr.F3216_COMPOSITE),
        net['R4'].add_local_sid(
            sr.END_DT46,
            sid='5f00:0:4:e104::',
            structure=sr.F3216_COMPOSITE,
            flavors=flavors,
        ),
    )
    encap = install_path(net, sids, compressed=compressed)
    # RFC 9800 section 6.2: NEXT + Arg=0 extends the series by LNFL=32.
    # Without NEXT, its 32+64-bit tail cannot fit the remaining 48 bits.
    single = compressed and bool(flavors & sr.NEXT_CSID)
    assert encap.entries == (
        (ip('5f00:0:e001:2:e002:4:e104:0'),)
        if single
        else (ip('5f00:0:e001:2:e002::'), ip('5f00:0:4:e104::'))
        if compressed
        else tuple(s.sid for s in sids)
    )
    packet = inner_template().to_packet()
    trace = net.trace('R1', packet)
    assert trace == interpreted_trace(net, packet, encap)
    assert (trace.outcome, trace.path) == (fw.DELIVER, ('R1', 'R2', 'R4'))
    assert [h.packet.dst for h in trace.hops[:2]] == [
        ip('5f00:0:2:e002:4:e104::') if single else sids[1].sid,
        sids[2].sid,
    ]
    assert [h.packet.hop_limit for h in trace.hops[:2]] == [63, 62]
    if single:
        assert all(h.packet.srh is None for h in trace.hops[:2])
    else:
        assert [h.packet.srh.segments_left for h in trace.hops[:2]] == [1, 0]
    assert [frame_bytes(h.packet) for h in trace.hops[:2]] == [
        1074 if single else 1098 if compressed else 1114
    ] * 2
    assert net['R4'].fib(6).lookup(sids[2].sid).prefix == (sids[2].sid, 64)
    assert trace.hops[-1].packet == packet


def test_custom_local_range_cover_has_no_holes_and_does_not_shadow_summary():
    net = Network()
    dev = net.add_device('R1')
    ranges = sr.SidRanges(lib=(0xE011, 0xE01F), wlib=(0xFFF8, 0xFFFA))
    dev.add_locator('loc', structure=sr.F3216_GIB, ranges=ranges)
    dev.add_route('5f00::/32', [Nexthop.receive()])
    net.converge()
    for function in (0xDFFF, 0xE000, *range(0xE010, 0xE021), *range(0xFFF7, 0xFFFC)):
        address = ip('5f00::') | function << 80 | 0x1234 << 64
        trace = net.trace(
            'R1', PacketTemplate(6, ip('2001:db8::1'), address).to_packet()
        )
        covered = 0xE011 <= function <= 0xE01F or 0xFFF8 <= function <= 0xFFFA
        assert (trace.outcome, trace.reason) == (
            (fw.DROP, sr.SID_UNKNOWN) if covered else (fw.DELIVER, None)
        )


@pytest.mark.parametrize(
    'value',
    ['5f00:0:e000::', '5f00:0:fff6:1234::', '5f00:0:fff7:1234::', '5f00:0:ffff:ffff::'],
)
def test_unknown_local_lib_and_wlib_drop_instead_of_following_summary(value):
    net, _ = diamond()
    net['R1'].add_route('5f00::/32', ['toR2'])
    net['R2'].add_route('5f00::/32', [Nexthop.receive()])
    net.converge()
    packet = PacketTemplate(6, ip('2001:db8::1'), ip(value)).to_packet()
    trace = net.trace('R1', packet)
    assert (trace.path, trace.reason) == (('R1',), sr.SID_UNKNOWN)
    assert net['R1'].fib(6).lookup(ip(value)).prefix[1] > 32
    # GIB and outside-block addresses must not be caught by the LIB cover.
    other = replace(packet, dst=ip('5f00:0:100::'))
    assert net.trace('R1', other).outcome == fw.DELIVER
    assert net['R1'].fib(6).lookup(other.dst).prefix[1] == 32
    assert net['R1'].fib(6).lookup(ip('5f00:1:e000::')) is None


def test_multiblock_exhaustion_collision_and_rollback_preserve_live_path():
    net, _ = diamond()
    encap = install_path(net, strict_sids(net))
    before_trace = net.trace('R1', inner_template().to_packet())
    ranges = sr.SidRanges(gib=(1, 1), lib=(0xE000, 0xE003), wlib=(0xFFFE, 0xFFFF))
    dev = net['R1']
    for name, block in (('tiny', '5f01::/32'), ('tiny2', '5f02::/32')):
        dev.add_locator(name, block=block, structure=sr.F3216_GIB, ranges=ranges)
        sid = dev.add_local_sid(
            sr.END_X,
            structure=sr.F3216_LIB,
            flavors=sr.NEXT_CSID,
            interface='toR2',
            locator=name,
        )
        assert sid.sid == ip(block.split('/')[0]) | 0xE000 << 80
        old = net.state
        with pytest.raises(ValueError, match='exhausted'):
            dev.add_local_sid(
                sr.END_X, structure=sr.F3216_LIB, interface='toR3', locator=name
            )
        assert net.state is old
        with pytest.raises(ValueError, match='collision'):
            dev.add_local_sid(
                sr.END_X,
                structure=sr.F3216_LIB,
                sid=sid.sid,
                interface='toR3',
                locator=name,
            )
        assert net.state is old
        with pytest.raises(ValueError, match='exhausted'):
            net['R3'].add_locator(
                name, block=block, structure=sr.F3216_GIB, ranges=ranges
            )
        assert net.state is old
        with pytest.raises(ValueError, match='unique'):
            net['R3'].add_locator(
                name, block=block, structure=sr.F3216_GIB, ranges=ranges, node_id=1
            )
        assert net.state is old
    net.converge()
    assert net.validate() == []
    assert net.trace('R1', inner_template().to_packet()) == before_trace
    assert interpreted_trace(net, inner_template().to_packet(), encap) == before_trace


@pytest.mark.parametrize('kind', ['loopback', 'ethernet'])
@pytest.mark.parametrize('locator_first', [True, False])
def test_locator_block_never_covers_interface_or_loopback(kind, locator_first):
    net = Network()
    a, b = net.add_device('a'), net.add_device('b')
    add_address = b.add_loopback if kind == 'loopback' else b.add_ethernet
    if locator_first:
        a.add_locator('loc', structure=sr.F3216_GIB)
        before = net.state
        with pytest.raises(ValueError, match='covers interface address'):
            add_address('p', ipv6=['5f00:0:1234::1/128'])
    else:
        add_address('p', ipv6=['5f00:0:1234::1/128'])
        before = net.state
        with pytest.raises(ValueError, match='covers interface address'):
            a.add_locator('loc', structure=sr.F3216_GIB)
    assert net.state is before


@pytest.mark.parametrize('compressed', [True, False])
@pytest.mark.parametrize('af', [4, 6])
def test_transit_inner_decrement_and_outer_sid_budget_are_separate(compressed, af):
    net, _ = diamond()
    encap = install_path(net, strict_sids(net), compressed=compressed)
    packet = inner_template(af, ttl=2, hop_limit=2).to_packet()
    step = fw.forward_ip(net.view('R1'), packet, 'toR3', TRANSIT)
    assert step.outcome == fw.TRANSMIT and step.packet.hop_limit == 63
    assert step.packet.payload == replace(
        packet, **({'ttl': 1} if af == 4 else {'hop_limit': 1})
    )
    originated = fw.forward_ip(net.view('R1'), packet, None, ORIGINATED, encap=encap)
    assert originated.packet.payload == packet
    # Two uA actions need two outer decrements even with no SRH.
    net['R1'].configure(srv6_hop_limit=2)
    assert net.trace('R1', packet).reason == fw.TTL_EXPIRED
    net['R1'].configure(srv6_hop_limit=3)
    assert net.trace('R1', packet).outcome == fw.DELIVER


def install_policy_path(net, sids, *, symbolic=False, destination=4):
    """Pending G5 counterpart: no direct Srv6Encap route is installed."""
    segments = (
        (sr.AdjSeg('R1', 'toR2'), sr.AdjSeg('R2', 'toR4'), sr.TermSeg('R4'))
        if symbolic
        else tuple(sr.LiteralSid(s.sid, s.structure, s.flavors) for s in sids)
    )
    policy = sr.SrPolicy(
        STATIC,
        10,
        ip('2001:db8::4'),
        candidate_paths=(sr.CandidatePath(segment_lists=(sr.SegmentList(segments),)),),
    )
    net['R1'].policy_client().add(policy)
    for prefix in (f'10.0.0.{destination}/32', f'2001:db8::{destination}/128'):
        net['R1'].add_route(prefix, [Nexthop(policy=sr.PolicyRef(*policy.key))])
    net.converge()
    return policy


@pytest.mark.skip(reason='needs G5 policies')
@pytest.mark.parametrize('path', ['strict-literal', 'strict-symbolic', 'loose'])
def test_policy_counterpart_compresses_and_delivers_the_configured_path(path):
    net, _ = diamond()
    sids = loose_sids(net) if path == 'loose' else strict_sids(net)
    policy = install_policy_path(net, sids, symbolic=path == 'strict-symbolic')
    state = net['R1'].node.srv6_policies.states[policy.key]
    expected = (
        (ip('5f00:0:1:e001:2:e002:3:e003'), ip('5f00:0:e104::'))
        if path == 'loose'
        else (ip('5f00:0:e001:e002:e104::'),)
    )
    assert state.status == sr.POLICY_UP
    assert state.valid_lists == ((0, 0, expected),)
    trace = net.trace('R1', inner_template().to_packet())
    assert trace.outcome == fw.DELIVER
    assert trace.path == (
        ('R1', 'R2', 'R3', 'R4') if path == 'loose' else ('R1', 'R2', 'R4')
    )


def form_path(net, form):
    """Concrete SID forms with independently packed entries and physical paths."""
    if form in ('bare', 'composite'):
        bare = ua(net, 1, 3, '5f00:0:e002::')
        remote = ua(net, 2, 4, '5f00:0:2:e002::', structure=sr.F3216_COMPOSITE)
        last = un(net, 4, flavors=sr.USD)
        sids = (bare if form == 'bare' else remote, last)
        expected = (ip('5f00:0:e002:4::' if form == 'bare' else '5f00:0:2:e002:4::'),)
        path = ('R1', 'R3', 'R4') if form == 'bare' else ('R1', 'R2', 'R4')
    elif form == 'wlib':
        sids = (
            ua(net, 1, 2, '5f00:0:e001::'),
            ua(net, 2, 4, '5f00:0:fff7:1234::', structure=sr.F3216_WLIB),
            terminal(net),
        )
        expected, path = (ip('5f00:0:e001:fff7:1234:e104::'),), ('R1', 'R2', 'R4')
    else:
        for node in (2, 4):
            net[f'R{node}'].add_locator(
                'other', block='5f00:1::/32', structure=sr.F3216_GIB, node_id=node
            )
        sids = (
            ua(net, 1, 2, '5f00:0:e001::'),
            ua(net, 2, 4, '5f00:1:e001::', locator='other'),
            terminal(net, value='5f00:1:e104::', locator='other'),
        )
        expected = (ip('5f00:0:e001::'), ip('5f00:1:e001:e104::'))
        path = ('R1', 'R2', 'R4')
    return sids, expected, path


@pytest.mark.skip(reason='needs G5 policies')
@pytest.mark.parametrize('form', ['bare', 'composite', 'wlib', 'two-blocks'])
def test_policy_counterpart_preserves_forms_widths_and_blocks(form):
    net, _ = diamond(numbered=True)
    sids, expected, path = form_path(net, form)
    policy = install_policy_path(net, sids)
    state = net['R1'].node.srv6_policies.states[policy.key]
    assert state.status == sr.POLICY_UP and state.valid_lists == ((0, 0, expected),)
    trace = net.trace('R1', inner_template().to_packet())
    assert (trace.outcome, trace.path) == (fw.DELIVER, path)
    assert trace == interpreted_trace(
        net, inner_template().to_packet(), sr.Srv6Encap(expected)
    )
