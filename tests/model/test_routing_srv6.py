"""SR FIB compilation and dependency tests using public route installation."""

from dataclasses import replace

import pytest

from netsim.model import forwarding as fw
from netsim.model.derive import DeviceContext
from netsim.model.routing import (
    SRV6_LOCAL_NH,
    Nexthop,
    ResolutionPolicy,
    resolve_fib,
)
from netsim.model.srv6 import (
    END,
    END_DT46,
    END_X,
    F3216_GIB,
    NEXT_CSID,
    LocalSid,
    Srv6Encap,
)
from netsim.model.state import validate_immutable
from tests.model.test_forwarding_srv6 import (
    address,
    inner_packet,
    install_sid,
    path_network,
)


def encap_entry(router):
    fib = router.fib(4)
    entry = fib.lookup(address('10.0.0.4'))
    return entry, fib.group(entry)


def test_local_entry_carries_exact_sid_without_group():
    net, routers, _ = path_network()
    sid = LocalSid(address('2001:db8:9::'), 128, END_X, interface='toR4')
    install_sid(routers['R2'], sid)
    net.converge()
    fib = routers['R2'].fib(6)
    entry = fib.lookup(sid.sid)
    assert entry.action == fw.SRV6_LOCAL
    assert entry.sid is sid and entry.group_id is None and fib.group(entry) is None
    validate_immutable(entry)


@pytest.mark.parametrize('compressed', [False, True])
def test_encap_leg_preserves_header_transform_and_dependencies(compressed):
    net, routers, encap = path_network(compressed=compressed)
    entry, group = encap_entry(routers['R1'])
    assert entry.action == fw.FORWARD
    assert len(group.adjacencies) == 1
    leg = group.adjacencies[0]
    assert leg.encap == encap and leg.interface == 'toR2'
    assert (6, encap.entries[0]) in entry.depends_on.lookups
    assert 'toR2' in entry.depends_on.interfaces
    assert any(af == 6 for af, _, _ in entry.depends_on.prefixes)
    validate_immutable(net.state)


def test_local_end_resolution_records_updated_da_lookup():
    net, routers, encap = path_network()
    own_sid = address('2001:db8:1:1::')
    install_sid(routers['R1'], LocalSid(own_sid, 128, END))
    encap = replace(encap, entries=(own_sid,) + encap.entries)
    routers['R1'].add_route('10.0.0.4/32', [Nexthop(srv6=encap)])
    net.converge()
    entry, group = encap_entry(routers['R1'])
    assert group.adjacencies[0].encap == encap
    assert (6, own_sid) in entry.depends_on.lookups
    assert (6, encap.entries[1]) in entry.depends_on.lookups
    t = net.trace('R1', inner_packet())
    assert t.outcome == fw.DELIVER and t.hops[0].packet.hop_limit == 63
    assert t.hops[0].packet.srh.segments_left == 1


def test_local_un_resolution_records_shifted_da():
    net, routers, _ = path_network(compressed=True)
    install_sid(
        routers['R1'], LocalSid(address('5f00:0:1::'), 48, END, NEXT_CSID, F3216_GIB)
    )
    original = address('5f00:0:1:e001:e002:e004::')
    shifted = address('5f00:0:e001:e002:e004::')
    routers['R1'].add_route('10.0.0.4/32', [Nexthop(srv6=Srv6Encap((original,)))])
    net.converge()
    entry, group = encap_entry(routers['R1'])
    assert set(entry.depends_on.lookups) >= {(6, original), (6, shifted)}
    assert group.adjacencies[0].interface == 'toR2'


def test_cross_connect_compilation_does_not_lookup_new_da():
    _, routers, encap = path_network(compressed=True)
    entry, _ = encap_entry(routers['R1'])
    assert entry.depends_on.lookups == ((6, encap.entries[0]),)


def test_failed_first_sid_query_survives_fallback():
    net, routers, _ = path_network()
    missing = address('2001:db8:dead::')
    routers['R1'].add_route('10.99.0.0/16', [Nexthop(srv6=Srv6Encap((missing,)))])
    routers['R1'].add_route(
        '10.99.0.0/16',
        [('toR2', '2001:db8:12::2')],
        distance=20,
        distinguisher=('backup',),
    )
    net.converge()
    entry = routers['R1'].fib(4).lookup(address('10.99.0.1'))
    assert entry.action == fw.FORWARD
    assert (6, missing) in entry.depends_on.lookups


def test_self_covering_encapsulation_does_not_resolve_through_itself():
    net, routers, _ = path_network()
    route = routers['R1'].add_route(
        '2001:db8:dead::/48', [Nexthop(srv6=Srv6Encap((address('2001:db8:dead::1'),)))]
    )
    net.converge()
    assert routers['R1'].fib(6).lookup(address('2001:db8:dead::1')) is None
    assert routers['R1'].route_status(6, route.key) == ('NOT_INSTALLED', 'UNRESOLVED')


def test_encapsulation_does_not_resolve_through_nested_encapsulation():
    net, routers, encap = path_network()
    routers['R1'].add_route('2001:db8:dead::1/128', [Nexthop(srv6=encap)])
    route = routers['R1'].add_route(
        '10.99.0.0/16', [Nexthop(srv6=Srv6Encap((address('2001:db8:dead::1'),)))]
    )
    net.converge()
    assert routers['R1'].route_status(4, route.key) == ('NOT_INSTALLED', 'UNRESOLVED')


@pytest.mark.parametrize(
    'special', [Nexthop.blackhole(), Nexthop.receive(), Nexthop.unreachable()]
)
def test_first_entry_drop_or_receive_is_not_an_egress(special):
    net, routers, _ = path_network()
    first = address('2001:db8:dead::1')
    routers['R1'].add_route('2001:db8:dead::1/128', [special])
    route = routers['R1'].add_route('10.99.0.0/16', [Nexthop(srv6=Srv6Encap((first,)))])
    net.converge()
    assert routers['R1'].route_status(4, route.key) == ('NOT_INSTALLED', 'UNRESOLVED')


def test_local_conflicts_are_rejected_deterministically():
    net, routers, _ = path_network()
    sid = address('2001:db8:9::')
    nhs = [
        Nexthop(special=SRV6_LOCAL_NH, behavior=LocalSid(sid, 128, behavior))
        for behavior in (END, END_DT46)
    ]
    route = routers['R1'].add_route('2001:db8:9::/128', nhs)
    net.converge()
    assert routers['R1'].route_status(6, route.key) == (
        'NOT_INSTALLED',
        'AMBIGUOUS_ACTION',
    )


def test_srv6_rebuild_reuses_unchanged_fibs():
    net, routers, _ = path_network(compressed=True)
    for name in routers:
        for af in (4, 6):
            old = routers[name].fib(af)
            new, _ = resolve_fib(
                routers[name].rib(af),
                DeviceContext(net.state, name),
                ResolutionPolicy(),
                99,
                99,
                old,
            )
            assert new is old


def test_unknown_local_cover_maps_to_sid_unknown_only_for_srv6_client():
    net, routers, _ = path_network(compressed=True)
    routers['R2'].add_locator('sr', structure=F3216_GIB)
    net.converge()
    from tests.model.test_forwarding_srv6 import outer_packet

    assert (
        net.trace('R2', outer_packet(da='5f00:0:e777::', srh=False)).reason
        == 'SID_UNKNOWN'
    )
    # Ordinary IP unreachable routes retain their established drop reason.
    routers['R2'].add_route('2001:db8:dead::/48', [Nexthop.unreachable()])
    net.converge()
    assert (
        net.trace('R2', outer_packet(da='2001:db8:dead::1', srh=False)).reason
        == 'DROP_UNREACHABLE'
    )


def test_recursive_route_retains_encap_with_interface_only_local_ua():
    net, routers, encap = path_network(compressed=True)
    # The recursive inner address is not an on-link IPv4 neighbor.
    routers['R1'].add_route('10.99.0.0/16', [Nexthop.recursive(address('10.0.0.4'), 4)])
    net.converge()
    fib = routers['R1'].fib(4)
    entry = fib.lookup(address('10.99.0.1'))
    assert fib.group(entry).adjacencies[0].encap == encap


def test_deterministic_order_for_different_encaps_on_same_adjacency():
    net, routers, encap = path_network()
    a = Nexthop(srv6=encap)
    b = Nexthop(srv6=replace(encap, source=address('2001:db8::123')))
    routers['R1'].add_route('10.99.0.0/16', [a, b])
    net.converge()
    old = routers['R1'].fib(4)
    routers['R1'].add_route('10.99.0.0/16', [b, a])
    net.converge()
    assert routers['R1'].fib(4) is old
