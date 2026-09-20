"""Hand-computed Gate B forwarding vectors; networks use the public API."""

from dataclasses import replace
from ipaddress import IPv6Address
from random import Random

import pytest

from netsim import Environment
from netsim.model import forwarding as fw
from netsim.model.addressing import to_int
from netsim.model.hashing import flow_label_for
from netsim.model.network import Network
from netsim.model.packets import (
    AFTER_ENCAP,
    ETHERTYPE_IPV4,
    ETHERTYPE_IPV6,
    ORIGINATED,
    PROTO_IPV4,
    PROTO_IPV6,
    PROTO_ROUTING,
    SRH,
    TRANSIT,
    EthernetFrame,
    FlowKey,
    IPv4Packet,
    IPv6Packet,
    L4Header,
    PacketTemplate,
    encapsulate,
    ip_bytes,
)
from netsim.model.routing import SRV6_LOCAL_NH, Nexthop
from netsim.model.srv6 import (
    END,
    END_B6_ENCAPS,
    END_DT4,
    END_DT6,
    END_DT46,
    END_DX4,
    END_DX6,
    END_X,
    F3216_GIB,
    F3216_LIB,
    F3216_TERMINAL,
    H_ENCAPS,
    H_ENCAPS_RED,
    NESTED_ENCAP_UNSUPPORTED,
    NEXT_CSID,
    PSP,
    SRH_MALFORMED,
    SRH_SL_NONZERO,
    UPPER_LAYER_NOT_ALLOWED,
    USD,
    USP,
    LocalSid,
    Srv6Encap,
)
from netsim.runtime import Simulation


def address(text):
    return to_int(text)[0]


def install_sid(device, sid):
    return device.add_route(
        f'{IPv6Address(sid.sid)}/{sid.length}',
        [Nexthop(special=SRV6_LOCAL_NH, behavior=sid)],
    )


def path_network(*, compressed=False, psp=0, mtu=1500):
    net = Network()
    routers = {name: net.add_device(name) for name in ('R1', 'R2', 'R4')}
    for i, router in ((1, routers['R1']), (2, routers['R2']), (4, routers['R4'])):
        router.add_loopback('lo', ipv4=[f'10.0.0.{i}/32'], ipv6=[f'2001:db8::{i}/128'])
    for left, right, subnet in (('R1', 'R2', 12), ('R2', 'R4', 24)):
        net.add_p2p(
            routers[left],
            f'to{right}',
            routers[right],
            f'to{left}',
            mtu=mtu,
            ipv6=(f'2001:db8:{subnet}::1/64', f'2001:db8:{subnet}::2/64'),
            ipv4=(f'10.1.{subnet}.0/31', f'10.1.{subnet}.1/31'),
        )
    if compressed:
        for name, function, iface in (('R1', 'e001', 'toR2'), ('R2', 'e002', 'toR4')):
            install_sid(
                routers[name],
                LocalSid(
                    address(f'5f00:0:{function}::'),
                    48,
                    END_X,
                    NEXT_CSID,
                    F3216_LIB,
                    interface=iface,
                ),
            )
        install_sid(
            routers['R4'],
            LocalSid(
                address('5f00:0:e004::'),
                48,
                END_DT46,
                NEXT_CSID,
                F3216_TERMINAL,
            ),
        )
        entries = (address('5f00:0:e001:e002:e004::'),)
    else:
        first, last = address('2001:db8:2:1::'), address('2001:db8:4:1::')
        install_sid(routers['R2'], LocalSid(first, 128, END_X, psp, interface='toR4'))
        install_sid(routers['R4'], LocalSid(last, 128, END_DT46))
        routers['R1'].add_route('2001:db8:2:1::/128', [('toR2', '2001:db8:12::2')])
        entries = (first, last)
    encap = Srv6Encap(entries)
    for prefix in ('10.0.0.4/32', '2001:db8::4/128'):
        routers['R1'].add_route(prefix, [Nexthop(srv6=encap)])
    net.converge()
    return net, routers, encap


def inner_packet(af=4, **kwargs):
    return PacketTemplate(
        af,
        address('10.0.0.1' if af == 4 else '2001:db8::1'),
        address('10.0.0.4' if af == 4 else '2001:db8::4'),
        **kwargs,
    ).to_packet()


@pytest.mark.parametrize('af', [4, 6])
def test_uncompressed_acceptance(af):
    net, _, encap = path_network()
    result = net.trace('R1', inner_packet(af))
    assert (result.outcome, result.path) == (fw.DELIVER, ('R1', 'R2', 'R4'))
    p1, p2, p4 = (hop.packet for hop in result.hops)
    assert (p1.dst, p1.hop_limit) == (encap.entries[0], 64)
    assert (p1.srh.entries, p1.srh.segments_left, p1.srh.last_entry) == (
        (encap.entries[1],),
        1,
        0,
    )
    assert (p2.dst, p2.hop_limit, p2.srh.segments_left) == (encap.entries[1], 63, 0)
    assert p4 == inner_packet(af)


def test_f3216_headend_executes_own_adjacency():
    net, _, _ = path_network(compressed=True)
    result = net.trace('R1', inner_packet())
    assert (result.outcome, result.path) == (fw.DELIVER, ('R1', 'R2', 'R4'))
    p1, p2, p4 = (hop.packet for hop in result.hops)
    assert (p1.dst, p1.hop_limit, p1.srh) == (address('5f00:0:e002:e004::'), 63, None)
    assert (p2.dst, p2.hop_limit, p2.srh) == (address('5f00:0:e004::'), 62, None)
    assert p4 == inner_packet()


def outer_packet(*, af=4, da='2001:db8:2:1::', sl=1, hop_limit=64, srh=True):
    proto = PROTO_IPV4 if af == 4 else PROTO_IPV6
    return IPv6Packet(
        address('2001:db8::1'),
        address(da),
        PROTO_ROUTING if srh else proto,
        hop_limit=hop_limit,
        srh=SRH((address('2001:db8:4:1::'),), sl, 0, next_header=proto)
        if srh
        else None,
        payload=inner_packet(af),
    )


@pytest.mark.parametrize('behavior', [END, END_X])
@pytest.mark.parametrize('flavors', [0, PSP, USD, PSP | USD])
def test_classic_end_and_end_x_branches(behavior, flavors):
    net, routers, _ = path_network()
    install_sid(
        routers['R2'],
        LocalSid(
            address('2001:db8:2:1::'),
            128,
            behavior,
            flavors,
            interface='toR4',
        ),
    )
    routers['R2'].add_route('2001:db8:4:1::/128', [('toR4', '2001:db8:24::2')])
    net.converge()
    t = net.trace('R2', outer_packet())
    assert (t.outcome, t.path) == (fw.DELIVER, ('R2', 'R4'))
    sent = t.hops[0].packet
    assert (sent.dst, sent.hop_limit) == (address('2001:db8:4:1::'), 63)
    if flavors & PSP:
        assert sent.srh is None and sent.next_header == PROTO_IPV4
    else:
        assert sent.srh.segments_left == 0
    assert t.hops[-1].packet == inner_packet()


@pytest.mark.parametrize('af', [4, 6])
@pytest.mark.parametrize('behavior', [END, END_X, END_DT46])
@pytest.mark.parametrize('srh', [False, True])
def test_terminal_decap_no_header_decrement(af, behavior, srh):
    net, routers, _ = path_network()
    flavors = USD if behavior != END_DT46 else 0
    install_sid(
        routers['R2'],
        LocalSid(
            address('2001:db8:2:1::'),
            128,
            behavior,
            flavors,
            interface='toR4',
        ),
    )
    routers['R2'].add_route('10.0.0.4/32', [('toR4', '10.1.24.1')])
    routers['R2'].add_route('2001:db8::4/128', [('toR4', '2001:db8:24::2')])
    net.converge()
    p = outer_packet(af=af, sl=0, srh=srh, hop_limit=1)
    # Inner TTL=1 is valid: neither decap nor final delivery pays a transit hop.
    inner = replace(p.payload, **({'ttl': 1} if af == 4 else {'hop_limit': 1}))
    t = net.trace('R2', replace(p, payload=inner))
    assert (t.outcome, t.path) == (fw.DELIVER, ('R2', 'R4'))
    assert all(h.packet == inner for h in t.hops)


@pytest.mark.parametrize('behavior', [END, END_X])
@pytest.mark.parametrize('srh', [False, True])
def test_end_without_usd_rejects_upper_layer(behavior, srh):
    net, routers, _ = path_network()
    install_sid(
        routers['R2'],
        LocalSid(address('2001:db8:2:1::'), 128, behavior, interface='toR4'),
    )
    net.converge()
    assert (
        net.trace('R2', outer_packet(sl=0, srh=srh)).reason == UPPER_LAYER_NOT_ALLOWED
    )


@pytest.mark.parametrize(
    'malformation',
    [
        'negative_sl',
        'oversized_sl',
        'wrong_le',
        'empty',
        'missing_srh',
        'routing_mismatch',
        'inner_family_mismatch',
        'inner_transport_mismatch',
    ],
)
def test_malformed_chain_before_sid_processing(malformation):
    net, _, _ = path_network()
    p = outer_packet()
    changes = {
        'negative_sl': {'srh': replace(p.srh, segments_left=-1)},
        'oversized_sl': {'srh': replace(p.srh, segments_left=2)},
        'wrong_le': {'srh': replace(p.srh, last_entry=1)},
        'empty': {'srh': replace(p.srh, entries=())},
        'missing_srh': {'srh': None},
        'routing_mismatch': {'next_header': 17},
        'inner_family_mismatch': {'srh': replace(p.srh, next_header=PROTO_IPV6)},
        'inner_transport_mismatch': {
            'payload': replace(p.payload, protocol=PROTO_IPV6)
        },
    }
    assert net.trace('R2', replace(p, **changes[malformation])).reason == SRH_MALFORMED


def test_dt46_requires_zero_sl_and_an_ip_payload():
    net, _, _ = path_network()
    p = outer_packet(da='2001:db8:4:1::')
    assert net.trace('R4', p).reason == SRH_SL_NONZERO
    p = replace(
        p, srh=replace(p.srh, segments_left=0, next_header=17), payload=L4Header(1, 2)
    )
    assert net.trace('R4', p).reason == UPPER_LAYER_NOT_ALLOWED


@pytest.mark.parametrize('compressed', [False, True])
@pytest.mark.parametrize('af', [4, 6])
def test_transit_encapsulation_decrements_inner_once(compressed, af):
    net, _, _ = path_network(compressed=compressed)
    p = inner_packet(af, ttl=5, hop_limit=5)
    res = fw.forward_ip(net.view('R1'), p, 'toR2', TRANSIT)
    expected = replace(p, **({'ttl': 4} if af == 4 else {'hop_limit': 4}))
    assert res.packet.payload == expected
    assert res.packet.hop_limit == (63 if compressed else 64)
    expired = inner_packet(af, ttl=1, hop_limit=1)
    assert (
        fw.forward_ip(net.view('R1'), expired, 'toR2', TRANSIT).reason == fw.TTL_EXPIRED
    )
    assert net.trace('R1', expired).outcome == fw.DELIVER


def test_hop_limit_expiry_inside_container_and_classic_end():
    net, routers, _ = path_network(compressed=True)
    routers['R1'].configure(srv6_hop_limit=2)
    t = net.trace('R1', inner_packet())
    assert (t.path, t.reason, t.hops[0].packet.hop_limit) == (
        ('R1', 'R2'),
        fw.TTL_EXPIRED,
        1,
    )
    net, _, _ = path_network()
    assert net.trace('R2', outer_packet(hop_limit=1)).reason == fw.TTL_EXPIRED


@pytest.mark.parametrize('compressed,size', [(False, 1484), (True, 1460)])
def test_mtu_measures_entire_transmitted_stack(compressed, size):
    net, routers, _ = path_network(compressed=compressed, mtu=size)
    t = net.trace('R1', inner_packet(payload_size=1400))
    assert t.outcome == fw.DELIVER and ip_bytes(t.hops[0].packet) == size
    routers['R1']['toR2'].configure(mtu=size - 1)
    assert net.trace('R1', inner_packet(payload_size=1400)).reason == fw.MTU_EXCEEDED


@pytest.mark.parametrize('compressed', [False, True])
def test_physical_failure_keeps_transmitted_header_in_trace(compressed):
    net, _, _ = path_network(compressed=compressed)
    net.links['R1:toR2--R2:toR1'].fail()
    t = net.trace('R1', inner_packet())
    assert (t.path, t.reason) == (('R1',), fw.LINK_DOWN)
    assert t.hops[0].packet.hop_limit == (63 if compressed else 64)
    assert t.hops[0].packet.payload == inner_packet()


@pytest.mark.parametrize(
    'behavior,flavors',
    [
        (END_B6_ENCAPS, 0),
        (END_B6_ENCAPS, NEXT_CSID),
        (END_DT4, 0),
        (END_DT6, 0),
        (END_DX4, 0),
        (END_DX6, 0),
        (END, USP),
        (END_DT46, PSP),
    ],
)
def test_unsupported_behaviors_are_explicit(behavior, flavors):
    net, routers, _ = path_network()
    install_sid(
        routers['R2'], LocalSid(address('2001:db8:2:1::'), 128, behavior, flavors)
    )
    net.converge()
    assert net.trace('R2', outer_packet()).reason == fw.SRV6_UNSUPPORTED


def test_nested_encapsulation_rejected():
    net, _, encap = path_network()
    assert (
        fw.forward_ip(
            net.view('R1'), outer_packet(), None, ORIGINATED, encap=encap
        ).reason
        == NESTED_ENCAP_UNSUPPORTED
    )


def test_uA_cross_connect_does_not_execute_shifted_local_sid():
    net, routers, _ = path_network(compressed=True)
    # A trap: this function is local at R1 as well, but must execute at R2.
    install_sid(
        routers['R1'],
        LocalSid(address('5f00:0:e002::'), 48, END_DT46, NEXT_CSID, F3216_TERMINAL),
    )
    net.converge()
    assert net.trace('R1', inner_packet()).outcome == fw.DELIVER


def test_uN_relooks_up_and_executes_next_local_sid():
    net, routers, _ = path_network(compressed=True)
    install_sid(
        routers['R1'], LocalSid(address('5f00:0:1::'), 48, END, NEXT_CSID, F3216_GIB)
    )
    encap = Srv6Encap((address('5f00:0:1:e001:e002:e004::'),))
    routers['R1'].add_route('10.0.0.4/32', [Nexthop(srv6=encap)])
    net.converge()
    t = net.trace('R1', inner_packet())
    assert t.outcome == fw.DELIVER
    assert (t.hops[0].packet.dst, t.hops[0].packet.hop_limit) == (
        address('5f00:0:e002:e004::'),
        62,
    )
    assert t.hops[1].packet.hop_limit == 61


def test_seven_csids_cross_container_boundary_with_psp():
    net, routers, _ = path_network(compressed=True)
    for i in range(1, 7):
        install_sid(
            routers['R2'],
            LocalSid(address(f'5f00:0:{i}::'), 48, END, NEXT_CSID | PSP, F3216_GIB),
        )
    routers['R2'].add_route('5f00:0:e004::/48', [('toR4', '2001:db8:24::2')])
    net.converge()
    p = replace(
        outer_packet(da='5f00:0:1:2:3:4:5:6'),
        srh=SRH((address('5f00:0:e004::'),), 1, 0, next_header=PROTO_IPV4),
    )
    t = net.trace('R2', p)
    assert (t.outcome, t.path) == (fw.DELIVER, ('R2', 'R4'))
    sent = t.hops[0].packet
    assert (sent.dst, sent.hop_limit, sent.srh, sent.next_header) == (
        address('5f00:0:e004::'),
        58,
        None,
        PROTO_IPV4,
    )


def test_uDT46_rejects_trailing_argument_even_with_zero_al():
    net, _, _ = path_network(compressed=True)
    p = outer_packet(da='5f00:0:e004:1234::', srh=False)
    assert net.trace('R4', p).reason == SRH_MALFORMED


def test_steering_hook_runs_at_ingress_and_decap_only(monkeypatch):
    calls = []

    def hook(view, packet, ingress, stage):
        calls.append((view.name, stage))

    monkeypatch.setattr(fw, 'steer', hook)
    net, _, _ = path_network(compressed=True)
    assert net.trace('R1', inner_packet()).outcome == fw.DELIVER
    assert calls == [('R1', ORIGINATED), ('R2', TRANSIT), ('R4', TRANSIT), ('R4', 3)]


def test_encap_decap_cycle_has_independent_transition_budget(monkeypatch):
    net, routers, _ = path_network()
    sid = address('2001:db8:1:1::')
    install_sid(routers['R1'], LocalSid(sid, 128, END_DT46))
    net.converge()
    monkeypatch.setattr(fw, 'steer', lambda *args: Srv6Encap((sid,)))
    assert net.trace('R1', inner_packet()).reason == fw.LOOP


def interpreted_trace(net, inner, encap):
    """ENCAP explicitly, then interpret lookups without using compiled inner-route legs."""
    outer = encapsulate(
        inner,
        encap.entries,
        behavior=encap.behavior,
        source=encap.source or address('2001:db8::1'),
        hop_limit=64,
        flow_label=flow_label_for(FlowKey.from_packet(inner), 0),
        transit=False,
    )
    res = fw.forward_ip(net.view('R1'), outer, None, AFTER_ENCAP)
    hops = []
    device, ingress = 'R1', None
    for _ in range(10):
        hops.append(
            fw.Hop(
                device,
                ingress,
                res.egress,
                res.member,
                res.link_id,
                res.edge_id,
                res.packet,
                res.outcome,
                res.reason,
            )
        )
        if res.outcome != fw.TRANSMIT:
            return fw.Trace(tuple(hops), res.outcome, res.reason)
        device, ingress = res.peer
        et = ETHERTYPE_IPV4 if isinstance(res.packet, IPv4Packet) else ETHERTYPE_IPV6
        frame = EthernetFrame(res.mac_dst, res.mac_src, et, res.packet)
        res = fw.receive_frame(net.view(device), ingress, frame)
    raise AssertionError('unexpected loop')


@pytest.mark.parametrize('compressed', [False, True])
@pytest.mark.parametrize('behavior', [H_ENCAPS, H_ENCAPS_RED])
def test_random_compiled_and_interpreted_traces_agree(compressed, behavior):
    net, routers, encap = path_network(compressed=compressed)
    encap = replace(encap, behavior=behavior)
    for prefix in ('10.0.0.4/32', '2001:db8::4/128'):
        routers['R1'].add_route(prefix, [Nexthop(srv6=encap)])
    net.converge()
    rng = Random(1234)
    for _ in range(80):
        inner = inner_packet(
            rng.choice((4, 6)),
            sport=rng.randrange(65536),
            ttl=rng.randrange(1, 256),
            hop_limit=rng.randrange(1, 256),
            dscp=rng.randrange(64),
            payload_size=rng.randrange(1, 1450),
        )
        assert net.trace('R1', inner) == interpreted_trace(net, inner, encap)


@pytest.mark.parametrize('compressed', [False, True])
def test_timed_send_and_trace_share_interpreter(compressed):
    net, _, _ = path_network(compressed=compressed)
    sim = Simulation(Environment(), net)
    expected = net.trace('R1', inner_packet())
    proc = sim.send('R1', inner_packet())
    sim.env.run()
    assert proc.value == expected


def test_invalid_csid_structure_becomes_modeled_drop():
    from netsim.model.srv6 import SidStructure

    net, routers, _ = path_network(compressed=True)
    install_sid(
        routers['R2'],
        LocalSid(
            address('5f00:0:e002::'),
            48,
            END_X,
            NEXT_CSID,
            SidStructure(0, 16, 0, 112),
            interface='toR4',
        ),
    )
    net.converge()
    assert (
        net.trace('R2', outer_packet(da='5f00:0:e002:e004::', srh=False)).reason
        == SRH_MALFORMED
    )


@pytest.mark.parametrize(
    'structure,da,shifted',
    [
        ('F3216_COMPOSITE', '5f00:0:2:e002:e004::', '5f00:0:e004::'),
        ('F3216_WLIB', '5f00:0:fff7:1234:e004::', '5f00:0:e004::'),
    ],
)
def test_next_csid_uses_matched_sid_width(structure, da, shifted):
    from netsim.model import srv6

    net, routers, _ = path_network(compressed=True)
    layout = getattr(srv6, structure)
    sid_value = address(da) >> 64 << 64
    install_sid(
        routers['R2'],
        LocalSid(sid_value, 64, END_X, NEXT_CSID, layout, interface='toR4'),
    )
    net.converge()
    t = net.trace('R2', outer_packet(da=da, srh=False))
    assert (t.outcome, t.path) == (fw.DELIVER, ('R2', 'R4'))
    assert (t.hops[0].packet.dst, t.hops[0].packet.hop_limit) == (address(shifted), 63)


def test_uA_shift_preserves_srh_sl():
    net, _, _ = path_network(compressed=True)
    p = outer_packet(da='5f00:0:e002:e004::')
    res = fw.forward_ip(net.view('R2'), p, 'toR1', TRANSIT)
    assert res.outcome == fw.TRANSMIT
    assert res.packet.srh is p.srh and res.packet.srh.segments_left == 1
    assert (res.packet.dst, res.packet.hop_limit) == (address('5f00:0:e004::'), 63)


@pytest.mark.parametrize('behavior', [END, END_X])
def test_next_csid_zero_arg_uses_classic_usd(behavior):
    net, routers, _ = path_network(compressed=True)
    install_sid(
        routers['R2'],
        LocalSid(
            address('5f00:0:e002::'),
            48,
            behavior,
            NEXT_CSID | USD,
            F3216_LIB,
            interface='toR4',
        ),
    )
    routers['R2'].add_route('10.0.0.4/32', [('toR4', '10.1.24.1')])
    net.converge()
    t = net.trace('R2', outer_packet(da='5f00:0:e002::', srh=False, hop_limit=1))
    assert t.outcome == fw.DELIVER and t.hops[0].packet == inner_packet()


def test_headend_source_hop_limit_flow_label_and_dscp():
    net, routers, encap = path_network()
    routers['R1'].configure(srv6_source=address('2001:db8::abcd'), srv6_hop_limit=99)
    inner = inner_packet(dscp=46)
    p = net.trace('R1', inner).hops[0].packet
    assert (p.src, p.hop_limit, p.dscp) == (address('2001:db8::abcd'), 99, 46)
    assert p.flow_label == flow_label_for(FlowKey.from_packet(inner), 0)
    encap = replace(encap, source=address('2001:db8::1234'))
    res = fw.forward_ip(net.view('R1'), inner, None, ORIGINATED, encap=encap)
    assert res.packet.src == encap.source


def test_no_ipv6_loopback_and_no_explicit_source():
    net, routers, _ = path_network()
    routers['R1']['lo'].configure(ipv6=())
    assert net.trace('R1', inner_packet()).reason == fw.NO_SOURCE


def test_cross_connect_checks_l3_and_neighbor():
    net, routers, _ = path_network(compressed=True)
    routers['R2']['toR4'].configure(forwarding_v6=False)
    assert (
        net.trace('R2', outer_packet(da='5f00:0:e002:e004::', srh=False)).reason
        == fw.EGRESS_DOWN
    )
    routers['R2']['toR4'].configure(forwarding_v6=True)
    install_sid(
        routers['R2'],
        LocalSid(
            address('5f00:0:e002::'),
            48,
            END_X,
            NEXT_CSID,
            F3216_LIB,
            interface='toR4',
            nexthop=address('2001:db8:24::dead'),
        ),
    )
    net.converge()
    assert (
        net.trace('R2', outer_packet(da='5f00:0:e002:e004::', srh=False)).reason
        == fw.ADJ_UNRESOLVED
    )


def test_headend_local_cross_connect_over_bundle_hashes_shifted_outer():
    from netsim.model.hashing import BalancerKind

    net, routers, _ = path_network(compressed=True)
    net.add_lag(
        routers['R1'],
        'Po1',
        ('m1', 'm2'),
        routers['R2'],
        'Po1',
        ('m1', 'm2'),
        ipv6=('2001:db8:100::1/64', '2001:db8:100::2/64'),
    )
    install_sid(
        routers['R1'],
        LocalSid(
            address('5f00:0:e001::'), 48, END_X, NEXT_CSID, F3216_LIB, interface='Po1'
        ),
    )
    net.converge()
    seen = set()
    for i in range(60):
        t = net.trace('R1', inner_packet(sport=30000 + i))
        assert t.outcome == fw.DELIVER
        hop = t.hops[0]
        expected = (
            net.view('R1')
            .load_balancer(BalancerKind.AGGREGATE_PORT)
            .select(FlowKey.from_packet(hop.packet), (1, 1))
        )
        assert hop.egress == 'Po1' and hop.member == ('m1', 'm2')[expected]
        seen.add(hop.member)
    assert seen == {'m1', 'm2'}


def test_compiled_ecmp_hashes_outer_after_local_end():
    net, routers, encap = path_network()
    net.add_p2p(
        routers['R1'],
        'alternate',
        routers['R2'],
        'alternate',
        ipv6=('2001:db8:100::1/64', '2001:db8:100::2/64'),
    )
    routers['R1'].add_route(
        '2001:db8:2:1::/128',
        [('toR2', '2001:db8:12::2'), ('alternate', '2001:db8:100::2')],
    )
    install_sid(routers['R1'], LocalSid(address('2001:db8:1:1::'), 128, END))
    encap = replace(encap, entries=(address('2001:db8:1:1::'),) + encap.entries)
    routers['R1'].add_route('10.0.0.4/32', [Nexthop(srv6=encap)])
    net.converge()
    seen = set()
    for i in range(80):
        packet = inner_packet(sport=30000 + i)
        compiled = net.trace('R1', packet)
        assert compiled == interpreted_trace(net, packet, encap)
        seen.add(compiled.hops[0].egress)
    assert seen == {'toR2', 'alternate'}


def test_sr_member_unavailable_precedes_mtu_without_changing_plain_ip(monkeypatch):
    net, routers, _ = path_network(compressed=True)
    net.add_lag(
        routers['R1'],
        'Po1',
        ('m1', 'm2'),
        routers['R2'],
        'Po1',
        ('m1', 'm2'),
        ipv4=('10.100.0.0/31', '10.100.0.1/31'),
        ipv6=('2001:db8:100::1/64', '2001:db8:100::2/64'),
    )
    install_sid(
        routers['R1'],
        LocalSid(
            address('5f00:0:e001::'), 48, END_X, NEXT_CSID, F3216_LIB, interface='Po1'
        ),
    )
    routers['R1'].add_route('10.99.0.0/16', [('Po1', '10.100.0.1')])
    net.converge()
    # Model the DeviceView contract returning no available bundle member.
    view = net.view('R1')
    monkeypatch.setattr(type(view), 'active_members', lambda *args: ())
    p = inner_packet(payload_size=2000)
    assert fw.forward_ip(view, p, None, ORIGINATED).reason == fw.EGRESS_DOWN
    plain = replace(p, dst=address('10.99.0.1'))
    assert fw.forward_ip(view, plain, None, ORIGINATED).reason == fw.MTU_EXCEEDED


def srdb_path_network(*, compressed):
    """Use G1's public SR-DB API to produce the same acceptance path."""
    from netsim.model.contracts import STATIC

    net, routers, encap = path_network(compressed=compressed)
    for name, router in routers.items():
        rows = tuple(
            row
            for row in router.rib(6).rows_of(STATIC)
            if row.nexthops[0].special == SRV6_LOCAL_NH
        )
        if not rows:
            continue
        router.rib_client(STATIC, 6).delete_routes(row.key for row in rows)
        if compressed:
            router.add_locator('sr', structure=F3216_GIB, node_id=int(name[1:]))
        else:
            sid = rows[0].nexthops[0].behavior
            router.add_locator(
                'sr', f'{IPv6Address(sid.sid >> 64 << 64)}/64', structure=sid.structure
            )
        for row in rows:
            sid = row.nexthops[0].behavior
            router.add_local_sid(
                sid.behavior,
                structure=sid.structure,
                flavors=sid.flavors,
                sid=sid.sid,
                interface=sid.interface,
                nexthop=sid.nexthop,
            )
    net.converge()
    return net, routers, encap


@pytest.mark.parametrize('compressed', [False, True])
@pytest.mark.parametrize('af', [4, 6])
def test_public_srdb_rows_drive_trace_and_timed_send(compressed, af):
    from netsim.model.contracts import SRV6_LOCAL

    net, routers, encap = srdb_path_network(compressed=compressed)
    assert net.validate() == []
    for router in routers.values():
        if router.node.srv6_sids is None:
            continue
        for row in router.rib(6).rows_of(SRV6_LOCAL):
            entry = router.fib(6).lookup(row.prefix[0])
            if row.distinguisher == ('unknown',):
                assert row.nexthops[0] == Nexthop.unreachable()
            else:
                assert entry.action == fw.SRV6_LOCAL
                assert entry.sid == row.nexthops[0].behavior
                assert entry.prefix[1] == entry.sid.structure.installed_length
    packet = inner_packet(af)
    result = net.trace('R1', packet)
    assert result.outcome == fw.DELIVER and result.path == ('R1', 'R2', 'R4')
    assert [hop.packet.hop_limit for hop in result.hops[:2]] == (
        [63, 62] if compressed else [64, 63]
    )
    assert result == interpreted_trace(net, packet, encap)
    sim = Simulation(Environment(), net)
    proc = sim.send('R1', packet)
    sim.env.run()
    assert proc.value == result


def test_public_srdb_withdrawal_keeps_unknown_cover_and_restore_recovers():
    net, routers, _ = srdb_path_network(compressed=True)
    sid = address('5f00:0:e002::')
    link = net.links['R2:toR4--R4:toR2']
    link.fail()
    net.converge()
    assert not routers['R2'].node.srv6_sids.sids[sid].adjacency_up
    result = net.trace('R1', inner_packet())
    assert (result.path, result.reason) == (('R1', 'R2'), 'SID_UNKNOWN')
    link.restore()
    net.converge()
    assert routers['R2'].node.srv6_sids.sids[sid].adjacency_up
    assert net.trace('R1', inner_packet()).outcome == fw.DELIVER


@pytest.mark.parametrize('compressed,wire', [(False, 109.8e6), (True, 107.4e6)])
def test_public_srdb_hash_placement_uses_actual_transmitted_headers(compressed, wire):
    from netsim.model import flows

    net, _, _ = srdb_path_network(compressed=compressed)
    net.add_demand(
        'sr',
        'R1',
        '10.0.0.4',
        100e6,
        mode=flows.HASH,
        flows=32,
        template=PacketTemplate(4, address('10.0.0.1'), address('10.0.0.4')),
    )
    net.converge()
    report = net.placement
    assert report.delivered_total == pytest.approx(100e6)
    for link_id, device in (('R1:toR2--R2:toR1', 'R1'), ('R2:toR4--R4:toR2', 'R2')):
        edge = net.links[link_id].edge(device)
        assert report.offered[edge] == pytest.approx(wire)
        assert report.carried[edge] == pytest.approx(wire)
    assert not report.dropped_by_reason
