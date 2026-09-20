"""Gate B2 policy validity, programmed actions and steering regressions."""

from dataclasses import replace

import pytest

from netsim.model import forwarding as fw
from netsim.model import srv6 as sr
from netsim.model.contracts import STATIC
from netsim.model.igp import oracle_igp
from netsim.model.interfaces import EthernetNode, PortChannelNode
from netsim.model.packets import IPv4Packet
from netsim.model.routing import Nexthop, ResolutionPolicy
from tests.model.test_network import A, build_diamond


def diamond(*, compressed=True, **kw):
    net, routers = build_diamond(**kw)
    for i, dev in enumerate(routers.values(), 1):
        for name, node in dev.node.interfaces.sorted_items():
            if isinstance(node, (EthernetNode, PortChannelNode)):
                dev[name].configure(forwarding_v6=True)
        dev.add_locator(
            'sr', structure=sr.F3216_GIB
        ) if compressed else dev.add_locator('sr', prefix=f'2001:db8:{i}::/64')
        dev.add_local_sid(
            sr.END,
            flavors=sr.NEXT_CSID if compressed else 0,
            structure=sr.F3216_GIB if compressed else sr.UNCOMPRESSED,
        )
        dev.add_local_sid(
            sr.END_DT46, structure=sr.F3216_TERMINAL if compressed else sr.UNCOMPRESSED
        )
        for name in ('Po1', 'eth3', 'eth2'):
            node = dev.node.interfaces.get(name)
            if isinstance(node, PortChannelNode) or (
                isinstance(node, EthernetNode) and node.config.aggregate_id is None
            ):
                dev.add_local_sid(
                    sr.END_X,
                    interface=name,
                    flavors=sr.NEXT_CSID if compressed else 0,
                    structure=sr.F3216_LIB if compressed else sr.UNCOMPRESSED,
                )
    net.add_source(oracle_igp)
    net.converge()
    for name, dev in routers.items():
        for other in routers.values():
            loc = other.node.srv6_sids.locators['sr']
            if other.name != dev.name:
                assert dev.fib(6).lookup(loc.prefix[0]) is not None, name
    return net, routers


def path(via='R2', *, weight=1):
    return sr.SegmentList(
        (
            sr.AdjSeg('R1', 'Po1' if via == 'R2' else 'eth3'),
            sr.AdjSeg(via, 'eth3' if via == 'R2' else 'eth2'),
            sr.TermSeg('R4'),
        ),
        weight,
    )


def policy(routers, *, lists=None, backup=False, fallback=sr.FALLBACK_DROP):
    paths = (sr.CandidatePath(200, lists or (path(),), name='primary'),)
    if backup:
        paths += (sr.CandidatePath(100, (path('R3'),), name='backup', discriminator=1),)
    p = sr.SrPolicy(
        STATIC, 10, A('2001:db8::4'), candidate_paths=paths, fallback=fallback
    )
    routers['R1'].policy_client().add(p)
    return p


def state_of(routers, p):
    return routers['R1'].node.srv6_policies.states[p.key]


def packet():
    return IPv4Packet(A('10.0.0.1'), A('10.0.0.4'), 17, payload_size=1000)


def test_policy_derivation_and_demand_steering():
    net, routers = diamond()
    p = policy(routers, backup=True)
    net.add_demand('d', 'R1', '10.0.0.4', 100e6, steer=sr.PolicyRef(*p.key))
    net.converge()
    status = state_of(routers, p)
    assert status.status == sr.POLICY_UP and status.active_path == 0
    assert net.placement.delivered_total == 100e6
    edge = net.links['R1:eth1--R2:eth1'].edge('R1')
    assert net.placement.offered[edge] == pytest.approx(53.7e6)


@pytest.mark.parametrize('compressed,wire', [(True, 107.4e6), (False, 109.8e6)])
@pytest.mark.parametrize('mode', [1, 2])
def test_weighted_lists_steering_and_wire(compressed, wire, mode):
    net, routers = diamond(compressed=compressed)
    lists = (
        (path(weight=3), path('R3'))
        if compressed
        else (
            sr.SegmentList((sr.AdjSeg('R2', 'eth3'), sr.TermSeg('R4')), 3),
            sr.SegmentList((sr.AdjSeg('R3', 'eth2'), sr.TermSeg('R4'))),
        )
    )
    p = policy(routers, lists=lists)
    routers['R1'].policy_client().set_steering(
        [sr.SteeringRule('class', p.key, dscp=10)]
    )
    # A final plain-IP leg proves wire accounting after decapsulation.
    target = net.add_device('target')
    target.add_loopback('lo', ipv4=['10.0.0.99/32'])
    final = net.add_p2p(routers['R4'], 'out', target, 'in', unnumbered=True)
    net.add_demand('d', 'R1', '10.0.0.99', 100e6, mode=mode, flows=2000, dscp=10)
    net.converge()
    assert net.placement.delivered_total == pytest.approx(100e6)
    left = sum(
        net.placement.offered[net.links[f'R1:eth{i}--R2:eth{i}'].edge('R1')]
        for i in (1, 2)
    )
    right = net.placement.offered[net.links['R1:eth3--R3:eth1'].edge('R1')]
    assert left / (left + right) == pytest.approx(0.75, abs=0 if mode == 1 else 0.035)
    assert left + right == pytest.approx(wire)
    assert net.placement.offered[final.edge('R4')] == pytest.approx(103.4e6)


@pytest.mark.parametrize('form', ['destination', 'bsid', 'flow', 'demand'])
@pytest.mark.parametrize('fallback', [sr.FALLBACK_DROP, sr.FALLBACK_IGP])
def test_steering_forms_and_down_fallback(form, fallback):
    net, routers = diamond()
    p = policy(routers, fallback=fallback)
    ref = sr.PolicyRef(*p.key)
    options = {}
    if form == 'destination':
        routers['R1'].add_route(
            '10.0.0.4/32', [Nexthop(policy=ref)], distance=0, distinguisher=('sr',)
        )
    elif form == 'bsid':
        bsid = routers['R1'].node.srv6_policies.policies[p.key].bsid
        routers['R1'].add_route(
            '10.0.0.4/32',
            [Nexthop.recursive(bsid, 6)],
            distance=0,
            distinguisher=('sr',),
        )
    elif form == 'flow':
        routers['R1'].policy_client().set_steering([sr.SteeringRule('all', p.key)])
    else:
        options['steer'] = ref
    net.add_demand('d', 'R1', '10.0.0.4', 100e6, **options)
    net.converge()
    assert net.placement.delivered_total == 100e6
    if form != 'demand':
        trace = net.trace('R1', packet())
        assert trace.path == ('R1', 'R2', 'R4')
        assert (
            trace.hops[0].packet.dst
            != routers['R1'].node.srv6_policies.policies[p.key].bsid
        )
    net.links['R1:eth1--R2:eth1'].fail()
    net.converge()
    assert state_of(routers, p).status == sr.POLICY_DOWN
    assert state_of(routers, p).programming == 'INSTALLED'
    if fallback == sr.FALLBACK_DROP:
        assert dict(net.placement.dropped_by_reason) == {fw.POLICY_DOWN: 100e6}
        if form in ('destination', 'bsid'):
            assert (
                routers['R1'].fib(4).lookup(A('10.0.0.4')).action == fw.DROP_UNREACHABLE
            )
    else:
        assert net.placement.delivered_total == 100e6
        assert not net.placement.dropped_by_reason
    before = net.state
    net.converge()
    assert net.state is before


@pytest.mark.parametrize(
    'segments,reason',
    [
        ((), sr.EMPTY_LIST),
        ((sr.AdjSeg('missing', 'p'), sr.TermSeg('R4')), sr.SYMBOLIC_UNRESOLVABLE),
        ((sr.TermSeg('R1'), sr.TermSeg('R4')), sr.TERMINAL_NOT_LAST),
        ((sr.NodeSeg('R4'),), sr.ENDPOINT_NO_DECAP),
        ((sr.AdjSeg('R1', 'eth3'), sr.TermSeg('R3')), sr.ENDPOINT_MISMATCH),
        ((sr.AdjSeg('R2', 'eth3'), sr.TermSeg('R4')), sr.PATH_UNREACHABLE),
    ],
)
def test_validity_layers_and_traps(segments, reason):
    net, routers = diamond()
    p = policy(routers, lists=(sr.SegmentList(segments),))
    net.converge()
    status = state_of(routers, p)
    assert status.status == sr.POLICY_DOWN
    assert reason in {r for _, _, r in status.reasons}
    assert any(dep[-1] == reason for dep in status.dependencies)


def test_zero_weight_and_missing_symbolic_sid_recovery():
    net, routers = diamond()
    p = policy(routers, lists=(replace(path(), weight=0),))
    net.converge()
    assert state_of(routers, p).basic_valid == ()
    assert (0, 0, sr.ZERO_WEIGHT) in state_of(routers, p).reasons
    p = replace(
        p,
        candidate_paths=(
            sr.CandidatePath(
                segment_lists=(sr.SegmentList((sr.NodeSeg('new'), sr.TermSeg('R4'))),)
            ),
        ),
    )
    routers['R1'].policy_client().replace(p)
    net.converge()
    assert any(
        dep[0] == 'new' and dep[-1] == 'MISSING'
        for dep in state_of(routers, p).dependencies
    )


def test_literals_keep_unknown_compression_metadata_and_basic_ignores_nonfirst():
    net, routers = diamond(compressed=False)
    sid2 = next(
        s for s in routers['R2'].node.srv6_sids.sids.values() if s.interface == 'eth3'
    )
    term = next(
        s
        for s in routers['R4'].node.srv6_sids.sids.values()
        if s.behavior == sr.END_DT46
    )
    p = policy(
        routers,
        lists=(sr.SegmentList((sr.LiteralSid(sid2.sid), sr.LiteralSid(term.sid))),),
    )
    net.converge()
    status = state_of(routers, p)
    assert status.valid_lists == ((0, 0, (sid2.sid, term.sid)),)
    routers['R1'].configure(resolution_policy=ResolutionPolicy(validate_all_sids=False))
    p = replace(
        p,
        candidate_paths=(
            sr.CandidatePath(
                segment_lists=(sr.SegmentList((sid2.sid, A('2001:db8:dead::1'))),)
            ),
        ),
    )
    routers['R1'].policy_client().replace(p)
    net.converge()
    status = state_of(routers, p)
    assert status.basic_valid == status.first_valid == ((0, 0),)
    assert status.strict_valid == () and status.status == sr.POLICY_UP


@pytest.mark.parametrize('partial', [False, True])
def test_strict_walk_checks_transit_blackholes_and_partial_ecmp(partial):
    net, routers = diamond(compressed=False)
    term = next(
        s
        for s in routers['R4'].node.srv6_sids.sids.values()
        if s.behavior == sr.END_DT46
    )
    text = str(__import__('ipaddress').IPv6Address(term.sid)) + '/128'
    routers['R1'].add_route(
        text, [Nexthop.via('Po1')] + ([Nexthop.via('eth3')] if partial else [])
    )
    routers['R2'].add_route(text, [Nexthop.blackhole()])
    p = policy(routers, lists=(sr.SegmentList((sr.LiteralSid(term.sid),)),))
    net.converge()
    status = state_of(routers, p)
    assert status.first_valid == ((0, 0),)
    assert (0, 0, sr.PARTIAL_ECMP if partial else sr.PATH_UNREACHABLE) in status.reasons
    assert any(d[0] == 'R2' and d[1] == 'failure' for d in status.dependencies)


def test_strict_underlay_loop_and_global_adjacency_updates_current_node():
    net, routers = diamond(compressed=False)
    p = policy(
        routers, lists=(sr.SegmentList((sr.AdjSeg('R2', 'eth3'), sr.TermSeg('R4'))),)
    )
    net.converge()
    assert state_of(routers, p).strict_valid == ((0, 0),)
    sid2 = next(
        s for s in routers['R2'].node.srv6_sids.sids.values() if s.interface == 'eth3'
    )
    prefix = str(__import__('ipaddress').IPv6Address(sid2.sid)) + '/128'
    routers['R1'].add_route(prefix, [Nexthop.via('eth3')])
    routers['R3'].add_route(prefix, [Nexthop.via('eth1')])
    net.converge()
    assert (0, 0, 'LOOP_DETECTED') in state_of(routers, p).reasons


def test_candidate_ties_and_opt_in_installed_preference():
    net, routers = diamond()
    p = policy(routers)
    a = sr.CandidatePath(100, (path(),), originator=(1, 0), discriminator=1)
    b = sr.CandidatePath(100, (path('R3'),), originator=(2, 0), discriminator=2)
    routers['R1'].policy_client().replace(replace(p, candidate_paths=(b,)))
    net.converge()
    # Install B at index zero, then add lower-originator A. Default is history free.
    routers['R1'].policy_client().replace(replace(p, candidate_paths=(b, a)))
    net.converge()
    assert state_of(routers, p).active_path == 1
    routers['R1'].configure(resolution_policy=ResolutionPolicy(prefer_installed=True))
    routers['R1'].policy_client().replace(
        replace(p, candidate_paths=(replace(b, originator=(0, 0)), a))
    )
    net.converge()
    assert state_of(routers, p).active_path == 1


@pytest.mark.parametrize('size', [1000, 1220])
def test_production_fluid_encap_mtu_matches_hash(size):
    from netsim.model import flows
    from tests.model.test_forwarding_srv6 import path_network

    net, routers, _ = path_network()
    routers['R1']['toR2'].configure(mtu=1280)
    for mode in (flows.FLUID, flows.HASH):
        net.add_demand(str(mode), 'R1', '10.0.0.4', 100e6, mode=mode, payload_size=size)
    net.converge()
    for result in net.placement.demands.values():
        if size == 1220:
            assert result.delivered == 0
            assert result.drops == ((fw.MTU_EXCEEDED, 'R1', 100e6),)
        else:
            assert result.delivered == 100e6
    if size == 1000:
        edge = net.links['R1:toR2--R2:toR1'].edge('R1')
        assert net.placement.offered[edge] == pytest.approx(2 * 109.8e6)


def test_fluid_class_sorting_optional_srh_and_inner_records():
    from netsim.model.packets import SRH, PacketTemplate

    net, _ = diamond()
    base = PacketTemplate(6, A('2001:db8::1'), A('2001:db8::4'))
    variants = (
        base,
        replace(base, srh=SRH((base.dst,), 0, 0)),
        replace(base, inner=PacketTemplate(4, 1, 2)),
    )
    for i, template in enumerate(variants):
        net.add_demand(str(i), 'R1', '2001:db8::4', 100e6, template=template)
    net.converge()
    assert len(net.placement.classes) == 3


@pytest.mark.parametrize('mode', [1, 2])
@pytest.mark.parametrize('lossy', [False, True])
def test_nested_template_uses_innermost_payload_for_rate(mode, lossy):
    from netsim.model import flows
    from netsim.model.network import Network
    from netsim.model.packets import SRH, PacketTemplate

    net = Network()
    a, b = net.add_device('a'), net.add_device('b')
    link = net.add_p2p(a, 'p', b, 'p', unnumbered=True, speed=150e6, mtu=3000)
    b.add_loopback('lo', ipv6=['2001:db8::2/128'])
    net.add_source(oracle_igp)
    if lossy:
        net.capacity_model = flows.LOSSY
    template = PacketTemplate(
        6,
        A('2001:db8::1'),
        A('2001:db8::2'),
        srh=SRH((A('2001:db8::2'),), 0, 0),
        inner=PacketTemplate(4, 1, 2, payload_size=2000),
    )
    net.add_demand('d', 'a', '2001:db8::2', 100e6, template=template, mode=mode)
    net.converge()
    assert net.placement.offered[link.edge('a')] == pytest.approx(104.9e6)
    assert net.placement.delivered_total == 100e6


def test_nested_template_validates_innermost_payload_only():
    from netsim.model.flows import Demand
    from netsim.model.packets import PacketTemplate

    outer = PacketTemplate(
        6, 1, 2, payload_size=0, inner=PacketTemplate(4, 1, 2, payload_size=2000)
    )
    assert Demand('valid', 'R1', 2, 6, 100, template=outer).rate == 100
    with pytest.raises(ValueError, match='template needs payload_size'):
        Demand(
            'invalid',
            'R1',
            2,
            6,
            100,
            template=replace(
                outer, payload_size=2000, inner=replace(outer.inner, payload_size=0)
            ),
        )


def test_steering_precedence_and_demand_override():
    from netsim.model.network import _View
    from netsim.model.packets import L4Header

    net, routers = diamond()
    p = policy(routers)
    rules = [
        sr.SteeringRule('v6', p.key, dst=(A('2001:db8::4'), 128), af=6),
        sr.SteeringRule('z', (5, 5)),
        sr.SteeringRule('sport', (4, 4), sport=42),
        sr.SteeringRule('dscp', (3, 3), dscp=10),
        sr.SteeringRule('dst', p.key, dst=(A('10.0.0.4'), 32), af=4),
        sr.SteeringRule('a', (6, 6)),
    ]
    for color in (3, 4, 5, 6):
        routers['R1'].policy_client().add(sr.SrPolicy(STATIC, color, color))
    routers['R1'].policy_client().set_steering(rules)
    net.converge()
    view = _View(net.state, 'R1')
    probe = replace(packet(), dscp=10, payload=L4Header(42, 80))
    assert fw.steering_policy(view, probe) == sr.PolicyRef(*p.key)
    assert fw.steering_policy(view, replace(probe, dst=1)) == sr.PolicyRef(3, 3)
    assert fw.steering_policy(view, replace(probe, dst=1, dscp=0)) == sr.PolicyRef(4, 4)
    assert fw.steering_policy(
        view, replace(probe, dst=1, dscp=0, payload=L4Header(1, 2))
    ) == sr.PolicyRef(6, 6)
    net.add_demand('d', 'R1', '10.0.0.4', 100, steer=sr.PolicyRef(999, 999))
    net.converge()
    assert dict(net.placement.dropped_by_reason) == {fw.POLICY_DOWN: 100}


@pytest.mark.parametrize('mode', [1, 2])
def test_transit_destination_policy_and_decap_steering(mode):
    # A -> R1 enters via ordinary forwarding; R4's after-decap rule starts a
    # second policy to a local terminal. Its subsequent re-steer is a LOOP.
    net, routers = diamond()
    p = policy(routers)
    routers['R1'].add_route(
        '10.0.0.4/32', [Nexthop(policy=sr.PolicyRef(*p.key))], distance=0
    )
    a = net.add_device('source')
    a.add_loopback('lo', ipv4=['10.0.0.99/32'])
    net.add_p2p(a, 'out', routers['R1'], 'in', unnumbered=True)
    net.add_demand('d', 'source', '10.0.0.4', 100, mode=mode)
    net.converge()
    assert net.placement.delivered_total == 100
    assert net.placement.demands['d'].policies[0].device == 'R1'
    second = sr.SrPolicy(
        STATIC,
        20,
        A('2001:db8::4'),
        candidate_paths=(
            sr.CandidatePath(segment_lists=(sr.SegmentList((sr.TermSeg('R4'),)),)),
        ),
    )
    routers['R4'].policy_client().add(second)
    routers['R4'].policy_client().set_steering([sr.SteeringRule('again', second.key)])
    net.converge()
    assert dict(net.placement.dropped_by_reason) == {fw.LOOP: 100}
    assert {p.device for p in net.placement.demands['d'].policies} == {'R1', 'R4'}


@pytest.mark.parametrize('mode', [1, 2])
def test_demand_override_is_consumed_once_with_igp_fallback(mode):
    net, routers = diamond()
    p = policy(routers, lists=(sr.SegmentList(()),), fallback=sr.FALLBACK_IGP)
    net.add_demand('d', 'R1', '10.0.0.4', 100, mode=mode, steer=sr.PolicyRef(*p.key))
    net.converge()
    assert net.placement.delivered_total == 100
    assert not net.placement.dropped_by_reason


def test_bare_scope_trap_is_not_repaired_by_basic_validation():
    net, routers = diamond()
    # R2's adjacency has the same bare function as a different adjacency on R1.
    sid = next(
        s for s in routers['R2'].node.srv6_sids.sids.values() if s.interface == 'eth3'
    )
    own = next(
        s for s in routers['R1'].node.srv6_sids.sids.values() if s.sid == sid.sid
    )
    assert own.interface != 'Po1'
    p = policy(
        routers, lists=(sr.SegmentList((sr.AdjSeg('R2', 'eth3'), sr.TermSeg('R4'))),)
    )
    routers['R1'].policy_client().set_steering([sr.SteeringRule('all', p.key)])
    net.converge()
    assert state_of(routers, p).strict_valid == ()
    routers['R1'].configure(resolution_policy=ResolutionPolicy(validate_all_sids=False))
    net.converge()
    assert state_of(routers, p).status == sr.POLICY_UP
    trace = net.trace('R1', packet())
    assert trace.path != ('R1', 'R2', 'R4')


def test_candidate_protocol_origin_discriminator_and_reorder():
    net, routers = diamond()
    p = policy(routers)
    a = sr.CandidatePath(100, (path(),), protocol_origin=20, discriminator=1)
    b = replace(a, discriminator=2)
    c = replace(a, protocol_origin=30)
    for paths, expected in (((a, b), 1), ((a, b, c), 2)):
        routers['R1'].policy_client().replace(replace(p, candidate_paths=paths))
        net.converge()
        assert state_of(routers, p).active_path == expected
    routers['R1'].configure(resolution_policy=ResolutionPolicy(prefer_installed=True))
    routers['R1'].policy_client().replace(replace(p, candidate_paths=(c, b, a)))
    net.converge()
    assert state_of(routers, p).active_path == 0
