import pytest

from netsim.model import interfaces as ifc
from netsim.model.addressing import IPV4, IPV6
from netsim.model.interfaces import (
    AdminState,
    BundleInput,
    EthernetConfig,
    EthernetNode,
    EthernetOper,
    IfKind,
    LoopbackConfig,
    LoopbackNode,
    LoopbackOper,
    MemberInput,
    OperState,
    PortChannelConfig,
    PortChannelNode,
    PortChannelOper,
    StateReason,
    derive_ethernet,
    derive_lag_component,
    derive_loopback,
    l3_usable,
)
from netsim.model.links import (
    LINK_FAILED,
    LINK_UP,
    LinkConfig,
    LinkNode,
    LinkOper,
    link_id,
    transfer_blocked,
)


class TestEnums:
    def test_rfc2863_values_and_iana_names(self):
        assert (
            OperState.UP == 1
            and OperState.DOWN == 2
            and OperState.NOT_PRESENT == 6
            and OperState.LOWER_LAYER_DOWN == 7
        )
        assert IfKind.PORT_CHANNEL.iana_name == 'ieee8023adLag'
        assert IfKind.LOOPBACK.iana_name == 'softwareLoopback'


class TestLoopback:
    def test_rules_and_since(self):
        cfg = LoopbackConfig(ipv4=((0x0A000001, 32),))
        o1 = derive_loopback(cfg, True, LoopbackOper(), now=1.0)
        assert (o1.oper, o1.reason, o1.since) == (OperState.UP, StateReason.UP, 1.0)
        assert derive_loopback(cfg, True, o1, now=5.0) is o1  # unchanged: same object
        o2 = derive_loopback(cfg, False, o1, now=5.0)
        assert (o2.oper, o2.reason, o2.since) == (
            OperState.DOWN,
            StateReason.DEVICE_DISABLED,
            5.0,
        )
        o3 = derive_loopback(LoopbackConfig(admin=AdminState.DOWN), True, o2, now=6.0)
        assert o3.reason == StateReason.ADMIN
        node = LoopbackNode('lo0', 1, 1, cfg, o1)
        assert l3_usable(node, IPV4) and not l3_usable(node, IPV6)


def _eth(name='eth1', link='L', **cfg):
    return EthernetNode(
        name, 1, 1, mac=0x020000000001, config=EthernetConfig(**cfg), link=link
    )


class TestEthernet:
    def test_truth_table(self):
        up = LinkOper(LINK_UP, 0.0)
        cases = [
            # (device_enabled, admin, link presence, link oper, peer admin, peer enabled) -> (oper, reason)
            (
                False,
                AdminState.UP,
                'L',
                up,
                AdminState.UP,
                True,
                OperState.DOWN,
                StateReason.DEVICE_DISABLED,
            ),
            (
                True,
                AdminState.DOWN,
                'L',
                up,
                AdminState.UP,
                True,
                OperState.DOWN,
                StateReason.ADMIN,
            ),
            (
                True,
                AdminState.UP,
                None,
                None,
                AdminState.UP,
                True,
                OperState.NOT_PRESENT,
                StateReason.NO_LINK,
            ),
            (
                True,
                AdminState.UP,
                'L',
                LinkOper(LINK_FAILED, 0.0),
                AdminState.UP,
                True,
                OperState.DOWN,
                StateReason.CARRIER,
            ),
            (
                True,
                AdminState.UP,
                'L',
                up,
                AdminState.DOWN,
                True,
                OperState.DOWN,
                StateReason.PEER_ADMIN,
            ),
            (
                True,
                AdminState.UP,
                'L',
                up,
                AdminState.UP,
                False,
                OperState.DOWN,
                StateReason.PEER_ADMIN,
            ),
            (
                True,
                AdminState.UP,
                'L',
                up,
                AdminState.UP,
                True,
                OperState.UP,
                StateReason.UP,
            ),
        ]
        for (
            enabled,
            admin,
            link,
            link_oper,
            peer_admin,
            peer_enabled,
            oper,
            reason,
        ) in cases:
            node = _eth(link=link, admin=admin)
            got = derive_ethernet(
                node,
                enabled,
                link_oper,
                EthernetConfig(admin=peer_admin) if link else None,
                peer_enabled,
                None,
                now=1.0,
            )
            assert (got.oper, got.reason) == (oper, reason), (
                enabled,
                admin,
                link,
                link_oper,
                peer_admin,
                peer_enabled,
            )

    def test_raw_since_and_effective_carrier(self):
        node = _eth()
        o1 = derive_ethernet(
            node, True, LinkOper(LINK_UP, 0.0), EthernetConfig(), True, None, now=1.0
        )
        assert (
            o1.carrier_raw and o1.carrier_raw_since == 1.0 and o1.oper == OperState.UP
        )
        node = EthernetNode('eth1', 1, 1, 0x020000000001, EthernetConfig(), o1, 'L')
        # Raw drops at t=10 but the effective carrier is still up (debounce pending).
        o2 = derive_ethernet(
            node,
            True,
            LinkOper(LINK_FAILED, 10.0),
            EthernetConfig(),
            True,
            True,
            now=10.0,
        )
        assert (
            not o2.carrier_raw
            and o2.carrier_raw_since == 10.0
            and o2.oper == OperState.UP
            and o2.since == 1.0
        )
        node = EthernetNode('eth1', 1, 1, 0x020000000001, EthernetConfig(), o2, 'L')
        # Effective transition applied at 11: reason is the raw failing reason.
        o3 = derive_ethernet(
            node,
            True,
            LinkOper(LINK_FAILED, 10.0),
            EthernetConfig(),
            True,
            False,
            now=11.0,
        )
        assert (o3.oper, o3.reason, o3.since, o3.carrier_raw_since) == (
            OperState.DOWN,
            StateReason.CARRIER,
            11.0,
            10.0,
        )
        assert (
            derive_ethernet(
                EthernetNode('eth1', 1, 1, 1, EthernetConfig(), o3, 'L'),
                True,
                LinkOper(LINK_FAILED, 10.0),
                EthernetConfig(),
                True,
                False,
                now=12.0,
            )
            is o3
        )

    def test_l3_usable_rules(self):
        up = EthernetOper(OperState.UP, StateReason.UP, 0.0, True, 0.0)
        n = EthernetNode(
            'eth1', 1, 1, 1, EthernetConfig(ipv4=((0x0A010D00, 31),)), up, 'L'
        )
        assert l3_usable(n, IPV4) and not l3_usable(n, IPV6)
        n6 = EthernetNode(
            'eth1',
            1,
            1,
            1,
            EthernetConfig(ipv4=((0x0A010D00, 31),), forwarding_v6=True),
            up,
            'L',
        )
        assert l3_usable(n6, IPV6)
        unnum = EthernetNode('eth1', 1, 1, 1, EthernetConfig(unnumbered=True), up, 'L')
        assert l3_usable(unnum, IPV4) and l3_usable(unnum, IPV6)
        member = EthernetNode(
            'eth1',
            1,
            1,
            1,
            EthernetConfig(aggregate_id='Po1', ipv4=((1, 32),)),
            up,
            'L',
        )
        assert not l3_usable(member, IPV4)
        down = EthernetNode(
            'eth1', 1, 1, 1, EthernetConfig(ipv4=((1, 32),)), EthernetOper(), 'L'
        )
        assert not l3_usable(down, IPV4)


def _bundle(key, members, min_links=1, enabled=True, admin=AdminState.UP, old=None):
    return BundleInput(
        key,
        PortChannelConfig(min_links=min_links, admin=admin),
        enabled,
        tuple(members),
        old or PortChannelOper(),
    )


def _m(name, index, up, peer, cap=100e6):
    return MemberInput(name, index, up, peer, cap)


class TestLag:
    def test_symmetric_min_links(self):
        a, b = ('R1', 'Po1'), ('R2', 'Po1')
        bundles = {
            a: _bundle(
                a, [_m('eth1', 1, True, b), _m('eth2', 2, True, b)], min_links=2
            ),
            b: _bundle(
                b, [_m('eth1', 1, True, a), _m('eth2', 2, True, a)], min_links=1
            ),
        }
        out = derive_lag_component(bundles, now=1.0)
        assert out[a].oper == OperState.UP and out[b].oper == OperState.UP
        assert out[a].bandwidth == 200e6 and out[a].members['eth1'].active
        # R1.eth1 goes down: R1 has 1 candidate < min_links 2 -> not distributing -> both ends down.
        bundles[a] = _bundle(
            a,
            [_m('eth1', 1, False, b), _m('eth2', 2, True, b)],
            min_links=2,
            old=out[a],
        )
        bundles[b] = _bundle(
            b,
            [_m('eth1', 1, False, a), _m('eth2', 2, True, a)],
            min_links=1,
            old=out[b],
        )
        out2 = derive_lag_component(bundles, now=10.0)
        assert (out2[a].oper, out2[a].reason) == (
            OperState.LOWER_LAYER_DOWN,
            StateReason.MIN_LINKS,
        )
        assert (out2[b].oper, out2[b].reason) == (
            OperState.LOWER_LAYER_DOWN,
            StateReason.PARTNER,
        )
        assert out2[a].bandwidth == 0.0 and out2[b].since == 10.0
        # With min_links=1 on R1 the bundle stays up on the remaining member.
        bundles[a] = _bundle(
            a,
            [_m('eth1', 1, False, b), _m('eth2', 2, True, b)],
            min_links=1,
            old=out[a],
        )
        out3 = derive_lag_component(bundles, now=10.0)
        assert (
            out3[a].oper == OperState.UP
            and out3[a].bandwidth == 100e6
            and out3[b].oper == OperState.UP
        )
        assert out3[b].members['eth2'].active and not out3[b].members['eth1'].active

    def test_unbundled_peer_and_no_members(self):
        a = ('R1', 'Po1')
        bundles = {a: _bundle(a, [_m('eth1', 1, True, None)])}
        out = derive_lag_component(bundles, now=0.0)
        assert (out[a].oper, out[a].reason) == (
            OperState.LOWER_LAYER_DOWN,
            StateReason.MIN_LINKS,
        )
        assert not out[a].members['eth1'].candidate
        empty = {a: _bundle(a, [])}
        assert derive_lag_component(empty, now=0.0)[a].reason == StateReason.MEMBERS

    def test_admin_and_multiple_partners(self):
        a, b, c = ('R1', 'Po1'), ('R2', 'Po1'), ('R3', 'Po1')
        bundles = {
            a: _bundle(a, [_m('eth1', 1, True, b), _m('eth2', 2, True, c)]),
            b: _bundle(b, [_m('eth1', 1, True, a)]),
            c: _bundle(c, [_m('eth1', 1, True, a)]),
        }
        out = derive_lag_component(bundles, now=0.0)
        assert (
            out[a].reason == StateReason.MULTIPLE_PARTNERS
            and not out[a].members['eth1'].candidate
        )
        assert out[b].reason == StateReason.PARTNER
        down = {
            a: _bundle(a, [_m('eth1', 1, True, b)], admin=AdminState.DOWN),
            b: _bundle(b, [_m('eth1', 1, True, a)]),
        }
        out2 = derive_lag_component(down, now=0.0)
        assert (out2[a].oper, out2[a].reason) == (OperState.DOWN, StateReason.ADMIN)

    def test_member_delay_and_canonicalization(self):
        a, b = ('R1', 'Po1'), ('R2', 'Po1')
        bundles = {
            a: _bundle(a, [_m('eth1', 1, True, b)]),
            b: _bundle(b, [_m('eth1', 1, True, a)]),
        }
        out = derive_lag_component(
            bundles, now=0.0, delay_elapsed=lambda key, name, since: False
        )
        assert (
            out[a].reason == StateReason.PARTNER and not out[a].members['eth1'].active
        )
        out2 = derive_lag_component(
            {k: _bundle(k, v.members, old=out[k]) for k, v in bundles.items()}, now=1.0
        )
        assert (
            out2[a].oper == OperState.UP
            and out2[a].members['eth1'].candidate_since == 0.0
        )
        out3 = derive_lag_component(
            {k: _bundle(k, v.members, old=out2[k]) for k, v in bundles.items()}, now=2.0
        )
        assert out3[a] is out2[a]


class TestLink:
    def test_ids_and_edges(self):
        assert link_id(('R2', 'eth1'), ('R1', 'eth3')) == 'R1:eth3--R2:eth1'
        link = LinkNode(
            'R1:eth3--R2:eth1',
            4,
            1,
            ('R1', 'eth3'),
            ('R2', 'eth1'),
            LinkConfig(),
            LinkOper(),
        )
        assert link.other(('R1', 'eth3')) == ('R2', 'eth1')
        assert link.edge_id(('R1', 'eth3')) == 8 and link.edge_id(('R2', 'eth1')) == 9
        with pytest.raises(ValueError):
            link.direction(('R9', 'x'))

    def test_transfer_blocked(self):
        assert transfer_blocked(LINK_UP, True, True, True, True) is None
        assert transfer_blocked(LINK_FAILED, True, True, True, True) == 'LINK_DOWN'
        assert transfer_blocked(LINK_UP, False, True, True, True) == 'LINK_DOWN'
        assert transfer_blocked(LINK_UP, True, True, False, True) == 'RX_DOWN'
        assert transfer_blocked(LINK_UP, True, True, True, False) == 'RX_DOWN'


def test_port_channel_node_kind():
    assert PortChannelNode('Po1', 3, 1, 1).kind is IfKind.PORT_CHANNEL
    assert ifc.is_bundled(
        EthernetNode('e', 1, 1, 1, EthernetConfig(aggregate_id='Po1'))
    )
