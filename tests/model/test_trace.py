from netsim.model import forwarding as fw
from netsim.model.addressing import IPV4, to_int
from netsim.model.igp import oracle_igp
from netsim.model.packets import PacketTemplate
from tests.model.test_network import build_diamond


def A(text):
    return to_int(text)[0]


def probe(dst='10.0.0.4', sport=49152, **kw):
    return PacketTemplate(IPV4, A('10.0.0.1'), A(dst), sport=sport, **kw).to_packet()


class TestTrace:
    def test_probe_reaches_r4_with_ttl_accounting(self):
        net, R = build_diamond()
        net.converge()
        t = net.trace('R1', probe())
        assert (
            t.outcome == fw.DELIVER
            and t.path[0] == 'R1'
            and t.path[-1] == 'R4'
            and len(t.hops) == 3
        )
        transit = t.hops[1]
        assert t.hops[0].packet.ttl == 64  # originated: no decrement at the source
        assert (
            transit.packet.ttl == 63 and t.hops[2].packet.ttl == 63
        )  # transit decrements once; delivery does not
        assert t.hops[0].edge_id is not None
        # Both ECMP branches are used by different flows.
        paths = {net.trace('R1', probe(sport=49152 + i)).path[1] for i in range(64)}
        assert paths == {'R2', 'R3'}
        # LAG member selection spreads flows across both members when going via R2.
        members = {
            h.member
            for i in range(64)
            for h in net.trace('R1', probe(sport=49152 + i)).hops
            if h.device == 'R1' and h.egress == 'Po1'
        }
        assert members == {'eth1', 'eth2'}

    def test_ttl_expiry_and_no_route(self):
        net, R = build_diamond()
        net.converge()
        t = net.trace('R1', probe(ttl=1))
        assert (
            t.outcome == fw.DROP
            and t.reason == fw.TTL_EXPIRED
            and t.path == ('R1', t.path[1])
        )
        t2 = net.trace('R1', probe(dst='10.9.9.9'))
        assert (t2.outcome, t2.reason) == (fw.DROP, fw.NO_ROUTE)

    def test_stale_routing_over_a_failed_link_drops_at_transmission(self):
        net, R = build_diamond(min_links=1)
        net.converge()
        # Fail the R1-R3 link but do not converge: the FIB still points at eth3.
        net.links['R1:eth3--R3:eth1'].fail()
        results = {net.trace('R1', probe(sport=49152 + i)).reason for i in range(32)}
        assert fw.LINK_DOWN in results
        net.converge()
        assert all(
            net.trace('R1', probe(sport=49152 + i)).outcome == fw.DELIVER
            for i in range(32)
        )

    def test_mac_mismatch_and_loop(self):
        net, R = build_diamond()
        net.add_source(oracle_igp)
        net.converge()
        from netsim.model.packets import ETHERTYPE_IPV4, EthernetFrame

        view = net.view('R3')
        frame = EthernetFrame(0x1234, 0x5678, ETHERTYPE_IPV4, probe())
        assert fw.receive_frame(view, 'eth1', frame).reason == fw.MAC_MISMATCH
        # A mutual static pair is a forwarding loop: the probe expires.
        R['R2'].add_route('10.9.0.0/16', [('Po1', '10.1.12.0')])
        R['R1'].add_route('10.9.0.0/16', [('Po1', '10.1.12.1')])
        net.converge()
        t = net.trace('R1', probe(dst='10.9.9.9', ttl=8))
        assert (t.outcome, t.reason) == (fw.DROP, fw.TTL_EXPIRED) and len(
            t.hops
        ) == 9  # originator + 8 transit steps
