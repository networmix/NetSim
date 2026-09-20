import pytest

from netsim.model import forwarding as fw
from netsim.model.addressing import IPV4, to_int
from netsim.model.contracts import CONNECTED, LOCAL, STATIC
from netsim.model.entities import StaleHandleError
from netsim.model.interfaces import OperState, StateReason
from netsim.model.links import LINK_FAILED
from netsim.model.network import Network
from netsim.model.routing import Nexthop
from netsim.model.state import PMap


def A(text):
    return to_int(text)[0]


def build_diamond(min_links=2, **kw):
    net = Network()
    R = {n: net.add_device(n, **kw) for n in ('R1', 'R2', 'R3', 'R4')}
    for i, r in enumerate(R.values(), 1):
        r.add_loopback('lo0', ipv4=[f'10.0.0.{i}/32'], ipv6=[f'2001:db8::{i}/128'])
    net.add_p2p(
        R['R1'],
        'eth3',
        R['R3'],
        'eth1',
        ipv4=('10.1.13.0/31', '10.1.13.1/31'),
        speed=100e6,
    )
    net.add_p2p(
        R['R2'],
        'eth3',
        R['R4'],
        'eth1',
        ipv4=('10.1.24.0/31', '10.1.24.1/31'),
        speed=100e6,
    )
    net.add_p2p(
        R['R3'],
        'eth2',
        R['R4'],
        'eth2',
        ipv4=('10.1.34.0/31', '10.1.34.1/31'),
        speed=100e6,
    )
    net.add_lag(
        R['R1'],
        'Po1',
        ['eth1', 'eth2'],
        R['R2'],
        'Po1',
        ['eth1', 'eth2'],
        ipv4=('10.1.12.0/31', '10.1.12.1/31'),
        min_links=min_links,
        speed=100e6,
    )
    R['R1'].add_route('10.0.0.2/32', [('Po1', '10.1.12.1')])
    R['R1'].add_route('10.0.0.3/32', [('eth3', '10.1.13.1')])
    R['R1'].add_route('10.0.0.4/32', ['10.0.0.2', '10.0.0.3'])
    R['R2'].add_route('10.0.0.4/32', [('eth3', '10.1.24.1')])
    R['R3'].add_route('10.0.0.4/32', [('eth2', '10.1.34.1')])
    return net, R


class TestBuildAndConverge:
    def test_diamond_converges(self):
        net, R = build_diamond()
        net.converge()
        r1 = R['R1']
        assert (
            r1['eth3'].oper.oper == OperState.UP
            and r1['eth1'].oper.oper == OperState.UP
        )
        po = r1['Po1']
        assert po.oper.oper == OperState.UP and po.oper.bandwidth == 200e6
        assert R['R2']['Po1'].oper.members['eth2'].active
        rib = r1.rib(IPV4)
        assert (
            len(rib.rows_of(CONNECTED)) == 2 and len(rib.rows_of(LOCAL)) == 3
        )  # /31s + lo0/32 host routes
        fib = r1.fib(IPV4)
        e = fib.lookup(A('10.0.0.4'))
        g = fib.group(e)
        assert [(a.interface, a.nexthop) for a in g.adjacencies] == [
            ('Po1', A('10.1.12.1')),
            ('eth3', A('10.1.13.1')),
        ]
        assert (
            g.adjacencies[0].mac == R['R2']['Po1'].mac
            and g.adjacencies[1].mac == R['R3']['eth1'].mac
        )
        assert fib.lookup(A('10.0.0.1')).action == fw.RECEIVE
        assert R['R4'].node.oper.router_id == A('10.0.0.4')
        nt = r1.node.neighbors
        assert (
            nt.mac('Po1', A('10.1.12.1')) == R['R2']['Po1'].mac
            and nt.peer_mac('eth3') == R['R3']['eth1'].mac
        )
        key = (A('10.0.0.4'), 32, STATIC, ())
        assert r1.route_status(IPV4, key) == ('INSTALLED', None)
        # Converging again changes nothing (canonical tree).
        v = net.state.version
        net.converge()
        assert net.state.version == v

    @pytest.mark.parametrize(
        'min_links, expected_ifaces', [(2, ['eth3']), (1, ['Po1', 'eth3'])]
    )
    def test_member_failure(self, min_links, expected_ifaces):
        net, R = build_diamond(min_links=min_links)
        net.converge()
        key = (A('10.0.0.4'), 32, STATIC, ())
        net.links['R1:eth1--R2:eth1'].fail()
        assert R['R1'].route_status(IPV4, key) == (
            'INSTALLED',
            None,
        )  # a link failure is not a FIB input until CARRIER runs
        net.converge()
        r1 = R['R1']
        assert (
            r1['eth1'].oper.oper == OperState.DOWN
            and r1['eth1'].oper.reason == StateReason.CARRIER
        )
        po = r1['Po1'].oper
        g = r1.fib(IPV4).group(r1.fib(IPV4).lookup(A('10.0.0.4')))
        assert [a.interface for a in g.adjacencies] == expected_ifaces
        if min_links == 2:
            assert (po.oper, po.reason) == (
                OperState.LOWER_LAYER_DOWN,
                StateReason.MIN_LINKS,
            )
            assert (
                R['R2']['Po1'].oper.reason == StateReason.MIN_LINKS
            )  # add_lag applies min_links to both ends
            assert r1.route_status(IPV4, (A('10.0.0.2'), 32, STATIC, ())) == (
                'NOT_INSTALLED',
                'UNRESOLVED',
            )
        else:
            assert po.oper == OperState.UP and po.bandwidth == 100e6
        net.links['R1:eth1--R2:eth1'].restore()
        net.converge()
        assert r1['Po1'].oper.oper == OperState.UP and r1['Po1'].oper.bandwidth == 200e6

    def test_admin_down_and_device_disable(self):
        net, R = build_diamond()
        net.converge()
        R['R3']['eth1'].admin_down()
        net.converge()
        assert R['R1']['eth3'].oper.reason == StateReason.PEER_ADMIN
        g = R['R1'].fib(IPV4).group(R['R1'].fib(IPV4).lookup(A('10.0.0.4')))
        assert [a.interface for a in g.adjacencies] == ['Po1']
        R['R3']['eth1'].admin_up()
        R['R2'].configure(enabled=False)
        net.converge()
        assert R['R1']['eth1'].oper.reason == StateReason.PEER_ADMIN
        assert [
            a.interface
            for a in R['R1']
            .fib(IPV4)
            .group(R['R1'].fib(IPV4).lookup(A('10.0.0.4')))
            .adjacencies
        ] == ['eth3']


class TestUpdateSemantics:
    def test_failed_update_leaves_root_and_allocators(self):
        net = Network()
        r1 = net.add_device('R1')
        before = net.state
        with pytest.raises(ValueError):
            r1.add_ethernet('eth1', speed=0)
        assert net.state is before
        with pytest.raises(ValueError):
            net.add_device('R1')
        assert net.state is before

    def test_nested_update_raises_and_hooks_see_delta(self):
        net = Network()
        seen = []

        def hook(t, origin, delta):
            seen.append((origin, delta.devices().added))
            if origin == ('add_device', 'R2'):
                with pytest.raises(RuntimeError):
                    net.add_device('R3')

        net.on_delta.append(hook)
        net.add_device('R1')
        net.add_device('R2')
        assert seen[0] == (('add_device', 'R1'), ('R1',)) and len(seen) == 2

    def test_noop_update_returns_none(self):
        net = Network()
        r1 = net.add_device('R1')
        r1.add_loopback('lo0', ipv4=['10.0.0.1/32'])
        v = net.state.version
        r1['lo0'].configure(description='')
        assert net.state.version == v
        assert net.update(lambda s: s) is None

    def test_validation(self):
        net = Network()
        r1, r2, r3 = (net.add_device(n) for n in ('R1', 'R2', 'R3'))
        r1.add_ethernet('e1')
        r2.add_ethernet('e1')
        net.add_link(r1['e1'], r2['e1'])
        with pytest.raises(ValueError):
            net.add_link(r1['e1'], r2['e1'])
        r1.add_ethernet('e2')
        with pytest.raises(ValueError):
            net.add_link(r1['e2'], r1['e1'])
        with pytest.raises(ValueError):
            r1.add_loopback('lo0', ipv4=['2001:db8::1/128'])
        r1.add_portchannel('Po1')
        with pytest.raises(ValueError):
            r1.add_ethernet('m1', aggregate_id='Po1', ipv4=['10.0.0.1/31'])
        with pytest.raises(ValueError):
            r1.add_ethernet('m2', aggregate_id='Po9')
        with pytest.raises(ValueError):
            r1.add_portchannel('Po2', min_links=0)
        with pytest.raises(ValueError):
            r1.add_ethernet('e3', ipv6=['2001:db8::9/64'], mtu=1000)
        with pytest.raises(ValueError):
            net.add_device('bad|name')
        # Multiple partners rejected.
        r2.add_portchannel('Po1')
        r3.add_portchannel('Po1')
        r1.add_ethernet('m1', aggregate_id='Po1')
        r1.add_ethernet('m2', aggregate_id='Po1')
        r2.add_ethernet('m1', aggregate_id='Po1')
        r3.add_ethernet('m1', aggregate_id='Po1')
        net.add_link(r1['m1'], r2['m1'])
        with pytest.raises(ValueError):
            net.add_link(r1['m2'], r3['m1'])

    def test_handles_and_generations(self):
        net = Network()
        r1 = net.add_device('R1')
        e = r1.add_ethernet('e1')
        assert e == r1['e1'] and hash(e) == hash(r1['e1']) and e.exists
        assert r1.interfaces == [e]
        with pytest.raises(KeyError):
            r1['nope']
        assert net['R1'] is r1
        stale = net.device('R1')
        assert stale.generation == r1.generation
        assert isinstance(net.state.devices, PMap)

    def test_parse_nexthops(self):
        from netsim.model.entities import parse_nexthop

        assert parse_nexthop('10.0.0.2').is_recursive
        assert parse_nexthop('eth3') == Nexthop.via('eth3')
        assert parse_nexthop(('eth3', '10.1.13.1')) == Nexthop.via(
            'eth3', A('10.1.13.1'), IPV4
        )
        assert parse_nexthop('blackhole').special is not None
        with pytest.raises(TypeError):
            parse_nexthop(3.5)

    def test_link_fail_state_and_edge_ids(self):
        net, R = build_diamond()
        link = net.links['R1:eth3--R3:eth1']
        assert (
            link.edge('R1') == 2 * link.node.index
            and link.edge('R3') == 2 * link.node.index + 1
        )
        link.fail()
        assert link.state == LINK_FAILED
        with pytest.raises(ValueError):
            link.edge('R9')
        assert not isinstance(StaleHandleError(), ValueError)


class TestFork:
    def test_fork_shares_the_root_and_diverges(self):
        net, R = build_diamond()
        net.add_demand('d1', 'R1', '10.0.0.4', 100e6)
        net.converge()
        base = net.state
        f = net.fork()
        assert f.state is base
        assert f.sources == net.sources and f.capacity_model == net.capacity_model
        f.links['R1:eth3--R3:eth1'].fail()
        f.converge()
        assert net.state is base  # the original is untouched
        assert f.state.placement.delivered_total == pytest.approx(100e6)
        assert f.devices['R1'].fib(4) is not net.devices['R1'].fib(4)
        # handles belong to their network
        assert R['R1'].network is net and f.devices['R1'].network is f
