from netsim.model import forwarding as fw
from netsim.model.addressing import IPV4, IPV6, to_int
from netsim.model.contracts import IGP
from netsim.model.igp import oracle_igp, shortest_path_routes
from netsim.model.network import Network


def A(text):
    return to_int(text)[0]


def build(unnumbered=False):
    net = Network()
    R = {n: net.add_device(n) for n in ('R1', 'R2', 'R3', 'R4')}
    for i, r in enumerate(R.values(), 1):
        r.add_loopback('lo0', ipv4=[f'10.0.0.{i}/32'], ipv6=[f'2001:db8::{i}/128'])
    kw = dict(unnumbered=True) if unnumbered else {}

    def pair(x):
        return None if unnumbered else x

    net.add_p2p(
        R['R1'],
        'eth3',
        R['R3'],
        'eth1',
        ipv4=pair(('10.1.13.0/31', '10.1.13.1/31')),
        speed=100e6,
        **kw,
    )
    net.add_p2p(
        R['R2'],
        'eth3',
        R['R4'],
        'eth1',
        ipv4=pair(('10.1.24.0/31', '10.1.24.1/31')),
        speed=100e6,
        **kw,
    )
    net.add_p2p(
        R['R3'],
        'eth2',
        R['R4'],
        'eth2',
        ipv4=pair(('10.1.34.0/31', '10.1.34.1/31')),
        speed=100e6,
        **kw,
    )
    net.add_lag(
        R['R1'],
        'Po1',
        ['eth1', 'eth2'],
        R['R2'],
        'Po1',
        ['eth1', 'eth2'],
        ipv4=pair(('10.1.12.0/31', '10.1.12.1/31')),
        min_links=2,
        speed=100e6,
        **kw,
    )
    net.add_source(oracle_igp)
    return net, R


class TestOracleIgp:
    def test_ecmp_rows_and_fib(self):
        net, R = build()
        net.converge()
        rows = shortest_path_routes(net.state, 'R1', IPV4)
        by_prefix = {r.prefix: r for r in rows}
        r4 = by_prefix[(A('10.0.0.4'), 32)]
        assert r4.metric == 2 and r4.source == IGP and r4.distance == 110
        assert sorted((nh.interface, nh.address) for nh in r4.nexthops) == [
            ('Po1', A('10.1.12.1')),
            ('eth3', A('10.1.13.1')),
        ]
        fib = R['R1'].fib(IPV4)
        g = fib.group(fib.lookup(A('10.0.0.4')))
        assert [a.interface for a in g.adjacencies] == ['Po1', 'eth3']
        # Interface subnets of remote devices are reachable too, own subnets are connected not igp.
        assert fib.lookup(A('10.1.24.1')).action == fw.FORWARD
        assert (A('10.1.13.0'), 31) not in {r.prefix for r in rows}
        # IPv6 loopbacks are unreachable: no IPv6 forwarding on the links (Gate A IPv4-only fixture).
        assert not shortest_path_routes(net.state, 'R1', IPV6)

    def test_reconverges_after_failure_and_is_canonical(self):
        net, R = build()
        net.converge()
        v = net.state.version
        net.converge()
        assert net.state.version == v
        net.links['R1:eth1--R2:eth1'].fail()
        net.converge()
        fib = R['R1'].fib(IPV4)
        g = fib.group(fib.lookup(A('10.0.0.4')))
        assert [a.interface for a in g.adjacencies] == ['eth3']
        assert fib.lookup(A('10.0.0.2')).group_id == fib.lookup(A('10.0.0.4')).group_id
        assert (
            R['R1'].rib(IPV4).rows((A('10.0.0.2'), 32))[0].metric == 3
        )  # via R3, R4: three hops

    def test_unnumbered_uses_interface_nexthops(self):
        net, R = build(unnumbered=True)
        net.converge()
        fib4 = R['R1'].fib(IPV4)
        g = fib4.group(fib4.lookup(A('10.0.0.4')))
        assert [(a.interface, a.nexthop) for a in g.adjacencies] == [
            ('Po1', None),
            ('eth3', None),
        ]
        assert g.adjacencies[0].mac == R['R2']['Po1'].mac
        fib6 = R['R1'].fib(IPV6)
        assert fib6.lookup(to_int('2001:db8::4')[0]).action == fw.FORWARD
