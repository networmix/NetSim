"""Clock-free ``converge()`` and the timed pipeline reach identical trees
(modulo version and since fields) for the same operations."""

import pytest

import netsim
from netsim.model.addressing import IPV4, IPV6, to_int
from netsim.model.igp import oracle_igp
from netsim.model.state import tree_equal
from netsim.runtime import Simulation
from tests.model.test_network import build_diamond


def A(text):
    return to_int(text)[0]


def _ops(net, R):
    return [
        lambda: net.links['R1:eth1--R2:eth1'].fail(),
        lambda: R['R3']['eth1'].admin_down(),
        lambda: net.links['R1:eth1--R2:eth1'].restore(),
        lambda: R['R3']['eth1'].admin_up(),
        lambda: R['R2'].configure(enabled=False),
        lambda: R['R2'].configure(enabled=True),
    ]


@pytest.mark.parametrize('with_igp', [False, True])
def test_converge_equals_timed_pipeline(with_igp):
    net_a, R_a = build_diamond()
    net_b, R_b = build_diamond()
    for net in (net_a, net_b):
        net.add_demand('d1', 'R1', '10.0.0.4', 100e6)
        if with_igp:
            net.add_source(oracle_igp)
    net_a.converge()
    env = netsim.Environment()
    sim = Simulation(env, net_b)
    assert tree_equal(net_a.state, net_b.state)
    for i, (op_a, op_b) in enumerate(
        zip(_ops(net_a, R_a), _ops(net_b, R_b), strict=True)
    ):
        op_a()
        net_a.converge()
        sim.at(10 * (i + 1), op_b)
        sim.run_until(10 * (i + 1) + 1)
        assert tree_equal(net_a.state, net_b.state), f'diverged after operation {i}'
    assert net_b.placement.delivered_total == pytest.approx(
        net_a.placement.delivered_total
    )


def test_ipv6_variant_matches_ipv4_loads():
    net, R = build_diamond()
    # Enable IPv6 forwarding on every L3 link interface and give the links /127 pairs.
    pairs = {
        ('R1', 'eth3'): '2001:db8:13::/127',
        ('R3', 'eth1'): '2001:db8:13::1/127',
        ('R2', 'eth3'): '2001:db8:24::/127',
        ('R4', 'eth1'): '2001:db8:24::1/127',
        ('R3', 'eth2'): '2001:db8:34::/127',
        ('R4', 'eth2'): '2001:db8:34::1/127',
        ('R1', 'Po1'): '2001:db8:12::/127',
        ('R2', 'Po1'): '2001:db8:12::1/127',
    }
    for (dev, iface), addr in pairs.items():
        R[dev][iface].configure(ipv6=[addr])
    R['R1'].add_route('2001:db8::2/128', [('Po1', '2001:db8:12::1')])
    R['R1'].add_route('2001:db8::3/128', [('eth3', '2001:db8:13::1')])
    R['R1'].add_route('2001:db8::4/128', ['2001:db8::2', '2001:db8::3'])
    R['R2'].add_route('2001:db8::4/128', [('eth3', '2001:db8:24::1')])
    R['R3'].add_route('2001:db8::4/128', [('eth2', '2001:db8:34::1')])
    net.add_demand('v4', 'R1', '10.0.0.4', 100e6)
    net.add_demand('v6', 'R1', '2001:db8::4', 100e6)
    net.converge()
    rep = net.placement
    assert rep.demands['v4'].delivered == pytest.approx(100e6) and rep.demands[
        'v6'
    ].delivered == pytest.approx(100e6)
    fib6 = R['R1'].fib(IPV6)
    g = fib6.group(fib6.lookup(to_int('2001:db8::4')[0]))
    assert [a.interface for a in g.adjacencies] == ['Po1', 'eth3']
    e = net.links['R1:eth3--R3:eth1'].edge('R1')
    assert rep.carried[e] == pytest.approx(50e6 * 1034 / 1000 + 50e6 * 1054 / 1000)
    assert R['R1'].fib(IPV4).lookup(A('10.0.0.4')) is not None
