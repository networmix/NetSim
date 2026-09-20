import dataclasses
import heapq
from collections import Counter

import pytest

from netsim.model import forwarding as fw
from netsim.model import igp
from netsim.model.addressing import IPV4, IPV6, to_int
from netsim.model.contracts import IGP, IGP_PROFILE
from netsim.model.igp import oracle_igp, shortest_path_routes
from netsim.model.network import Network
from netsim.model.routing import Nexthop, RibState, Route, rib_apply
from netsim.model.state import NetworkState
from tests.model.clos import build_clos


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


# Pre-reuse implementation: intentionally rebuild for every source and family.
def reference_shortest_path_routes(
    state: NetworkState, device: str, af: int
) -> tuple[Route, ...]:
    """ECMP shortest-path rows from *device* to every originated prefix."""
    if not state.devices[device].config.enabled:
        return ()
    graph: dict[str, list[tuple[str, str, int, int | None, int | None]]] = {}
    for name, dev in state.devices.items():
        if dev.config.enabled:
            graph[name] = igp._egresses(state, name, af)
    dist: dict[str, int] = {device: 0}
    first_hops: dict[str, set[tuple[str, int | None, int | None]]] = {device: set()}
    heap = [(0, device)]
    done: set[str] = set()
    while heap:
        d, u = heapq.heappop(heap)
        if u in done:
            continue
        done.add(u)
        for iface, v, metric, peer_addr, peer_af in graph.get(u, ()):
            nd = d + metric
            hops = {(iface, peer_addr, peer_af)} if u == device else first_hops[u]
            if v not in dist or nd < dist[v]:
                dist[v] = nd
                first_hops[v] = set(hops)
                heapq.heappush(heap, (nd, v))
            elif nd == dist[v]:
                first_hops[v] |= hops
    rows: list[Route] = []
    local = set(igp._destinations(state, device, af))
    for target in sorted(dist):
        if target == device:
            continue
        hops = sorted(first_hops[target], key=lambda h: (h[0], h[1] or -1))
        if not hops:
            continue
        nexthops = tuple(Nexthop.via(iface, addr, paf) for iface, addr, paf in hops)
        for prefix in igp._destinations(state, target, af):
            if prefix in local:
                continue
            rows.append(
                Route(
                    prefix,
                    af,
                    IGP,
                    IGP_PROFILE.distance,
                    nexthops,
                    metric=dist[target],
                    distinguisher=(target,),
                )
            )
    # Several targets may originate the same prefix (anycast): keep distinct rows by distinguisher.
    return tuple(rows)


def reference_oracle(state: NetworkState, now: float) -> NetworkState:
    """RouteSource: sync ``igp`` rows on every device for both families."""
    devices = state.devices.builder()
    for name, dev in state.devices.sorted_items():
        new_dev = dev
        ribs = dev.ribs.builder()
        for af in (IPV4, IPV6):
            rib = dev.ribs.get(af) or RibState.empty(af)
            rows = reference_shortest_path_routes(state, name, af)
            new_rib = rib_apply(rib, sync=(IGP, rows))
            if new_rib is not rib:
                ribs.set(af, new_rib)
        built = ribs.build()
        if built is not dev.ribs:
            new_dev = dataclasses.replace(dev, ribs=built)
            devices.set(name, new_dev)
    new_devices = devices.build()
    return (
        state
        if new_devices is state.devices
        else dataclasses.replace(state, devices=new_devices)
    )


def build_ring(n=64):
    """The scale fixture's unnumbered ring with square-root chords."""
    net = Network()
    devices = [net.add_device(f'r{i:05}') for i in range(n)]
    for i, dev in enumerate(devices):
        dev.add_loopback('lo', ipv4=[f'10.0.0.{i}/32'])
    pairs = sorted(
        {
            tuple(sorted((i, (i + step) % n)))
            for i in range(n)
            for step in (1, max(2, int(n**0.5)))
        }
    )
    for a, b in pairs:
        net.add_p2p(
            devices[a], f'e{b}', devices[b], f'e{a}', unnumbered=True, speed=10e9
        )
    net.add_source(oracle_igp)
    net.add_demand('across', devices[0].name, f'10.0.0.{n - 1}', 1e6)
    return net


def oracle_fingerprint(state):
    return sorted(
        (
            name,
            af,
            row.prefix,
            tuple((nh.interface, nh.address, nh.af) for nh in row.nexthops),
            row.metric,
            row.distinguisher,
        )
        for name, dev in state.devices.sorted_items()
        for af, rib in dev.ribs.sorted_items()
        for row in rib.rows_of(IGP)
    )


def assert_same_routes(actual, expected):
    assert oracle_fingerprint(actual) == oracle_fingerprint(expected)
    for name, dev in actual.devices.items():
        other = expected.devices[name]
        assert dev.ribs == other.ribs
        for af in (IPV4, IPV6):
            assert shortest_path_routes(actual, name, af) == (
                reference_shortest_path_routes(actual, name, af)
            )


class TestOracleReuse:
    @pytest.mark.parametrize('unnumbered', [False, True])
    def test_builds_each_family_once(self, mocker, unnumbered):
        net, _ = build(unnumbered)
        net.converge()
        egresses = mocker.spy(igp, '_egresses')
        destinations = mocker.spy(igp, '_destinations')
        assert oracle_igp(net.state, 0) is net.state
        expected = Counter((name, af) for name in net.devices for af in (IPV4, IPV6))
        assert Counter(c.args[1:] for c in egresses.call_args_list) == expected
        assert Counter(c.args[1:] for c in destinations.call_args_list) == expected

    def test_empty_family_does_not_build_egresses(self, mocker):
        net = build_clos(8, 4)
        net.converge()
        egresses = mocker.spy(igp, '_egresses')
        destinations = mocker.spy(igp, '_destinations')
        assert oracle_igp(net.state, 0) is net.state
        assert Counter(c.args[1:] for c in egresses.call_args_list) == Counter(
            (name, IPV4) for name in net.devices
        )
        assert Counter(c.args[1:] for c in destinations.call_args_list) == Counter(
            (name, af) for name in net.devices for af in (IPV4, IPV6)
        )

    @pytest.mark.parametrize('topology', ['diamond', 'unnumbered', 'clos', 'ring'])
    def test_reference_equivalence(self, topology):
        if topology in ('diamond', 'unnumbered'):
            net, _ = build(unnumbered=topology == 'unnumbered')
            net.add_demand('across', 'R1', '10.0.0.4', 1e6)
        else:
            net = build_clos(8, 4) if topology == 'clos' else build_ring()
        reference = net.fork()
        reference.sources = [reference_oracle]
        for failed in (False, True):
            if failed:
                link = sorted(net.links)[0]
                net.links[link].fail()
                reference.links[link].fail()
            net.converge()
            reference.converge()
            assert_same_routes(net.state, reference.state)
            for name, dev in net.state.devices.items():
                assert dev.fibs == reference.state.devices[name].fibs
            assert net.placement == reference.placement

    @pytest.mark.parametrize('af, field', [(IPV4, 'ipv4'), (IPV6, 'ipv6')])
    def test_last_destination_withdraws_only_igp(self, mocker, af, field):
        net, devices = build(unnumbered=True)
        prefix = '192.0.2.0/24' if af == IPV4 else '2001:db8:ffff::/64'
        devices['R1'].add_route(prefix, [Nexthop.blackhole()])
        net.converge()
        assert devices['R1'].rib(af).rows_of(IGP)
        for dev in devices.values():
            dev['lo0'].configure(**{field: []})
        before = net.state
        egresses = mocker.spy(igp, '_egresses')
        after = oracle_igp(before, 0)
        assert all(call.args[2] != af for call in egresses.call_args_list)
        assert_same_routes(after, reference_oracle(before, 0))
        other_af = IPV6 if af == IPV4 else IPV4
        for name, dev in after.devices.items():
            assert not dev.ribs[af].rows_of(IGP)
            assert dev.ribs[other_af] is before.devices[name].ribs[other_af]
            for shard in before.devices[name].ribs[af].shards.values():
                for row in shard.values():
                    if row.source != IGP:
                        assert dev.ribs[af].shards[row.prefix[1]][row.key] is row
        assert any(
            row.nexthops == (Nexthop.blackhole(),)
            for shard in after.devices['R1'].ribs[af].shards.values()
            for row in shard.values()
        )
        assert oracle_igp(after, 0) is after

    def test_addressless_transit_and_source(self):
        net = Network()
        a, transit, b = [net.add_device(n) for n in ('a', 'transit', 'b')]
        for dev, i in ((a, 1), (b, 2)):
            dev.add_loopback('lo', ipv4=[f'10.0.0.{i}/32'], ipv6=[f'2001:db8::{i}/128'])
        net.add_p2p(a, 'to-t', transit, 'to-a', unnumbered=True)
        net.add_p2p(transit, 'to-b', b, 'to-t', unnumbered=True)
        net.add_source(oracle_igp)
        net.converge()
        for af, addr, plen in ((IPV4, '10.0.0.2', 32), (IPV6, '2001:db8::2', 128)):
            rows = a.rib(af).rows((A(addr), plen))
            assert len(rows) == 1 and rows[0].metric == 2
            assert rows[0].nexthops == (Nexthop.via('to-t'),)
            assert len(transit.rib(af).rows_of(IGP)) == 2
        assert_same_routes(net.state, reference_oracle(net.state, 0))

    def test_unchanged_rows_and_devices_keep_identity_and_epochs(self):
        net, devices = build(unnumbered=True)
        net.converge()
        before = net.state
        assert oracle_igp(before, 1) is before
        stable = devices['R1'].rib(IPV4).rows((A('10.0.0.2'), 32))[0]
        devices['R1']['eth3'].configure(metric=5)
        net.converge()
        assert devices['R1'].rib(IPV4) is not before.devices['R1'].ribs[IPV4]
        assert devices['R1'].rib(IPV4).rows(stable.prefix)[0] is stable
        unchanged = net.state.devices['R4']
        assert unchanged.ribs is before.devices['R4'].ribs
        assert (
            unchanged.resolver_input_epoch is before.devices['R4'].resolver_input_epoch
        )

    def test_anycast_metrics_disconnected_and_disabled(self):
        net, devices = build()
        devices['R2'].add_loopback('anycast', ipv4=['192.0.2.1/32'])
        devices['R4'].add_loopback('anycast', ipv4=['192.0.2.1/32'])
        devices['R1']['eth3'].configure(metric=3)
        net.add_device('isolated').add_loopback('lo', ipv4=['192.0.2.2/32'])
        # Exercise numbered IPv6 peer addresses as well as the IPv4 diamond.
        devices['R1']['eth3'].configure(ipv6=['2001:db8:13::/127'])
        devices['R3']['eth1'].configure(ipv6=['2001:db8:13::1/127'])
        net.converge()
        rows = devices['R1'].rib(IPV4).rows((A('192.0.2.1'), 32))
        assert {r.distinguisher for r in rows} == {('R2',), ('R4',)}
        assert_same_routes(net.state, reference_oracle(net.state, 0))
        devices['R2'].configure(enabled=False)
        # The pure public function also preserves behavior before oper is rederived.
        assert_same_routes(oracle_igp(net.state, 0), reference_oracle(net.state, 0))
        net.converge()
        assert not devices['R2'].rib(IPV4).rows_of(IGP)
        assert_same_routes(net.state, reference_oracle(net.state, 0))

    def test_forks_recompute_from_their_own_tree(self):
        net, _ = build(unnumbered=True)
        net.converge()
        left, right = net.fork(), net.fork()
        left.links['R1:eth1--R2:eth1'].fail()
        right.links['R1:eth3--R3:eth1'].fail()
        for fork in (left, right, left, net):
            fork.converge()
            assert_same_routes(fork.state, reference_oracle(fork.state, 0))
        assert oracle_fingerprint(left.state) != oracle_fingerprint(right.state)

    def test_no_destinations_is_canonical(self, mocker):
        net = Network()
        a, b = net.add_device('a'), net.add_device('b')
        net.add_p2p(a, 'e', b, 'e', unnumbered=True)
        net.converge()
        egresses = mocker.spy(igp, '_egresses')
        assert oracle_igp(net.state, 0) is net.state
        assert not egresses.called
        assert oracle_igp(Network().state, 0).devices == Network().state.devices
