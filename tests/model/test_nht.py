"""Gate C2: prospective resolution, scoped registrations and epoch separation."""

from dataclasses import replace

import pytest

import netsim
from netsim.model import nht
from netsim.model.addressing import IPV4, IPV6, prefix_to_int
from netsim.model.contracts import IGP, LOCAL, STATIC, ClientId, ClientProfile, NhtKey
from netsim.model.derive import DeviceContext, derive_fib
from netsim.model.routing import Nexthop, ResolutionPolicy, Route
from netsim.model.state import PMap, StateDelta, validate_immutable
from netsim.runtime import Simulation
from tests.model.test_network import A, build_diamond


@pytest.fixture
def diamond():
    net, routers = build_diamond()
    net.converge()
    return net, routers['R1']


def key(address='10.0.0.2', **options):
    return NhtKey(STATIC, IPV6 if ':' in address else IPV4, A(address), **options)


def query(net, device, k, **options):
    dev = net.state.devices[device]
    return nht.resolve(
        DeviceContext(net.state, device),
        dev.config.resolution_policy or ResolutionPolicy(),
        k,
        input_epoch=dev.resolver_input_epoch.get(k.af, 0),
        **options,
    )


def route(prefix, source=IGP, metric=10, nh=None, distance=110):
    address, length, af = prefix_to_int(prefix)
    return Route(
        (address, length),
        af,
        source,
        distance,
        (nh or Nexthop.via('Po1', A('10.1.12.1'), IPV4),),
        metric,
    )


@pytest.mark.parametrize('address', ['10.0.0.2', '2001:db8:99::2'])
def test_ipv4_ipv6_and_pure_query(diamond, address):
    net, dev = diamond
    k = key(address)
    if k.af == IPV6:
        dev['Po1'].configure(forwarding_v6=True)
        dev.add_route(address + '/128', [('Po1', '10.1.12.1')])
        net.converge()
    before = net.state
    result = query(net, dev.name, k)
    assert result.eligible and result.legs[0].interface == 'Po1'
    assert result.queries[0] == (k.af, k.address, True)
    assert result.input_epoch == dev.node.resolver_input_epoch[k.af]
    assert net.state is before
    validate_immutable(result)


def test_local_receive_and_connected_only_controls(diamond):
    net, dev = diamond
    result = query(net, dev.name, key('10.0.0.1', connected_only=True))
    assert result.eligible and result.via_source == LOCAL and result.legs == ()
    # Explicit interface next hops are accepted, even on static routes.
    assert query(net, dev.name, key(connected_only=True)).eligible
    assert not query(net, dev.name, key('10.0.0.4', connected_only=True)).eligible
    assert query(net, dev.name, key('10.0.0.4')).eligible
    assert query(net, dev.name, key('10.1.12.1', connected_only=True)).eligible


@pytest.mark.parametrize('device_default', [False, True])
def test_default_permission_belongs_to_key(diamond, device_default):
    net, dev = diamond
    dev.configure(
        resolution_policy=ResolutionPolicy(resolve_via_default=device_default)
    )
    dev.add_route('0.0.0.0/0', [('Po1', '10.1.12.1')])
    assert not query(net, dev.name, key('203.0.113.99')).eligible
    result = query(net, dev.name, key('203.0.113.99', resolve_via_default=True))
    assert result.eligible and result.via_prefix == (0, 0)


def test_self_covering_candidate_and_same_prefix_overlay(diamond):
    net, dev = diamond
    candidate = dev.add_route('192.0.2.0/24', ['192.0.2.1'])
    k = key('192.0.2.1')
    result = query(net, dev.name, k, exclude_rows=frozenset({candidate.key}))
    assert not result.eligible and result.reason == 'SELF_COVERED'
    assert result.via_prefix == candidate.prefix
    assert result.queries == ((IPV4, k.address, False),)
    alternative = route('192.0.2.0/24', metric=7)
    dev.rib_client(IGP).add_routes([alternative])
    result = query(net, dev.name, k, exclude_rows=frozenset({candidate.key}))
    assert result.eligible and result.via_source == IGP and result.cost == 7
    assert result.cost_source == IGP


@pytest.mark.parametrize('af', [IPV4, IPV6])
def test_mutual_recursion_and_failed_queries(diamond, af):
    net, dev = diamond
    a, b = (
        ('192.0.2.1', '192.0.2.2')
        if af == IPV4
        else ('2001:db8:99::1', '2001:db8:99::2')
    )
    length = 32 if af == IPV4 else 128
    dev.add_route(f'{a}/{length}', [b])
    dev.add_route(f'{b}/{length}', [a])
    result = query(net, dev.name, key(a))
    assert not result.eligible and result.reason == 'LOOP_DETECTED'
    assert {q[:2] for q in result.queries} == {(af, A(a)), (af, A(b))}
    assert len(result.queries) == 3


def test_link_local_scope_and_interface_generation(diamond):
    net, dev = diamond
    for interface in ('Po1', 'eth3'):
        dev[interface].configure(forwarding_v6=True)
    net.converge()
    address = A('fe80::a')

    def neighbors(state):
        node = state.devices[dev.name]
        nt = node.neighbors
        nt = replace(
            nt,
            entries=nt.entries.set(('Po1', address), 100).set(('eth3', address), 200),
        )
        return replace(
            state, devices=state.devices.set(dev.name, replace(node, neighbors=nt))
        )

    net.update(neighbors)
    client = dev.nht_client()
    first = key('fe80::a', interface='Po1', interface_generation=dev['Po1'].generation)
    second = key(
        'fe80::a', interface='eth3', interface_generation=dev['eth3'].generation
    )
    client.register(first)
    client.register(second)
    assert len(dev.node.nht.registrations) == 2
    assert client.result(first).legs[0].mac == 100
    assert client.result(second).legs[0].mac == 200
    assert client.result(first).legs[0].interface == 'Po1'
    with pytest.raises(ValueError, match='scope'):
        client.register(key('fe80::a'))
    with pytest.raises(ValueError, match='stale'):
        client.register(replace(first, interface_generation=9999))
    stale = query(net, dev.name, replace(first, interface_generation=9999))
    assert stale.reason == 'SCOPE_STALE' and not stale.eligible


def test_best_same_prefix_source_withdrawal_and_cost(diamond):
    net, dev = diamond
    backup = ClientId('ls-backup')
    net.register_client(ClientProfile(backup, 120, link_state=True))
    a, b = (
        route('192.0.2.0/24', metric=7),
        route('192.0.2.0/24', backup, 19, distance=120),
    )
    dev.rib_client(IGP).add_routes([a])
    with net.batch():
        dev.rib_client(backup).add_routes([b])
    client, k = dev.nht_client(), key('192.0.2.1')
    client.register(k)
    assert client.result(k).via_source == IGP and client.result(k).cost == 7
    dev.rib_client(IGP).delete_routes([a.key])
    result = client.result(k)
    assert (
        result.via_source == backup
        and result.cost == 19
        and result.cost_source == backup
    )
    assert result.eligible


def test_metric_provenance_never_sums_different_sources(diamond):
    net, dev = diamond
    a = route('192.0.2.1/32', nh=Nexthop.recursive(A('10.0.0.2'), IPV4), metric=99)
    dev.rib_client(IGP).add_routes([a])
    result = query(net, dev.name, key('192.0.2.1'))
    assert result.eligible and result.reason == 'COST_UNAVAILABLE'
    assert result.cost is result.cost_source is None
    assert result.queries == ((IPV4, A('192.0.2.1'), True), (IPV4, A('10.0.0.2'), True))
    b = route('192.0.2.2/32', metric=5)
    a = replace(a, nexthops=(Nexthop.recursive(A('192.0.2.2'), IPV4),))
    dev.rib_client(IGP).add_routes([a, b])
    result = query(net, dev.name, key('192.0.2.1'))
    assert result.cost == 99 and result.cost_source == IGP


def test_failed_lookup_then_more_specific_insertion(diamond):
    net, dev = diamond
    k, client = key('192.0.2.1'), dev.nht_client()
    client.register(k)
    before = client.result(k)
    assert not before.eligible and before.queries == ((IPV4, k.address, False),)
    dev.add_route('192.0.2.0/24', [('Po1', '10.1.12.1')])
    after = client.result(k)
    assert after is not before and after.eligible
    assert after.via_prefix == (A('192.0.2.0'), 24)


def test_cost_only_change_keeps_identical_fib_but_new_result(diamond):
    net, dev = diamond
    row = route('192.0.2.1/32', metric=7)
    dev.rib_client(IGP).add_routes([row])
    net.converge()
    k, client = key('192.0.2.1'), dev.nht_client()
    client.register(k)
    before, fib = client.result(k), dev.fib(IPV4)
    dev.rib_client(IGP).add_routes([replace(row, metric=21)])
    net.converge()
    assert dev.fib(IPV4) is fib
    assert client.result(k) is not before
    assert client.result(k).cost == 21
    root, result = net.state, client.result(k)
    assert nht.refresh(root, dev.name) is root
    assert derive_fib(root) is root
    assert client.result(k) is result


def test_delayed_fib_rib_and_installed_are_separate(diamond):
    net, dev = diamond
    sim = Simulation(netsim.Environment(), net)
    dev.configure(fib_delay=2)
    sim.run_until(3)
    client, k = dev.nht_client(), key('192.0.2.1')
    client.register(k)
    assert client.installed(k).status == 'INSTALLED'
    old = client.installed(k)
    sim.at(10, lambda: dev.rib_client(IGP).add_routes([route('192.0.2.1/32')]))
    sim.run_until(10)
    result, installed = client.result(k), client.installed(k)
    assert result.eligible and result.input_epoch == dev.node.resolver_input_epoch[IPV4]
    assert installed.status == 'PENDING' and installed.prefix is None
    assert installed.processed_epoch < result.input_epoch
    assert installed.fib_version == old.fib_version
    sim.run_until(12.1)
    installed = client.installed(k)
    assert installed.status == 'INSTALLED' and installed.prefix == (k.address, 32)
    assert installed.processed_epoch == result.input_epoch
    assert installed.adjacencies == result.legs


def test_registration_never_makes_unrelated_routes_pending(diamond):
    net, dev = diamond
    row = next(iter(dev.rib(IPV4).rows_of(STATIC)))
    before = net.state
    epoch = dev.node.resolver_input_epoch
    client, k = dev.nht_client(), key()
    client.register(k)
    assert dev.node.resolver_input_epoch is epoch
    assert dev.route_status(IPV4, row.key)[0] == 'INSTALLED'
    registered = net.state
    assert nht.register(registered, dev.name, k) is registered
    client.register(k)
    assert net.state is registered
    client.unregister(k)
    assert dev.node.resolver_input_epoch is epoch
    assert dev.route_status(IPV4, row.key)[0] == 'INSTALLED'
    assert client.result(k) is None
    assert nht.unregister(net.state, dev.name, k) is net.state
    assert before.devices[dev.name].fibs is dev.node.fibs


def test_withdrawal_recovery_and_purge_owner(diamond):
    net, dev = diamond
    client, k = dev.nht_client(), key()
    client.register(k)
    row = dev.rib(IPV4).rows((k.address, 32))[0]
    dev.rib_client().delete_routes([row.key])
    assert not client.result(k).eligible
    dev.rib_client().add_routes([row])
    assert client.result(k).eligible
    other = replace(k, owner=IGP)
    dev.nht_client(IGP).register(other)
    root = nht.remove_client(net.state, dev.name, STATIC)
    assert k not in root.devices[dev.name].nht.registrations
    assert other in root.devices[dev.name].nht.registrations
    assert nht.remove_client(root, dev.name, STATIC) is root
    assert nht.remove_client(root, 'absent', STATIC) is root


def test_fib_refreshes_pure_registrations_without_epoch_bump(diamond):
    net, dev = diamond
    k = key()
    root = nht.register(net.state, dev.name, k)
    assert root.devices[dev.name].nht.registrations[k] is None
    refreshed = derive_fib(root)
    result = refreshed.devices[dev.name].nht.registrations[k]
    assert result.eligible
    assert (
        refreshed.devices[dev.name].resolver_input_epoch
        is dev.node.resolver_input_epoch
    )
    assert refreshed.devices[dev.name].fibs is dev.node.fibs


@pytest.mark.parametrize(
    'kind', ['insert', 'metric', 'remove', 'interface', 'neighbor', 'peer']
)
def test_dependency_changes_are_scoped(diamond, kind):
    net, dev = diamond
    client, k = dev.nht_client(), key('192.0.2.1')
    if kind != 'insert':
        dev.rib_client(IGP).add_routes([route('192.0.2.1/32')])
        net.converge()
    client.register(k)
    before = net.state
    if kind in ('insert', 'metric'):
        dev.rib_client(IGP).add_routes([route('192.0.2.1/32', metric=77)])
    elif kind == 'remove':
        dev.rib_client(IGP).sync([])
    elif kind == 'interface':
        dev['Po1'].admin_down()
    else:
        node = dev.node
        nt = node.neighbors
        if kind == 'neighbor':
            nt = replace(nt, entries=nt.entries.set(('Po1', A('10.1.12.1')), 999))
        else:
            nt = replace(nt, peers=nt.peers.set('Po1', 999))
        net.update(
            lambda state: replace(
                state, devices=state.devices.set(dev.name, replace(node, neighbors=nt))
            )
        )
    assert nht.affected_by(StateDelta(before, net.state), before, dev.name, IPV4)
    assert not nht.affected_by(StateDelta(before, net.state), before, dev.name, IPV6)


def test_unrelated_and_failed_recursive_dependency_ranges(diamond):
    net, dev = diamond
    client, k = dev.nht_client(), key('192.0.2.1')
    dev.add_route('192.0.2.1/32', ['2001:db8:99::1'])
    client.register(k)
    assert (IPV6, A('2001:db8:99::1'), False) in client.result(k).queries
    before = net.state
    dev.add_route('203.0.113.0/24', [('eth3', '10.1.13.1')])
    dev['eth3'].configure(metric=77)
    assert not nht.affected_by(StateDelta(before, net.state), before, dev.name, IPV4)
    before = net.state
    dev.add_route('2001:db8:99::/64', [('Po1', '10.1.12.1')])
    assert nht.affected_by(StateDelta(before, net.state), before, dev.name, IPV4)


def test_client_ownership_invalid_addresses_and_drop_routes(diamond):
    net, dev = diamond
    with pytest.raises(ValueError, match='belong'):
        dev.nht_client().register(replace(key(), owner=IGP))
    with pytest.raises(ValueError, match='family'):
        dev.nht_client().register(replace(key(), af=0))
    with pytest.raises(ValueError, match='generation'):
        dev.nht_client().register(replace(key(), interface_generation=1))
    dev.add_route('192.0.2.0/24', [Nexthop.blackhole()])
    assert not query(net, dev.name, key('192.0.2.1')).eligible


def test_rejected_best_row_does_not_contaminate_cost(diamond):
    net, dev = diamond
    bad = dev.add_route('192.0.2.1/32', ['203.0.113.1'])
    good = route('192.0.2.1/32', metric=31)
    dev.rib_client(IGP).add_routes([good])
    result = query(net, dev.name, key('192.0.2.1'))
    assert result.eligible and result.cost == 31 and result.cost_source == IGP
    assert (IPV4, A('203.0.113.1'), False) in result.queries
    assert result.via_prefix == bad.prefix


def test_failed_interface_dependency_and_recovery(diamond):
    net, dev = diamond
    dev['Po1'].admin_down()
    net.converge()
    client, k = dev.nht_client(), key()
    client.register(k)
    before = net.state
    result = client.result(k)
    assert not result.eligible and 'Po1' in result.interfaces
    dev['Po1'].admin_up()
    assert nht.affected_by(StateDelta(before, net.state), before, dev.name, IPV4)
    net.converge()
    assert client.result(k).eligible


def test_registration_scopes_survive_removal_without_rebinding(diamond):
    net, dev = diamond
    client = dev.nht_client()
    k = key(interface='Po1', interface_generation=dev['Po1'].generation)
    client.register(k)
    dev.remove_interface('Po1')
    assert client.result(k).reason == 'SCOPE_STALE'
    assert client.installed(k).adjacencies == ()
    dev.add_portchannel('Po1')
    assert dev['Po1'].generation != k.interface_generation
    assert client.result(k).reason == 'SCOPE_STALE'


def test_scoped_installed_answer_and_unregistered_client(diamond):
    net, dev = diamond
    k = key(interface='Po1', interface_generation=dev['Po1'].generation)
    installed = dev.nht_client().installed(k)
    assert installed.adjacencies[0].interface == 'Po1'
    assert dev.nht_client().resolve(k).eligible
    with pytest.raises(ValueError, match='unregistered'):
        dev.nht_client(ClientId('absent'))
    assert not nht.affected_by(
        StateDelta(net.state, net.state), net.state, 'absent', IPV4
    )


def test_bounded_prefix_probes_on_large_rib(diamond):
    from collections.abc import Mapping

    from netsim.model.lpm import FrozenPrefixTable
    from netsim.model.routing import RibState, rib_apply

    net, dev = diamond
    base = A('198.18.0.0')
    rows = tuple(route(f'198.18.{i >> 8}.{i & 255}/32') for i in range(10000))
    rib = rib_apply(RibState.empty(IPV4), add=rows)

    class Probes(Mapping):
        def __init__(self, data):
            self.data = data
            self.probes = 0

        def __getitem__(self, key):
            self.probes += 1
            return self.data[key]

        def __iter__(self):
            raise AssertionError('query scanned the RIB')

        def __len__(self):
            return len(self.data)

        def __contains__(self, key):
            self.probes += 1
            return key in self.data

    tables = {length: Probes(table) for length, table in rib.prefixes._tables.items()}
    indexed = FrozenPrefixTable._owned(32, tables, rib.prefixes._masks)
    rib = replace(rib, prefixes=indexed)
    state = replace(
        net.state,
        devices=net.state.devices.set(
            dev.name, replace(dev.node, ribs=dev.node.ribs.set(IPV4, rib))
        ),
    )
    ctx = DeviceContext(state, dev.name)
    for offset in (0, 9999, 10000):
        result = nht.resolve(
            ctx, ResolutionPolicy(), NhtKey(STATIC, IPV4, base + offset), input_epoch=1
        )
        assert result.eligible == (offset < 10000)
    assert sum(table.probes for table in tables.values()) <= 7


def benchmark_refresh():
    """Reproduce with venv/bin/python -m tests.model.test_nht --benchmark.

    1,000 registrations; 1k/10k/100k /32 rows; same resolved interface,
    direct IGP next hops, no agents/demands/timers or policy consumers. Times
    exclude construction, use seven repetitions after a warm-up, and refresh
    both first answers and canonical unchanged answers.
    """
    import gc
    import statistics
    import time

    from netsim.model.contracts import NhtTable
    from netsim.model.routing import RibState, rib_apply

    net, routers = build_diamond()
    net.converge()
    dev = routers['R1'].node
    base = A('198.18.0.0')
    registrations = PMap((NhtKey(STATIC, IPV4, base + i), None) for i in range(1000))
    for count in (1000, 10000, 100000):
        rows = tuple(
            Route(
                (base + i, 32),
                IPV4,
                IGP,
                110,
                (Nexthop.via('Po1', A('10.1.12.1'), IPV4),),
                metric=10,
            )
            for i in range(count)
        )
        rib = rib_apply(RibState.empty(IPV4), add=rows)
        initial = replace(
            net.state,
            devices=net.state.devices.set(
                'R1',
                replace(dev, ribs=dev.ribs.set(IPV4, rib), nht=NhtTable(registrations)),
            ),
        )
        warm = nht.refresh(initial, 'R1', IPV4)
        for label, root in (('initial', initial), ('unchanged', warm)):
            times = []
            gc.collect()
            gc.disable()
            try:
                for _ in range(7):
                    start = time.perf_counter()
                    result = nht.refresh(root, 'R1', IPV4)
                    times.append(time.perf_counter() - start)
                    assert len(result.devices['R1'].nht.registrations) == 1000
            finally:
                gc.enable()
            elapsed = statistics.median(times)
            print(
                f'{count:6d} rows {label:9s}: median {elapsed * 1000:.3f} ms; '
                f'{elapsed * 1e6 / 1000:.3f} us/registration; '
                f'range {min(times) * 1000:.3f}..{max(times) * 1000:.3f} ms'
            )


def test_unrelated_input_epoch_preserves_semantic_result_identity(diamond):
    net, dev = diamond
    client, k = dev.nht_client(), key()
    client.register(k)
    result = client.result(k)
    dev.add_route('203.0.113.0/24', [('eth3', '10.1.13.1')])
    assert client.result(k) is result
    assert dev.node.nht.input_epochs[k.af] == dev.node.resolver_input_epoch[k.af]
    assert result.input_epoch < dev.node.nht.input_epochs[k.af]
    # A direct prospective question always returns the requested current epoch.
    current = client.resolve(k)
    assert current.input_epoch == dev.node.nht.input_epochs[k.af]
    assert replace(current, input_epoch=result.input_epoch) == result
    net.converge()
    assert client.result(k) is result


def test_excluded_recursive_dependency_is_not_a_self_covering_candidate(diamond):
    net, dev = diamond
    child = route('192.0.2.2/32')
    parent = route('192.0.2.1/32', nh=Nexthop.recursive(A('192.0.2.2'), IPV4))
    dev.rib_client(IGP).add_routes([parent, child])
    result = query(net, dev.name, key('192.0.2.1'), exclude_rows=frozenset({child.key}))
    assert not result.eligible and result.reason == 'UNRESOLVED'
    assert (IPV4, A('192.0.2.2'), False) in result.queries


if __name__ == '__main__':
    import sys

    if sys.argv[1:] == ['--benchmark']:
        benchmark_refresh()
