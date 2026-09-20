from contextlib import nullcontext
from dataclasses import replace
from hashlib import sha256
from random import Random

import pytest

from netsim.model import forwarding as fw
from netsim.model import routing as rt
from netsim.model.addressing import IPV4, IPV6, prefix_to_int, to_int
from netsim.model.contracts import CONNECTED, IGP, STATIC, ClientId, ClientProfile
from netsim.model.routing import (
    Nexthop,
    ResolutionPolicy,
    RibState,
    Route,
    resolve_fib,
    rib_apply,
    row_status,
)
from netsim.model.state import PMap, validate_immutable


def P(text):
    net, plen, _ = prefix_to_int(text)
    return (net, plen)


def A(text):
    return to_int(text)[0]


def static(
    prefix, nexthops, *, distance=1, metric=0, source=STATIC, distinguisher=(), af=IPV4
):
    return Route(
        P(prefix),
        af,
        source,
        distance,
        tuple(nexthops),
        metric=metric,
        distinguisher=distinguisher,
    )


def routing_fingerprints(kind):
    """Content digests across initial convergence, one failure and restoration.

    Used by the A/B/A harness too; no versions, object addresses or hashes.
    """
    import netsim
    from netsim.runtime import Simulation
    from netsim.runtime.timeline import RouteEvent
    from tests.model.clos import build_clos
    from tests.model.test_network import build_diamond

    if kind == 'diamond':
        net, _ = build_diamond()
        net.add_demand('d', 'R1', '10.0.0.4', 100e6)
        lid = 'R1:eth1--R2:eth1'
    else:
        net = build_clos(8, 4)
        lid = sorted(net.links)[0]
    sim = Simulation(netsim.Environment(), net)
    fibs, placements = [], []
    for time, op in ((0, None), (1, net.links[lid].fail), (2, net.links[lid].restore)):
        if op is not None:
            sim.at(time, op)
            sim.run_until(time + 0.5)
        fibs.append(
            tuple(
                (name, int(af), fib.entries.items(), fib.groups.sorted_items())
                for name, dev in net.state.devices.sorted_items()
                for af, fib in dev.fibs.sorted_items()
            )
        )
        p = net.placement
        placements.append(
            (
                p.model,
                p.edge_count,
                tuple(p.offered),
                tuple(p.carried),
                tuple(p.dropped),
                tuple(p.capacity),
                p.edge_links.sorted_items(),
                p.demands.sorted_items(),
                p.classes.sorted_items(),
                p.delivered_total,
                p.dropped_by_reason.sorted_items(),
            )
        )
    events = sim.timeline.select(kind=RouteEvent)
    return tuple(
        sha256(repr(value).encode()).hexdigest() for value in (fibs, placements, events)
    )


@pytest.mark.parametrize(
    'kind, expected',
    [
        (
            'diamond',
            (
                'f8472dab530cfa9108d7159e1d4b1e3f8da91df5fbe87bf4a8a81c744f3d71bb',
                '6e77430a87aaf51ebe18e3d711f34321a74c7714c000195b27bc5f3165eb48b8',
                '853afb7d4b4ce1fee1067197b8d141233eae1f46e81efbd4b6819c7e08910a58',
            ),
        ),
        (
            'clos',
            (
                'd561a9ad210cc36feb1492809a0fc48c505a492b504ea862400590f5d0b28baa',
                '010e064a081b74ae0d27330c9016d488a0a50dc27254b79ae3e82328b8a496cb',
                'a07606041a5da63b13b80debd5a18a381b9efccbf6aa6a39202844d38ca70220',
            ),
        ),
    ],
)
def test_routing_matches_pre_index_fingerprints(kind, expected):
    # Captured from c2d658e; includes FIB dependencies, groups/weights,
    # placement arrays/demand results and the complete ordered RouteEvents.
    assert routing_fingerprints(kind) == expected


class _Ctx:
    """Stub resolution context: interfaces with L3 usability, neighbors and peer MACs."""

    def __init__(self, ribs, usable=(), neighbors=None, peers=None):
        self._ribs = ribs
        self._usable = set(usable)
        self._neighbors = neighbors or {}
        self._peers = peers or {}

    def interface_exists(self, name):
        return (
            name in self._usable
            or name in self._peers
            or any(k[0] == name for k in self._neighbors)
        )

    def l3_usable(self, name, af):
        return name in self._usable

    def neighbor_mac(self, interface, address):
        return self._neighbors.get((interface, address))

    def peer_mac(self, interface):
        return self._peers.get(interface)

    def rib(self, af):
        return self._ribs[af]


def _resolve(rib, ctx, policy=None, old=None):
    policy = policy or ResolutionPolicy()
    return resolve_fib(rib, ctx, policy, version=1, processed_epoch=1, old=old)


class TestNexthopAndRoute:
    def test_shapes(self):
        Nexthop.via('eth0')
        Nexthop.via('eth0', A('10.1.13.1'), IPV4)
        Nexthop.recursive(A('10.0.0.2'), IPV4)
        Nexthop.blackhole()
        with pytest.raises(ValueError):
            Nexthop()
        with pytest.raises(ValueError):
            Nexthop(interface='eth0', special=rt.BLACKHOLE)
        with pytest.raises(ValueError):
            Nexthop(address=1)  # missing af
        with pytest.raises(ValueError):
            Nexthop.via('eth0', weight=0)

    def test_route_validation(self):
        with pytest.raises(ValueError):
            Route((A('10.1.13.1'), 31), IPV4, STATIC, 1, (Nexthop.via('e'),))
        with pytest.raises(ValueError):
            Route(P('10.0.0.0/8'), IPV4, STATIC, 1, ())
        r = static('10.0.0.4/32', [Nexthop.via('e')])
        assert r.key == (A('10.0.0.4'), 32, STATIC, ())


@pytest.mark.parametrize('batched', [False, True])
@pytest.mark.parametrize('mixed', [False, True])
@pytest.mark.parametrize('foreign', [ClientId('bgp', 7), ClientId('static', 1)])
def test_client_rejects_foreign_deletions_atomically(batched, mixed, foreign):
    from netsim.model.network import Network

    net = Network()
    net.register_client(ClientProfile(foreign, distance=20))
    device = net.add_device('a')
    static_client = device.rib_client(STATIC)
    other_client = device.rib_client(foreign, distance=20)
    own = static('10.0.0.1/32', [Nexthop.blackhole()])
    other = replace(own, source=foreign)
    static_client.add_routes((own,))
    other_client.add_routes((other,))
    pending = replace(own, prefix=P('10.0.0.2/32'))

    with net.batch() if batched else nullcontext():
        static_client.add_routes((pending,))
        before = device.rib(IPV4)
        committed = net._state
        staged = net._batch_ops
        keys = (own.key, other.key, pending.key) if mixed else (other.key,)
        with pytest.raises(ValueError, match='does not belong'):
            static_client.delete_routes(iter(keys))
        assert net._batch_ops == staged
        assert device.rib(IPV4) == before
        assert net._state is committed
        # Even a missing foreign key is rejected before the valid key is staged.
        with pytest.raises(ValueError, match='does not belong'):
            static_client.delete_routes((own.key, replace(other, prefix=(0, 0)).key))
        assert device.rib(IPV4) == before
        assert net._state is committed

    assert device.rib(IPV4).rows_of(STATIC) == (own, pending)
    assert device.rib(IPV4).rows_of(foreign) == (other,)
    with net.batch() if batched else nullcontext():
        static_client.delete_routes((own.key, pending.key))
    assert device.rib(IPV4).rows_of(STATIC) == ()
    assert device.rib(IPV4).rows_of(foreign) == (other,)


class TestRibApply:
    @pytest.mark.parametrize('count', [1000, 10_000])
    @pytest.mark.parametrize('operation', ['add', 'delete', 'replace'])
    def test_small_updates_do_not_enumerate_the_client(
        self, monkeypatch, count, operation
    ):
        rows = tuple(
            Route((i, 32), IPV4, STATIC, 1, (Nexthop.blackhole(),))
            for i in range(count)
        )
        rib = rib_apply(RibState.empty(IPV4), sync=(STATIC, rows))
        added = replace(rows[0], prefix=(count, 32))
        key = rows[count // 2].key
        route_key = Route.key.fget
        visited = 0

        def counted(row):
            nonlocal visited
            visited += 1
            return route_key(row)

        monkeypatch.setattr(Route, 'key', property(counted))
        if operation == 'add':
            updated = rib_apply(rib, add=(added,))
        elif operation == 'delete':
            updated = rib_apply(rib, delete=(key,))
        else:
            updated = rib_apply(rib, add=(replace(rows[count // 2], metric=7),))
        assert visited <= 4, f'one-row {operation} visited {visited} rows'
        assert updated is not rib
        for old, new in (
            (rib.clients[STATIC], updated.clients[STATIC]),
            (rib.prefixes._tables[32], updated.prefixes._tables[32]),
            (rib.shards[32], updated.shards[32]),
        ):
            assert isinstance(old, PMap) and isinstance(new, PMap)
            assert len(old._shards) == len(new._shards) == 256
            assert (
                sum(a is not b for a, b in zip(old._shards, new._shards, strict=True))
                == 1
            )

    def test_indexed_reads_do_not_scan_or_sort_storage(self, monkeypatch):
        rows = tuple(
            Route((i, 32), IPV4, STATIC, 1, (Nexthop.blackhole(),)) for i in range(1000)
        )
        rib = rib_apply(RibState.empty(IPV4), sync=(STATIC, rows))

        def no_scan(*args, **kwargs):
            pytest.fail('indexed reads must not scan or sort RIB storage')

        with monkeypatch.context() as patch:
            patch.setattr(PMap, 'values', no_scan)
            patch.setattr(PMap, 'sorted_items', no_scan)
            patch.setattr(rt, '_rank', no_scan)
            assert rib.rows((500, 32)) == (rows[500],)
            assert rib.candidates((500, 32)) is rib.rows((500, 32))
            assert list(rib.best_groups((500, 32))) == [(rows[500],)]
        assert rib.rows_of(STATIC) == rows
        assert rib.rows_of(IGP) == ()
        assert rib.rows((2000, 32)) == rib.rows((0, 24)) == ()

    def test_rows_are_ranked_and_reranked_on_replacement(self):
        a = static('10.0.0.4/32', [Nexthop.blackhole()], distance=20)
        b = replace(a, distinguisher=('b',), distance=10, metric=5)
        c = replace(b, source=IGP, metric=10)
        d = replace(b, source=ClientId('static', 1))
        rib = rib_apply(RibState.empty(IPV4), add=(a, c, d, b))
        assert rib.rows(a.prefix) == (b, d, c, a)
        promoted = replace(a, distance=10, metric=1)
        updated = rib_apply(rib, add=(promoted,))
        assert updated.rows(a.prefix) == (promoted, b, d, c)
        demoted = replace(promoted, metric=20)
        updated = rib_apply(updated, sync=(STATIC, (demoted, b)))
        assert updated.rows(a.prefix) == (b, d, c, demoted)
        assert rib.rows(a.prefix) == (b, d, c, a)
        assert updated.rows_of(STATIC) == (demoted, b)

    @pytest.mark.parametrize('af, bits', [(IPV4, 32), (IPV6, 128)])
    def test_indexes_are_atomic_and_keep_other_clients(self, af, bits):
        a = Route((0, bits), af, STATIC, 1, (Nexthop.blackhole(),))
        b = replace(a, distinguisher=('b',))
        c = replace(a, source=IGP, distance=110)
        covering = replace(a, prefix=(0, 0))
        rib = rib_apply(RibState.empty(af), add=(a, b, c, covering))
        updated = rib_apply(rib, sync=(STATIC, (covering,)))
        assert updated.rows(a.prefix) == (c,)
        assert updated.rows_of(STATIC) == (covering,)
        assert updated.clients[IGP] is rib.clients[IGP]
        assert updated.prefixes.lookup(0) == (0, bits, (c,))
        assert len(updated) == 2 and len(updated.prefixes) == 2
        assert updated.prefixes._tables[0] is rib.prefixes._tables[0]
        assert updated.shards[0] is rib.shards[0]
        assert updated.rows(covering.prefix) is rib.rows(covering.prefix)
        removed = rib_apply(updated, delete=(c.key,))
        assert removed.prefixes.lookup(0) == (0, 0, (covering,))
        assert bits not in removed.shards and removed.rows_of(IGP) == ()
        assert IGP not in removed.clients
        assert rib.rows(a.prefix) == (a, b, c) and len(rib) == 4
        validate_immutable((rib.shards, rib.clients))
        validate_immutable((updated.shards, updated.clients))
        validate_immutable((removed.shards, removed.clients))

    def test_batch_order_duplicates_and_noop_identity(self):
        a = static('10.0.0.4/32', [Nexthop.blackhole()])
        b = replace(a, metric=20)
        rib = rib_apply(RibState.empty(IPV4), add=(a,))
        assert rib_apply(rib, sync=(STATIC, (replace(a),))) is rib
        updated = rib_apply(rib, sync=(STATIC, (b,)), delete=(a.key,), add=(a, b))
        assert updated.rows(a.prefix) == updated.rows_of(STATIC) == (b,)
        assert len(updated) == 1 and updated.shards[32][a.key] is b
        with pytest.raises(ValueError, match='sync rows'):
            rib_apply(updated, sync=(STATIC, (a, replace(b, source=IGP))))
        assert updated.rows(a.prefix) == updated.rows_of(STATIC) == (b,)
        assert rib.rows(a.prefix) == (a,)

    def test_mixed_batches_match_flat_reference(self):
        rng = Random(17)
        clients = (STATIC, IGP, ClientId('igp', 1))
        pool = tuple(
            Route(
                (net, plen),
                IPV4,
                client,
                10,
                (Nexthop.blackhole(),),
                distinguisher=(d,),
            )
            for net, plen in ((0, 0), (0, 24), (256, 24), (1, 32), (2, 32))
            for client in clients
            for d in range(3)
        )
        rib = RibState.empty(IPV4)
        expected = {}
        snapshots = []
        for _ in range(80):
            client = rng.choice(clients)
            synced = tuple(
                replace(r, distance=rng.randrange(3), metric=rng.randrange(3))
                for r in rng.sample(pool, 12)
                if r.source == client
            )
            deleted = tuple(r.key for r in rng.sample(pool, 6))
            added = tuple(
                replace(r, distance=rng.randrange(3), metric=rng.randrange(3))
                for r in rng.sample(pool, 10)
            )
            expected = {k: r for k, r in expected.items() if r.source != client}
            expected.update((r.key, r) for r in synced)
            for k in deleted:
                expected.pop(k, None)
            expected.update((r.key, r) for r in added)
            rib = rib_apply(rib, sync=(client, synced), delete=deleted, add=added)
            snapshots.append((rib, expected.copy()))

        # Check every retained snapshot after all later mutations.
        for rib, expected in snapshots:
            assert len(rib) == len(expected)
            assert {k: r for s in rib.shards.values() for k, r in s.items()} == expected
            assert rib.all_prefixes() == sorted({r.prefix for r in expected.values()})
            for prefix in {r.prefix for r in pool}:
                ranked = tuple(
                    sorted(
                        (r for r in expected.values() if r.prefix == prefix),
                        key=lambda r: (
                            r.distance,
                            r.metric,
                            r.source.name,
                            r.source.instance,
                            r.distinguisher,
                        ),
                    )
                )
                assert rib.rows(prefix) == ranked
                assert (prefix in rib.prefixes) == bool(ranked)
                assert tuple(r for g in rib.best_groups(prefix) for r in g) == ranked
                assert all(
                    rib.shards[r.prefix[1]][r.key] is r for r in rib.rows(prefix)
                )
            for client in clients:
                rows = tuple(
                    expected[k]
                    for k in sorted(expected)
                    if expected[k].source == client
                )
                assert rib.rows_of(client) == rows
                assert all(
                    rib.shards[r.prefix[1]][r.key] is r for r in rib.rows_of(client)
                )

    def test_add_delete_sync_and_identity(self):
        rib = RibState.empty(IPV4)
        r1 = static('10.0.0.4/32', [Nexthop.via('e1')])
        rib1 = rib_apply(rib, add=(r1,))
        assert (
            len(rib1) == 1
            and rib1.version == 1
            and rib1.prefixes.get(*P('10.0.0.4/32')) == (r1,)
        )
        assert rib_apply(rib1, add=(r1,)) is rib1  # identical row: no-op
        assert (
            rib_apply(rib1, delete=((1, 32, STATIC, ()),)) is rib1
        )  # missing key: no-op
        r2 = static('10.0.0.4/32', [Nexthop.via('e2')], distinguisher=('b',))
        rib2 = rib_apply(rib1, add=(r2,))
        assert len(rib2) == 2 and rib2.prefixes.get(*P('10.0.0.4/32')) == (r1, r2)
        rib3 = rib_apply(rib2, delete=(r1.key,))
        assert len(rib3) == 1 and rib3.prefixes.get(*P('10.0.0.4/32')) == (r2,)
        rib4 = rib_apply(rib3, delete=(r2.key,))
        assert (
            len(rib4) == 0
            and rib4.prefixes.lookup(A('10.0.0.4')) is None
            and 32 not in rib4.shards
        )

    def test_sync_replaces_only_the_client(self):
        rib = rib_apply(
            RibState.empty(IPV4), add=(static('10.0.0.1/32', [Nexthop.via('e')]),)
        )
        igp1 = Route(P('10.0.0.2/32'), IPV4, IGP, 110, (Nexthop.via('e'),))
        igp2 = Route(P('10.0.0.3/32'), IPV4, IGP, 110, (Nexthop.via('e'),))
        rib = rib_apply(rib, sync=(IGP, (igp1, igp2)))
        assert len(rib) == 3
        rib2 = rib_apply(rib, sync=(IGP, (igp2,)))
        assert [r.prefix for r in rib2.rows_of(IGP)] == [P('10.0.0.3/32')] and len(
            rib2.rows_of(STATIC)
        ) == 1
        assert rib_apply(rib2, sync=(IGP, (igp2,))) is rib2
        with pytest.raises(ValueError):
            rib_apply(rib2, sync=(IGP, (static('1.0.0.0/8', [Nexthop.via('e')]),)))
        with pytest.raises(ValueError):
            rib_apply(rib2, add=(static('2001:db8::/64', [Nexthop.via('e')], af=IPV6),))

    def test_best_groups_ranking(self):
        rib = rib_apply(
            RibState.empty(IPV4),
            add=(
                static('10.0.0.4/32', [Nexthop.via('e1')], distance=5),
                static(
                    '10.0.0.4/32',
                    [Nexthop.via('e2')],
                    distinguisher=('float',),
                    distance=1,
                ),
                Route(P('10.0.0.4/32'), IPV4, IGP, 110, (Nexthop.via('e3'),)),
                static(
                    '10.0.0.4/32',
                    [Nexthop.via('e4')],
                    distinguisher=('two',),
                    distance=1,
                ),
            ),
        )
        groups = list(rib.best_groups(P('10.0.0.4/32')))
        assert [tuple(r.distance for r in g) for g in groups] == [(1, 1), (5,), (110,)]
        assert {nh.interface for r in groups[0] for nh in r.nexthops} == {'e2', 'e4'}


class TestResolver:
    def _diamond_rib(self):
        rib = RibState.empty(IPV4)
        rows = (
            Route(P('10.1.12.0/31'), IPV4, CONNECTED, 0, (Nexthop.via('Po1'),)),
            Route(P('10.1.13.0/31'), IPV4, CONNECTED, 0, (Nexthop.via('eth3'),)),
            static('10.0.0.2/32', [Nexthop.via('Po1', A('10.1.12.1'), IPV4)]),
            static('10.0.0.3/32', [Nexthop.via('eth3', A('10.1.13.1'), IPV4)]),
            static(
                '10.0.0.4/32',
                [
                    Nexthop.recursive(A('10.0.0.2'), IPV4),
                    Nexthop.recursive(A('10.0.0.3'), IPV4),
                ],
            ),
        )
        return rib_apply(rib, add=rows)

    def _ctx(self, rib, usable=('Po1', 'eth3')):
        return _Ctx(
            {IPV4: rib},
            usable=usable,
            neighbors={('Po1', A('10.1.12.1')): 0x22, ('eth3', A('10.1.13.1')): 0x33},
            peers={'Po1': 0x22, 'eth3': 0x33},
        )

    def test_diamond_ecmp_and_dependencies(self):
        rib = self._diamond_rib()
        fib, outcome = _resolve(rib, self._ctx(rib))
        e = fib.lookup(A('10.0.0.4'))
        assert e is not None and e.action == fw.FORWARD
        g = fib.group(e)
        assert [(a.interface, a.nexthop, a.mac, a.weight) for a in g.adjacencies] == [
            ('Po1', A('10.1.12.1'), 0x22, 1),
            ('eth3', A('10.1.13.1'), 0x33, 1),
        ]
        assert (IPV4, *P('10.0.0.2/32')) in e.depends_on.prefixes and (
            IPV4,
            A('10.0.0.2'),
        ) in e.depends_on.lookups
        assert set(e.depends_on.interfaces) >= {'Po1', 'eth3'}
        key = static('10.0.0.4/32', [Nexthop.via('x')]).key
        assert outcome.rows[key].status == rt.INSTALLED
        connected = fib.lookup(A('10.1.13.0'))
        assert (
            connected.action == fw.FORWARD
            and fib.group(connected).adjacencies[0].nexthop is None
        )

    def test_partial_install_when_one_leg_fails(self):
        rib = self._diamond_rib()
        fib, outcome = _resolve(rib, self._ctx(rib, usable=('eth3',)))
        g = fib.group(fib.lookup(A('10.0.0.4')))
        assert [a.interface for a in g.adjacencies] == ['eth3']
        assert fib.lookup(A('10.0.0.2')) is None
        po_key = static('10.0.0.2/32', [Nexthop.via('x')]).key
        assert (
            outcome.rows[po_key].status == rt.NOT_INSTALLED
            and outcome.rows[po_key].reason == rt.UNRESOLVED
        )
        fib2, _ = _resolve(
            rib, self._ctx(rib, usable=('eth3',)), ResolutionPolicy(all_or_nothing=True)
        )
        assert fib2.lookup(A('10.0.0.4')) is None

    def test_floating_static_and_specials(self):
        rib = rib_apply(
            RibState.empty(IPV4),
            add=(
                static('10.0.0.9/32', [Nexthop.via('down')], distance=1),
                static(
                    '10.0.0.9/32',
                    [Nexthop.blackhole()],
                    distance=200,
                    distinguisher=('float',),
                ),
                static('10.0.0.8/32', [Nexthop.blackhole(), Nexthop.via('eth3')]),
                static('10.0.0.7/32', [Nexthop.blackhole(), Nexthop.unreachable()]),
            ),
        )
        ctx = _Ctx({IPV4: rib}, usable=('eth3',), peers={'eth3': 0x33})
        fib, outcome = _resolve(rib, ctx)
        assert (
            fib.lookup(A('10.0.0.9')).action == fw.DROP_BLACKHOLE
        )  # floating static wins after the first fails
        assert (
            fib.lookup(A('10.0.0.8')).action == fw.DROP_BLACKHOLE
        )  # homogeneous special overrides legs
        assert fib.lookup(A('10.0.0.7')) is None
        seven = static('10.0.0.7/32', [Nexthop.via('x')]).key
        assert outcome.rows[seven].reason == rt.AMBIGUOUS_ACTION

    def test_recursion_guard_default_knob_and_fallthrough(self):
        rib = rib_apply(
            RibState.empty(IPV4),
            add=(
                static('10.0.0.5/32', [Nexthop.recursive(A('10.0.0.6'), IPV4)]),
                static('10.0.0.6/32', [Nexthop.recursive(A('10.0.0.5'), IPV4)]),
                static('0.0.0.0/0', [Nexthop.via('eth3', A('10.1.13.1'), IPV4)]),
                static('10.9.0.0/16', [Nexthop.recursive(A('10.0.0.6'), IPV4)]),
                static('10.9.9.0/24', [Nexthop.via('down')]),
            ),
        )
        ctx = _Ctx(
            {IPV4: rib}, usable=('eth3',), neighbors={('eth3', A('10.1.13.1')): 0x33}
        )
        fib, outcome = _resolve(rib, ctx)
        assert (
            fib.entries.get(A('10.0.0.5'), 32) is None
            and fib.entries.get(A('10.0.0.6'), 32) is None
        )  # mutual recursion, no default
        fib2, _ = _resolve(rib, ctx, ResolutionPolicy(resolve_via_default=True))
        assert (
            fib2.lookup(A('10.0.0.5')).action == fw.FORWARD
        )  # resolves via the default when allowed
        # 10.9.9.0/24 is unresolvable (down); a lookup for 10.9.9.9 falls through to /16, itself recursive via
        # 10.0.0.6 which is unresolvable -> nothing; with the default allowed everything resolves.
        assert fib.lookup(A('10.9.9.9')) is not None and fib.lookup(
            A('10.9.9.9')
        ).prefix == P('0.0.0.0/0')

    def test_weights_flatten_exactly_and_groups_intern(self):
        rib = rib_apply(
            RibState.empty(IPV4),
            add=(
                Route(P('10.1.12.0/31'), IPV4, CONNECTED, 0, (Nexthop.via('Po1'),)),
                static('10.0.0.2/32', [Nexthop.via('Po1', A('10.1.12.1'), IPV4)]),
                static(
                    '10.0.0.3/32',
                    [
                        Nexthop.via('eth3', A('10.1.13.1'), IPV4),
                        Nexthop.via('eth4', A('10.1.14.1'), IPV4),
                    ],
                ),
                # Two recursive next-hops: 10.0.0.2 (one leg) and 10.0.0.3 (two legs) -> shares 1/2, 1/4, 1/4
                static(
                    '10.0.0.4/32',
                    [
                        Nexthop.recursive(A('10.0.0.2'), IPV4),
                        Nexthop.recursive(A('10.0.0.3'), IPV4),
                    ],
                ),
                static(
                    '10.0.0.5/32',
                    [
                        Nexthop.recursive(A('10.0.0.2'), IPV4),
                        Nexthop.recursive(A('10.0.0.3'), IPV4),
                    ],
                ),
                static(
                    '10.0.0.6/32',
                    [
                        Nexthop.via('Po1', A('10.1.12.1'), IPV4, weight=3),
                        Nexthop.via('eth3', A('10.1.13.1'), IPV4),
                    ],
                ),
            ),
        )
        ctx = _Ctx(
            {IPV4: rib},
            usable=('Po1', 'eth3', 'eth4'),
            neighbors={
                ('Po1', A('10.1.12.1')): 1,
                ('eth3', A('10.1.13.1')): 2,
                ('eth4', A('10.1.14.1')): 3,
            },
            peers={'Po1': 1},
        )
        fib, _ = _resolve(rib, ctx)
        g4 = fib.group(fib.lookup(A('10.0.0.4')))
        assert [(a.interface, a.weight) for a in g4.adjacencies] == [
            ('Po1', 2),
            ('eth3', 1),
            ('eth4', 1),
        ]
        assert (
            fib.lookup(A('10.0.0.5')).group_id == fib.lookup(A('10.0.0.4')).group_id
        )  # interned
        g6 = fib.group(fib.lookup(A('10.0.0.6')))
        assert [(a.interface, a.weight) for a in g6.adjacencies] == [
            ('Po1', 3),
            ('eth3', 1),
        ]
        # Group ids are assigned in a content-sorted order: rebuilding yields identical ids and the same Fib object.
        fib_again, _ = _resolve(rib, ctx, old=fib)
        assert fib_again is fib

    def test_row_status(self):
        rib = self._diamond_rib()
        fib, outcome = _resolve(rib, self._ctx(rib))
        key = static('10.0.0.4/32', [Nexthop.via('x')]).key
        assert row_status(1, outcome, key) == ('INSTALLED', None)
        assert row_status(2, outcome, key) == (rt.PENDING, None)
        assert row_status(1, None, key) == (rt.PENDING, None)
        assert row_status(1, outcome, (1, 32, ClientId('nobody'), ())) == (
            'NOT_INSTALLED',
            rt.SHADOWED,
        )


def test_unchanged_inputs_are_not_re_resolved(monkeypatch):
    """A converge with no input change must not resolve any FIB again."""
    from netsim.model import derive
    from tests.model.test_network import build_diamond

    net, R = build_diamond()
    net.add_demand('d1', 'R1', '10.0.0.4', 100e6)
    net.converge()
    before = {n: d.fibs for n, d in net.state.devices.items()}
    calls = []
    real = derive.resolve_fib

    def counting(*a, **k):
        calls.append(a[1].dev.name)
        return real(*a, **k)

    monkeypatch.setattr(derive, 'resolve_fib', counting)
    net.converge()
    assert calls == []
    assert all(net.state.devices[n].fibs is f for n, f in before.items())
    # a real change still resolves exactly the devices whose inputs changed
    net.links['R1:eth3--R3:eth1'].fail()
    net.converge()
    assert calls and set(calls) <= {'R1', 'R3'}


class TestCrossFamily:
    """RFC 8950: an IPv4 route may resolve through the IPv6 RIB, so a change
    in either family invalidates both resolvers."""

    @staticmethod
    def build():
        from netsim.model.network import Network

        net = Network()
        a, b = net.add_device('A'), net.add_device('B')
        a.add_loopback('lo', ipv4=['10.0.0.1/32'], ipv6=['2001:db8::1/128'])
        b.add_loopback('lo', ipv4=['10.0.0.2/32'], ipv6=['2001:db8::2/128'])
        net.add_p2p(
            a,
            'e1',
            b,
            'e1',
            ipv4=('10.1.0.0/31', '10.1.0.1/31'),
            ipv6=('2001:db8:1::/127', '2001:db8:1::1/127'),
            speed=1e9,
        )
        a.add_route('2001:db8::9/128', [('e1', '2001:db8:1::1')])
        a.add_route('192.0.2.0/24', ['2001:db8::9'])
        return net, a

    @staticmethod
    def entry(a):
        return a.fib(IPV4).lookup(to_int('192.0.2.1')[0])

    def test_clock_free(self):
        net, a = self.build()
        net.converge()
        e = self.entry(a)
        assert e is not None and e.action == fw.FORWARD
        adj = a.fib(IPV4).group(e).adjacencies[0]
        assert adj.af == IPV6 and adj.nexthop == to_int('2001:db8:1::1')[0]
        assert (IPV6, to_int('2001:db8::9')[0]) in e.depends_on.lookups
        a.add_route('2001:db8::9/128', ['blackhole'])
        net.converge()
        assert self.entry(a) is None

    def test_timed(self):
        import netsim
        from netsim.runtime import Simulation
        from netsim.runtime.timeline import FibEvent

        net, a = self.build()
        env = netsim.Environment()
        sim = Simulation(env, net)
        assert self.entry(a) is not None
        names = [
            e.nexthops
            for e in sim.timeline.select(kind=FibEvent, device='A')
            if e.prefix == '192.0.2.0/24'
        ]
        assert names == [('e1 2001:db8:1::1',)]  # the IPv6 leg prints as IPv6
        sim.at(5, lambda: a.add_route('2001:db8::9/128', ['blackhole']))
        sim.run_until(6)
        assert self.entry(a) is None
        assert sim.timeline.stage_names(5) == ['fib']


def test_dependencies_are_per_prefix_not_cumulative():
    """Independent routes must not inherit each other's dependencies."""
    from netsim.model import derive
    from tests.model.test_network import build_diamond

    net, R = build_diamond()
    net.converge()
    for i in range(300):
        R['R1'].add_route(f'11.0.{i // 256}.{i % 256}/32', ['blackhole'])
    net.converge()
    fib = R['R1'].fib(IPV4)
    sizes = [
        len(e.depends_on.prefixes)
        + len(e.depends_on.lookups)
        + len(e.depends_on.interfaces)
        for _, _, e in fib.entries.items()
    ]
    assert max(sizes) <= 8 and sum(sizes) <= 2 * len(sizes) + 40
    recursive = fib.lookup(to_int('10.0.0.4')[0])
    assert recursive is not None and len(recursive.depends_on.prefixes) >= 2
    assert derive.DeviceContext is not None
