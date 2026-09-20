from contextlib import nullcontext

import pytest

from netsim.model.network import Network
from netsim.model.state import tree_equal


def build(net):
    a = net.add_device('a')
    b = net.add_device('b')
    a.add_loopback('lo', ipv4=['10.0.0.1/32'])
    b.add_loopback('lo', ipv4=['10.0.0.2/32'])
    link = net.add_p2p(a, 'e', b, 'e', unnumbered=True)
    a.add_route('10.0.0.2/32', ['e'])
    a['e'].configure(metric=3)
    link.configure(capacity=1e9)
    link.fail()
    link.restore()
    net.add_demand('d', 'a', '10.0.0.2', 1e6)


def test_batch_one_commit_and_equivalence():
    expected = Network()
    build(expected)
    net = Network()
    calls = []
    net.on_delta.append(lambda *args: calls.append(args))
    old = net.state
    with net.batch() as b:
        assert b is net
        build(b)
        assert calls == []
    assert len(calls) == 1
    assert calls[0][1] == ('batch', 13)
    assert calls[0][2].old is old
    assert calls[0][2].new is net.state
    assert tree_equal(net.state, expected.state)
    for n in (net, expected):
        n.converge()
    assert tree_equal(net.state, expected.state)


@pytest.mark.parametrize('batched', [False, True])
def test_construction_context(batched):
    net = Network()
    with net.batch() if batched else nullcontext():
        build(net)
    assert len(net.devices) == 2


def test_abort_restores_allocators_and_permanently_invalidates_handles():
    from netsim.model.entities import StaleHandleError

    net = Network()
    existing = net.add_device('existing')
    old = net.state
    calls = []
    net.on_delta.append(lambda *args: calls.append(args))
    with pytest.raises(RuntimeError, match='abort'):
        with net.batch():
            provisional = net.add_device('new')
            iface = existing.add_ethernet('eth')
            peer = provisional.add_ethernet('eth')
            link = net.add_link(iface, peer)
            client = provisional.rib_client()
            provisional.add_route('10.0.0.0/24', ['blackhole'])
            existing.configure(enabled=False)
            net.add_demand('d', 'new', '10.0.0.1', 1)
            escaped = net.state
            hashes = [hash(h) for h in (provisional, iface, peer, link)]
            raise RuntimeError('abort')
    assert net.state is old
    assert existing.exists and existing.enabled
    assert net.state.allocators is old.allocators
    assert calls == []
    replacement = net.add_device('new')
    iface2 = existing.add_ethernet('eth')
    peer2 = replacement.add_ethernet('eth')
    link2 = net.add_link(iface2, peer2)
    for i, (handle, new) in enumerate(
        zip(
            (provisional, iface, peer, link),
            (replacement, iface2, peer2, link2),
            strict=True,
        )
    ):
        assert new.generation == handle.generation
        assert not handle.exists
        assert new.exists and handle != new
        assert hash(handle) == hashes[i]
        with pytest.raises(StaleHandleError):
            _ = handle.node
        with pytest.raises(StaleHandleError):
            handle.configure()
    with pytest.raises(StaleHandleError):
        provisional.add_loopback('stale')
    with pytest.raises(StaleHandleError):
        client.sync(())
    with pytest.raises(StaleHandleError):
        client.get_routes()
    with pytest.raises(StaleHandleError):
        net.add_link(iface, peer)
    assert len(escaped.demands) == 1 and len(net.state.demands) == 0
    assert escaped.devices['existing'].config.enabled is False


def test_empty_and_reverted_batches_preserve_identity():
    net = Network()
    build(net)
    old = net.state
    calls = []
    net.on_delta.append(lambda *args: calls.append(args))
    with net.batch():
        pass
    assert net.state is old
    with net.batch():
        net['a'].configure(enabled=False)
        net['a'].configure(enabled=True)
        net['a']['e'].configure(metric=4)
        net['a']['e'].configure(metric=3)
        net.remove_demand('d')
        net.add_demand('d', 'a', '10.0.0.2', 1e6)
    assert net.state is old
    assert calls == []


def test_snapshot_reads_are_immutable_and_read_your_writes():
    from netsim.model.state import validate_immutable

    net = Network()
    with net.batch():
        dev = net.add_device('a')
        dev.add_ethernet('e')
        validate_immutable(net.state)
        dev.add_route('10.0.0.0/24', ['blackhole'])
        snapshot = net.state
        node = dev.node
        assert len(dev.rib_client().get_routes()) == 1
        fork = net.fork()
        dev.add_ethernet('e2')
        dev.add_route('10.0.1.0/24', ['blackhole'])
        dev['e'].configure(metric=2)
        assert dev['e'].config.metric == 2
        assert len(dev.interfaces) == 2
    assert len(snapshot.devices['a'].interfaces) == 1
    assert len(node.interfaces) == 1
    assert snapshot.devices['a'].interfaces['e'].config.metric == 1
    assert len(fork['a'].rib_client().get_routes()) == 1
    assert len(dev.rib_client().get_routes()) == 2
    assert len(net.state.devices['a'].interfaces) == 2


def test_one_validation_delta_and_commit_time(monkeypatch):
    import netsim.model.network as module

    net = Network()
    net.debug_validate = True
    now = [2.0]
    net.clock = lambda: now[0]
    validations = []
    original = module.validate_immutable

    def validate(root):
        validations.append(root)
        original(root)

    monkeypatch.setattr(module, 'validate_immutable', validate)
    calls = []
    net.on_delta.append(lambda *args: calls.append(args))
    with net.batch():
        net.add_device('a')
        now[0] = 3.0
        net.add_device('b')
        now[0] = 7.0
        assert validations == calls == []
    assert validations == [net.state]
    assert len(calls) == 1 and calls[0][:2] == (7.0, ('batch', 2))
    assert net.state.devices['a'].config.enabled_since == 2.0
    assert net.state.devices['b'].config.enabled_since == 3.0


def test_nested_batch_rejected_and_outer_aborts():
    net = Network()
    old = net.state
    with pytest.raises(RuntimeError, match='nested Network.batch'):
        with net.batch():
            dev = net.add_device('a')
            with net.batch():
                pass
    assert net.state is old and not dev.exists
    with net.batch():
        net.add_device('b')
        with pytest.raises(RuntimeError, match='nested Network.batch'):
            with net.batch():
                pass
    assert net['b'].exists


def test_final_validation_failure_aborts(monkeypatch):
    import netsim.model.network as module

    net = Network()
    net.debug_validate = True
    old = net.state

    def reject(_):
        raise ValueError('validation failed')

    monkeypatch.setattr(module, 'validate_immutable', reject)
    with pytest.raises(ValueError, match='validation failed'):
        with net.batch():
            dev = net.add_device('a')
    assert net.state is old and not dev.exists


def test_post_commit_hook_failure_keeps_commit():
    net = Network()

    def hook(*args):
        raise ValueError('observer failed')

    net.on_delta.append(hook)
    with pytest.raises(ValueError, match='observer failed'):
        with net.batch():
            dev = net.add_device('a')
    assert dev.exists and net.state.version == 1
    net.on_delta.clear()
    with net.batch():
        net.add_device('b')
    assert net.state.version == 2


def test_caught_operation_failure_does_not_leak_partial_link_or_allocators():
    net = Network()
    with net.batch():
        a, b, c = (net.add_device(n) for n in ('a', 'b', 'c'))
        net.add_lag(a, 'po', ['e'], b, 'po', ['e'])
        c.add_portchannel('po')
        ae = a.add_ethernet('bad', aggregate_id='po')
        ce = c.add_ethernet('bad', aggregate_id='po')
        before = net.state
        with pytest.raises(ValueError, match='different peer bundles'):
            net.add_link(ae, ce)
        assert tree_equal(net.state, before)
        assert net.state.allocators == before.allocators
        assert ae.node.link is None and ce.node.link is None
        with pytest.raises(ValueError, match='positive'):
            a.add_ethernet('invalid', speed=0)
        assert net.state.allocators == before.allocators
        a.add_ethernet('good')
    assert len(net.links) == 1 and a['good'].exists


def test_bulk_routes_use_one_rib_apply_per_device_family(monkeypatch):
    import netsim.model.routing as routing
    from netsim.model.addressing import IPV4, IPV6

    original = routing.rib_apply
    calls = []

    def apply(rib, **kw):
        calls.append((rib.af, len(kw.get('add', ()))))
        return original(rib, **kw)

    monkeypatch.setattr(routing, 'rib_apply', apply)
    net = Network()
    with net.batch():
        a = net.add_device('a')
        b = net.add_device('b')
        for i in range(1000):
            a.add_route(f'10.{i // 256}.{i % 256}.0/24', ['blackhole'])
        a.add_route('2001:db8::/32', ['blackhole'])
        b.add_route('0.0.0.0/0', ['blackhole'])
        assert calls == []
    assert calls == [(IPV4, 1000), (IPV6, 1), (IPV4, 1)]


def test_routes_sync_delete_upsert_match_sequential():
    from dataclasses import replace

    from netsim.model.addressing import IPV4
    from netsim.model.contracts import IGP

    def edit(net):
        d = net['a']
        r1 = d.add_route('10.0.0.0/24', ['blackhole'])
        r2 = d.add_route('10.0.1.0/24', ['unreachable'])
        other = d.rib_client(IGP)
        other.add_routes((replace(r1, source=IGP),))
        client = d.rib_client()
        client.sync((r1, replace(r1, metric=7)))
        client.delete_routes((r2.key, r1.key))
        client.add_routes((r2,))
        other.sync(())
        client.sync((replace(r1, distinguisher=('extra',)), r2))

    baseline = Network()
    baseline.add_device('a').add_route('0.0.0.0/0', ['blackhole'])
    net = baseline.fork()
    edit(baseline)
    with net.batch():
        edit(net)
    assert tree_equal(net.state, baseline.state)
    assert tree_equal(net['a'].rib(IPV4), baseline['a'].rib(IPV4))
    old = net.state
    rows = tuple(r for r, _ in net['a'].rib_client().get_routes())
    with net.batch():
        net['a'].rib_client().sync(rows)
    assert net.state is old


def test_builders_do_not_use_persistent_set_or_freeze_per_operation(monkeypatch):
    from netsim.model.state import PMap, PMapBuilder

    net = Network()
    with net.batch():
        # Commit-time epoch bookkeeping can use set; construction must not.
        with monkeypatch.context() as patch:

            def unexpected(*args, **kw):
                pytest.fail('persistent map mutation/freeze during batch construction')

            patch.setattr(PMap, 'set', unexpected)
            patch.setattr(PMapBuilder, 'build', unexpected)
            for i in range(100):
                d = net.add_device(f'd{i}')
                d.add_ethernet('e')
                d.add_loopback('lo')
                d['e'].configure(metric=2)
                d.configure(fib_delay=1)
                d.add_route('0.0.0.0/0', ['blackhole'])
                net.add_demand(f'd{i}', d.name, '10.0.0.1', 1)
            for i in range(0, 100, 2):
                net.add_link(net[f'd{i}']['e'], net[f'd{i + 1}']['e'])
    assert len(net.devices) == 100 and len(net.links) == 50


def test_pure_update_receives_immutable_staged_snapshot():
    from dataclasses import replace

    from netsim.model.state import validate_immutable

    net = Network()
    with net.batch():
        net.add_device('a').add_loopback('lo')

        def update(root):
            validate_immutable(root)
            assert 'lo' in root.devices['a'].interfaces
            return replace(root, transport=('marker',))

        assert net.update(update) is None
        net['a'].add_ethernet('e')
    assert net.state.transport == ('marker',)
    assert len(net['a'].interfaces) == 2


def test_timed_runtime_sees_one_batch_commit():
    from netsim import Environment
    from netsim.runtime import Simulation

    net = Network()
    build(net)
    env = Environment()
    sim = Simulation(env, net)
    sim.settle()
    calls = []
    net.on_delta.append(lambda *args: calls.append(args))

    def edit():
        with net.batch():
            net['a']['e'].admin_down()
            net['b']['e'].admin_down()
        assert len(calls) == 1 and calls[0][:2] == (4, ('batch', 2))

    sim.at(4, edit)
    sim.run_until(4)
    assert [origin for _, origin, _ in calls].count(('batch', 2)) == 1


def test_update_boundaries_advance_inputs_after_each_staged_change():
    from netsim.model.addressing import IPV4

    net = Network()
    a = net.add_device('a')
    epochs = []
    with net.batch():
        a.configure(fib_delay=1)
        net.update(
            lambda root: epochs.append(root.devices['a'].resolver_input_epoch[IPV4])
            or root
        )
        a.configure(fib_delay=2)
        net.update(
            lambda root: epochs.append(root.devices['a'].resolver_input_epoch[IPV4])
            or root
        )
    assert epochs == [2, 3]


@pytest.mark.parametrize('timed', [False, True])
@pytest.mark.parametrize('af', [4, 6])
@pytest.mark.parametrize('method', ['converge', 'place'])
def test_batch_rejects_derivations_without_changing_staged_inputs(timed, af, method):
    from netsim import Environment
    from netsim.runtime import Simulation

    net = Network()
    a = net.add_device('A')
    net.converge()
    sim = Simulation(Environment(), net) if timed else None
    prefixes = (
        ('192.0.2.1/32', '192.0.2.2/32')
        if af == 4
        else ('2001:db8::1/128', '2001:db8::2/128')
    )
    rows = []
    commits = []
    net.on_delta.append(lambda time, origin, delta: commits.append(origin))

    def edit():
        with net.batch():
            rows.append(a.add_route(prefixes[0], ['blackhole']))
            staged = net.state
            with pytest.raises(RuntimeError) as error:
                getattr(net, method)()
            assert (
                str(error.value) == f'{method}() inside batch(): commit the batch first'
            )
            assert net.state == staged
            rows.append(a.add_route(prefixes[1], ['blackhole']))
            assert commits == []
        assert commits == [('batch', 2)]
        for row in rows:
            assert a.route_status(af, row.key) == ('PENDING', None)

    if sim is not None:
        sim.at(5, edit)
        sim.run()
    else:
        edit()
        net.converge()
    for row in rows:
        assert a.fib(af).lookup(row.prefix[0])
        assert a.route_status(af, row.key) == ('INSTALLED', None)
    root = net.state
    net.converge()
    assert net.state is root
    assert net.place() is net.placement


@pytest.mark.parametrize('method', ['converge', 'place'])
def test_uncaught_batch_derivation_aborts_provisional_entities(method):
    net = Network()
    old = net.state
    calls = []
    net.on_delta.append(lambda *args: calls.append(args))
    with pytest.raises(RuntimeError, match='commit the batch first'):
        with net.batch():
            provisional = net.add_device('A')
            getattr(net, method)()
    assert net.state is old
    assert not provisional.exists
    assert calls == []
    assert net.add_device('A').exists


@pytest.mark.parametrize('timed', [False, True])
@pytest.mark.parametrize('af', [4, 6])
def test_batch_update_callback_invalidates_its_changed_inputs(timed, af):
    from dataclasses import replace

    from netsim import Environment
    from netsim.model.routing import rib_apply
    from netsim.runtime import Simulation
    from netsim.runtime.pipeline import fib_affected

    net = Network()
    a = net.add_device('A', fib_delay=1)
    first = a.add_route('192.0.2.1/32' if af == 4 else '2001:db8::1/128', ['blackhole'])
    net.converge()
    sim = Simulation(Environment(), net) if timed else None
    old = net.state
    second = replace(first, prefix=(first.prefix[0] + 1, first.prefix[1]))
    commits = []
    net.on_delta.append(lambda time, origin, delta: commits.append((origin, delta)))

    def edit():
        def add_route(root):
            dev = root.devices['A']
            rib = rib_apply(dev.ribs[af], add=(second,))
            return replace(
                root,
                devices=root.devices.set('A', replace(dev, ribs=dev.ribs.set(af, rib))),
            )

        with net.batch():
            assert net.update(add_route) is None
            # The callback's result must be pending before it becomes the next
            # staging base, even when no builder edited inputs before it.
            staged = net.state.devices['A']
            for family in (4, 6):
                assert staged.resolver_input_epoch[family] == (
                    old.devices['A'].resolver_input_epoch[family] + 1
                )
                assert staged.resolver_input_epoch[family] > (
                    staged.resolver_outcomes[family].processed_epoch
                )
            assert a.route_status(af, second.key) == ('PENDING', None)
            assert net.update(lambda root: root) is None
            assert net.state.devices['A'] is staged
            assert commits == []
        assert len(commits) == 1
        origin, delta = commits[0]
        assert origin == ('batch', 2)
        assert delta.old is old and delta.new is net.state
        assert fib_affected(delta, net.state) == {('A', 4), ('A', 6)}
        assert a.route_status(af, second.key) == ('PENDING', None)

    if sim is not None:
        sim.at(5, edit)
        sim.run_until(5)
        assert a.fib(af).lookup(second.prefix[0]) is None
        sim.run()
        assert 'fib' in sim.timeline.stage_names(6)
    else:
        edit()
        net.converge()
    for row in (first, second):
        assert a.fib(af).lookup(row.prefix[0]) is not None
        assert a.route_status(af, row.key) == ('INSTALLED', None)


@pytest.mark.parametrize('timed', [False, True])
@pytest.mark.parametrize('change', ['policy', 'link'])
def test_reverted_batch_inputs_publish_no_delta(timed, change):
    from netsim import Environment
    from netsim.model.contracts import STATIC
    from netsim.model.interfaces import OperState
    from netsim.model.routing import ResolutionPolicy
    from netsim.runtime import Simulation
    from tests.model.test_network import A, build_diamond

    net, devices = build_diamond()
    net.converge()
    sim = Simulation(Environment(), net) if timed else None
    old = net.state
    a = devices['R1']
    link = net.links['R1:eth1--R2:eth1']
    policy = a.node.config.resolution_policy
    calls = []
    net.on_delta.append(lambda *args: calls.append(args))
    with net.batch():
        if change == 'policy':
            a.configure(resolution_policy=ResolutionPolicy(max_ecmp_paths=1))
            assert a.node.config.resolution_policy.max_ecmp_paths == 1
            a.configure(resolution_policy=policy)
        else:
            link.fail()
            _ = net.state
            link.restore()
    assert net.state is old
    assert calls == []
    if sim is not None:
        records = tuple(sim.timeline.records)
        sim.run()
        assert tuple(sim.timeline.records) == records
    else:
        net.converge()
    assert net.state is old
    fib = a.fib(4)
    assert len(fib.group(fib.lookup(A('10.0.0.4'))).adjacencies) == 2
    assert a.route_status(4, (A('10.0.0.4'), 32, STATIC, ())) == ('INSTALLED', None)
    assert a['eth1'].oper.oper == OperState.UP


def test_rib_snapshots_submit_only_pending_changed_rows(monkeypatch):
    from dataclasses import replace

    import netsim.model.routing as routing
    from netsim.model.addressing import IPV4, IPV6

    net = Network()
    a = net.add_device('A')
    b = net.add_device('B')
    r1 = a.add_route('192.0.2.1/32', ['blackhole'])
    r2 = a.add_route('192.0.2.2/32', ['blackhole'])
    v6 = a.add_route('2001:db8::1/128', ['blackhole'])
    other = b.add_route('198.51.100.1/32', ['blackhole'])
    original = routing.rib_apply
    calls = []

    def apply(rib, **kw):
        calls.append((rib.af, kw.get('add', ()), kw.get('delete', ())))
        return original(rib, **kw)

    monkeypatch.setattr(routing, 'rib_apply', apply)
    with net.batch():
        changed = replace(r1, metric=5)
        a.rib_client().add_routes((changed, r2))
        first = net.state
        assert calls == [(IPV4, (changed,), ())]
        calls.clear()
        assert a.rib(IPV4) is first.devices['A'].ribs[IPV4]
        assert net.state.devices['A'].ribs[IPV6] is first.devices['A'].ribs[IPV6]
        assert calls == []
        a.rib_client().delete_routes((r2.key,))
        second = net.state
        assert calls == [(IPV4, (), (r2.key,))]
        calls.clear()
        b.rib_client().add_routes((replace(other, metric=7),))
        _ = b.node
        assert calls == [(IPV4, (replace(other, metric=7),), ())]
        calls.clear()
        _ = net.state
        a.rib_client(af=IPV6).sync((v6,))
        _ = net.state
        assert calls == []
    assert calls == []
    assert len(first.devices['A'].ribs[IPV4]) == 2
    assert len(second.devices['A'].ribs[IPV4]) == 1
    assert first.devices['A'].ribs[IPV4].rows(r1.prefix) == (changed,)


def test_rib_snapshot_add_delete_does_not_enumerate_existing_rows(monkeypatch):
    from netsim.model.routing import RibState
    from netsim.model.state import PMap

    net = Network()
    a = net.add_device('A')
    existing = a.add_route('192.0.2.1/32', ['blackhole'])
    shards = a.rib(4).shards
    values = PMap.values
    calls = []
    rows_of = RibState.rows_of

    def checked_values(mapping):
        assert mapping is not shards, 'batch snapshot scans every RIB shard'
        return values(mapping)

    def checked_rows_of(rib, client):
        calls.append(client)
        return rows_of(rib, client)

    monkeypatch.setattr(PMap, 'values', checked_values)
    monkeypatch.setattr(RibState, 'rows_of', checked_rows_of)
    with net.batch():
        added = a.add_route('192.0.2.2/32', ['blackhole'])
        a.rib_client().delete_routes((existing.key,))
        snapshot = net.state
    assert snapshot.devices['A'].ribs[4].rows(added.prefix) == (added,)
    # The batch folding layer must not enumerate clients for ordinary
    # add/delete calls; the routing kernel may consult the touched client's
    # index at most once (the incremental index needs no materialization).
    assert len(calls) <= 1


def test_route_snapshot_reverts_preserve_canonical_root():
    from dataclasses import replace

    net = Network()
    a = net.add_device('A')
    row = a.add_route('192.0.2.1/32', ['blackhole'])
    old = net.state
    with net.batch():
        a.rib_client().add_routes((replace(row, metric=9),))
        changed = net.state
        a.rib_client().add_routes((row,))
        restored = net.state
    assert net.state is old
    assert restored.devices['A'].ribs[4] is old.devices['A'].ribs[4]
    assert changed.devices['A'].ribs[4].rows(row.prefix)[0].metric == 9


@pytest.mark.parametrize('snapshot', [False, True])
def test_new_route_add_delete_batch_preserves_absent_rib(snapshot):
    net = Network()
    a = net.add_device('A')
    old = net.state
    with net.batch():
        row = a.add_route('192.0.2.1/32', ['blackhole'])
        if snapshot:
            assert net.state.devices['A'].ribs[4].rows(row.prefix) == (row,)
        a.rib_client().delete_routes((row.key,))
    assert net.state is old


@pytest.mark.parametrize('snapshot', [False, True])
def test_route_sync_fold_order_across_clients_matches_sequential(snapshot):
    from dataclasses import replace
    from random import Random

    from netsim.model.contracts import IGP, STATIC
    from netsim.model.routing import BLACKHOLE, Nexthop, Route

    rows = tuple(
        Route((0xC0000200 + i, 32), 4, client, 1, (Nexthop(special=BLACKHOLE),))
        for client in (STATIC, IGP)
        for i in range(12)
    )
    net = Network()
    net.add_device('A')
    for client in (STATIC, IGP):
        net['A'].rib_client(client).add_routes(
            rows[:12] if client == STATIC else rows[12:]
        )
    expected = net.fork()
    rng = Random(29)
    with net.batch():
        for _ in range(100):
            client = rng.choice((STATIC, IGP))
            candidates = [r for r in rows if r.source == client]
            selected = tuple(
                replace(r, metric=rng.randrange(3))
                for r in rng.choices(candidates, k=rng.randrange(8))
            )
            operation = rng.choice(('add_routes', 'delete_routes', 'sync'))
            payload = (
                tuple(r.key for r in selected)
                if operation == 'delete_routes'
                else selected
            )
            for network in (net, expected):
                getattr(network['A'].rib_client(client), operation)(payload)
            if snapshot:
                assert tree_equal(net.state, expected.state)
    assert tree_equal(net.state, expected.state)


def test_reverted_inputs_inside_a_batch_still_resolve_in_the_timed_pipeline():
    """Two callbacks change and restore a resolver input: the published
    delta carries only the advanced epoch, which must still schedule the FIB."""
    import dataclasses

    import netsim
    from netsim.model.routing import ResolutionPolicy
    from netsim.runtime import Simulation
    from tests.model.test_network import build_diamond

    net, R = build_diamond()
    env = netsim.Environment()
    sim = Simulation(env, net)
    original = net.state.devices['R1'].config.resolution_policy
    fib = R['R1'].fib(4)
    entry = fib.lookup(
        __import__('netsim.model.addressing', fromlist=['to_int']).to_int('10.0.0.4')[0]
    )
    assert len(fib.group(entry).adjacencies) == 2

    def set_policy(policy):
        def fn(state):
            dev = state.devices['R1']
            cfg = dataclasses.replace(dev.config, resolution_policy=policy)
            return dataclasses.replace(
                state,
                devices=state.devices.set('R1', dataclasses.replace(dev, config=cfg)),
            )

        return fn

    def edit():
        with net.batch():
            net.update(set_policy(ResolutionPolicy(max_ecmp_paths=1)), 'narrow')
            net.update(set_policy(original), 'restore')

    sim.at(5, edit)
    sim.run_until(6)
    from netsim.model.addressing import to_int
    from netsim.model.contracts import STATIC

    fib = R['R1'].fib(4)
    entry = fib.lookup(to_int('10.0.0.4')[0])
    assert entry is not None and len(fib.group(entry).adjacencies) == 2
    assert (
        R['R1'].route_status(4, (to_int('10.0.0.4')[0], 32, STATIC, ()))[0]
        == 'INSTALLED'
    )
