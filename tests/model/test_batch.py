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


def test_batch_can_converge_and_read_placement_before_commit():
    net = Network()
    calls = []
    net.on_delta.append(lambda *args: calls.append(args))
    with net.batch():
        build(net)
        net.converge()
        assert net.placement.delivered_total == 1e6
        assert net.place() is net.placement
        assert calls == []
    assert len(calls) == 1
    assert net.placement.delivered_total == 1e6
