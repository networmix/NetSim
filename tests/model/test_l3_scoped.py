"""Scoped L3 must have the same outputs and fixed point as full derivation."""

import dataclasses
import random

import pytest

from netsim import Environment
from netsim.model import derive
from netsim.model.addressing import IPV4, IPV6, to_int
from netsim.model.contracts import CONNECTED, LOCAL
from netsim.model.interfaces import AdminState, OperState
from netsim.model.network import Network
from netsim.model.state import PMap, StateDelta, tree_equal, validate_immutable
from netsim.runtime import Simulation
from netsim.runtime.pipeline import carrier_affected, l3_affected, lag_affected
from tests.model.test_network import build_diamond


def network(n=50):
    net = Network()
    with net.batch():
        a, b = net.add_device('A'), net.add_device('B')
        for i in range(n):
            net.add_p2p(a, f'e{i}', b, f'e{i}', speed=1e9)
            a[f'e{i}'].configure(
                ipv4=[f'10.0.{i}.0/31'], ipv6=[f'2001:db8:{i:x}::/127']
            )
            b[f'e{i}'].configure(
                ipv4=[f'10.0.{i}.1/31'], ipv6=[f'2001:db8:{i:x}::1/127']
            )
        a.add_loopback('lo', ipv4=['192.0.2.1/32'])
    net.converge()
    return net


def scoped(state, now, device, *names):
    result = derive.derive_l3(state, now, [(device, frozenset(names))])
    full = derive.derive_l3(state, now, [device])
    assert tree_equal(result, full)
    assert derive.derive_l3(result, now, [device]) is result
    return result


@pytest.mark.parametrize('seed', [7, 91])
def test_scoped_matches_full_for_seeded_interface_changes(seed):
    net = network()
    rng = random.Random(seed)
    state = net.state
    for now in range(1, 101):
        name = f'e{rng.randrange(50)}'
        dev = state.devices['A']
        node = dev.interfaces[name]
        change = rng.randrange(5)
        if change == 0:
            node = dataclasses.replace(
                node,
                oper=dataclasses.replace(node.oper, oper=rng.choice(list(OperState))),
            )
        else:
            changes = (
                {'ipv4': ((to_int(f'10.{now}.0.1')[0], 24),)},
                {
                    'ipv6': ()
                    if rng.randrange(2)
                    else ((to_int(f'2001:db8::{now}')[0], 64),)
                },
                {'forwarding_v4': bool(rng.randrange(2))},
                {'admin': rng.choice(list(AdminState))},
            )[change - 1]
            node = dataclasses.replace(
                node, config=dataclasses.replace(node.config, **changes)
            )
        changed = dataclasses.replace(
            state,
            devices=state.devices.set(
                'A', dataclasses.replace(dev, interfaces=dev.interfaces.set(name, node))
            ),
        )
        state = scoped(changed, now, 'A', name)


def test_row_ownership_survives_overlapping_prefix_and_address_changes():
    net = network(2)
    net.devices['A']['e1'].configure(ipv4=['10.0.0.1/31'])
    net.converge()
    old = net.state.devices['A'].ribs[IPV4]
    untouched = {
        r.key: r
        for client in (CONNECTED, LOCAL)
        for r in old.rows_of(client)
        if r.distinguisher == ('e1',)
    }
    net.devices['A']['e0'].configure(ipv4=['203.0.113.1/24'])
    result = scoped(net.state, 1, 'A', 'e0')
    rib = result.devices['A'].ribs[IPV4]
    for key, row in untouched.items():
        assert rib.shards[key[1]][key] is row
    own = [
        r
        for client in (CONNECTED, LOCAL)
        for r in rib.rows_of(client)
        if r.distinguisher == ('e0',)
    ]
    assert {r.prefix for r in own} == {
        (to_int('203.0.113.0')[0], 24),
        (to_int('203.0.113.1')[0], 32),
    }


def test_neighbor_replacement_is_scoped_to_changed_interface():
    net = network(2)
    before = net.state.devices['A'].neighbors
    net.devices['B']['e0'].configure(
        ipv4=['10.0.0.8/31'], ipv6=['2001:db8:ffff::1/127']
    )
    result = scoped(net.state, 1, 'A', 'e0')
    after = result.devices['A'].neighbors
    assert after.mac('e0', to_int('10.0.0.1')[0]) is None
    assert after.mac('e1', to_int('10.0.1.1')[0]) == before.mac(
        'e1', to_int('10.0.1.1')[0]
    )
    assert after.peers is before.peers


def test_link_removal_withdraws_only_its_neighbor_entries():
    net = network(2)
    sim = Simulation(Environment(), net)
    before = net.state.devices['A'].l3_interfaces['e1']
    net.update(
        lambda state: dataclasses.replace(state, links=state.links.remove('A:e0--B:e0'))
    )
    sim.settle()
    for name in ('A', 'B'):
        dev = net.state.devices[name]
        assert dev.neighbors.peer_mac('e0') is None
        assert all(iface != 'e0' for iface, address in dev.neighbors.entries)
        assert dev.neighbors.peer_mac('e1') is not None
    assert net.state.devices['A'].l3_interfaces['e1'] is before
    assert derive.derive_l3(net.state, 0) is net.state


def test_loopback_add_remove_and_router_id():
    net = network(2)
    net.devices['A'].add_loopback('new', ipv4=['203.0.113.2/32'])
    added = scoped(net.state, 1, 'A', 'new')
    assert added.devices['A'].oper.router_id == to_int('203.0.113.2')[0]
    dev = added.devices['A']
    removed = dataclasses.replace(
        added,
        devices=added.devices.set(
            'A', dataclasses.replace(dev, interfaces=dev.interfaces.remove('new'))
        ),
    )
    result = scoped(removed, 2, 'A', 'new')
    assert result.devices['A'].oper.router_id == to_int('192.0.2.1')[0]
    assert all(
        r.distinguisher != ('new',)
        for r in result.devices['A'].ribs[IPV4].rows_of(LOCAL)
    )


def test_device_disable_requests_full_l3():
    net = network(2)
    before = net.state
    net.devices['A'].configure(enabled=False)
    affected = l3_affected(StateDelta(before, net.state), net.state)
    assert 'A' in affected
    state = derive.derive_carrier(net.state, 1)
    state = derive.derive_l3(state, 1, ['A'])
    for af in (IPV4, IPV6):
        assert not state.devices['A'].ribs[af].rows_of(CONNECTED)
        assert not state.devices['A'].ribs[af].rows_of(LOCAL)


def test_l3_invalidation_names_only_changed_interface_and_its_peer():
    net = network(2)
    before = net.state
    net.devices['A']['e0'].configure(ipv4=[])
    assert l3_affected(StateDelta(before, net.state), net.state) == {
        ('A', 'e0'),
        ('B', 'e0'),
    }


def test_scoped_noop_preserves_all_identities():
    net = network(2)
    assert scoped(net.state, 1, 'A', 'e0') is net.state


def test_scoped_work_does_not_visit_untouched_interfaces(monkeypatch):
    net = network()
    previous = net.state.devices['A'].l3_interfaces
    net.devices['A']['e0'].configure(ipv4=[])
    visited = []
    original = derive._interface_neighbors

    def neighbors(state, device, name):
        visited.append((device, name))
        return original(state, device, name)

    def router_id(_):
        pytest.fail('an Ethernet change must not walk loopbacks for the router ID')

    monkeypatch.setattr(derive, '_interface_neighbors', neighbors)
    monkeypatch.setattr(derive, '_router_id', router_id)
    result = derive.derive_l3(net.state, 1, [('A', frozenset({'e0'}))])
    assert visited == [('A', 'e0')]
    current = result.devices['A'].l3_interfaces
    assert all(current[f'e{i}'] is previous[f'e{i}'] for i in range(1, 50))


def test_mixed_and_overlapping_scopes_merge_with_full_device_causes():
    net = network(3)
    net.devices['A']['e0'].configure(ipv4=[])
    net.devices['A']['e1'].configure(ipv6=[])
    scopes = [('A', frozenset({'e0'})), ('A', frozenset({'e0', 'e1'}))]
    partial = derive.derive_l3(net.state, 1, scopes)
    full = derive.derive_l3(net.state, 1, ['A'])
    assert tree_equal(partial, full)
    assert tree_equal(derive.derive_l3(net.state, 1, scopes + ['A']), full)
    assert tree_equal(derive.derive_l3(net.state, 1, ['A'] + scopes), full)
    # The first scoped call on an uninitialized tree must initialize all owners.
    dev = net.state.devices['A']
    initial = dataclasses.replace(
        net.state,
        devices=net.state.devices.set(
            'A', dataclasses.replace(dev, l3_interfaces=None)
        ),
    )
    assert tree_equal(derive.derive_l3(initial, 1, scopes), full)


def test_runtime_scopes_stay_at_the_full_l3_fixed_point():
    net = network()
    sim = Simulation(Environment(), net)
    rng = random.Random(318)
    for now in range(1, 51):
        name = f'e{rng.randrange(50)}'
        device = rng.choice(['A', 'B'])
        choice = rng.randrange(4)

        def action(device=device, name=name, now=now, choice=choice):
            if choice == 0:
                net.devices[device][name].configure(ipv4=[f'172.16.{now}.1/24'])
            elif choice == 1:
                net.devices[device][name].configure(admin=rng.choice(list(AdminState)))
            elif choice == 2:
                net.link(f'A:{name}--B:{name}').fail()
            else:
                net.devices[device].configure(enabled=bool(now % 2))

        sim.at(now, action)
        sim.run_until(now)
        assert derive.derive_l3(net.state, now) is net.state


def test_bundle_peer_address_changes_and_member_moves_reach_l3_owners():
    net, devices = build_diamond(min_links=1)
    sim = Simulation(Environment(), net)
    devices['R2']['Po1'].configure(ipv4=['10.1.12.3/31'])
    sim.settle()
    assert derive.derive_l3(net.state, 0) is net.state
    assert net.state.devices['R1'].neighbors.mac('Po1', to_int('10.1.12.1')[0]) is None
    devices['R1']['eth1'].configure(aggregate_id=None)
    devices['R2']['eth1'].configure(aggregate_id=None)
    sim.settle()
    assert derive.derive_l3(net.state, 0) is net.state
    assert derive.derive_lag(net.state, 0) is net.state


def test_carrier_and_lag_invalidation_do_not_enumerate_unrelated_interfaces(
    monkeypatch,
):
    net, devices = build_diamond(min_links=1)
    net.converge()
    before = net.state
    devices['R1']['eth1'].configure(admin=AdminState.DOWN)
    delta = StateDelta(before, net.state)

    def no_scan(*args):
        pytest.fail('an interface change must use its direct bundle/peer lookup')

    monkeypatch.setattr('netsim.runtime.pipeline._ethernets_of', no_scan)
    monkeypatch.setattr('netsim.runtime.pipeline._bundles_of', no_scan)
    assert carrier_affected(delta, net.state) == {('R1', 'eth1'), ('R2', 'eth1')}
    assert lag_affected(delta, net.state) == {('R1', 'Po1'), ('R2', 'Po1')}


def test_bundle_member_index_handles_pure_candidates_without_interface_walks(
    monkeypatch,
):
    net, _ = build_diamond(min_links=1)
    net.converge()
    dev = net.state.devices['R1']
    validate_immutable(dev.interface_index)

    def no_scan(*args):
        pytest.fail('bundle lookup must not enumerate the device interfaces')

    monkeypatch.setattr(PMap, 'values', no_scan)
    monkeypatch.setattr(PMap, 'sorted_items', no_scan)
    assert [m.name for m in derive.bundle_members(dev, 'Po1')] == ['eth1', 'eth2']
    assert derive.derive_lag(net.state, 1, [('R1', 'Po1')]) is net.state
    node = dev.interfaces['eth1']
    moved = dataclasses.replace(
        node, config=dataclasses.replace(node.config, aggregate_id=None)
    )
    candidate = dataclasses.replace(dev, interfaces=dev.interfaces.set('eth1', moved))
    assert [m.name for m in derive.bundle_members(candidate, 'Po1')] == ['eth2']
    removed = dataclasses.replace(dev, interfaces=dev.interfaces.remove('eth1'))
    assert [m.name for m in derive.bundle_members(removed, 'Po1')] == ['eth2']
    extra = dataclasses.replace(node, name='extra', index=100)
    added = dataclasses.replace(dev, interfaces=dev.interfaces.set('extra', extra))
    assert [m.name for m in derive.bundle_members(added, 'Po1')] == [
        'eth1',
        'eth2',
        'extra',
    ]
    # Reading uncommitted candidates must not mutate the old snapshot's index.
    assert [m.name for m in derive.bundle_members(dev, 'Po1')] == ['eth1', 'eth2']
