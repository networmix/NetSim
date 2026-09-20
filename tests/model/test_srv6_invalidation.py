"""Scoped SR consumers and the reproducible C2F commit-cost workload."""

from dataclasses import replace

import pytest

from netsim.model import srv6 as sr
from netsim.model.contracts import STATIC
from netsim.model.igp import oracle_igp
from netsim.model.network import Network
from netsim.model.routing import Nexthop, RibState
from tests.model.test_network import A
from tests.model.test_policies import diamond, state_of
from tests.model.test_srdb_source import literal_policy


def ring64():
    """64 nodes, 64 unnumbered links, 8 two-hop policy headends, no demands.

    Each router has IPv4/IPv6 host loopbacks and one classic terminal SID.
    Headends r00/r08/.../r56 target the terminal two clockwise hops away;
    r63 is outside every recorded validation path. Oracle IGP supplies routes.
    """
    net = Network()
    routers = [net.add_device(f'r{i:02}') for i in range(64)]
    with net.batch():
        for i, dev in enumerate(routers):
            dev.add_loopback(
                'lo', ipv4=[f'10.0.0.{i + 1}/32'], ipv6=[f'2001:db8::{i + 1:x}/128']
            )
            net.add_p2p(
                dev, 'cw', routers[(i + 1) % 64], 'ccw', unnumbered=True, speed=10e9
            )
            dev.add_locator('sr', prefix=f'2001:db8:{i + 1:x}::/64')
            dev.add_local_sid(sr.END_DT46, structure=sr.UNCOMPRESSED)
    net.add_source(oracle_igp)
    net.converge()
    heads = tuple(routers[i].name for i in range(0, 64, 8))
    with net.batch():
        for i in range(0, 64, 8):
            target = routers[i + 2]
            sid = next(iter(target.node.srv6_sids.sids.values()))
            routers[i].policy_client().add(
                sr.SrPolicy(
                    STATIC,
                    10,
                    A(f'2001:db8::{i + 3:x}'),
                    candidate_paths=(
                        sr.CandidatePath(100, (sr.SegmentList((sid.sid,)),)),
                    ),
                )
            )
    net.converge()
    for name in heads:
        table = net[name].node.srv6_policies
        assert all(value.status == sr.POLICY_UP for value in table.states.values())
        assert all(
            dep[0] != 'r63'
            for value in table.states.values()
            for dep in value.dependencies
        )
    return net, heads


def count_validations(monkeypatch):
    calls = []
    original = sr.derive_policy_states

    def record(state, device):
        calls.append(device)
        return original(state, device)

    monkeypatch.setattr(sr, 'derive_policy_states', record)
    return calls


def test_ring_unrelated_loopback_commit_does_not_validate_headends(monkeypatch):
    net, heads = ring64()
    epochs = {name: net[name].node.resolver_input_epoch for name in heads}
    calls = count_validations(monkeypatch)
    before = net.state
    net['r63'].add_loopback('unrelated', ipv4=['192.0.2.1/32'])
    assert calls == []
    assert sr.consumers_affected(before, net.state) == set()
    assert all(net[name].node.resolver_input_epoch is epochs[name] for name in heads)


def test_unrelated_loopback_on_disconnected_device_retains_policy_identity(monkeypatch):
    net, routers = diamond(compressed=False)
    p, _ = literal_policy(routers)
    spare = net.add_device('spare')
    net.converge()
    head = routers['R1']
    before = head.node.srv6_policies
    calls = count_validations(monkeypatch)
    spare.add_loopback('lo', ipv6=['2001:db8:ffff::1/128'])
    assert calls == []
    assert head.node.srv6_policies is before
    assert state_of(routers, p).status == sr.POLICY_UP


@pytest.mark.parametrize('change', ['endpoint', 'rib_miss', 'more_specific'])
def test_consulted_devices_invalidate_withdrawal_and_recovery(change, monkeypatch):
    net, routers = diamond(compressed=False)
    p, terminal = literal_policy(routers)
    net.converge()
    calls = count_validations(monkeypatch)
    if change == 'endpoint':
        old = routers['R4']['lo0'].node.config.ipv6
        routers['R4']['lo0'].configure(ipv6=())

        def restore():
            routers['R4']['lo0'].configure(ipv6=old)
    elif change == 'rib_miss':
        old = routers['R2'].rib(6)

        def put(rib):
            net.update(
                lambda state: replace(
                    state,
                    devices=state.devices.set(
                        'R2',
                        replace(
                            state.devices['R2'],
                            ribs=state.devices['R2'].ribs.set(6, rib),
                        ),
                    ),
                )
            )

        put(RibState.empty(6))

        def restore():
            put(old)
    else:
        from ipaddress import IPv6Address

        row = routers['R4'].add_route(
            f'{IPv6Address(terminal.sid)}/128', [Nexthop.blackhole()]
        )

        def restore():
            routers['R4'].rib_client(af=6).delete_routes([row.key])

    assert 'R1' in calls
    assert state_of(routers, p).status == sr.POLICY_DOWN
    calls.clear()
    restore()
    assert 'R1' in calls
    assert state_of(routers, p).status == sr.POLICY_UP


def test_global_sid_inventory_and_missing_dependencies_keep_fallback(monkeypatch):
    net, routers = diamond(compressed=False)
    p, _ = literal_policy(routers)
    spare = net.add_device('spare')
    net.converge()
    calls = count_validations(monkeypatch)
    spare.add_locator('sr', prefix='2001:db8:ff::/64')
    assert 'R1' in calls  # a new literal SID match can appear at any owner
    # A missing validation record cannot safely be used to suppress work.
    head = routers['R1'].node
    table = replace(head.srv6_policies, states=head.srv6_policies.states.remove(p.key))
    net.update(
        lambda state: replace(
            state, devices=state.devices.set('R1', replace(head, srv6_policies=table))
        )
    )
    calls.clear()
    spare.add_loopback('lo', ipv4=['192.0.2.1/32'])
    assert 'R1' in calls


def benchmark():
    """Run in both revisions: python -m tests.model.test_srv6_invalidation --benchmark.

    Forks are excluded from the timed region. Each commit starts from the same
    converged immutable root. No deferred convergence or protocol work is timed.
    """
    import gc
    import json
    import statistics
    import time
    from unittest.mock import patch

    net, heads = ring64()
    original = sr.derive_policy_states
    counts = []
    elapsed = []
    for _ in range(3 + 25):
        trial = net.fork()
        calls = []

        def recorded(state, device, calls=calls):
            calls.append(device)
            return original(state, device)

        with patch.object(sr, 'derive_policy_states', recorded):
            gc.disable()
            start = time.perf_counter()
            try:
                trial['r63'].add_loopback('unrelated', ipv4=['192.0.2.1/32'])
            finally:
                duration = time.perf_counter() - start
                gc.enable()
        elapsed.append(duration)
        counts.append(len(calls))
    values = elapsed[3:]
    print(
        json.dumps(
            {
                'devices': 64,
                'links': 64,
                'headends': len(heads),
                'commits': len(values),
                'rib_rows': sum(
                    len(rib)
                    for dev in net.state.devices.values()
                    for rib in dev.ribs.values()
                ),
                'median_ms': statistics.median(values) * 1000,
                'min_ms': min(values) * 1000,
                'max_ms': max(values) * 1000,
                'validations_per_commit': sorted(set(counts[3:])),
            },
            indent=2,
        )
    )


if __name__ == '__main__':
    import sys

    if sys.argv[1:] == ['--benchmark']:
        benchmark()
