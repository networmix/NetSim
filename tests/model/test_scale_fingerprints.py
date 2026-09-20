"""Forwarding oracle captured on network-layer-gate-a c2d658e67e62.

The digest includes sorted FIB entries AND groups (including dependencies),
all four placement arrays, edge labels and per-demand results. Test both the
normal promotion threshold and forced sharding of even the tiny diamond.
"""

import hashlib
import json
from pathlib import Path

import pytest

from netsim.model.flows import HASH
from netsim.model.state import PMap
from tests.model.clos import build_clos
from tests.model.test_network import build_diamond


def fingerprints(topology):
    if topology == 'diamond':
        net, _ = build_diamond()
        net.add_demand('fluid', 'R1', '10.0.0.4', 100e6)
        net.add_demand('hash', 'R1', '10.0.0.4', 25e6, mode=HASH, flows=32)
        link = 'R1:eth1--R2:eth1'
    else:
        net = build_clos(8, 4)
        net.add_demand('hash', 'leaf0', '10.1.0.7', 25e6, mode=HASH, flows=32)
        link = 'leaf0:eth0--spine0:eth0'
    result = []
    for phase in ('initial', 'failed', 'restored'):
        if phase == 'failed':
            net.links[link].fail()
        elif phase == 'restored':
            net.links[link].restore()
        net.converge()
        fibs = [
            (name, af, sorted(fib.entries.items()), fib.groups.sorted_items())
            for name, dev in net.state.devices.sorted_items()
            for af, fib in dev.fibs.sorted_items()
        ]
        report = net.placement
        placement = (
            report.edge_count,
            report.offered.tolist(),
            report.carried.tolist(),
            report.dropped.tolist(),
            report.capacity.tolist(),
            report.edge_links.sorted_items(),
            report.demands.sorted_items(),
            report.delivered_total,
            report.dropped_by_reason.sorted_items(),
        )
        result.append(
            [hashlib.sha256(repr(v).encode()).hexdigest() for v in (fibs, placement)]
        )
    return result


@pytest.mark.parametrize('topology', ['diamond', 'clos'])
@pytest.mark.parametrize('force_shards', [False, True])
def test_base_branch_fingerprints(topology, force_shards, monkeypatch):
    if force_shards:
        monkeypatch.setattr(PMap, '_FLAT_LIMIT', 0)
    expected = json.loads(Path(__file__).with_suffix('.json').read_text())
    assert fingerprints(topology) == expected[topology]
