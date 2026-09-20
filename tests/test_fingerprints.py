"""Equivalence oracle for structural changes: FIB and placement fingerprints
on fixed topologies must not move unless a change is meant to alter results."""

from __future__ import annotations

import hashlib
import json

from netsim.model.igp import oracle_igp
from netsim.model.network import Network
from tests.model.clos import build_clos
from tests.model.test_network import build_diamond

EXPECTED = {
    'diamond': 'e9ab519d20540fc110ec13a6df47026fda244ee0803e0e2343b1c2370dd5297d',
    'diamond-fail': 'e818493f1894e836cf411eb64220916c3a4c6647c8f96951474ca25105bb3a63',
    'clos8x4': 'b482ad7e1000a5262b7d8e1faa79580962add7a7e0693db150a71d2314e1e9f4',
    'clos8x4-fail5': '1d19c5fb25dbc2ae1d485a10d37db2620c32f19aa159e51de3543d1458dce2b8',
    'ring64': '4fcfa647bc95b8ceb8d6a468736b0496ef01ab6497d0a52b21f61edfb81c1661',
    'ring64-fail': '2d58f2e94c61d0f998cf9cdab021a739d2bd0b5cb8b08f4cc622d8ec29a2cb90',
}


def ring(n: int) -> Network:
    """Ring with chords (degree 4), unnumbered, oracle IGP, eight demands."""
    net = Network()
    devs = [net.add_device(f'r{i:05}') for i in range(n)]
    for i, d in enumerate(devs):
        d.add_loopback(
            'lo', ipv4=[f'10.{(i >> 16) & 255}.{(i >> 8) & 255}.{i & 255}/32']
        )
    step = max(2, int(n**0.5))
    pairs = sorted(
        {
            tuple(sorted((i, (i + off) % n)))
            for i in range(n)
            for off in (1, step)
            if i != (i + off) % n
        }
    )
    for a, b in pairs:
        net.add_p2p(devs[a], f'e{b}', devs[b], f'e{a}', speed=10e9, unnumbered=True)
    net.add_source(oracle_igp)
    for k in range(8):
        i = (k * 7919) % n
        net.add_demand(
            f'd{k}',
            devs[i].name,
            f'10.{((n - 1) >> 16) & 255}.{((n - 1) >> 8) & 255}.{(n - 1) & 255}',
            1e6,
        )
    return net


def fingerprint(net: Network) -> str:
    obj = []
    for name, dev in net.state.devices.sorted_items():
        for af, fib in dev.fibs.sorted_items():
            for n_, p_, e in sorted(fib.entries.items(), key=lambda x: (x[1], x[0])):
                g = fib.group(e)
                adj = (
                    tuple((a.interface, a.nexthop, a.weight) for a in g.adjacencies)
                    if g
                    else ()
                )
                obj.append((name, int(af), n_, p_, e.action, adj))
    rep = net.state.placement
    if rep is not None:
        obj.append(
            (
                'placement',
                round(rep.delivered_total, 6),
                [round(x, 6) for x in rep.offered],
                [round(x, 6) for x in rep.carried],
                sorted(rep.dropped_by_reason.items()),
            )
        )
    return hashlib.sha256(json.dumps(obj, default=str).encode()).hexdigest()


def test_fingerprints_are_stable():
    got = {}
    net, R = build_diamond()
    net.add_demand('d1', 'R1', '10.0.0.4', 100e6)
    net.converge()
    got['diamond'] = fingerprint(net)
    net.links['R1:eth1--R2:eth1'].fail()
    net.converge()
    got['diamond-fail'] = fingerprint(net)
    net = build_clos(8, 4, 100e9, 1e9, 1)
    net.converge()
    got['clos8x4'] = fingerprint(net)
    for lid in sorted(net.links)[:5]:
        net.links[lid].fail()
    net.converge()
    got['clos8x4-fail5'] = fingerprint(net)
    net = ring(64)
    net.converge()
    got['ring64'] = fingerprint(net)
    net.links[sorted(net.links)[3]].fail()
    net.converge()
    got['ring64-fail'] = fingerprint(net)
    assert got == EXPECTED
