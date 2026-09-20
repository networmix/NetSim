"""A/B/A payload for T2B: route edits with explicit state reads in a batch.

Run with the same interpreter and PYTHONPATH pointing to each revision.
The initial topology/RIB build and final fingerprint are outside the timer.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import platform
import statistics
import sys
from dataclasses import replace
from time import perf_counter

from netsim.model import routing
from netsim.model.addressing import IPV4
from netsim.model.network import Network


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=5000)
    parser.add_argument('--edits', type=int, default=200)
    parser.add_argument('--repeat', type=int, default=3)
    args = parser.parse_args()
    baseline = Network()
    with baseline.batch():
        a, b = baseline.add_device('A'), baseline.add_device('B')
        for i in range(args.rows):
            prefix = f'10.{i // 65536}.{i // 256 % 256}.{i % 256}/32'
            a.add_route(prefix, ['blackhole'])
            b.add_route(prefix, ['blackhole'])
    rows = a.rib_client().get_routes()
    changed = tuple(
        replace(rows[i % len(rows)][0], metric=i + 1) for i in range(args.edits)
    )
    original = routing.rib_apply
    counts = [0, 0, 0]

    def apply(rib, **kw):
        counts[0] += 1
        counts[1] += len(kw.get('add', ()))
        counts[2] += len(kw.get('delete', ()))
        return original(rib, **kw)

    routing.rib_apply = apply
    samples = []
    submissions = []
    digests = []
    for _ in range(args.repeat):
        net = baseline.fork()
        a, b = net['A'].rib_client(), net['B'].rib_client()
        counts[:] = [0, 0, 0]
        gc.collect()
        start = perf_counter()
        with net.batch():
            b.add_routes((changed[0],))
            _ = net.state
            for row in changed:
                a.add_routes((row,))
                _ = net.state
                _ = net.state
        samples.append(perf_counter() - start)
        submissions.append(tuple(counts))
        payload = tuple(
            (
                name,
                tuple(
                    (r.key, r.metric)
                    for shard in dev.ribs[IPV4].shards.values()
                    for r in shard.values()
                ),
            )
            for name, dev in net.state.devices.sorted_items()
        )
        digests.append(hashlib.sha256(repr(payload).encode()).hexdigest())
    assert len(set(digests)) == 1
    print(
        json.dumps(
            {
                'python': sys.version,
                'platform': platform.platform(),
                'network_source': sys.modules[Network.__module__].__file__,
                'rows_per_device': args.rows,
                'edits': args.edits,
                'samples_s': samples,
                'median_s': statistics.median(samples),
                'rib_apply_calls_add_delete_rows': submissions,
                'rib_sha256': digests[0],
            }
        )
    )


if __name__ == '__main__':
    main()
