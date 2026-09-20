"""T3B A/B/A harness: single-row mutations and 10k-row syncs.

Use --source build/t3b-baseline to load the archived integrated baseline.
Run each leg serially with the same interpreter and PYTHONHASHSEED.
"""

import argparse
import gc
import json
import platform
import statistics
import sys
from dataclasses import replace
from pathlib import Path
from time import perf_counter


def measure(fn, repeats):
    fn()
    samples = []
    for _ in range(7):
        gc.collect()
        gc.disable()
        result = None
        try:
            start = perf_counter()
            for _ in range(repeats):
                result = fn()
            samples.append((perf_counter() - start) * 1e6 / repeats)
        finally:
            gc.enable()
        del result
    return {'median_us': statistics.median(samples), 'samples_us': samples}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    project = Path(__file__).resolve().parents[1]
    parser.add_argument('--source', type=Path, default=project)
    parser.add_argument('--label', default='current')
    args = parser.parse_args()
    sys.path[:0] = [str(args.source.resolve()), str(project)]

    import netsim
    from netsim.model.addressing import IPV4
    from netsim.model.contracts import STATIC
    from netsim.model.routing import Nexthop, RibState, Route, rib_apply
    from tests.model.test_routing import routing_fingerprints

    nh = (Nexthop.blackhole(),)
    rows = tuple(Route((i, 32), IPV4, STATIC, 1, nh) for i in range(10_000))
    changed = tuple(replace(row, metric=1) for row in rows)
    empty = RibState.empty(IPV4)
    rib = rib_apply(empty, sync=(STATIC, rows))
    extra = replace(rows[0], prefix=(10_000, 32))
    deleted = rows[5000].key
    operations = {
        'add_one': lambda: rib_apply(rib, add=(extra,)),
        'delete_one': lambda: rib_apply(rib, delete=(deleted,)),
        'replace_one': lambda: rib_apply(rib, add=(changed[5000],)),
        'sync_10k_empty': lambda: rib_apply(empty, sync=(STATIC, rows)),
        'sync_10k_replace': lambda: rib_apply(rib, sync=(STATIC, changed)),
    }
    result = {
        'label': args.label,
        'python': platform.python_version(),
        'gil': getattr(sys, '_is_gil_enabled', lambda: True)(),
        'source': netsim.__file__,
        'measurements': {
            name: measure(fn, 1 if name.startswith('sync') else 100)
            for name, fn in operations.items()
        },
        'fingerprints': {
            kind: routing_fingerprints(kind) for kind in ('diamond', 'clos')
        },
    }
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
