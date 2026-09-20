"""Serial RIB microbenchmarks and forwarding fingerprints for an A/B/A run.

Run with the same interpreter, selecting an archived baseline via --source.
The supplied convergence script is copied to build/verify_converge.py separately.
"""

import argparse
import gc
import json
import platform
import statistics
import sys
from pathlib import Path
from time import perf_counter


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--source', type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument('--label', default='current')
    args = parser.parse_args()
    project = Path(__file__).resolve().parents[1]
    sys.path[:0] = [str(args.source.resolve()), str(project)]

    import netsim
    from netsim.model.addressing import IPV4
    from netsim.model.contracts import STATIC
    from netsim.model.routing import Nexthop, RibState, Route, rib_apply
    from tests.model.test_routing import routing_fingerprints

    nh = (Nexthop.blackhole(),)
    rows = tuple(Route((i, 32), IPV4, STATIC, 1, nh) for i in range(10_000))
    empty = RibState.empty(IPV4)
    rib = rib_apply(empty, sync=(STATIC, rows))
    prefixes = tuple(((i * 7919) % 10_000, 32) for i in range(200))

    def lookups():
        for prefix in prefixes:
            rib.rows(prefix)

    def sync():
        return rib_apply(empty, sync=(STATIC, rows))

    result = {
        'label': args.label,
        'python': platform.python_version(),
        'gil': getattr(sys, '_is_gil_enabled', lambda: True)(),
        'source': netsim.__file__,
    }
    for name, fn in (('lookup_200_ms', lookups), ('sync_10k_ms', sync)):
        fn()  # warm up; all route creation is outside the timed region
        samples = []
        for _ in range(7):
            gc.collect()
            gc.disable()
            start = perf_counter()
            value = fn()
            samples.append((perf_counter() - start) * 1000)
            gc.enable()
            del value
        result[name] = {'median': statistics.median(samples), 'samples': samples}
    result['fingerprints'] = {
        kind: routing_fingerprints(kind) for kind in ('diamond', 'clos')
    }
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
