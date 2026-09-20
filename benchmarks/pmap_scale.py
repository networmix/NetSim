"""T1 benchmark; use one interpreter for serial base / candidate / base runs.

Example: venv/bin/python benchmarks/pmap_scale.py --repo /tmp/base --label A1
The repo argument takes precedence over editable installs. No benchmark plugin
or coverage instrumentation is involved; construction is outside the timers.
"""

from __future__ import annotations

import argparse
import dataclasses
import gc
import json
import os
import platform
import statistics
import sys
import time
import tracemalloc


def timed(fn, count, rounds=7):
    fn()
    samples = []
    gc.collect()
    gc.disable()
    try:
        for _ in range(rounds):
            start = time.perf_counter_ns()
            for _ in range(count):
                fn()
            samples.append((time.perf_counter_ns() - start) / count)
    finally:
        gc.enable()
    return statistics.median(samples)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', default=os.getcwd())
    parser.add_argument('--label', default='candidate')
    parser.add_argument('--shards', type=int, choices=(256, 512))
    args = parser.parse_args()
    sys.path.insert(0, os.path.abspath(args.repo))

    from netsim.model import derive
    from netsim.model.network import Network
    from netsim.model.state import DeviceState, NetworkState, PMap, StateDelta

    if args.shards:
        PMap._SHARD_COUNT = args.shards
    state = NetworkState(
        devices=PMap((f'r{i}', DeviceState(f'r{i}', i + 1)) for i in range(100_000))
    )

    def edited(s):
        d = s.devices['r0']
        d = dataclasses.replace(
            d, config=dataclasses.replace(d.config, seed=d.config.seed + 1)
        )
        return dataclasses.replace(s, devices=s.devices.set('r0', d))

    candidate = edited(state)
    net = Network()
    net._state = state
    assert not net.debug_validate
    keys = [f'r{i}' for i in range(0, 100_000, 100)]

    def get_prebuilt():
        for key in keys:
            state.devices.get(key)

    def get_formatted():
        for i in range(1000):
            state.devices.get(f'r{i}')

    metrics = {
        'set_ms': timed(lambda: state.devices.set('r0', candidate.devices['r0']), 50)
        / 1e6,
        'bump_ms': timed(lambda: derive.bump_epochs(state, candidate), 25) / 1e6,
        'delta_ms': timed(lambda: StateDelta(state, candidate).is_empty(), 25) / 1e6,
        'commit_ms': timed(lambda: net.update(edited), 25) / 1e6,
        'get_prebuilt_ns': timed(get_prebuilt, 500) / len(keys),
        'get_formatted_ns': timed(get_formatted, 500) / 1000,
    }
    # Measure retained allocations only, after the entire baseline tree exists.
    # Keep that baseline plus 64 successive committed roots alive.
    net._state = state
    gc.collect()
    tracemalloc.start()
    before = tracemalloc.get_traced_memory()[0]
    roots = []
    for _ in range(64):
        net.update(edited)
        roots.append(net.state)
    gc.collect()
    metrics['retained_64_MB'] = (tracemalloc.get_traced_memory()[0] - before) / 1e6
    tracemalloc.stop()
    assert len(roots) == 64 and roots[-1].devices['r0'].config.seed == 64
    metrics['empty_map_bytes'] = sys.getsizeof(PMap()) + sys.getsizeof({})
    print(
        json.dumps(
            {
                'label': args.label,
                'python': sys.version,
                'platform': platform.platform(),
                'gil_enabled': getattr(sys, '_is_gil_enabled', lambda: True)(),
                'hash_seed': os.environ.get('PYTHONHASHSEED', 'random'),
                'source': sys.modules[PMap.__module__].__file__,
                'shards': getattr(PMap, '_SHARD_COUNT', 1),
                'metrics': metrics,
            },
            indent=2,
        )
    )


if __name__ == '__main__':
    main()
