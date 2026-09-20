"""T6B metrics A/B/A against the pre-fix integrated implementation.

Run: venv/bin/python -m tests.runtime.benchmark_study_metrics
Loads only the old study module from git; does not change any checkout.
Both implementations see the same immutable network and placement samples.
Construction and operation counting are outside timing. No counter wrappers
remain in the timed samples. This measures metrics extraction, not derivation.
"""

from __future__ import annotations

import argparse
import gc
import json
import platform
import subprocess
import sys
import time
import types
from dataclasses import replace

from netsim import Environment
from netsim.runtime import Schedule, Simulation
from netsim.study import Study
from tests.runtime.test_study import two_rate_network


class Visits:
    def __init__(self, pairs: tuple[tuple[str, float], ...]) -> None:
        self.pairs = pairs
        self.count = 0

    def __iter__(self):
        for pair in self.pairs:
            self.count += 1
            yield pair


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--old', default='7d4976a')
    parser.add_argument('--repeats', type=int, default=20)
    args = parser.parse_args()
    source = subprocess.check_output(
        ['git', 'show', f'{args.old}:netsim/study.py'], text=True
    )
    old = types.ModuleType('_t6b_old_study')
    sys.modules[old.__name__] = old
    exec(compile(source, f'{args.old}:netsim/study.py', 'exec'), old.__dict__)
    print(
        json.dumps(
            {
                'python': sys.version,
                'platform': platform.platform(),
                'old': args.old,
                'gc': gc.isenabled(),
                'repeats': args.repeats,
            }
        ),
        flush=True,
    )
    for size in (100, 1000):
        study = Study(two_rate_network([1] * size))
        sim = Simulation(
            Environment(), study.network.fork(), keep_roots=0, keep_deltas=0
        )
        registry = sim.failures(Schedule([]))
        baseline = next(sim.timeline.placement_events())
        pairs = tuple((name, 1.0) for name in sorted(study.network.state.demands))
        counter = Visits(pairs)
        sim.timeline.events[:] = [replace(baseline, demand_delivered=counter)]
        counts = []
        for function in (old.Study._metrics, Study._metrics):
            counter.count = 0
            function(study, sim, registry, 0, 10)
            counts.append(counter.count)
        print(
            json.dumps({'demands': size, 'single_sample_tuple_visits_old_new': counts}),
            flush=True,
        )
        # Alternate healthy/degraded samples, ending with a nonempty tail interval.
        sim.timeline.events[:] = [
            replace(
                baseline,
                time=float(i),
                demand_delivered=tuple(
                    (name, value if i % 2 == 0 else 0.5) for name, value in pairs
                ),
            )
            for i in range(16)
        ]
        expected = old.Study._metrics(study, sim, registry, 0, 16)
        actual = Study._metrics(study, sim, registry, 0, 16)
        assert expected['per_demand'] == actual['per_demand']
        assert expected['loss_integral'] == actual['loss_integral']
        for label, function in [
            ('A1_old', old.Study._metrics),
            ('B_new', Study._metrics),
            ('A2_old', old.Study._metrics),
        ]:
            gc.collect()
            start = time.perf_counter()
            for _ in range(args.repeats):
                function(study, sim, registry, 0, 16)
            elapsed = time.perf_counter() - start
            print(
                json.dumps(
                    {
                        'phase': label,
                        'demands': size,
                        'samples': 16,
                        'ms_per_metrics': elapsed * 1000 / args.repeats,
                    }
                ),
                flush=True,
            )


if __name__ == '__main__':
    main()
