"""Reproducible manual-loop / Study / manual-loop timing (no pytest overhead).

Run: venv/bin/python -m tests.runtime.benchmark_study
Build/converge and source selection are outside all timed regions. Fifty
unique draws prevent deduplication from making Study appear faster. Both paths
fork, fail at 1, sample at 2, restore at 2, and run to 3 with identical timeline
retention. Study additionally leases, serializes flow records and integrates
metrics. A/B/A runs in one interpreter, with GC enabled and a collection before
each block. Output includes fingerprints to check settled placement equality.
"""

from __future__ import annotations

import gc
import hashlib
import json
import platform
import random
import sys
import time

from netsim import Environment
from netsim.runtime import Draws, FailureSet, Simulation
from netsim.study import Study
from tests.model.clos import build_clos


def main() -> None:
    study = Study(build_clos(16, 4))
    names = random.Random(42).sample(sorted(study.network.state.links), 50)
    draws = Draws(FailureSet(excluded_links=(name,)) for name in names)

    def manual() -> list[float]:
        values = []
        for name in names:
            net = study.network.fork()
            sim = Simulation(
                Environment(), net, keep_roots=0, keep_deltas=0, keep_arrays=False
            )
            sim.at(1, net.link(name).fail)
            sim.run_until(2)
            sim.run()
            values.append(net.placement.delivered_total)
            sim.at(sim.env.now, net.link(name).restore)
            sim.run_until(sim.env.now + 1)
            sim.run()
        return values

    def wrapped() -> list[float]:
        result = study.iterations(draws)
        return [row['summary']['total_placed'] for row in result.flow_results]

    # Warm the common code paths without a full 50-iteration timing block.
    study.iterations(list(draws)[:1])
    runs = []
    expected = None
    for label, operation in [
        ('A1_manual', manual),
        ('B_study', wrapped),
        ('A2_manual', manual),
    ]:
        gc.collect()
        before = time.perf_counter()
        values = operation()
        elapsed = time.perf_counter() - before
        if expected is None:
            expected = values
        assert values == expected
        row = {
            'phase': label,
            'iterations': 50,
            'seconds': elapsed,
            'ms_per_iteration': elapsed * 1000 / 50,
            'placement_sha256': hashlib.sha256(json.dumps(values).encode()).hexdigest(),
        }
        print(json.dumps(row), flush=True)
        runs.append(row)
    print(
        json.dumps(
            {
                'python': sys.version,
                'platform': platform.platform(),
                'gc': gc.isenabled(),
                'study_over_manual_mean': runs[1]['seconds']
                / ((runs[0]['seconds'] + runs[2]['seconds']) / 2),
            }
        )
    )


if __name__ == '__main__':
    main()
