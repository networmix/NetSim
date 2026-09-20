"""Manual deadline benchmark; run old/new/old with the same Python executable.

Save the baseline with ``git show <base>:netsim/runtime/pipeline.py > /tmp/old.py``.
Run ``python tests/runtime/verify_pending.py --pipeline-source /tmp/old.py``,
then without that option, then with it again. Add ``--carrier --repeats 1``
to run the carrier-debounce workload (which also measures derivations,
commits and delta dispatch). Construction and initial convergence are untimed.
Handles are captured once; --legacy-link-lookup reproduces the original
quadratic net.links[id] handle-map construction inside fail_all.
--deadline-only excludes the initial failures and zero-delay work from timing.
--derive-source can accompany --pipeline-source to compare scoped L3 changes.
"""

import argparse
import dataclasses
import hashlib
import json
import runpy
import statistics
import sys
import time

from netsim import Environment
from netsim.model import derive
from netsim.model.lpm import FrozenPrefixTable
from netsim.model.network import Network
from netsim.model.state import FloatArray, PMap
from netsim.runtime import pipeline, simulation


def freeze(value):
    """Stable values, including prefix tables whose default repr includes an address."""
    if dataclasses.is_dataclass(value):
        return (
            type(value).__name__,
            tuple(
                (f.name, freeze(getattr(value, f.name)))
                for f in dataclasses.fields(value)
                if f.name
                not in ('l3_interfaces', 'interface_index')  # internal indexes
            ),
        )
    if isinstance(value, PMap):
        return tuple(
            (freeze(k), freeze(v))
            for k, v in sorted(value.items(), key=lambda kv: repr(kv[0]))
        )
    if isinstance(value, FrozenPrefixTable):
        return freeze(value.items())
    if isinstance(value, (list, tuple, FloatArray)):
        return tuple(freeze(v) for v in value)
    if isinstance(value, frozenset):
        return tuple(sorted((freeze(v) for v in value), key=repr))
    assert isinstance(value, (str, int, float, bytes, bool, type(None))), type(value)
    return value


def isolated(n):
    env = Environment()
    net = Network()
    calls = 0

    def run(state, now, entities):
        nonlocal calls
        calls += len(entities)
        return state

    kind = pipeline.Kind(0, pipeline.DEBOUNCE, run, lambda d, s: set())
    pipe = pipeline.Pipeline(env, net, [kind])
    for i in range(n):
        pipe.schedule_entity(kind, i, i + 1, 0)
    start = time.perf_counter()
    env.run()
    elapsed = time.perf_counter() - start
    assert calls == n and not kind.pending
    assert not pipe.scheduled and not pipe.round_end_scheduled
    return elapsed, {'calls': calls}


def carrier(n, *, legacy_link_lookup=False, deadline_only=False):
    net = Network()
    a, b = net.add_device('A'), net.add_device('B')
    a.add_loopback('lo', ipv4=['10.0.0.1/32'])
    b.add_loopback('lo', ipv4=['10.0.0.2/32'])
    for i in range(n):
        net.add_p2p(a, f'e{i}', b, f'e{i}', speed=1e9, unnumbered=True)
        a.interface(f'e{i}').configure(carrier_delay_down=0.001 * (i + 1))
    env = Environment()
    sim = simulation.Simulation(env, net, extract_events=False)
    links = [net.link(name) for name in sorted(net.state.links)]

    def fail_all():
        for link in links:
            if legacy_link_lookup:
                net.links[link.id].fail()
            else:
                link.fail()

    sim.at(1, fail_all)
    deadline_start = []
    sim.at(1.001, lambda: deadline_start.append(time.perf_counter()))
    if deadline_only:
        sim.run_until(1)
    start = time.perf_counter()
    sim.run_until(1 + 0.001 * (n + 2))
    end = time.perf_counter()
    elapsed = end - start
    # Include the full final tree and round/origin history, not just run counts.
    evidence = repr(freeze((net.state, sim.timeline.records))).encode()
    return elapsed, {
        'records': len(sim.timeline.records),
        'fingerprint': hashlib.sha256(evidence).hexdigest(),
        'deadline_seconds': end - deadline_start[0],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pipeline-source')
    parser.add_argument('--derive-source')
    parser.add_argument('--carrier', action='store_true')
    parser.add_argument('--legacy-link-lookup', action='store_true')
    parser.add_argument('--deadline-only', action='store_true')
    parser.add_argument('--repeats', type=int, default=7)
    parser.add_argument(
        '--sizes', type=int, nargs='+', default=[250, 500, 1000, 2000, 4000]
    )
    args = parser.parse_args()
    if args.derive_source:
        old = runpy.run_path(args.derive_source)
        for name, value in old.items():
            if not name.startswith('__'):
                setattr(derive, name, value)
    if args.pipeline_source:
        old = runpy.run_path(args.pipeline_source)
        for name in ('Kind', 'Pipeline', 'build_kinds', 'dirty_everything'):
            setattr(pipeline, name, old[name])
        for name in ('Pipeline', 'build_kinds', 'dirty_everything'):
            setattr(simulation, name, old[name])
    print(
        json.dumps(
            {
                'python': sys.version,
                'pipeline': args.pipeline_source or 'working tree',
                'derive': args.derive_source or 'working tree',
                'workload': 'carrier' if args.carrier else 'isolated',
                'legacy_link_lookup': args.legacy_link_lookup,
                'deadline_only': args.deadline_only,
            }
        ),
        flush=True,
    )

    def measure(n):
        return (
            carrier(
                n,
                legacy_link_lookup=args.legacy_link_lookup,
                deadline_only=args.deadline_only,
            )
            if args.carrier
            else isolated(n)
        )

    if not args.carrier:
        for _ in range(3):
            isolated(1000)
    for n in args.sizes:
        samples = []
        deadline_samples = []
        evidence = {}
        for _ in range(args.repeats):
            elapsed, result = measure(n)
            if 'deadline_seconds' in result:
                deadline_samples.append(result.pop('deadline_seconds'))
            if evidence:
                assert evidence == result
            evidence = result
            samples.append(elapsed)
        median = statistics.median(samples)
        print(
            json.dumps(
                {
                    'n': n,
                    'seconds': samples,
                    'median_seconds': median,
                    'us_per_deadline': median / n * 1e6,
                    'deadline_seconds': deadline_samples,
                    **evidence,
                }
            ),
            flush=True,
        )


if __name__ == '__main__':
    main()
