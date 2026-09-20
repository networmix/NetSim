"""T2 A/B/A payload: run with the same interpreter and PYTHONPATH per revision.

    PYTHONPATH=<revision> <python> benchmarks/batch.py devices --count 10000
    PYTHONPATH=<revision> <NetGraph-python> benchmarks/batch.py scenario

Imports/YAML expansion are outside the timed region; each sample builds a fresh
network. This script works on the pre-batch revision as well as the new code.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import importlib
import json
import platform
import statistics
import sys
from contextlib import nullcontext
from dataclasses import fields, is_dataclass
from fractions import Fraction
from pathlib import Path
from time import perf_counter
from typing import Any

from netsim.model.lpm import FrozenPrefixTable
from netsim.model.network import Network
from netsim.model.state import FloatArray, PMap, _is_ignored_field


def normalized(value: Any) -> Any:
    """Deterministic tree content, with the same bookkeeping exclusions as tree_equal."""
    if isinstance(value, PMap):
        return [(normalized(k), normalized(v)) for k, v in value.sorted_items()]
    if isinstance(value, FrozenPrefixTable):
        return normalized(value.items())
    if isinstance(value, FloatArray):
        return value.tolist()
    if is_dataclass(value) and not isinstance(value, type):
        return {
            f.name: normalized(getattr(value, f.name))
            for f in fields(value)
            if not _is_ignored_field(f.name)
        }
    if isinstance(value, (tuple, list)):
        return [normalized(v) for v in value]
    if isinstance(value, frozenset):
        return sorted((normalized(v) for v in value), key=repr)
    if isinstance(value, Fraction):
        return (value.numerator, value.denominator)
    if isinstance(value, bytes):
        return value.hex()
    return value


def fingerprint(value: Any) -> str:
    payload = json.dumps(normalized(value), sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(payload.encode()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('workload', choices=('devices', 'scenario'))
    parser.add_argument('--fingerprint', action='store_true')
    parser.add_argument('--count', type=int, default=10000)
    parser.add_argument('--repeat', type=int, default=3)
    parser.add_argument(
        '--scenario',
        default='/Users/networmix/ws/NetGraph/scenarios/backbone_clos.yml',
    )
    args = parser.parse_args()
    scenario = None
    adapter = None
    if args.workload == 'scenario':
        adapter = importlib.import_module('netsim.adapters.ngraph')
        Scenario = importlib.import_module('ngraph.scenario').Scenario
        scenario = Scenario.from_yaml(Path(args.scenario).read_text())
    timings = []
    sizes = {}
    for _ in range(args.repeat):
        gc.collect()
        start = perf_counter()
        if scenario is None:
            net = Network()
            batch = getattr(net, 'batch', None)
            with batch() if batch is not None else nullcontext():
                for i in range(args.count):
                    net.add_device(f'd{i}')
        else:
            assert adapter is not None
            net, _, _ = adapter.from_scenario(scenario)
        timings.append(perf_counter() - start)
        sizes = {
            'devices': len(net.state.devices),
            'links': len(net.state.links),
            'demands': len(net.state.demands),
            'commits': net.state.version,
        }
        if args.fingerprint:
            sizes['tree_sha256'] = fingerprint(net.state)
            net.converge()
            sizes['fibs_sha256'] = fingerprint(
                tuple(
                    (name, dev.fibs) for name, dev in net.state.devices.sorted_items()
                )
            )
            sizes['placement_sha256'] = fingerprint(net.placement)
        del net
    print(
        json.dumps(
            {
                'python': sys.version,
                'network_source': sys.modules[Network.__module__].__file__,
                'platform': platform.platform(),
                'workload': args.workload,
                'samples_s': timings,
                'median_s': statistics.median(timings),
                **sizes,
            }
        )
    )


if __name__ == '__main__':
    main()
