"""Serial transient and availability studies over independent network forks.

Core imports require only Python's standard library. NetGraph is loaded only
by the scenario/policy entry points. Flow rates use the imported scenario's
capacity unit (bit/s for a native network); loss integrals always use bits.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from netsim.core import Environment
from netsim.model.addressing import to_address
from netsim.model.network import Network
from netsim.runtime.failures import (
    Draws,
    FailureSet,
    LeaseRegistry,
    Process,
    Schedule,
    _nonnegative,
    resolve_groups,
)
from netsim.runtime.simulation import Simulation


@dataclass
class StudyResult:
    baseline: dict[str, Any]
    flow_results: list[dict[str, Any]]
    metadata: dict[str, Any] = field(default_factory=dict)
    netsim: dict[str, Any] = field(default_factory=dict)

    def to_ngraph(self) -> dict[str, Any]:
        """A detached JSON-safe NetGraph results.json **step** document."""
        return deepcopy(
            {
                'metadata': self.metadata,
                'data': {
                    'baseline': self.baseline,
                    'flow_results': self.flow_results,
                    'netsim': self.netsim,
                },
            }
        )

    def rows(self) -> list[dict[str, Any]]:
        """One row per pattern/flow, carrying the pattern's sampling weight."""
        return [
            {
                'failure_id': result['failure_id'],
                'occurrence_count': result['occurrence_count'],
                **deepcopy(flow),
            }
            for result in self.flow_results
            for flow in result['flows']
        ]


def _integrate(
    series: list[tuple[float, float]],
    offered: float,
    start: float,
    end: float,
) -> tuple[float, float]:
    """Left-constant delivered samples → loss bits and downtime seconds."""
    value, cursor, loss, down = 0.0, start, 0.0, 0.0
    for time, delivered in series:
        if time <= start:
            value = delivered
            continue
        if time > end:
            break
        dt = time - cursor
        loss += max(0.0, offered - value) * dt
        if value < offered:
            down += dt
        cursor, value = time, delivered
    dt = end - cursor
    loss += max(0.0, offered - value) * dt
    if value < offered:
        down += dt
    return loss, down


def _drop_reasons(network: Network) -> dict[str, float]:
    report = network.placement
    return dict(report.dropped_by_reason.sorted_items()) if report is not None else {}


class Study:
    def __init__(
        self, network: Network, *, keep: Mapping[str, Any] | None = None
    ) -> None:
        # Freeze the baseline on a private fork; never bind or modify the caller.
        self.network = network.fork()
        self.network.converge()
        self.keep = dict(keep or {})
        unknown = self.keep.keys() - {
            'roots',
            'deltas',
            'arrays',
            'reports',
            'timeline',
        }
        if unknown:
            raise ValueError(f'unknown keep options: {sorted(unknown)}')
        self.risk_groups = resolve_groups(getattr(network, 'netsim_risk_groups', {}))
        self.failure_parameters = deepcopy(
            getattr(network, 'netsim_failure_parameters', {})
        )
        self.capacity_unit = float(getattr(network, 'netsim_capacity_unit', 1.0))
        self.destinations = dict(getattr(network, 'netsim_demand_destinations', {}))
        self.scenario: Any = None

    @classmethod
    def from_scenario(cls, scenario: Any, **adapter_kwargs: Any) -> Study:
        from netsim.adapters.ngraph import from_scenario

        keep = adapter_kwargs.pop('keep', None)
        network, _, _ = from_scenario(scenario, **adapter_kwargs)
        study = cls(network, keep=keep)
        study.scenario = scenario
        return study

    def _simulation(self) -> Simulation:
        return Simulation(
            Environment(),
            self.network.fork(),
            keep_roots=self.keep.get('roots', 0),
            keep_deltas=self.keep.get('deltas', 0),
            keep_arrays=self.keep.get('arrays', False),
            keep_reports=self.keep.get('reports', False),
        )

    def _record(self, network: Network, failures: FailureSet) -> dict[str, Any]:
        report = network.placement
        flows = []
        for name, demand in network.state.demands.sorted_items():
            placed = report.demands[name].delivered / self.capacity_unit
            offered = demand.rate / self.capacity_unit
            flows.append(
                {
                    'source': demand.source,
                    'destination': self.destinations.get(
                        name, str(to_address(demand.dst, demand.af))
                    ),
                    'priority': demand.priority,
                    'demand': offered,
                    'placed': placed,
                    'dropped': max(0.0, offered - placed),
                    'cost_distribution': {},
                    'data': {'demand_id': name},
                }
            )
        total = sum(f['demand'] for f in flows)
        placed = sum(f['placed'] for f in flows)
        return {
            'failure_id': failures.failure_id,
            'failure_state': {
                'excluded_nodes': list(failures.excluded_nodes),
                'excluded_links': list(failures.excluded_links),
            },
            'failure_trace': None,
            'occurrence_count': failures.occurrence_count,
            'flows': flows,
            'summary': {
                'total_demand': total,
                'total_placed': placed,
                'overall_ratio': placed / total if total else 1.0,
                'dropped_flows': sum(f['dropped'] > 0 for f in flows),
                'num_flows': len(flows),
            },
            'data': {},
        }

    def _metrics(
        self,
        sim: Simulation,
        registry: LeaseRegistry,
        start: float,
        end: float,
    ) -> dict[str, Any]:
        per_demand = {}
        for name, demand in self.network.state.demands.sorted_items():
            loss, downtime = _integrate(
                sim.timeline.delivered_series(name), demand.rate, start, end
            )
            per_demand[name] = {
                'downtime': downtime,
                'unavailability': downtime / (end - start) if end > start else 0.0,
                'loss_integral': loss,
            }
        histogram: dict[int, float] = {}
        cursor, active = start, 0
        for time, count in registry.history:
            if time <= start:
                active = count
                continue
            if time > end:
                break
            histogram[active] = histogram.get(active, 0.0) + time - cursor
            cursor, active = time, count
        histogram[active] = histogram.get(active, 0.0) + end - cursor
        histogram = {k: v for k, v in sorted(histogram.items()) if v > 0}
        counts = Counter(type(event).__name__ for event in sim.timeline.events)
        extras: dict[str, Any] = {
            'start': start,
            'end': end,
            'loss_integral': sum(v['loss_integral'] for v in per_demand.values()),
            'per_demand': per_demand,
            'concurrent_failure_histogram': {str(k): v for k, v in histogram.items()},
            'event_counts': dict(sorted(counts.items())),
            'commits': len(sim.timeline.records),
            'fault_events': sum(
                start <= event.start <= end for event in registry.trace
            ),
            'drop_reasons': _drop_reasons(sim.network),
            'observed_drop_reasons': sorted(
                {
                    reason
                    for event in sim.timeline.placement_events()
                    for reason, _ in event.dropped
                }
            ),
            'max_utilization': max(
                (e.max_utilization for e in sim.timeline.placement_events()),
                default=0.0,
            ),
        }
        # Failed links may have zero capacity; keep JSON strictly finite.
        import math

        if not math.isfinite(extras['max_utilization']):
            extras['max_utilization'] = None
        if self.keep.get('arrays', False):
            extras['utilization_series'] = {
                str(edge): [
                    [time, value if math.isfinite(value) else None]
                    for time, value in sim.timeline.utilization_series(edge)
                ]
                for edge in range(
                    sim.network.placement.edge_count
                    if sim.network.placement is not None
                    else 0
                )
            }
        if self.keep.get('timeline', False):
            # Placement row() deliberately omits bulk arrays/reports.
            extras['timeline'] = sim.timeline.rows()
        return extras

    @staticmethod
    def _settled_time(sim: Simulation, start: float) -> float:
        return max(
            (r.time for r in sim.timeline.records if r.time >= start), default=start
        )

    def iterations(
        self,
        draws: Iterable[FailureSet],
        *,
        t0: float = 1.0,
        settle: float = 1.0,
        restore: bool = True,
        parallelism: int = 1,
    ) -> StudyResult:
        """Fork once per unique pattern, holding it for at least ``settle`` seconds.

        Pending delayed derivations are drained before sampling/restoring, even
        when they exceed ``settle``. Recovery is observed for the same minimum
        window. ``parallelism`` is reserved: only 1 is supported for now.
        """
        if parallelism != 1:
            raise ValueError('only serial parallelism=1 is supported')
        _nonnegative(t0, 't0')
        _nonnegative(settle, 'settle')
        unique: dict[str, FailureSet] = {}
        for draw in draws:
            draw = draw.resolve(self.risk_groups)
            old = unique.get(draw.failure_id)
            unique[draw.failure_id] = FailureSet(
                draw.excluded_nodes,
                draw.excluded_links,
                occurrence_count=draw.occurrence_count
                + (old.occurrence_count if old else 0),
            )
        records = []
        for draw in unique.values():
            sim = self._simulation()
            registry = sim.failures(Schedule(()), risk_groups=self.risk_groups)
            tokens: list[int] = []
            sim.at(
                t0,
                lambda draw=draw, tokens=tokens, registry=registry: tokens.append(
                    registry.acquire(draw.entities)
                ),
            )
            sim.run_until(t0 + settle)
            sim.run()  # Only derivation events remain, never a future fault.
            failed_at = self._settled_time(sim, t0)
            record = self._record(sim.network, draw)
            failure_drops = _drop_reasons(sim.network)
            windows = [(t0, failed_at)]
            recovery_time = None
            if restore:
                recovery_start = sim.env.now
                sim.at(
                    recovery_start,
                    lambda registry=registry, token=tokens[0]: registry.release(token),
                )
                sim.run_until(recovery_start + settle)
                sim.run()
                recovered_at = self._settled_time(sim, recovery_start)
                recovery_time = recovered_at - recovery_start
                windows.append((recovery_start, recovered_at))
            extras = self._metrics(sim, registry, t0, sim.env.now)
            offered = sum(d.rate for d in self.network.state.demands.values())
            extras.update(
                settle_time=failed_at - t0,
                recovery_settle_time=recovery_time,
                bits_lost_transient=sum(
                    _integrate(sim.timeline.delivered_series(), offered, a, b)[0]
                    for a, b in windows
                ),
                drop_reasons=failure_drops,
                fault_events=1,
            )
            record['data']['netsim'] = extras
            records.append(record)
        return StudyResult(
            self._record(self.network, FailureSet()),
            records,
            {
                'mode': 'iterations',
                'iterations': sum(d.occurrence_count for d in unique.values()),
                'unique_patterns': len(unique),
                'parallelism': 1,
                'capacity_unit': self.capacity_unit,
                'loss_unit': 'bits',
            },
        )

    def enumerate(
        self, scope: str = 'links', k: int = 1, **options: Any
    ) -> StudyResult:
        return self.iterations(
            Draws.enumerate(self.network, scope, k, risk_groups=self.risk_groups),
            **options,
        )

    def process(self, source: Process | Schedule, horizon: float) -> StudyResult:
        """Observe [0, horizon], without draining repairs beyond the horizon."""
        _nonnegative(horizon, 'horizon')
        if horizon == 0:
            raise ValueError('horizon must be > 0')
        sim = self._simulation()
        registry = sim.failures(source, horizon, risk_groups=self.risk_groups)
        sim.run_until(horizon)
        extras = self._metrics(sim, registry, 0.0, horizon)
        # This record is a terminal snapshot; availability covers the whole run.
        inverse = {v: k for k, v in sim.network.ngraph_link_ids.items()}
        failures = FailureSet(
            tuple(n for k, n in registry.counts if k == 'device'),
            tuple(inverse.get(n, n) for k, n in registry.counts if k == 'link'),
        )
        terminal = self._record(sim.network, failures)
        terminal['data']['netsim'] = extras
        return StudyResult(
            self._record(self.network, FailureSet()),
            [terminal],
            {
                'mode': 'process',
                'horizon': horizon,
                'seed': source.seed if isinstance(source, Process) else None,
                'capacity_unit': self.capacity_unit,
                'loss_unit': 'bits',
            },
            extras,
        )

    def replay(
        self,
        results_json: str | Path | Mapping[str, Any],
        step: str | None,
        select: Iterable[str] | str | None = None,
        **options: Any,
    ) -> StudyResult:
        result = self.iterations(Draws.replay(results_json, step, select), **options)
        result.metadata['mode'] = 'replay'
        return result
