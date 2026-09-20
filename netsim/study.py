"""Serial transient and availability studies over independent network forks.

Core imports require only Python's standard library. NetGraph is loaded only
by the scenario/policy entry points. Flow rates use the imported scenario's
capacity unit (bit/s for a native network); loss integrals always use bits.
"""

from __future__ import annotations

import csv
import json
from collections import Counter
from collections.abc import Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from ipaddress import IPv6Address
from pathlib import Path
from typing import Any

from netsim.core import Environment
from netsim.model.addressing import to_address
from netsim.model.network import Network
from netsim.model.srv6 import policy_status
from netsim.model.state import StateDelta
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
                    'netsim': {
                        **self.netsim,
                        'policy_iterations': [
                            {
                                'failure_id': result['failure_id'],
                                'policies': result['data']
                                .get('netsim', {})
                                .get('policies', []),
                            }
                            for result in self.flow_results
                        ],
                    },
                },
            }
        )

    def rows(self, *, events: bool = False) -> list[dict[str, Any]]:
        """Flow rows, or retained PolicyEvent/SidEvent rows with ``events=True``.

        Policy delivery is observed demand payload rate, in capacity_unit;
        validity/selection/programming come separately from the immutable tree.
        """
        if events:
            return [
                {
                    'failure_id': result['failure_id'],
                    'occurrence_count': result['occurrence_count'],
                    **deepcopy(row),
                }
                for result in self.flow_results
                for row in result['data'].get('netsim', {}).get('timeline', [])
                if row['event'] in ('PolicyEvent', 'SidEvent')
            ]
        return [
            {
                'failure_id': result['failure_id'],
                'occurrence_count': result['occurrence_count'],
                **deepcopy(flow),
                **{
                    f'policy_{key}': deepcopy(value)
                    for key, value in flow['data'].get('policy', {}).items()
                },
            }
            for result in self.flow_results
            for flow in result['flows']
        ]

    def to_csv(self, path: str | Path, *, events: bool = False) -> None:
        """Write rows with sorted columns; structured cells contain JSON."""
        rows = self.rows(events=events)
        fields = sorted({key for row in rows for key in row})
        with Path(path).open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerows(
                {
                    key: json.dumps(value, sort_keys=True)
                    if isinstance(value, (dict, list, tuple))
                    else value
                    for key, value in row.items()
                }
                for row in rows
            )


def _policy_rows(network: Network, capacity_unit: float) -> list[dict[str, Any]]:
    """Committed validation/programming and measured placement, all steering forms."""
    return policy_status(network.state, capacity_unit=capacity_unit)


def _shortfall(offered: float, delivered: float) -> float:
    """Unmet bit/s, ignoring <= max(1e-12 bit/s, 1e-9 * offered) roundoff.

    Compare in payload bit/s, before any result-unit conversion. Use this same
    residual for exported drops, integrals and downtime so they cannot disagree.
    """
    residual = offered - delivered
    return residual if residual > max(1e-12, abs(offered) * 1e-9) else 0.0


class _DeliveryIntegrals:
    """One pass over placement samples, with O(D) accumulator storage.

    A sample replaces the delivered values for all demands. Equal timestamps
    have zero measure, so only their final values affect the next interval.
    """

    def __init__(self, offered: Mapping[str, float], start: float) -> None:
        self.offered = dict(offered)
        self.start = self.time = start
        self.values: dict[str, float] = {}
        self.loss = dict.fromkeys(offered, 0.0)
        self.downtime = dict.fromkeys(offered, 0.0)

    def sample(self, time: float, values: Iterable[tuple[str, float]]) -> None:
        dt = max(0.0, time - self.time)
        if dt:
            for name, rate in self.offered.items():
                shortfall = _shortfall(rate, self.values.get(name, 0.0))
                self.loss[name] += shortfall * dt
                if shortfall:
                    self.downtime[name] += dt
            self.time = time
        self.values = dict(values)

    def metrics(self, end: float) -> dict[str, dict[str, float]]:
        dt = max(0.0, end - self.time)
        result = {}
        for name, rate in self.offered.items():
            shortfall = _shortfall(rate, self.values.get(name, 0.0))
            downtime = self.downtime[name] + (dt if shortfall else 0.0)
            result[name] = {
                'downtime': downtime,
                'unavailability': downtime / (end - self.start)
                if end > self.start
                else 0.0,
                'loss_integral': self.loss[name] + shortfall * dt,
            }
        return result

    def total_loss(self, end: float) -> float:
        return sum(row['loss_integral'] for row in self.metrics(end).values())


class _StudyObservation:
    """Streaming integrals survive event/record eviction without keeping roots."""

    def __init__(self, sim: Simulation, start: float) -> None:
        self.integrals = _DeliveryIntegrals(
            {name: demand.rate for name, demand in sim.state.demands.sorted_items()},
            start,
        )
        self.phase = self.integrals
        self.last_commit_time = sim.env.now
        self.max_utilization = 0.0
        self.drop_reasons: set[str] = set()
        self._placement(sim.env.now, sim.state.placement)
        sim.network.on_delta.append(self.on_delta)

    def _placement(self, time: float, report: Any) -> None:
        values = (
            tuple((name, result.delivered) for name, result in report.demands.items())
            if report is not None
            else ()
        )
        self.integrals.sample(time, values)
        if self.phase is not self.integrals:
            self.phase.sample(time, values)
        if report is not None:
            self.max_utilization = max(self.max_utilization, report.max_utilization())
            self.drop_reasons.update(report.dropped_by_reason)

    def on_delta(self, time: float, origin: Any, delta: StateDelta) -> None:
        self.last_commit_time = time
        if delta.old.placement is not delta.new.placement:
            self._placement(time, delta.new.placement)

    def start_phase(self, time: float) -> None:
        self.phase = _DeliveryIntegrals(self.integrals.offered, time)
        self.phase.sample(time, self.integrals.values.items())


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
        for alias in ('keep_events', 'keep_records'):
            if alias in self.keep:
                key = alias.removeprefix('keep_')
                if key in self.keep and self.keep[key] != self.keep[alias]:
                    raise ValueError(f'conflicting keep options: {key}, {alias}')
                self.keep[key] = self.keep.pop(alias)
        unknown = self.keep.keys() - {
            'roots',
            'deltas',
            'arrays',
            'reports',
            'timeline',
            'events',
            'records',
        }
        if unknown:
            raise ValueError(f'unknown keep options: {sorted(unknown)}')
        self.risk_groups = resolve_groups(getattr(network, 'netsim_risk_groups', {}))
        self.failure_parameters = deepcopy(
            getattr(network, 'netsim_failure_parameters', {})
        )
        self.capacity_unit = float(getattr(network, 'netsim_capacity_unit', 1.0))
        self.destinations = dict(getattr(network, 'netsim_demand_destinations', {}))
        self.priorities = dict(getattr(network, 'netsim_demand_priorities', {}))
        self.scenario: Any = None

    @classmethod
    def from_scenario(cls, scenario: Any, **adapter_kwargs: Any) -> Study:
        from netsim.adapters.ngraph import from_scenario

        keep = adapter_kwargs.pop('keep', None)
        network, _, _ = from_scenario(scenario, **adapter_kwargs)
        study = cls(network, keep=keep)
        study.scenario = scenario
        return study

    def _simulation(self, start: float = 0.0) -> Simulation:
        sim = Simulation(
            Environment(),
            self.network.fork(),
            keep_roots=self.keep.get('roots', 0),
            keep_deltas=self.keep.get('deltas', 0),
            keep_arrays=self.keep.get('arrays', False),
            keep_reports=self.keep.get('reports', False),
            keep_events=self.keep.get('events'),
            keep_records=self.keep.get('records'),
        )
        sim._study_observation = _StudyObservation(sim, start)  # type: ignore[attr-defined]
        return sim

    def _record(self, network: Network, failures: FailureSet) -> dict[str, Any]:
        report = network.placement
        flows = []
        policies = _policy_rows(network, self.capacity_unit)
        policy_by_key = {
            (row['device'], row['color'], row['endpoint']): row for row in policies
        }
        for name, demand in network.state.demands.sorted_items():
            shortfall = _shortfall(demand.rate, report.demands[name].delivered)
            offered = demand.rate / self.capacity_unit
            dropped = shortfall / self.capacity_unit
            placed = offered - dropped
            flows.append(
                {
                    'source': demand.source,
                    'destination': self.destinations.get(
                        name, str(to_address(demand.dst, demand.af))
                    ),
                    'priority': self.priorities.get(name, demand.priority),
                    'demand': offered,
                    'placed': placed,
                    'dropped': dropped,
                    'cost_distribution': {},
                    'data': {
                        'demand_id': name,
                        **(
                            {
                                'policy': deepcopy(
                                    policy_by_key.get(
                                        (
                                            demand.source,
                                            demand.steer.color,
                                            str(IPv6Address(demand.steer.endpoint)),
                                        ),
                                        {},
                                    )
                                )
                            }
                            if demand.steer is not None
                            else {}
                        ),
                    },
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
            'data': {'netsim': {'policies': policies}},
        }

    def _metrics(
        self,
        sim: Simulation,
        registry: LeaseRegistry,
        start: float,
        end: float,
    ) -> dict[str, Any]:
        observation: _StudyObservation | None = getattr(sim, '_study_observation', None)
        if observation is not None:
            integrals = observation.integrals
        else:
            integrals = _DeliveryIntegrals(
                {
                    name: demand.rate
                    for name, demand in self.network.state.demands.sorted_items()
                },
                start,
            )
            for sample in sim.timeline.placement_events():
                if sample.time > end:
                    break
                integrals.sample(sample.time, sample.demand_delivered)
        per_demand = integrals.metrics(end)
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
            'commits': len(sim.timeline.records) + sim.timeline.dropped_records,
            'dropped_events': sim.timeline.dropped_events,
            'dropped_records': sim.timeline.dropped_records,
            'fault_events': sum(
                start <= event.start <= end for event in registry.trace
            ),
            'drop_reasons': _drop_reasons(sim.network),
            'observed_drop_reasons': sorted(
                observation.drop_reasons
                if observation is not None
                else {
                    reason
                    for event in sim.timeline.placement_events()
                    for reason, _ in event.dropped
                }
            ),
            'max_utilization': observation.max_utilization
            if observation is not None
            else max(
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
        observation: _StudyObservation | None = getattr(sim, '_study_observation', None)
        if observation is not None:
            return max(start, observation.last_commit_time)
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
            sim = self._simulation(start=t0)
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
            observation: _StudyObservation | None = getattr(
                sim, '_study_observation', None
            )
            assert observation is not None
            transient_loss = observation.phase.total_loss(failed_at)
            recovery_time = None
            if restore:
                recovery_start = sim.env.now
                observation.start_phase(recovery_start)
                sim.at(
                    recovery_start,
                    lambda registry=registry, token=tokens[0]: registry.release(token),
                )
                sim.run_until(recovery_start + settle)
                sim.run()
                recovered_at = self._settled_time(sim, recovery_start)
                recovery_time = recovered_at - recovery_start
                transient_loss += observation.phase.total_loss(recovered_at)
            extras = self._metrics(sim, registry, t0, sim.env.now)
            extras.update(
                settle_time=failed_at - t0,
                recovery_settle_time=recovery_time,
                bits_lost_transient=transient_loss,
                drop_reasons=failure_drops,
                fault_events=1,
            )
            record['data']['netsim'].update(extras)
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
        terminal['data']['netsim'].update(extras)
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
