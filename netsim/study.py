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
from dataclasses import dataclass, field, replace
from enum import Enum
from ipaddress import IPv6Address
from pathlib import Path
from time import perf_counter
from typing import Any

from netsim.core import Environment
from netsim.model.addressing import to_address
from netsim.model.network import Network
from netsim.model.srv6 import policy_status
from netsim.model.state import NetworkState, StateDelta
from netsim.runtime.failures import (
    Draws,
    FailureSet,
    FaultEvent,
    LeaseRegistry,
    Process,
    Schedule,
    _nonnegative,
    resolve_groups,
)
from netsim.runtime.simulation import Simulation
from netsim.runtime.timeline import Event, PlacementEvent, Record


class Stability(str, Enum):
    """Outputs that must stay quiet for the requested simulated interval."""

    ROUTING = 'routing'
    PROGRAMMING = 'programming'
    DELIVERY = 'delivery'
    ALL = 'all'


@dataclass(frozen=True)
class Workload:
    """Baseline workload, not a protocol throughput or scale guarantee.

    Prefixes are distinct (AF, network, length) in the baseline RIBs;
    rib_rows counts their per-device/client copies. Unknown future event
    rate and duration are None; describe() accepts declared values.
    """

    devices: int
    links: int
    prefixes: int
    rib_rows: int
    demands: int
    classes: int
    agents: int
    sessions: int
    event_rate: float | None
    duration: float | None
    retention: tuple[tuple[str, Any], ...]


def _future(start: float, delay: float, name: str) -> float:
    import math

    _nonnegative(delay, name)
    end = float(start + delay)
    if not math.isfinite(end) or (delay > 0 and end <= start):
        raise ValueError(f'{name} must advance the finite float clock')
    return end


def _window_options(
    warmup: float,
    event_budget: int | None,
    stability: Stability | str | None,
    quiet: float,
) -> Stability:
    _nonnegative(warmup, 'warmup')
    _nonnegative(quiet, 'quiet')
    if event_budget is not None and (
        isinstance(event_budget, bool)
        or not isinstance(event_budget, int)
        or event_budget < 0
    ):
        raise ValueError('event_budget must be a non-negative integer or None')
    return Stability.ALL if stability is None else Stability(stability)


def _device_programmed(dev: Any) -> bool:
    for af, epoch in dev.resolver_input_epoch.items():
        outcome = dev.resolver_outcomes.get(af)
        if outcome is None or outcome.processed_epoch != epoch:
            return False
    if dev.srv6_policies is not None:
        table = dev.srv6_policies
        if any(
            table.states.get(key) is None
            or table.states[key].programming != 'INSTALLED'
            for key in table.policies
        ):
            return False
    return True


class _StabilityWindow:
    def __init__(self, sim: Simulation, start: float, kind: Stability, quiet: float):
        self.kind, self.quiet = kind, quiet
        self.start = start
        self.observation: _StudyObservation = sim._study_observation  # type: ignore[attr-defined]
        self.routing = kind in (Stability.ALL, Stability.ROUTING)
        self.programming = kind in (Stability.ALL, Stability.PROGRAMMING)
        self.delivery = kind in (Stability.ALL, Stability.DELIVERY)
        self.pending = {
            name
            for name, dev in sim.state.devices.items()
            if self.programming and not _device_programmed(dev)
        }
        self.since: float | None = None if self.pending else start
        self.loss = 0.0
        self.reason_base = self.observation.loss_by_reason(start)
        self.reasons = dict(self.reason_base)

    def ready(self, state: NetworkState) -> bool:
        return not self.programming or all(
            _device_programmed(dev) for dev in state.devices.values()
        )

    def on_delta(self, time: float, origin: Any, delta: StateDelta) -> None:
        changed = False
        if self.routing or self.programming:
            for name in delta.devices().keys:
                old, new = delta.old.devices.get(name), delta.new.devices.get(name)
                if self.routing and (
                    old is None or new is None or old.fibs != new.fibs
                ):
                    changed = True
                if self.programming:
                    if new is None or _device_programmed(new):
                        self.pending.discard(name)
                    else:
                        self.pending.add(name)
        if self.delivery:
            old, new = delta.old.placement, delta.new.placement
            if old is not new and (
                old is None
                or new is None
                or old.delivered_total != new.delivered_total
                or old.dropped_by_reason != new.dropped_by_reason
            ):
                changed = True
        if self.pending:
            self.since = None
        elif self.since is None or changed:
            self.since = max(time, self.start)
            self.loss = self.observation.phase.total_loss(self.since)
            self.reasons = self.observation.loss_by_reason(self.since)

    def transient(self, end: float, converged: bool) -> tuple[float, dict[str, float]]:
        loss = self.loss if converged else self.observation.phase.total_loss(end)
        reasons = self.reasons if converged else self.observation.loss_by_reason(end)
        return loss, {
            reason: value - self.reason_base.get(reason, 0.0)
            for reason, value in reasons.items()
        }

    def result(self, end: float, exhausted: bool) -> tuple[str, float | None]:
        if exhausted:
            return 'budget_exceeded', None
        if self.since is not None and end >= _future(self.since, self.quiet, 'quiet'):
            return 'converged', self.since
        return 'deadline_exceeded', None


class _Runner:
    """Bound all dispatched engine events, including NORMAL liveness timers.

    Advancing an empty clock consumes no synthetic event or budget. All events
    at the deadline are included; work past it is left pending.
    """

    def __init__(self, sim: Simulation, budget: int | None):
        self.sim, self.budget = sim, budget
        self.events = 0
        self.exhausted = False

    def until(self, deadline: float) -> None:
        env = self.sim.env
        observation: _StudyObservation = self.sim._study_observation  # type: ignore[attr-defined]
        while env.peek() <= deadline:
            if not observation.begun and env.peek() >= observation.integrals.start:
                observation.begin(self.sim)
            if self.budget is not None and self.events >= self.budget:
                self.exhausted = True
                return
            env.step()
            self.events += 1
        if not observation.begun and deadline >= observation.integrals.start:
            observation.begin(self.sim)
        env._now = deadline


class _StudyRegistry(LeaseRegistry):
    """Aggregate completed lease history instead of retaining every transition.

    The source API still materializes scheduled future faults; this bounds
    *past* history, independently of Timeline retention.
    """

    def __init__(self, sim: Simulation, start: float, risk_groups: Any):
        super().__init__(sim, risk_groups=risk_groups)
        self.cursor, self.active = start, 0
        self.histogram: dict[int, float] = {}
        self.fault_events = 0
        self.history.clear()

    def _sample(self) -> None:
        now = max(self.cursor, self.sim.env.now)
        dt = now - self.cursor
        if dt:
            self.histogram[self.active] = self.histogram.get(self.active, 0.0) + dt
        self.cursor, self.active = now, len(self.counts)
        self.history.clear()

    def acquire(self, entities: Any) -> int:
        token = super().acquire(entities)
        self._sample()
        return token

    def release(self, token: int) -> None:
        super().release(token)
        self._sample()

    def schedule(self, events: Iterable[FaultEvent]) -> None:
        super().schedule(events)
        self.trace.clear()

    def _start(self, event: FaultEvent) -> None:
        super()._start(event)
        self.fault_events += 1

    def histogram_at(self, end: float) -> dict[int, float]:
        result = dict(self.histogram)
        dt = max(0.0, end - self.cursor)
        if dt:
            result[self.active] = result.get(self.active, 0.0) + dt
        return result


@dataclass
class StudyResult:
    baseline: dict[str, Any]
    flow_results: list[dict[str, Any]]
    metadata: dict[str, Any] = field(default_factory=dict)
    netsim: dict[str, Any] = field(default_factory=dict)
    costs: dict[str, Any] = field(default_factory=dict)
    """Wall costs, excluded from rows()/to_ngraph() for reproducible exports."""

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
    return (
        residual if residual > 0 and residual > max(1e-12, abs(offered) * 1e-9) else 0.0
    )


class _DeliveryIntegrals:
    """One pass over placement samples, with O(D) accumulator storage.

    A sample replaces the delivered values for all demands. Equal timestamps
    have zero measure, so only their final values affect the next interval.
    """

    def __init__(self, offered: Mapping[str, float], start: float) -> None:
        self.offered = dict(offered)
        self.start = self.time = start
        self.values: dict[str, float] = {}
        self.shortfalls = dict(offered)
        self.loss = dict.fromkeys(offered, 0.0)
        self.downtime = dict.fromkeys(offered, 0.0)

    def sample(self, time: float, values: Iterable[tuple[str, float]]) -> None:
        dt = max(0.0, time - self.time)
        if dt:
            for name, shortfall in self.shortfalls.items():
                self.loss[name] += shortfall * dt
                if shortfall:
                    self.downtime[name] += dt
            self.time = time
        self.values = dict(values)
        self.shortfalls = {
            name: _shortfall(rate, self.values.get(name, 0.0))
            for name, rate in self.offered.items()
        }

    def metrics(self, end: float) -> dict[str, dict[str, float]]:
        dt = max(0.0, end - self.time)
        result = {}
        for name, shortfall in self.shortfalls.items():
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
        # Stability checkpoints need only the total, not detached per-demand rows.
        dt = max(0.0, end - self.time)
        return sum(
            self.loss[name] + shortfall * dt
            for name, shortfall in self.shortfalls.items()
        )


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
        self.reason_rates: dict[str, float] = {}
        self.reason_loss: dict[str, float] = {}
        self.reason_time = start
        self.event_counts: Counter[str] = Counter()
        self.commits = self.rounds = 0
        self.last_round: tuple[float, int] | None = None
        # Seed one logical observation baseline, independent of constructor
        # events that may already have been evicted by the time we attach.
        if sim.state.placement is not None:
            self.event_counts['PlacementEvent'] = 1
            self.commits = 1
        self.begun = False
        sim.timeline.on_record.append(self.on_record)
        self._placement(start, sim.state.placement)
        sim.network.on_delta.append(self.on_delta)

    def begin(self, sim: Simulation) -> None:
        """Take the baseline at the observation boundary, after pre-window work."""
        self.begun = True
        self.max_utilization = 0.0
        self.drop_reasons.clear()
        self._placement(self.integrals.start, sim.state.placement)

    def on_record(self, record: Record, events: list[Event]) -> None:
        if record.time >= self.integrals.start:
            self.commits += 1
            self.event_counts.update(type(event).__name__ for event in events)
            key = (record.time, record.round)
            if record.origin.kind == 'stage' and key != self.last_round:
                self.rounds += 1
                self.last_round = key
        for event in events:
            if isinstance(event, PlacementEvent):
                self._sample(
                    event.time,
                    event.demand_delivered,
                    dict(event.dropped),
                    event.max_utilization,
                )

    def _sample(
        self,
        time: float,
        values: Iterable[tuple[str, float]],
        reasons: dict[str, float],
        utilization: float,
    ) -> None:
        values = tuple(values)
        self.integrals.sample(time, values)
        if self.phase is not self.integrals:
            self.phase.sample(time, values)
        dt = max(0.0, time - self.reason_time)
        for reason, rate in self.reason_rates.items():
            self.reason_loss[reason] = self.reason_loss.get(reason, 0.0) + rate * dt
        self.reason_time = max(time, self.reason_time)
        self.reason_rates = reasons
        if time >= self.integrals.start:
            self.max_utilization = max(self.max_utilization, utilization)
            self.drop_reasons.update(reasons)

    def loss_by_reason(self, end: float) -> dict[str, float]:
        dt = max(0.0, end - self.reason_time)
        return {
            reason: self.reason_loss.get(reason, 0.0)
            + self.reason_rates.get(reason, 0.0) * dt
            for reason in sorted(self.reason_loss.keys() | self.reason_rates.keys())
        }

    def _placement(self, time: float, report: Any) -> None:
        values = (
            tuple((name, result.delivered) for name, result in report.demands.items())
            if report is not None
            else ()
        )
        self._sample(
            time,
            values,
            dict(report.dropped_by_reason) if report else {},
            report.max_utilization() if report else 0.0,
        )

    def on_delta(self, time: float, origin: Any, delta: StateDelta) -> None:
        self.last_commit_time = time

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
        # Many demands share a destination. Cache only baseline address strings;
        # iteration-specific demands use a fallback without growing this cache.
        self._addresses = {
            (demand.af, demand.dst): str(to_address(demand.dst, demand.af))
            for demand in self.network.state.demands.values()
        }
        self.scenario: Any = None

    def describe(
        self,
        *,
        event_rate: float | None = None,
        duration: float | None = None,
    ) -> Workload:
        """Count the frozen baseline; event rate/duration are declared, not guessed."""
        for name, value in (('event_rate', event_rate), ('duration', duration)):
            if value is not None:
                _nonnegative(value, name)
        state = self.network.state
        prefixes: set[tuple[int, int, int]] = set()
        rows = 0
        for dev in state.devices.values():
            for af, rib in dev.ribs.items():
                for shard in rib.shards.values():
                    rows += len(shard)
                    prefixes.update((af, key[0], key[1]) for key in shard)
        retention = {
            'roots': 0,
            'deltas': 0,
            'arrays': False,
            'reports': False,
            'timeline': False,
            'events': None,
            'records': None,
            **self.keep,
        }
        return Workload(
            len(state.devices),
            len(state.links),
            len(prefixes),
            rows,
            len(state.demands),
            len(state.placement.classes) if state.placement else 0,
            len(self.network.agents),
            len(state.transport.connections) if state.transport else 0,
            event_rate,
            duration,
            tuple(sorted(retention.items())),
        )

    @classmethod
    def from_scenario(cls, scenario: Any, **adapter_kwargs: Any) -> Study:
        from netsim.adapters.ngraph import from_scenario

        keep = adapter_kwargs.pop('keep', None)
        network, _, _ = from_scenario(scenario, **adapter_kwargs)
        study = cls(network, keep=keep)
        study.scenario = scenario
        return study

    def _simulation(self, start: float = 0.0, *, warmup: float = 0.0) -> Simulation:
        network = self.network.fork()
        if network.agents and network.state.transport is not None:
            # Runtime queues/connections cannot be restored from a model fork.
            # Leave agent nodes intact: Simulation calls AgentRuntime.restart_all
            # to assign new generations before initial convergence, without purging.
            network.update(
                lambda state: replace(state, transport=None), origin='study_restart'
            )
        sim = Simulation(
            Environment(initial_time=-float(warmup)),
            network,
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
            destination = self._addresses.get((demand.af, demand.dst))
            if destination is None:
                destination = str(to_address(demand.dst, demand.af))
            flows.append(
                {
                    'source': demand.source,
                    'destination': self.destinations.get(name, destination),
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
        if isinstance(registry, _StudyRegistry):
            histogram = registry.histogram_at(end)
        histogram = {k: v for k, v in sorted(histogram.items()) if v > 0}
        counts = (
            observation.event_counts
            if observation is not None
            else Counter(type(event).__name__ for event in sim.timeline.events)
        )
        extras: dict[str, Any] = {
            'start': start,
            'end': end,
            'loss_integral': sum(v['loss_integral'] for v in per_demand.values()),
            'per_demand': per_demand,
            'concurrent_failure_histogram': {str(k): v for k, v in histogram.items()},
            'event_counts': dict(sorted(counts.items())),
            'commits': observation.commits
            if observation is not None
            else len(sim.timeline.records) + sim.timeline.dropped_records,
            'rounds': observation.rounds
            if observation is not None
            else len(
                {
                    (r.time, r.round)
                    for r in sim.timeline.records
                    if r.origin.kind == 'stage'
                }
            ),
            'bits_lost_by_reason': observation.loss_by_reason(end)
            if observation is not None
            else {},
            'dropped_events': sim.timeline.dropped_events,
            'dropped_records': sim.timeline.dropped_records,
            'fault_events': registry.fault_events
            if isinstance(registry, _StudyRegistry)
            else sum(start <= event.start <= end for event in registry.trace),
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
        warmup: float = 0.0,
        horizon: float | None = None,
        event_budget: int | None = None,
        stability: Stability | str | None = None,
        quiet: float = 0.0,
    ) -> StudyResult:
        """Observe failures independently, optionally followed by recovery.

        With no new options and no agents, preserve the legacy minimum
        ``settle`` window followed by run_derivations(). Otherwise ``horizon``
        is a hard observation duration; if omitted, ``settle`` is its alias
        (an upper bound, no draining beyond it). An explicit horizon wins.
        Each failure/recovery phase gets that duration. The single event
        budget includes warm-up, pre-failure, observation and recovery events.

        Warm-up runs [-warmup, 0], then the failure is at t0. Agents always
        get a fresh runtime; Simulation restarts initialized nodes with fresh
        generations and retains client rows until sync (reset_agent(purge=False)).
        A model fork is not a warm protocol restart. Wall warm-up cost lives in
        result.costs; deterministic warmup_events also appears in iteration metrics.

        Stability is checked over the *final* quiet interval of each full
        observation window; liveness events do not restart it. None selects
        'all'. Legacy settle_time remains compatible; converged_at is always
        the beginning of an observed quiet interval, never the last commit.
        """
        if parallelism != 1:
            raise ValueError('only serial parallelism=1 is supported')
        _nonnegative(t0, 't0')
        _nonnegative(settle, 'settle')
        kind = _window_options(warmup, event_budget, stability, quiet)
        legacy = (
            horizon is None
            and not warmup
            and event_budget is None
            and stability is None
            and not quiet
            and not self.network.agents
        )
        duration = settle if horizon is None else horizon
        deadline = _future(t0, duration, 'horizon')
        _future(t0, quiet, 'quiet')
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
        warmup_seconds = 0.0
        warmup_events = 0
        for draw in unique.values():
            sim = (
                self._simulation(start=t0, warmup=warmup)
                if warmup
                else self._simulation(start=t0)
            )
            runner = _Runner(sim, event_budget)
            elapsed, warmed = self._warmup(sim, runner, warmup)
            warmup_seconds += elapsed
            warmup_events += warmed
            registry = _StudyRegistry(sim, t0, self.risk_groups)
            sim._failure_registry = registry
            tokens: list[int] = []
            sim.at(
                t0,
                lambda draw=draw, tokens=tokens, registry=registry: tokens.append(
                    registry.acquire(draw.entities)
                ),
            )
            window = _StabilityWindow(sim, t0, kind, quiet)
            sim.network.on_delta.append(window.on_delta)
            if not runner.exhausted:
                runner.until(deadline)
            if legacy:
                runner.events += sim.run_derivations()
            status, converged_at = window.result(sim.env.now, runner.exhausted)
            # The failure must actually have occurred before claiming convergence.
            if not tokens and status == 'converged':
                status, converged_at = 'deadline_exceeded', None
            failed_at = (
                self._settled_time(sim, t0)
                if legacy
                else (
                    converged_at if converged_at is not None else max(t0, sim.env.now)
                )
            )
            record = self._record(sim.network, draw)
            failure_drops = _drop_reasons(sim.network)
            observation: _StudyObservation = sim._study_observation  # type: ignore[attr-defined]
            transient_loss, transient_reasons = window.transient(
                sim.env.now, status == 'converged'
            )
            if legacy:
                transient_loss = observation.phase.total_loss(failed_at)
                transient_reasons = observation.loss_by_reason(failed_at)
            recovery_time = recovery_status = recovery_converged_at = None
            sim.network.on_delta.remove(window.on_delta)
            if restore and tokens and not runner.exhausted:
                recovery_start = sim.env.now
                observation.start_phase(recovery_start)
                recovery = _StabilityWindow(sim, recovery_start, kind, quiet)
                sim.network.on_delta.append(recovery.on_delta)
                sim.at(
                    recovery_start,
                    lambda registry=registry, token=tokens[0]: registry.release(token),
                )
                runner.until(_future(recovery_start, duration, 'horizon'))
                if legacy:
                    runner.events += sim.run_derivations()
                recovery_status, recovery_converged_at = recovery.result(
                    sim.env.now, runner.exhausted
                )
                recovered_at = (
                    self._settled_time(sim, recovery_start)
                    if legacy
                    else recovery_converged_at
                )
                recovery_time = (
                    None if recovered_at is None else recovered_at - recovery_start
                )
                recovery_loss, recovery_reasons = recovery.transient(
                    sim.env.now, recovery_status == 'converged'
                )
                transient_loss += (
                    observation.phase.total_loss(recovered_at)
                    if legacy and recovered_at is not None
                    else recovery_loss
                )
                if legacy and recovered_at is not None:
                    recovery_reasons = {
                        reason: value - recovery.reason_base.get(reason, 0.0)
                        for reason, value in observation.loss_by_reason(
                            recovered_at
                        ).items()
                    }
                for reason, value in recovery_reasons.items():
                    transient_reasons[reason] = (
                        transient_reasons.get(reason, 0.0) + value
                    )
                if recovery_status == 'budget_exceeded':
                    status = recovery_status
                elif status == 'converged' and recovery_status != 'converged':
                    status = recovery_status
            extras = self._metrics(sim, registry, t0, max(t0, sim.env.now))
            extras.update(
                status=status,
                converged_at=converged_at,
                convergence_time=None if converged_at is None else converged_at - t0,
                settle_time=failed_at - t0
                if legacy
                else (None if converged_at is None else converged_at - t0),
                recovery_settle_time=recovery_time,
                recovery_status=recovery_status,
                recovery_converged_at=recovery_converged_at,
                bits_lost_transient=transient_loss,
                bits_lost_transient_by_reason=dict(sorted(transient_reasons.items())),
                drop_reasons=failure_drops,
                fault_events=int(bool(tokens)),
                engine_events=runner.events,
                warmup_events=warmed,
                observation_end=sim.env.now,
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
            costs={'warmup_seconds': warmup_seconds, 'warmup_events': warmup_events},
        )

    @staticmethod
    def _warmup(sim: Simulation, runner: _Runner, warmup: float) -> tuple[float, int]:
        if not warmup:
            return 0.0, 0
        before = perf_counter()
        runner.until(0.0)
        return perf_counter() - before, runner.events

    def enumerate(
        self,
        scope: str = 'links',
        k: int = 1,
        *,
        warmup: float = 0.0,
        horizon: float | None = None,
        event_budget: int | None = None,
        stability: Stability | str | None = None,
        quiet: float = 0.0,
        **options: Any,
    ) -> StudyResult:
        return self.iterations(
            Draws.enumerate(self.network, scope, k, risk_groups=self.risk_groups),
            warmup=warmup,
            horizon=horizon,
            event_budget=event_budget,
            stability=stability,
            quiet=quiet,
            **options,
        )

    def process(
        self,
        source: Process | Schedule,
        horizon: float,
        *,
        warmup: float = 0.0,
        event_budget: int | None = None,
        stability: Stability | str | None = None,
        quiet: float = 0.0,
    ) -> StudyResult:
        """Observe [0, horizon], after optional [-warmup, 0] warm-up.

        Schedule/Process times remain relative to zero. Report terminal-window
        stability; future failures/repairs are never drained past the deadline.
        Costs are separate from deterministic exports, as for iterations().
        """
        _nonnegative(horizon, 'horizon')
        if horizon == 0:
            raise ValueError('horizon must be > 0')
        kind = _window_options(warmup, event_budget, stability, quiet)
        _future(0.0, quiet, 'quiet')
        sim = self._simulation(warmup=warmup) if warmup else self._simulation()
        runner = _Runner(sim, event_budget)
        seconds, warmed = self._warmup(sim, runner, warmup)
        registry = _StudyRegistry(sim, 0.0, self.risk_groups)
        sim._failure_registry = registry
        sim.failures(source, horizon, risk_groups=self.risk_groups)
        window = _StabilityWindow(sim, 0.0, kind, quiet)
        sim.network.on_delta.append(window.on_delta)
        if not runner.exhausted:
            runner.until(horizon)
        status, converged_at = window.result(sim.env.now, runner.exhausted)
        end = max(0.0, sim.env.now)
        extras = self._metrics(sim, registry, 0.0, end)
        transient_loss, transient_reasons = window.transient(end, status == 'converged')
        extras.update(
            status=status,
            converged_at=converged_at,
            convergence_time=converged_at,
            engine_events=runner.events,
            warmup_events=warmed,
            observation_end=sim.env.now,
            bits_lost_transient=transient_loss,
            bits_lost_transient_by_reason=transient_reasons,
        )
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
            costs={'warmup_seconds': seconds, 'warmup_events': warmed},
        )

    def replay(
        self,
        results_json: str | Path | Mapping[str, Any],
        step: str | None,
        select: Iterable[str] | str | None = None,
        *,
        warmup: float = 0.0,
        horizon: float | None = None,
        event_budget: int | None = None,
        stability: Stability | str | None = None,
        quiet: float = 0.0,
        **options: Any,
    ) -> StudyResult:
        result = self.iterations(
            Draws.replay(results_json, step, select),
            warmup=warmup,
            horizon=horizon,
            event_budget=event_budget,
            stability=stability,
            quiet=quiet,
            **options,
        )
        result.metadata['mode'] = 'replay'
        return result
