"""Failure sets, timed faults and reproducible sources, independent of NetGraph.

An entity is ``('device' | 'link' | 'risk_group', name)``. Link names may
be NetGraph IDs on an imported network. Groups contain entities (including
other groups) and are expanded once by the registry. Times are seconds.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import math
import random
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from netsim.model.network import Network
    from netsim.runtime.simulation import Simulation

Entity = tuple[str, str]


def _entities(values: Iterable[Entity]) -> tuple[Entity, ...]:
    out = set()
    for kind, name in values:
        if kind not in ('device', 'link', 'risk_group') or not isinstance(name, str):
            raise ValueError(f'invalid failure entity: {(kind, name)!r}')
        out.add((kind, name))
    return tuple(sorted(out))


def _nonnegative(value: float, name: str) -> None:
    if not math.isfinite(value) or value < 0:
        raise ValueError(f'{name} must be finite and >= 0')


def derive_seed(master: int, kind: str, name: str) -> int:
    """NetGraph SeedManager(master).derive_seed('netsim', kind, name)."""
    digest = hashlib.sha256(f'{master}:netsim:{kind}:{name}'.encode()).digest()
    return int.from_bytes(digest[:4], 'big') & 0x7FFFFFFF


@dataclass(frozen=True, slots=True)
class FailureSet:
    excluded_nodes: tuple[str, ...] = ()
    excluded_links: tuple[str, ...] = ()
    risk_groups: tuple[str, ...] = ()
    occurrence_count: int = 1

    def __post_init__(self) -> None:
        for field in ('excluded_nodes', 'excluded_links', 'risk_groups'):
            values = getattr(self, field)
            if any(not isinstance(v, str) for v in values):
                raise ValueError('failure names must be strings')
            object.__setattr__(self, field, tuple(sorted(set(values))))
        if not isinstance(self.occurrence_count, int) or self.occurrence_count < 1:
            raise ValueError('occurrence_count must be a positive integer')

    @property
    def entities(self) -> tuple[Entity, ...]:
        return tuple(
            [('device', n) for n in self.excluded_nodes]
            + [('link', n) for n in self.excluded_links]
            + [('risk_group', n) for n in self.risk_groups]
        )

    @property
    def failure_id(self) -> str:
        if self.risk_groups:
            raise ValueError('resolve risk groups before computing failure_id')
        # NetGraph reserves the empty string for the baseline/empty pattern.
        if not self.excluded_nodes and not self.excluded_links:
            return ''
        payload = ','.join(self.excluded_nodes) + '|' + ','.join(self.excluded_links)
        return hashlib.blake2s(payload.encode(), digest_size=8).hexdigest()

    def resolve(self, groups: Mapping[str, Iterable[Entity]]) -> FailureSet:
        entities = expand_entities(self.entities, groups)
        return FailureSet(
            tuple(n for k, n in entities if k == 'device'),
            tuple(n for k, n in entities if k == 'link'),
            occurrence_count=self.occurrence_count,
        )


def resolve_groups(
    groups: Mapping[str, Iterable[Entity]],
) -> dict[str, tuple[Entity, ...]]:
    """Copy and expand a group graph, rejecting unknown groups and cycles."""
    raw = {name: _entities(members) for name, members in groups.items()}
    resolved: dict[str, tuple[Entity, ...]] = {}
    visiting: set[str] = set()

    def visit(name: str) -> tuple[Entity, ...]:
        if name in resolved:
            return resolved[name]
        if name in visiting:
            raise ValueError(f'cyclic risk group: {name}')
        if name not in raw:
            raise ValueError(f'unknown risk group: {name}')
        visiting.add(name)
        members: set[Entity] = set()
        for kind, key in raw[name]:
            members.update(visit(key) if kind == 'risk_group' else ((kind, key),))
        visiting.remove(name)
        resolved[name] = tuple(sorted(members))
        return resolved[name]

    for name in sorted(raw):
        visit(name)
    return resolved


def expand_entities(
    entities: Iterable[Entity], groups: Mapping[str, Iterable[Entity]]
) -> tuple[Entity, ...]:
    out: set[Entity] = set()
    for kind, name in _entities(entities):
        if kind == 'risk_group':
            if name not in groups:
                raise ValueError(f'unknown risk group: {name}')
            out.update(groups[name])
        else:
            out.add((kind, name))
    return tuple(sorted(out))


@dataclass(frozen=True, slots=True)
class FaultEvent:
    entities: tuple[Entity, ...]
    start: float
    duration: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, 'entities', _entities(self.entities))
        _nonnegative(self.start, 'start')
        if self.duration is not None:
            _nonnegative(self.duration, 'duration')
            _nonnegative(self.start + self.duration, 'end')


class LeaseRegistry:
    """One registry per simulation; independent sources share entity leases.

    The first lease saves the original state, and the last restores it.
    An already-disabled entity therefore stays disabled after a repair.
    External direct fail/restore calls during a lease are outside this contract.
    """

    def __init__(
        self,
        sim: Simulation,
        *,
        risk_groups: Mapping[str, Iterable[Entity]] | None = None,
    ) -> None:
        self.sim = sim
        self.risk_groups = resolve_groups(risk_groups or {})
        self.counts: dict[Entity, int] = {}
        self._original: dict[Entity, bool] = {}
        self._leases: dict[int, tuple[Entity, ...]] = {}
        self._next = 0
        self.history: list[tuple[float, int]] = [(sim.env.now, 0)]
        self.trace: list[FaultEvent] = []

    def resolve(self, entities: Iterable[Entity]) -> tuple[Entity, ...]:
        net = self.sim.network
        out = set()
        for kind, name in expand_entities(entities, self.risk_groups):
            if kind == 'link':
                name = net.ngraph_link_ids.get(name, name)
                _ = net.link(name).node  # Validate before scheduling any events.
            else:
                _ = net.device(name).node
            out.add((kind, name))
        return tuple(sorted(out))

    @property
    def active_leases(self) -> tuple[int, ...]:
        """Live tokens, including acquisitions whose post-commit observer raised."""
        return tuple(sorted(self._leases))

    def acquire(self, entities: Iterable[Entity]) -> int:
        members = self.resolve(entities)
        token = self._next
        self._next += 1
        net = self.sim.network
        previous = {entity: self.counts.get(entity, 0) for entity in members}
        originals = {
            (kind, name): net.device(name).enabled
            if kind == 'device'
            else bool(net.link(name).state)
            for kind, name in members
            if previous[(kind, name)] == 0
        }
        old = net.state
        entered = False
        try:
            with net.batch():
                entered = True
                for kind, name in originals:
                    if kind == 'device':
                        net.device(name).configure(enabled=False)
                    else:
                        net.link(name).fail()
                # Publish registry state before the batch dispatches observers.
                self._leases[token] = members
                self._original.update(originals)
                for entity, count in previous.items():
                    self.counts[entity] = count + 1
        except BaseException:
            if not entered or net.state is old:
                # A rejected batch did not commit. Undo only this lease's edits.
                self._leases.pop(token, None)
                for entity, count in previous.items():
                    if count:
                        self.counts[entity] = count
                    else:
                        self.counts.pop(entity, None)
                        self._original.pop(entity, None)
            else:
                # Network commits before observer dispatch; preserve the lease.
                self.history.append((self.sim.env.now, len(self.counts)))
            raise
        self.history.append((self.sim.env.now, len(self.counts)))
        return token

    def release(self, token: int) -> None:
        """Release once; retrying a consumed token is harmless after an error."""
        if token not in self._leases:
            if 0 <= token < self._next:
                return
            raise KeyError(token)
        members = self._leases[token]
        previous = {entity: self.counts[entity] for entity in members}
        originals = {
            entity: self._original[entity]
            for entity, count in previous.items()
            if count == 1
        }
        net = self.sim.network
        old = net.state
        entered = False
        try:
            with net.batch():
                entered = True
                for (kind, name), was_up in originals.items():
                    if kind == 'device':
                        net.device(name).configure(enabled=was_up)
                    elif was_up:
                        net.link(name).restore()
                del self._leases[token]
                for entity, count in previous.items():
                    if count > 1:
                        self.counts[entity] = count - 1
                    else:
                        del self.counts[entity]
                        del self._original[entity]
        except BaseException:
            if not entered or net.state is old:
                self._leases[token] = members
                self.counts.update(previous)
                self._original.update(originals)
            else:
                self.history.append((self.sim.env.now, len(self.counts)))
            raise
        self.history.append((self.sim.env.now, len(self.counts)))

    def schedule(self, events: Iterable[FaultEvent]) -> None:
        # Resolve all groups and names before adding to the engine queue.
        prepared = []
        for event in events:
            if event.start < self.sim.env.now:
                raise ValueError('cannot schedule a fault in the past')
            prepared.append(
                FaultEvent(self.resolve(event.entities), event.start, event.duration)
            )
        for event in sorted(prepared, key=lambda e: (e.start, e.entities)):
            self.trace.append(event)
            self.sim.at(event.start, lambda event=event: self._start(event))

    def _start(self, event: FaultEvent) -> None:
        token = self._next
        try:
            self.acquire(event.entities)
        finally:
            # A post-commit observer exception still leaves a real fault. Arm its
            # repair before propagating the exception; an aborted fault has none.
            if token in self._leases and event.duration is not None:
                self.sim.at(event.start + event.duration, lambda: self.release(token))


class Schedule:
    """Explicit ``FaultEvent`` objects or ``(entities, start, duration)`` rows."""

    def __init__(
        self, events: Iterable[FaultEvent | tuple | Mapping[str, Any]]
    ) -> None:
        self.events = tuple(
            event
            if isinstance(event, FaultEvent)
            else FaultEvent(**event)
            if isinstance(event, Mapping)
            else FaultEvent(*event)
            for event in events
        )

    def __iter__(self) -> Iterator[FaultEvent]:
        return iter(self.events)

    @classmethod
    def replay(
        cls,
        results_json: str | Path | Mapping[str, Any],
        step: str | None = None,
        select: Iterable[str] | str | None = None,
        *,
        start: float = 1.0,
        dwell: float = 1.0,
        duration: float | None = None,
    ) -> Schedule:
        _nonnegative(dwell, 'dwell')
        draws = Draws.replay(results_json, step, select)
        return cls(
            FaultEvent(
                draw.entities,
                start + i * dwell,
                dwell if duration is None else duration,
            )
            for i, draw in enumerate(draws)
        )


class Draws:
    """A replayable sequence of failure sets (deduplication is Study's job)."""

    def __init__(self, draws: Iterable[FailureSet]) -> None:
        self.draws = tuple(draws)

    def __iter__(self) -> Iterator[FailureSet]:
        return iter(self.draws)

    @classmethod
    def from_policy(
        cls,
        network: Any,
        failure_policy_set: Any,
        *,
        policy: str | None = None,
        iterations: int = 1,
        seed: int | None = None,
    ) -> Draws:
        if not isinstance(iterations, int) or iterations < 0:
            raise ValueError('iterations must be a nonnegative integer')
        module = importlib.import_module('ngraph.analysis.failure_manager')
        manager = module.FailureManager(network, failure_policy_set, policy_name=policy)
        pol = manager.get_failure_policy()
        if pol is None or not any(mode.rules for mode in pol.modes):
            return cls(())
        effective_seed = seed if seed is not None else pol.seed
        out = []
        for i in range(iterations):
            nodes, links = manager.compute_exclusions(
                pol,
                seed_offset=effective_seed + i if effective_seed is not None else None,
            )
            out.append(FailureSet(tuple(nodes), tuple(links)))
        return cls(out)

    @classmethod
    def enumerate(
        cls,
        network: Network,
        scope: str = 'links',
        k: int = 1,
        *,
        risk_groups: Mapping[str, Iterable[Entity]] | None = None,
    ) -> Draws:
        if k < 1:
            raise ValueError('k must be >= 1')
        if scope == 'links':
            inverse = {v: key for key, v in network.ngraph_link_ids.items()}
            names = sorted(inverse.get(n, n) for n in network.state.links)
            return cls(
                FailureSet(excluded_links=tuple(c)) for c in combinations(names, k)
            )
        if scope == 'devices':
            return cls(
                FailureSet(tuple(c))
                for c in combinations(sorted(network.state.devices), k)
            )
        if scope == 'risk_groups':
            groups = resolve_groups(
                risk_groups
                if risk_groups is not None
                else getattr(network, 'netsim_risk_groups', {})
            )
            return cls(
                FailureSet(risk_groups=tuple(c)).resolve(groups)
                for c in combinations(sorted(groups), k)
            )
        raise ValueError('scope must be links, devices or risk_groups')

    @classmethod
    def replay(
        cls,
        results_json: str | Path | Mapping[str, Any],
        step: str | None = None,
        select: Iterable[str] | str | None = None,
    ) -> Draws:
        if isinstance(results_json, (str, Path)):
            doc = json.loads(Path(results_json).read_text())
        else:
            doc = results_json
        if step is not None:
            doc = doc['steps'][step]
        requested = (
            None
            if select is None
            else set([select] if isinstance(select, str) else select)
        )
        out, found = [], set()
        for row in doc['data']['flow_results']:
            if requested is not None and row['failure_id'] not in requested:
                continue
            state = row.get('failure_state')
            if not isinstance(state, Mapping):
                raise ValueError('replay requires failure_state exclusions')
            draw = FailureSet(
                tuple(state['excluded_nodes']),
                tuple(state['excluded_links']),
                occurrence_count=row.get('occurrence_count', 1),
            )
            if draw.failure_id != row['failure_id']:
                raise ValueError('failure_id does not match failure_state')
            out.append(draw)
            found.add(draw.failure_id)
        if requested is not None and requested - found:
            raise ValueError(f'unknown failure_ids: {sorted(requested - found)}')
        return cls(out)


@dataclass(frozen=True, slots=True)
class Distribution:
    """A distribution parameterized by its arithmetic mean, in seconds.

    Lognormal uses log-space ``sigma``; Weibull uses ``shape``. Scale/mu
    are calculated to preserve the supplied MTBF/MTTR as the actual mean.
    """

    mean: float
    kind: str = 'exponential'
    sigma: float = 1.0
    shape: float = 1.0

    def __post_init__(self) -> None:
        _nonnegative(self.mean, 'mean')
        _nonnegative(self.sigma, 'sigma')
        if not math.isfinite(self.shape) or self.shape <= 0:
            raise ValueError('shape must be finite and > 0')
        if self.kind not in ('exponential', 'lognormal', 'weibull', 'constant'):
            raise ValueError(f'unknown distribution: {self.kind}')

    @classmethod
    def from_spec(cls, spec: Any, *, mean: float | None = None) -> Distribution:
        if isinstance(spec, cls):
            return spec
        if isinstance(spec, (int, float)):
            return cls(float(spec), 'constant')
        if isinstance(spec, str):
            if mean is None:
                raise ValueError('a distribution name needs a mean')
            return cls(mean, spec)
        if isinstance(spec, Mapping):
            options = dict(spec)
            if mean is not None:
                options.setdefault('mean', mean)
            return cls(**options)
        raise ValueError('expected a duration number or distribution specification')

    def sample(self, rng: random.Random) -> float:
        if self.mean == 0 or self.kind == 'constant':
            return self.mean
        if self.kind == 'exponential':
            value = rng.expovariate(1 / self.mean)
        elif self.kind == 'lognormal':
            value = rng.lognormvariate(
                math.log(self.mean) - self.sigma**2 / 2, self.sigma
            )
        else:
            scale = self.mean / math.gamma(1 + 1 / self.shape)
            value = rng.weibullvariate(scale, self.shape)
        _nonnegative(value, 'sample')
        return value


class Process:
    """Per-entity renewal, or Poisson policy arrivals via ``from_policy``.

    ``Process({('link', name): {'mtbf': 100, 'mttr': 2}}, seed=42)``
    starts healthy. TTF starts again after each repair; ``ttf``/``ttr``
    select distributions (default exponential) by name or dict.
    """

    def __init__(
        self, entities: Mapping[Entity, Mapping[str, Any]], *, seed: int = 0
    ) -> None:
        self.seed = seed
        self.parameters: dict[Entity, tuple[Distribution, Distribution]] = {}
        for entity in _entities(entities):
            params = entities[entity]
            ttf = Distribution.from_spec(
                params.get('ttf', 'exponential'), mean=float(params['mtbf'])
            )
            ttr = Distribution.from_spec(
                params.get('ttr', 'exponential'), mean=float(params['mttr'])
            )
            if ttf.mean <= 0:
                raise ValueError('mtbf must be > 0')
            self.parameters[entity] = ttf, ttr
        self._policy: Any = None
        self._manager: Any = None
        self.rate = 0.0
        self.duration = Distribution(0, 'constant')
        self.name = ''

    @classmethod
    def from_network(cls, network: Network, *, seed: int | None = None) -> Process:
        return cls(
            getattr(network, 'netsim_failure_parameters', {}),
            seed=network.seed if seed is None else seed,
        )

    @classmethod
    def from_policy(
        cls,
        network: Any,
        failure_policy_set: Any,
        *,
        policy: str,
        rate: float,
        duration: Distribution | float | Mapping[str, Any],
        seed: int = 0,
    ) -> Process:
        _nonnegative(rate, 'rate')
        source = cls({}, seed=seed)
        module = importlib.import_module('ngraph.analysis.failure_manager')
        source._manager = module.FailureManager(
            network, failure_policy_set, policy_name=policy
        )
        source._policy = source._manager.get_failure_policy()
        source.rate = rate
        source.duration = Distribution.from_spec(duration)
        source.name = policy
        return source

    def events(self, horizon: float) -> tuple[FaultEvent, ...]:
        _nonnegative(horizon, 'horizon')
        out = []
        for entity, (ttf, ttr) in self.parameters.items():
            rng = random.Random(derive_seed(self.seed, *entity))
            now = 0.0
            while now < horizon:
                start = now + ttf.sample(rng)
                if start >= horizon:
                    break
                duration = ttr.sample(rng)
                if start + duration <= now:
                    raise ValueError('renewal distribution failed to advance time')
                out.append(FaultEvent((entity,), start, duration))
                now = start + duration
        if self._manager is not None and self.rate > 0:
            rng = random.Random(derive_seed(self.seed, 'arrivals', self.name))
            durations = random.Random(derive_seed(self.seed, 'duration', self.name))
            selection_seed = derive_seed(self.seed, 'policy', self.name)
            now, i = 0.0, 0
            while True:
                start = now + rng.expovariate(self.rate)
                if start >= horizon:
                    break
                if start <= now:
                    raise ValueError('arrival distribution failed to advance time')
                nodes, links = self._manager.compute_exclusions(
                    self._policy, seed_offset=selection_seed + i
                )
                out.append(
                    FaultEvent(
                        FailureSet(tuple(nodes), tuple(links)).entities,
                        start,
                        self.duration.sample(durations),
                    )
                )
                now, i = start, i + 1
        return tuple(sorted(out, key=lambda e: (e.start, e.entities)))
