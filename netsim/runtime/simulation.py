"""``Simulation``: binds a ``Network`` to an ``Environment``."""

from __future__ import annotations

from typing import Any, Callable

from netsim import core
from netsim.model import derive
from netsim.model.network import Network
from netsim.model.state import NetworkState
from netsim.runtime.agents import AgentRuntime
from netsim.runtime.events import EventBus, Stats
from netsim.runtime.failures import Entity, LeaseRegistry, Process, Schedule
from netsim.runtime.pipeline import Pipeline, build_kinds, dirty_everything
from netsim.runtime.timeline import Timeline
from netsim.runtime.transport import TransportRuntime


class BudgetExceeded(RuntimeError):
    """The simulation exhausted its cumulative dispatched-event allowance."""

    def __init__(self, budget: int, dispatched: int, time: float) -> None:
        self.budget, self.dispatched, self.time = budget, dispatched, time
        super().__init__(
            f'event budget {budget} exhausted after {dispatched} events at t={time}'
        )


class Simulation:
    def __init__(
        self,
        env: core.Environment,
        network: Network,
        *,
        settle_delay: float = 0.0,
        event_budget: int | None = None,
        stats_samples: int = 0,
        max_rounds_per_timestamp: int = 10_000,
        keep_roots: int | None = 256,
        keep_deltas: int = 64,
        extract_events: bool = True,
        keep_events: int | None = None,
        keep_records: int | None = None,
        keep_arrays: bool = True,
        keep_reports: bool = False,
    ) -> None:
        if getattr(network, '_simulation', None) is not None:
            raise RuntimeError('network is already bound to a Simulation')
        if event_budget is not None and (
            isinstance(event_budget, bool)
            or not isinstance(event_budget, int)
            or event_budget < 0
        ):
            raise ValueError('event_budget must be a non-negative integer or None')
        self.event_budget = event_budget
        self.events_dispatched = 0
        self.env = env
        self.network = network
        network.clock = lambda: env.now
        self.timeline = Timeline(
            keep_roots=keep_roots,
            keep_deltas=keep_deltas,
            extract=extract_events,
            keep_events=keep_events,
            keep_records=keep_records,
            keep_arrays=keep_arrays,
            keep_reports=keep_reports,
        )
        self._failure_registry: LeaseRegistry | None = None
        self.stats = Stats(samples=stats_samples)
        self.bus = EventBus(env, network)
        ref: dict[str, Any] = {}
        # Runtime-owned bands (Gate C): agents and transport keep their
        # inboxes, timers and queues outside the tree; their kinds join the
        # pipeline next to the model-owned derivations.
        self.agents = AgentRuntime(self)
        self.transport = TransportRuntime(self)
        extra = [
            k for k in (self.agents.kind(), self.transport.kind()) if k is not None
        ]
        kinds = build_kinds(network, ref, settle_delay=settle_delay, extra=extra)
        self.pipeline = Pipeline(
            env,
            network,
            kinds,
            max_rounds_per_timestamp=max_rounds_per_timestamp,
            on_settled=self.bus.release,
        )
        ref['pipeline'] = self.pipeline
        self.timeline.round_of = self.pipeline.round_of
        network.on_delta.append(self._on_delta)
        network._simulation = self  # type: ignore[attr-defined]
        # Initial synchronous derivation with delays ignored (timeline root 0).
        self.pipeline.suspended = True
        self.timeline.initializing = True
        try:
            # A fresh runtime restarts agents another runtime initialized
            # (a fork is not a warm protocol restart), then derives the tree.
            self.agents.restart_all()
            network.converge(env.now)
        finally:
            self.pipeline.suspended = False
            self.timeline.initializing = False
        self.bus.release(env.now)  # nothing subscribes yet: drops the queue
        self.timeline.baseline(env.now, network.state)
        self.agents.bind()

    @property
    def recorder(self) -> Timeline:
        """Alias of ``timeline`` (the name used by the design document)."""
        return self.timeline

    def _on_delta(self, time: float, origin: Any, delta: Any) -> None:
        self.pipeline.on_delta(time, origin, delta)
        self.timeline.on_delta(time, origin, delta)
        self.bus.enqueue(time, origin, delta)
        if not self.pipeline.round_open(time):
            # Nothing derives from this commit: it is settled as it stands,
            # so its deliveries go out now instead of waiting for a round
            # that will never end (and retaining the delta's roots).
            self.bus.release(time)

    # -- control -------------------------------------------------------------

    def _step(self) -> None:
        if (
            self.event_budget is not None
            and self.events_dispatched >= self.event_budget
        ):
            raise BudgetExceeded(
                self.event_budget, self.events_dispatched, self.env.now
            )
        self.events_dispatched += 1
        self.env.step()

    def settle(self, max_steps: int = 100_000) -> int:
        """Process every event at the current time; returns the step count."""
        steps = 0
        while self.env.peek() == self.env.now and steps < max_steps:
            self._step()
            steps += 1
        if steps >= max_steps:
            raise RuntimeError('settle() exceeded max_steps')
        return steps

    def run_derivations(self, max_steps: int = 100_000) -> int:
        """Finish pending derivation work only: step while the next event is
        in the DEFERRED band (a stage or settled delivery), advancing time to
        delayed stages such as ``fib_delay``. Timers and other NORMAL events
        are left alone, so a recurring protocol timer never keeps this
        running. Returns the step count."""
        steps = 0
        while steps < max_steps:
            priority = self.env.peek_priority()
            if priority is None or priority < core.DEFERRED:
                break
            self._step()
            steps += 1
        if steps >= max_steps:
            raise RuntimeError('run_derivations() exceeded max_steps')
        return steps

    def run_until(self, time: float) -> None:
        if time > self.env.now:
            # The urgent horizon marker also advances an otherwise idle clock.
            from netsim.runtime.pipeline import StageEvent

            StageEvent(
                self.env, core.URGENT, time - self.env.now, lambda _: None, -1, time, 0
            )
            while self.env.peek() < time:
                self._step()
            self._step()  # horizon marker, before NORMAL/DEFERRED work at time
        self.settle()

    def run(self) -> None:
        while self.env.peek() != float('inf'):
            self._step()

    def at(self, time: float, fn: Callable[[], Any]) -> core.Event:
        """Schedule an operation at *time* (a NORMAL event)."""
        delay = time - self.env.now
        if delay < 0:
            raise ValueError('cannot schedule in the past')
        ev = self.env.timeout(delay)
        assert ev.callbacks is not None
        ev.callbacks.append(lambda e: fn())
        return ev

    def retry(self) -> None:
        self.pipeline.retry()

    def reset_agent(self, device: str, name: str, *, purge: bool = False) -> None:
        """Restart an agent: invalidate its generation (receipts, timers,
        sessions, pending runs, inbox and outbox), reset its state and run
        ``on_init``. Routes, policies, SIDs and NHT registrations stay until
        the first ``sync`` unless ``purge`` is set."""
        self.agents.reset_agent(device, name, purge=purge)

    def failures(
        self,
        source: Schedule | Process,
        horizon: float | None = None,
        *,
        risk_groups: dict[str, tuple[Entity, ...]] | None = None,
    ) -> LeaseRegistry:
        """Schedule faults through this simulation's shared lease registry.

        Process horizons and event times are absolute simulation times.
        Groups are fixed by the first call; later sources share that mapping.
        """
        if isinstance(source, Process):
            if horizon is None:
                raise ValueError('a Process requires a horizon')
            events = source.events(horizon)
        else:
            events = source.events
        if self._failure_registry is None:
            self._failure_registry = LeaseRegistry(
                self,
                risk_groups=risk_groups
                if risk_groups is not None
                else getattr(self.network, 'netsim_risk_groups', {}),
            )
        elif risk_groups is not None:
            from netsim.runtime.failures import resolve_groups

            if resolve_groups(risk_groups) != self._failure_registry.risk_groups:
                raise ValueError('risk groups cannot change within a simulation')
        self._failure_registry.schedule(events)
        return self._failure_registry

    def dirty_all(self) -> None:
        dirty_everything(self.pipeline, self.network.state, self.env.now)

    @property
    def state(self) -> NetworkState:
        return self.network.state

    # -- timed packets --------------------------------------------------------

    def send(self, device: str, packet: Any, max_hops: int = 64) -> core.Process:
        """Deliver a packet hop by hop through the DES, waiting ``link.delay``
        per hop, so mid-flight state changes are honoured."""
        from netsim.model import forwarding as fw
        from netsim.model.packets import (
            ETHERTYPE_IPV4,
            ORIGINATED,
            EthernetFrame,
            IPv4Packet,
        )

        env = self.env
        net = self.network

        def process():
            hops = []
            current = device
            ingress = None
            frame = None
            pkt = packet
            for _ in range(max_hops):
                view = net.view(current)
                if frame is None:
                    res = fw.forward_ip(view, pkt, None, ORIGINATED)
                else:
                    assert ingress is not None
                    res = fw.receive_frame(view, ingress, frame)
                hops.append(
                    fw.Hop(
                        current,
                        ingress,
                        res.egress,
                        res.member,
                        res.link_id,
                        res.edge_id,
                        res.packet,
                        res.outcome,
                        res.reason,
                    )
                )
                if res.outcome != fw.TRANSMIT:
                    return fw.Trace(tuple(hops), res.outcome, res.reason)
                assert res.link_id is not None and res.peer is not None
                link = net.state.links[res.link_id]
                if link.config.delay > 0:
                    yield env.timeout(link.config.delay)
                pkt = res.packet
                ethertype = ETHERTYPE_IPV4 if isinstance(pkt, IPv4Packet) else 0x86DD
                frame = EthernetFrame(
                    res.mac_dst or 0, res.mac_src or 0, ethertype, pkt
                )
                current, ingress = res.peer
            return fw.Trace(tuple(hops), fw.DROP, fw.LOOP)

        return env.process(process(), name=f'send:{device}')


__all__ = ['Simulation', 'BudgetExceeded', 'derive']
