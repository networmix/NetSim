"""Agent scheduler: the AGENT kind, subscriptions, inboxes, timers and
publication (Gate C slice C1).

This module owns every runtime object of an agent that is not in the tree:
the inbox deque and its captured prefix, armed timers, the per-run RNG
handoff and stat buffers, the subscription index and the receipt journal.
The tree keeps ``contracts.AgentNode``; the transport keeps queues.

Contract between this runtime and ``netsim.runtime.transport`` (both sides
are implemented by their own slice; the method names below are fixed):

- ``AgentRuntime.deliver(device, agent, entry)`` appends an inbox entry for
  the agent's *current* generation and schedules a run (``run_delay``);
  it returns ``False`` when the agent is gone, the generation is stale or
  the inbox is full (the caller reports the outcome, never drops silently).
- ``AgentRuntime.generation(device, agent)`` is the live generation or
  ``None``.
- ``TransportRuntime.send_datagram / send_message / session_op`` are called
  by publication with the agent's generation; ``TransportRuntime.
  cancel_agent(device, agent, generation)`` aborts everything of a
  generation on reset or removal.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from netsim.model.contracts import InboxEntry
from netsim.runtime.pipeline import Kind

if TYPE_CHECKING:
    from netsim.runtime.simulation import Simulation


class AgentRuntime:
    """Placeholder filled by slice C1; the interface is the contract above."""

    def __init__(self, sim: Simulation) -> None:
        self.sim = sim

    def kind(self) -> Kind | None:
        """The AGENT ``Kind`` bound to this simulation, or ``None``."""
        return None

    def bind(self) -> None:
        """Called once after the initial convergence: schedule ``on_init``
        for every registered agent whose node is not initialized."""

    def generation(self, device: str, agent: str) -> int | None:
        node = self.sim.network.state.devices.get(device)
        entry = node.agents.get(agent) if node is not None else None
        return None if entry is None else entry.generation

    def deliver(self, device: str, agent: str, entry: InboxEntry) -> bool:
        raise NotImplementedError('slice C1')

    def reset_agent(self, device: str, name: str, *, purge: bool = False) -> None:
        raise NotImplementedError('slice C1')

    def budget(self) -> dict[str, Any]:
        """Live counters: scheduled runs, inbox sizes, stale events."""
        return {}


__all__ = ['AgentRuntime']
