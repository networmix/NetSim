"""Transport: link channels and sessions (Gate C slice C3).

``LinkChannel(device, interface)`` datagrams and ``Session`` connections
(a reliable-message abstraction, not TCP) as specified in the design's
"Transport" subsection: physical transfer and receive availability checked
at send and delivery, ordered release per channel incarnation, distinct
connection ids, abort versus drain-then-close, no-progress timeouts and
bounded admission with explicit rejections. The TRANSPORT kind derives
path reachability in ``NetworkState.transport``.

The runtime interface used by publication (``netsim.runtime.agents``):
``send_datagram``, ``send_message``, ``session_op`` and ``cancel_agent``;
deliveries call back ``AgentRuntime.deliver``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from netsim.model.contracts import Datagram, Message, SessionOp
from netsim.runtime.pipeline import Kind

if TYPE_CHECKING:
    from netsim.runtime.simulation import Simulation


class TransportRuntime:
    """Placeholder filled by slice C3; the interface is the contract above."""

    def __init__(self, sim: Simulation) -> None:
        self.sim = sim

    def kind(self) -> Kind | None:
        """The TRANSPORT ``Kind`` bound to this simulation, or ``None``."""
        return None

    def send_datagram(
        self, device: str, agent: str, generation: int, datagram: Datagram
    ) -> Any:
        raise NotImplementedError('slice C3')

    def send_message(
        self, device: str, agent: str, generation: int, message: Message
    ) -> Any:
        raise NotImplementedError('slice C3')

    def session_op(
        self, device: str, agent: str, generation: int, op: SessionOp
    ) -> Any:
        raise NotImplementedError('slice C3')

    def cancel_agent(self, device: str, agent: str, generation: int) -> None:
        raise NotImplementedError('slice C3')

    def budget(self) -> dict[str, Any]:
        """Live counters: in-flight datagrams, queued messages and bytes."""
        return {}


__all__ = ['TransportRuntime']
