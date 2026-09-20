"""Point-to-point links between two Ethernet interfaces."""

from __future__ import annotations

from netsim.model.state import record

LINK_UP = 1
LINK_FAILED = 0

Endpoint = tuple[str, str]
"""``(device name, interface name)``."""


@record
class LinkConfig:
    capacity: float | None = None
    """Bit/s override; ``None`` means min of the endpoint speeds."""
    delay: float = 0.0
    risk_groups: tuple[str, ...] = ()


@record
class LinkOper:
    state: int = LINK_UP
    since: float = 0.0


@record
class LinkNode:
    id: str
    index: int
    generation: int
    a: Endpoint
    b: Endpoint
    config: LinkConfig
    oper: LinkOper

    def other(self, endpoint: Endpoint) -> Endpoint:
        if endpoint == self.a:
            return self.b
        if endpoint == self.b:
            return self.a
        raise ValueError(f'{endpoint} is not an endpoint of {self.id}')

    def direction(self, tx: Endpoint) -> int:
        """0 for a→b, 1 for b→a (``ext_edge_id = 2*index + direction``)."""
        if tx == self.a:
            return 0
        if tx == self.b:
            return 1
        raise ValueError(f'{tx} is not an endpoint of {self.id}')

    def edge_id(self, tx: Endpoint) -> int:
        return 2 * self.index + self.direction(tx)


def canonical_endpoints(x: Endpoint, y: Endpoint) -> tuple[Endpoint, Endpoint]:
    return (x, y) if x <= y else (y, x)


def link_id(x: Endpoint, y: Endpoint) -> str:
    a, b = canonical_endpoints(x, y)
    return f'{a[0]}:{a[1]}--{b[0]}:{b[1]}'


# -- physical transfer availability ------------------------------------------

LINK_DOWN = 'LINK_DOWN'
RX_DOWN = 'RX_DOWN'


def transfer_blocked(
    link_state: int,
    tx_admin_up: bool,
    tx_device_enabled: bool,
    rx_admin_up: bool,
    rx_device_enabled: bool,
) -> str | None:
    """Why a directed edge cannot carry traffic right now, or ``None``.

    Physical availability is independent of observed oper state: during a
    carrier delay routing may still point at a failed link, and this test
    is what turns that into ``LINK_DOWN`` drops instead of carried traffic.
    """
    if link_state != LINK_UP or not tx_admin_up or not tx_device_enabled:
        return LINK_DOWN
    if not rx_admin_up or not rx_device_enabled:
        return RX_DOWN
    return None
