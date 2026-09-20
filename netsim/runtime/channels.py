"""Physical channel queries. No protocol sees these remote model details.

Bundles sample the minimum-delay physically available active member (link id
breaks ties). A datagram retains that wire and its endpoint incarnations;
changing the membership or delay never reorders the channel's release queue.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Any

from netsim.model.addressing import MacAddress, is_link_local_v6
from netsim.model.contracts import Endpoint
from netsim.model.interfaces import EthernetNode, PortChannelNode, l3_usable
from netsim.model.links import transfer_blocked
from netsim.model.network import _View
from netsim.model.state import NetworkState


def future(now: float, delay: float) -> float:
    target = float(now + delay)
    if not isfinite(delay) or delay <= 0 or not isfinite(target) or target <= now:
        raise ValueError('transport delay must produce a strictly future finite time')
    return target


def interface(state: NetworkState, device: str, name: str | None) -> Any:
    dev = state.devices.get(device)
    return dev.interfaces.get(name) if dev is not None and name is not None else None


def scoped(endpoint: Endpoint) -> bool:
    return endpoint.af == 6 and is_link_local_v6(endpoint.address)


def configured(state: NetworkState, device: str, ep: Endpoint) -> Any:
    dev = state.devices.get(device)
    if dev is None or ep.af not in (4, 6):
        return None
    if scoped(ep):
        node = interface(state, device, ep.scope)
        if isinstance(node, (EthernetNode, PortChannelNode)) and (
            ep.address == MacAddress(node.mac).link_local_int()
            or any(a == ep.address for a, _ in node.config.ipv6)
        ):
            return node
        return None
    for _, node in dev.interfaces.sorted_items():
        addresses = node.config.ipv4 if ep.af == 4 else node.config.ipv6
        if any(a == ep.address for a, _ in addresses):
            return node
    return None


def source_address(node: Any, af: int) -> int | None:
    addresses = node.config.ipv4 if af == 4 else node.config.ipv6
    if addresses:
        return min(a for a, _ in addresses)
    return MacAddress(node.mac).link_local_int() if af == 6 else None


@dataclass(frozen=True, slots=True)
class Wire:
    link: str
    link_generation: int
    tx: tuple[str, str]
    rx: tuple[str, str]
    local: str
    remote: str
    generations: tuple[int, int, int, int]
    delay: float

    def blocked(self, state: NetworkState, af: int, *, effective: bool) -> str | None:
        link = state.links.get(self.link)
        tx = interface(state, *self.tx)
        rx = interface(state, *self.rx)
        local = interface(state, self.tx[0], self.local)
        remote = interface(state, self.rx[0], self.remote)
        if (
            link is None
            or link.generation != self.link_generation
            or tx is None
            or rx is None
            or local is None
            or remote is None
            or (tx.generation, rx.generation, local.generation, remote.generation)
            != self.generations
            or tx.link != self.link
            or rx.link != self.link
        ):
            return 'STALE_ENDPOINT'
        if effective and not eligible(local, af):
            return 'INTERFACE_DOWN'
        view = _View(state, self.tx[0])
        reason = transfer_blocked(
            link.oper.state,
            *view.endpoint_usable(self.tx),
            *view.endpoint_usable(self.rx),
        )
        if reason is not None:
            return reason
        if local.config.admin != 1:
            return 'INTERFACE_DOWN'
        if remote.config.admin != 1:
            return 'RX_DOWN'
        if self.local != self.tx[1] and tx.config.aggregate_id != self.local:
            return 'INTERFACE_DOWN'
        if self.remote != self.rx[1] and rx.config.aggregate_id != self.remote:
            return 'RX_DOWN'
        return None


def eligible(node: Any, af: int) -> bool:
    if af not in (4, 6) or not isinstance(node, (EthernetNode, PortChannelNode)):
        return False
    return (
        node.oper.oper == 1
        if isinstance(node, PortChannelNode)
        else l3_usable(node, af)
    )


def wires(
    state: NetworkState, device: str, name: str, *, physical_only: bool = False
) -> tuple[Wire, ...]:
    node = interface(state, device, name)
    if not isinstance(node, (EthernetNode, PortChannelNode)):
        return ()
    members = (name,)
    if isinstance(node, PortChannelNode):
        members = tuple(sorted(n for n, m in node.oper.members.items() if m.active))
        # A route-free session can discover its physical peer even while the
        # effective bundle has no active members (for dependencies/recovery).
        if physical_only and not members:
            members = tuple(
                sorted(
                    n
                    for n, m in state.devices[device].interfaces.items()
                    if isinstance(m, EthernetNode) and m.config.aggregate_id == name
                )
            )
    out = []
    for member in members:
        tx = interface(state, device, member)
        link = state.links.get(tx.link) if tx is not None else None
        if link is None:
            continue
        peer = link.other((device, member))
        rx = interface(state, *peer)
        if not isinstance(rx, EthernetNode):
            continue
        remote_name = rx.config.aggregate_id or peer[1]
        remote = interface(state, peer[0], remote_name)
        if remote is None:
            continue
        out.append(
            Wire(
                link.id,
                link.generation,
                (device, member),
                peer,
                name,
                remote_name,
                (tx.generation, rx.generation, node.generation, remote.generation),
                link.config.delay,
            )
        )
    return tuple(sorted(out, key=lambda w: (w.delay, w.link)))
