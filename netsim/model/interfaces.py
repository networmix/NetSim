"""Interface records and the pure rules that derive their oper state.

Enums are API-boundary values; records store their ``int`` values so the
hot paths compare small ints. ``OperState`` uses the RFC 2863
ifOperStatus numbering, which the Linux kernel ``operstate`` also uses.
"""

from __future__ import annotations

from dataclasses import field
from enum import IntEnum
from typing import Callable

from netsim.model.addressing import IPV4
from netsim.model.links import LINK_UP, LinkOper
from netsim.model.state import PMap, empty_pmap, record


class IfKind(IntEnum):
    LOOPBACK = 1
    ETHERNET = 2
    PORT_CHANNEL = 3

    @property
    def iana_name(self) -> str:
        return {1: 'softwareLoopback', 2: 'ethernetCsmacd', 3: 'ieee8023adLag'}[
            int(self)
        ]


class AdminState(IntEnum):
    DOWN = 0
    UP = 1


class OperState(IntEnum):
    """RFC 2863 ifOperStatus values."""

    UP = 1
    DOWN = 2
    TESTING = 3
    UNKNOWN = 4
    DORMANT = 5
    NOT_PRESENT = 6
    LOWER_LAYER_DOWN = 7


class StateReason(IntEnum):
    INIT = 0
    UP = 1
    ADMIN = 2
    DEVICE_DISABLED = 3
    NO_LINK = 4
    CARRIER = 5
    PEER_ADMIN = 6
    MIN_LINKS = 7
    PARTNER = 8
    MEMBERS = 9
    MULTIPLE_PARTNERS = 10


Address = tuple[int, int]
"""``(host address int, prefix length)``."""


# -- config records -----------------------------------------------------------


@record
class LoopbackConfig:
    admin: int = AdminState.UP
    admin_since: float = 0.0
    description: str = ''
    ipv4: tuple[Address, ...] = ()
    ipv6: tuple[Address, ...] = ()


@record
class EthernetConfig:
    admin: int = AdminState.UP
    admin_since: float = 0.0
    description: str = ''
    mtu: int = 1500
    speed: float = 10e9
    metric: int = 1
    ipv4: tuple[Address, ...] = ()
    ipv6: tuple[Address, ...] = ()
    unnumbered: bool = False
    forwarding_v4: bool | None = None
    forwarding_v6: bool | None = None
    aggregate_id: str | None = None
    carrier_delay_up: float = 0.0
    carrier_delay_down: float = 0.0


@record
class PortChannelConfig:
    admin: int = AdminState.UP
    admin_since: float = 0.0
    description: str = ''
    mtu: int = 1500
    metric: int = 1
    ipv4: tuple[Address, ...] = ()
    ipv6: tuple[Address, ...] = ()
    unnumbered: bool = False
    forwarding_v4: bool | None = None
    forwarding_v6: bool | None = None
    min_links: int = 1
    member_delay_up: float = 0.0
    member_delay_down: float = 0.0


# -- oper records -------------------------------------------------------------


@record
class LoopbackOper:
    oper: int = OperState.DOWN
    reason: int = StateReason.INIT
    since: float = 0.0


@record
class EthernetOper:
    oper: int = OperState.DOWN
    reason: int = StateReason.INIT
    since: float = 0.0
    carrier_raw: bool = False
    carrier_raw_since: float = 0.0
    carrier_effective: bool = False


@record
class MemberOper:
    candidate: bool = False
    candidate_since: float = 0.0
    active: bool = False


@record
class PortChannelOper:
    oper: int = OperState.DOWN
    reason: int = StateReason.INIT
    since: float = 0.0
    members: PMap[str, MemberOper] = field(default_factory=empty_pmap)
    bandwidth: float = 0.0


# -- nodes --------------------------------------------------------------------


@record
class LoopbackNode:
    name: str
    index: int
    generation: int
    config: LoopbackConfig = field(default_factory=LoopbackConfig)
    oper: LoopbackOper = field(default_factory=LoopbackOper)

    kind = IfKind.LOOPBACK


@record
class EthernetNode:
    name: str
    index: int
    generation: int
    mac: int
    config: EthernetConfig = field(default_factory=EthernetConfig)
    oper: EthernetOper = field(default_factory=EthernetOper)
    link: str | None = None
    """Link id, assigned by the builder."""

    kind = IfKind.ETHERNET


@record
class PortChannelNode:
    name: str
    index: int
    generation: int
    mac: int
    config: PortChannelConfig = field(default_factory=PortChannelConfig)
    oper: PortChannelOper = field(default_factory=PortChannelOper)

    kind = IfKind.PORT_CHANNEL


InterfaceNode = LoopbackNode | EthernetNode | PortChannelNode


# -- helpers ------------------------------------------------------------------


def _with_since(
    old_since: float, old_oper: int, old_reason: int, oper: int, reason: int, now: float
) -> float:
    return old_since if (old_oper == oper and old_reason == reason) else now


def has_global_address(
    config: EthernetConfig | PortChannelConfig | LoopbackConfig, af: int
) -> bool:
    return bool(config.ipv4 if af == IPV4 else config.ipv6)


def forwarding_enabled(config: EthernetConfig | PortChannelConfig, af: int) -> bool:
    """Explicit per-AF flag, else enabled iff a global address of that AF or ``unnumbered``."""
    flag = config.forwarding_v4 if af == IPV4 else config.forwarding_v6
    if flag is not None:
        return flag
    return has_global_address(config, af) or config.unnumbered


def l3_usable(node: InterfaceNode, af: int) -> bool:
    """Oper UP, not bundled, and forwarding enabled for *af*."""
    if node.oper.oper != OperState.UP:
        return False
    if isinstance(node, LoopbackNode):
        return has_global_address(node.config, af)
    if isinstance(node, EthernetNode) and node.config.aggregate_id is not None:
        return False
    return forwarding_enabled(node.config, af)


def is_bundled(node: InterfaceNode) -> bool:
    return isinstance(node, EthernetNode) and node.config.aggregate_id is not None


# -- derivations --------------------------------------------------------------


def derive_loopback(
    config: LoopbackConfig, device_enabled: bool, old: LoopbackOper, now: float
) -> LoopbackOper:
    if not device_enabled:
        oper, reason = OperState.DOWN, StateReason.DEVICE_DISABLED
    elif config.admin != AdminState.UP:
        oper, reason = OperState.DOWN, StateReason.ADMIN
    else:
        oper, reason = OperState.UP, StateReason.UP
    since = _with_since(old.since, old.oper, old.reason, oper, reason, now)
    new = LoopbackOper(oper, reason, since)
    return old if new == old else new


def raw_carrier(
    link: LinkOper | None,
    peer_config: EthernetConfig | None,
    peer_device_enabled: bool,
) -> tuple[bool, int]:
    """Physical carrier as seen before debounce, with the failing reason."""
    if link is None or peer_config is None:
        return False, StateReason.NO_LINK
    if link.state != LINK_UP:
        return False, StateReason.CARRIER
    if peer_config.admin != AdminState.UP or not peer_device_enabled:
        return False, StateReason.PEER_ADMIN
    return True, StateReason.UP


def derive_ethernet(
    node: EthernetNode,
    device_enabled: bool,
    link: LinkOper | None,
    peer_config: EthernetConfig | None,
    peer_device_enabled: bool,
    effective_carrier: bool | None,
    now: float,
) -> EthernetOper:
    """Oper state from admin, presence and (debounced) carrier.

    ``effective_carrier`` is the carrier after debounce; the clock-free
    path passes ``None`` to use the raw carrier directly. ``carrier_raw``
    and ``carrier_raw_since`` are recorded on every raw change so the
    runtime's CARRIER kind can schedule the effective transition.
    """
    old = node.oper
    raw, raw_reason = raw_carrier(link, peer_config, peer_device_enabled)
    raw_since = old.carrier_raw_since if raw == old.carrier_raw else now
    if effective_carrier is None:
        effective_carrier = raw
    config = node.config
    if not device_enabled:
        oper, reason = OperState.DOWN, StateReason.DEVICE_DISABLED
    elif config.admin != AdminState.UP:
        oper, reason = OperState.DOWN, StateReason.ADMIN
    elif node.link is None:
        oper, reason = OperState.NOT_PRESENT, StateReason.NO_LINK
    elif effective_carrier:
        oper, reason = OperState.UP, StateReason.UP
    else:
        oper, reason = (
            OperState.DOWN,
            (raw_reason if raw_reason != StateReason.UP else StateReason.CARRIER),
        )
    since = _with_since(old.since, old.oper, old.reason, oper, reason, now)
    new = EthernetOper(oper, reason, since, raw, raw_since, effective_carrier)
    return old if new == old else new


@record
class MemberInput:
    """What the LAG derivation needs to know about one member port."""

    name: str
    index: int
    oper_up: bool
    peer_bundle: tuple[str, str] | None
    """``(device, PortChannel name)`` of the peer port's bundle, if bundled."""
    link_capacity: float


@record
class BundleInput:
    key: tuple[str, str]
    config: PortChannelConfig
    device_enabled: bool
    members: tuple[MemberInput, ...]
    old: PortChannelOper


def derive_lag_component(
    bundles: dict[tuple[str, str], BundleInput],
    now: float,
    delay_elapsed: Callable[[tuple[str, str], str, float], bool] | None = None,
) -> dict[tuple[str, str], PortChannelOper]:
    """Three-pass derivation for one connected component of bundles.

    Pass 1 computes candidates from one snapshot, pass 2 every bundle's
    ``min_links_ok`` ("I am distributing"), pass 3 active sets, oper and
    bandwidth, so the result cannot depend on the order bundles are
    visited. ``delay_elapsed(bundle_key, member_name, candidate_since)``
    returns whether a member's join delay has elapsed; ``None`` means no
    delays (clock-free).
    """
    candidates: dict[tuple[str, str], dict[str, MemberOper]] = {}
    partner: dict[tuple[str, str], tuple[str, str] | None] = {}
    invalid: set[tuple[str, str]] = set()
    for key, b in bundles.items():
        partners = {m.peer_bundle for m in b.members if m.peer_bundle is not None}
        if len(partners) > 1:
            invalid.add(key)
            partner[key] = None
        else:
            partner[key] = next(iter(partners)) if partners else None
        members: dict[str, MemberOper] = {}
        for m in b.members:
            cand = key not in invalid and m.oper_up and m.peer_bundle is not None
            old_m = b.old.members.get(m.name)
            cand_since = (
                old_m.candidate_since
                if (old_m is not None and old_m.candidate == cand)
                else now
            )
            members[m.name] = MemberOper(cand, cand_since, False)
        candidates[key] = members

    def min_links_ok(key: tuple[str, str]) -> bool:
        b = bundles.get(key)
        if b is None:
            return False
        n = sum(1 for mo in candidates[key].values() if mo.candidate)
        return (
            b.device_enabled
            and b.config.admin == AdminState.UP
            and n >= b.config.min_links
        )

    out: dict[tuple[str, str], PortChannelOper] = {}
    for key, b in bundles.items():
        peer = partner[key]
        local_ok = min_links_ok(key)
        peer_ok = local_ok and (min_links_ok(peer) if peer is not None else False)
        members = candidates[key]
        active_names = []
        new_members: dict[str, MemberOper] = {}
        for m in b.members:
            mo = members[m.name]
            elapsed = (
                True
                if delay_elapsed is None
                else bool(delay_elapsed(key, m.name, mo.candidate_since))
            )  # type: ignore[operator]
            active = mo.candidate and peer_ok and elapsed
            new_members[m.name] = MemberOper(mo.candidate, mo.candidate_since, active)
            if active:
                active_names.append(m.name)
        by_name = {m.name: m for m in b.members}
        bandwidth = float(sum(by_name[n].link_capacity for n in active_names))
        if not b.device_enabled:
            oper, reason = OperState.DOWN, StateReason.DEVICE_DISABLED
        elif b.config.admin != AdminState.UP:
            oper, reason = OperState.DOWN, StateReason.ADMIN
        elif key in invalid:
            oper, reason = OperState.LOWER_LAYER_DOWN, StateReason.MULTIPLE_PARTNERS
        elif not b.members:
            oper, reason = OperState.LOWER_LAYER_DOWN, StateReason.MEMBERS
        elif len(active_names) >= b.config.min_links:
            oper, reason = OperState.UP, StateReason.UP
        elif not local_ok:
            oper, reason = OperState.LOWER_LAYER_DOWN, StateReason.MIN_LINKS
        else:
            oper, reason = OperState.LOWER_LAYER_DOWN, StateReason.PARTNER
        old = b.old
        since = _with_since(old.since, old.oper, old.reason, oper, reason, now)
        # Reuse member records that did not change (canonicalization).
        mb = old.members.builder()
        for n, mo in new_members.items():
            if old.members.get(n) != mo:
                mb.set(n, mo)
        for n in list(old.members):
            if n not in new_members:
                mb.remove(n)
        new = PortChannelOper(oper, reason, since, mb.build(), bandwidth)
        out[key] = old if new == old else new
    return out
