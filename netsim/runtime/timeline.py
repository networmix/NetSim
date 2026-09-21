"""The timeline: an append-only log of what happened, in order.

Every committed delta becomes one ``Record(seq, time, round, origin)`` and
a list of compact, typed **events** extracted from it (device and link
lifecycle, interface transitions with RFC 2863 names and reasons, carrier
debounce, bundle membership, RIB rows, FIB entries with their resolved
next hops, demands, placement summaries). Events are frozen records of
plain values with stable entity keys, so post-analysis needs no state
tree; ``rows()`` and ``to_csv()`` flatten them for pandas or a file.

Retention is explicit: events and records are kept for the whole run;
raw deltas and roots live in bounded deques, because a delta references
two whole roots. Placement events carry the per-edge arrays (24 bytes per
directed edge) unless ``keep_arrays`` is off, and never the report unless
``keep_reports`` is on.
"""

from __future__ import annotations

import csv
import dataclasses
from collections import deque
from dataclasses import field
from typing import Any, Callable, Iterable, Iterator

from netsim.model import forwarding as fw
from netsim.model import routing, srv6
from netsim.model.addressing import IPV4, IPV6, to_address
from netsim.model.interfaces import (
    EthernetNode,
    OperState,
    PortChannelNode,
    StateReason,
)
from netsim.model.links import LINK_UP
from netsim.model.state import (
    FloatArray,
    NetworkState,
    PMap,
    StateDelta,
    diff_pmap,
    record,
)

# ---------------------------------------------------------------------------
# Names for small ints (no shared lookup tables on the extraction path)
# ---------------------------------------------------------------------------


def _action_name(action: int) -> str:
    if action == fw.FORWARD:
        return 'FORWARD'
    if action == fw.RECEIVE:
        return 'RECEIVE'
    if action == fw.DROP_BLACKHOLE:
        return 'DROP_BLACKHOLE'
    if action == fw.DROP_UNREACHABLE:
        return 'DROP_UNREACHABLE'
    if action == fw.DROP_PROHIBIT:
        return 'DROP_PROHIBIT'
    if action == fw.SRV6_LOCAL:
        return 'SRV6_LOCAL'
    return str(action)


def _special_name(special: int) -> str:
    if special == routing.BLACKHOLE:
        return 'blackhole'
    if special == routing.UNREACHABLE:
        return 'unreachable'
    if special == routing.PROHIBIT:
        return 'prohibit'
    if special == routing.RECEIVE_NH:
        return 'receive'
    if special == routing.SRV6_LOCAL_NH:
        return 'srv6-local'
    return f'special:{special}'


def _oper_name(value: int) -> str:
    try:
        return OperState(value).name
    except ValueError:
        return str(value)


def _reason_name(value: int) -> str:
    try:
        return StateReason(value).name
    except ValueError:
        return str(value)


def _prefix_str(prefix: tuple[int, int], af: int) -> str:
    return f'{to_address(prefix[0], af)}/{prefix[1]}'  # type: ignore[arg-type]


def _fmt(name: str, value: Any) -> str:
    """Human-readable config value: prefixes, MACs, everything else repr."""
    if value is None:
        return ''
    if name in ('ipv4', 'ipv6') and isinstance(value, (tuple, list)):
        af = IPV4 if name == 'ipv4' else IPV6
        try:
            return ','.join(_prefix_str(p, af) for p in value)
        except Exception:
            return repr(value)
    if name == 'mac' and isinstance(value, int):
        return f'{value:012x}'
    return repr(value)


def _changes(old: Any, new: Any) -> tuple[tuple[str, str, str], ...]:
    """``(field, old, new)`` for every differing dataclass field."""
    out = []
    for f in dataclasses.fields(new):
        ov = getattr(old, f.name, None) if old is not None else None
        nv = getattr(new, f.name)
        if ov != nv:
            out.append((f.name, _fmt(f.name, ov), _fmt(f.name, nv)))
    return tuple(out)


def _nexthop_str(nh: Any, af: int) -> str:
    if nh.special is not None:
        return _special_name(nh.special)
    parts = []
    if nh.interface:
        parts.append(nh.interface)
    if nh.address is not None:
        parts.append(str(to_address(nh.address, nh.af or af)))  # type: ignore[arg-type]
    if nh.weight != 1:
        parts.append(f'w{nh.weight}')
    return ' '.join(parts)


def _adjacency_names(fib: Any, entry: Any, af: int) -> tuple[str, ...]:
    """Resolved legs of a FIB entry: ``interface [address] [wN] [unresolved]``."""
    if fib is None or entry is None or entry.group_id is None:
        return ()
    group = fib.groups.get(entry.group_id)
    if group is None:
        return ()
    out = []
    for a in group.adjacencies:
        s = a.interface
        if a.nexthop is not None:
            s += f' {to_address(a.nexthop, a.af or af)}'  # type: ignore[arg-type]
        if a.weight != 1:
            s += f' w{a.weight}'
        if a.mac is None:
            s += ' unresolved'
        out.append(s)
    return tuple(out)


# ---------------------------------------------------------------------------
# Records and events
# ---------------------------------------------------------------------------


@record
class Origin:
    kind: str
    """``op`` (an operation), ``stage`` (a kind run) or ``init``."""
    name: str
    """Operation name or kind name."""
    entity: str = ''

    def __str__(self) -> str:
        return f'{self.kind}:{self.name}' + (f'({self.entity})' if self.entity else '')


def origin_from(raw: Any, *, initializing: bool = False) -> tuple[Origin, int | None]:
    """``(origin, round)``: the round comes with kind origins (their
    generation); operations report ``None`` and take the pipeline's answer."""
    if isinstance(raw, Origin):
        return raw, None
    if isinstance(raw, tuple) and raw:
        if raw[0] == 'kind':
            items: tuple[Any, ...] = tuple(raw)
            gen = items[2] if len(items) > 2 and isinstance(items[2], int) else None
            return Origin('init' if initializing else 'stage', str(raw[1])), gen
        return Origin('op', str(raw[0]), ','.join(str(x) for x in raw[1:])), None
    return Origin('init' if initializing else 'op', str(raw)), None


@record
class Record:
    seq: int
    time: float
    round: int
    origin: Origin
    event_count: int
    version: int
    """Tree version after the commit."""


class Event:
    """Base for timeline events; subclasses are frozen slotted records with
    ``seq`` (the record), ``idx`` (position in the record), ``time``,
    ``round`` and ``origin`` first."""

    __slots__ = ()

    seq: int
    idx: int
    time: float
    round: int
    origin: Origin

    @property
    def kind(self) -> str:
        return type(self).__name__

    def row(self) -> dict[str, Any]:
        d: dict[str, Any] = {'event': self.kind}
        for f in dataclasses.fields(self):  # type: ignore[arg-type]
            if f.compare:
                d[f.name] = getattr(self, f.name)
        d['origin'] = str(self.origin)
        return d


@record
class LocatorEvent(Event):
    seq: int
    idx: int
    time: float
    round: int
    origin: Origin
    device: str
    action: str
    name: str
    prefix: str


@record
class SidEvent(Event):
    seq: int
    idx: int
    time: float
    round: int
    origin: Origin
    device: str
    action: str
    sid: str
    behavior: str
    flavors: tuple[str, ...]
    owner: str
    interface: str | None
    adjacency_up: bool


@record
class PolicyEvent(Event):
    seq: int
    idx: int
    time: float
    round: int
    origin: Origin
    device: str
    action: str
    color: int
    endpoint: str
    name: str | None
    owner: str
    active_path: int | None
    status: str
    reasons: tuple[tuple[int, int, str], ...]
    programmed_version: int
    basic_valid: tuple[tuple[int, int], ...] = ()
    """RFC 9256 §5.1 lists, including mandatory first-entry resolution."""
    strict_valid: tuple[tuple[int, int], ...] = ()
    first_valid: tuple[tuple[int, int], ...] = ()
    programming: str = 'PENDING'


@record
class AgentRunEvent(Event):
    seq: int
    idx: int
    time: float
    round: int
    origin: Origin
    device: str
    agent: str
    generation: int
    run_id: int
    status: str
    causes_count: int
    ops_count: int


@record
class DeviceEvent(Event):
    seq: int
    idx: int
    time: float
    round: int
    origin: Origin
    device: str
    action: str
    """``added`` | ``removed`` | ``config``."""
    changes: tuple[tuple[str, str, str], ...] = ()


@record
class LinkStateEvent(Event):
    seq: int
    idx: int
    time: float
    round: int
    origin: Origin
    link: str
    old: str
    new: str
    """``UP`` | ``FAILED`` | ``ABSENT``."""


@record
class LinkConfigEvent(Event):
    seq: int
    idx: int
    time: float
    round: int
    origin: Origin
    link: str
    changes: tuple[tuple[str, str, str], ...]


@record
class InterfaceOperEvent(Event):
    seq: int
    idx: int
    time: float
    round: int
    origin: Origin
    device: str
    interface: str
    old: str
    new: str
    reason: str
    carrier_raw: bool | None = None


@record
class CarrierRawEvent(Event):
    """Raw carrier changed while the debounced oper state did not (yet)."""

    seq: int
    idx: int
    time: float
    round: int
    origin: Origin
    device: str
    interface: str
    carrier_raw: bool
    effective: bool


@record
class LagEvent(Event):
    seq: int
    idx: int
    time: float
    round: int
    origin: Origin
    device: str
    portchannel: str
    oper: str
    reason: str
    active_members: tuple[str, ...]
    bandwidth: float


@record
class ConfigEvent(Event):
    """An interface's configuration (or the interface itself) changed."""

    seq: int
    idx: int
    time: float
    round: int
    origin: Origin
    device: str
    interface: str
    action: str
    """``added`` | ``removed`` | ``changed``."""
    changes: tuple[tuple[str, str, str], ...] = ()


@record
class RouteEvent(Event):
    seq: int
    idx: int
    time: float
    round: int
    origin: Origin
    device: str
    af: int
    prefix: str
    source: str
    instance: int
    distinguisher: str
    action: str
    """``add`` | ``delete`` | ``update``."""
    distance: int
    metric: int
    nexthops: tuple[str, ...]


@record
class FibEvent(Event):
    seq: int
    idx: int
    time: float
    round: int
    origin: Origin
    device: str
    af: int
    prefix: str
    action: str
    """``install`` | ``withdraw`` | ``update``."""
    fib_action: str
    nexthops: tuple[str, ...]


@record
class DemandEvent(Event):
    seq: int
    idx: int
    time: float
    round: int
    origin: Origin
    demand: str
    action: str
    source: str
    dst: str
    af: int
    mode: str
    rate: float


@record
class PlacementEvent(Event):
    """A new placement: totals plus (by default) the per-edge arrays, so
    series never need the report or a root."""

    seq: int
    idx: int
    time: float
    round: int
    origin: Origin
    version: int
    delivered: float
    dropped: tuple[tuple[str, float], ...]
    max_utilization: float
    oversubscribed: int
    offered: FloatArray | None = field(compare=False, repr=False, default=None)
    carried: FloatArray | None = field(compare=False, repr=False, default=None)
    capacity: FloatArray | None = field(compare=False, repr=False, default=None)
    demand_delivered: tuple[tuple[str, float], ...] = field(
        compare=False, repr=False, default=()
    )
    report: Any = field(default=None, compare=False, repr=False)
    """The full report, kept only when the timeline was asked to keep reports."""

    def utilization(self, edge_id: int) -> float | None:
        """Carried over capacity for a directed edge; ``None`` when the
        arrays were not kept or the edge did not exist yet."""
        if self.carried is None or self.capacity is None:
            return None
        if edge_id < 0 or edge_id >= len(self.capacity):
            return None
        cap = self.capacity[edge_id]
        return self.carried[edge_id] / cap if cap > 0 else float('inf')

    def row(self) -> dict[str, Any]:
        # Explicit base call: zero-argument super() does not work inside a
        # slots dataclass before Python 3.14 (the decorator recreates the class).
        d = Event.row(self)
        d['dropped'] = dict(self.dropped)
        return d


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------


def extract_events(
    delta: StateDelta,
    seq: int,
    time: float,
    round_: int,
    origin: Origin,
    *,
    keep_report: bool = False,
    keep_arrays: bool = True,
) -> list[Event]:
    """Semantic events from one delta, in a deterministic order."""
    out: list[Event] = []
    old, new = delta.old, delta.new

    def emit(cls: type, **kw: Any) -> None:
        out.append(
            cls(seq=seq, idx=len(out), time=time, round=round_, origin=origin, **kw)
        )

    for lid in delta.links().keys:
        ol, nl = old.links.get(lid), new.links.get(lid)
        os_ = (
            'ABSENT' if ol is None else ('UP' if ol.oper.state == LINK_UP else 'FAILED')
        )
        ns_ = (
            'ABSENT' if nl is None else ('UP' if nl.oper.state == LINK_UP else 'FAILED')
        )
        if os_ != ns_:
            emit(LinkStateEvent, link=lid, old=os_, new=ns_)
        if ol is not None and nl is not None and ol.config != nl.config:
            emit(LinkConfigEvent, link=lid, changes=_changes(ol.config, nl.config))
    d = delta.devices()
    for name in d.removed:
        emit(DeviceEvent, device=name, action='removed')
        _srv6_events(emit, old.devices[name], None, name)
    for name in d.added + d.changed:
        odev, ndev = old.devices.get(name), new.devices.get(name)
        if ndev is None:
            continue
        if odev is None:
            emit(
                DeviceEvent,
                device=name,
                action='added',
                changes=_changes(None, ndev.config),
            )
        elif odev.config != ndev.config:
            emit(
                DeviceEvent,
                device=name,
                action='config',
                changes=_changes(odev.config, ndev.config),
            )
        for agent_name in delta.agents(name).added + delta.agents(name).changed:
            node = ndev.agents[agent_name]
            previous = odev.agents.get(agent_name) if odev else None
            receipt = node.receipt
            if receipt is not None and (
                previous is None or previous.receipt is not receipt
            ):
                emit(
                    AgentRunEvent,
                    device=name,
                    agent=agent_name,
                    generation=node.generation,
                    run_id=receipt.run_id,
                    status=receipt.status,
                    causes_count=receipt.causes_count,
                    ops_count=receipt.ops_count,
                )
        _srv6_events(emit, odev, ndev, name)
        _interface_events(emit, odev, ndev, name, delta)
        _route_events(emit, odev, ndev, name, delta)
        _fib_events(emit, odev, ndev, name, delta)
    for did in delta.demands().keys:
        odm, ndm = old.demands.get(did), new.demands.get(did)
        dm = ndm if ndm is not None else odm
        assert dm is not None
        emit(
            DemandEvent,
            demand=did,
            action='add' if odm is None else ('delete' if ndm is None else 'update'),
            source=dm.source,
            dst=str(to_address(dm.dst, dm.af)),
            af=int(dm.af),
            mode='HASH' if dm.mode == 2 else 'FLUID',
            rate=dm.rate,
        )
    rep = new.placement
    if delta.placement_changed() and rep is not None:
        if old.placement is None or old.placement.version != rep.version:
            emit(
                PlacementEvent,
                version=rep.version,
                delivered=rep.delivered_total,
                dropped=tuple(sorted(rep.dropped_by_reason.items())),
                max_utilization=rep.max_utilization(),
                oversubscribed=rep.oversubscribed_count(),
                offered=rep.offered if keep_arrays else None,
                carried=rep.carried if keep_arrays else None,
                capacity=rep.capacity if keep_arrays else None,
                demand_delivered=tuple(
                    (k, r.delivered) for k, r in rep.demands.sorted_items()
                ),
                report=rep if keep_report else None,
            )
    return out


def _srv6_events(emit: Callable[..., None], old: Any, new: Any, name: str) -> None:
    before = old.srv6_sids if old else None
    after = new.srv6_sids if new else None
    if before is not after:
        locs_old = before.locators if before else PMap()
        locs_new = after.locators if after else PMap()
        for key in diff_pmap(locs_old, locs_new).keys:
            a, b = locs_old.get(key), locs_new.get(key)
            loc = b if b is not None else a
            assert loc is not None
            emit(
                LocatorEvent,
                device=name,
                action='add' if a is None else 'remove' if b is None else 'replace',
                name=key,
                prefix=_prefix_str(loc.prefix, IPV6),
            )
        sids_old = before.sids if before else PMap()
        sids_new = after.sids if after else PMap()
        for key in diff_pmap(sids_old, sids_new).keys:
            a, b = sids_old.get(key), sids_new.get(key)
            sid = b if b is not None else a
            assert sid is not None
            action = (
                'add'
                if a is None
                else 'remove'
                if b is None
                else 'replace'
                if dataclasses.replace(a, adjacency_up=b.adjacency_up) != b
                else 'adjacency_up'
                if b.adjacency_up
                else 'adjacency_down'
            )
            emit(
                SidEvent,
                device=name,
                action=action,
                sid=str(to_address(key, IPV6)),
                behavior=srv6.behavior_name(sid.behavior),
                flavors=srv6.flavor_names(sid.flavors),
                owner=f'{sid.owner.name}:{sid.owner.instance}',
                interface=sid.interface,
                adjacency_up=sid.adjacency_up,
            )
    a_table = old.srv6_policies if old else None
    b_table = new.srv6_policies if new else None
    if a_table is b_table:
        return
    policies_old = a_table.policies if a_table else PMap()
    policies_new = b_table.policies if b_table else PMap()
    states_old = a_table.states if a_table else PMap()
    states_new = b_table.states if b_table else PMap()
    changed = set(diff_pmap(policies_old, policies_new).keys) | set(
        diff_pmap(states_old, states_new).keys
    )
    for key in sorted(changed):
        a, b = policies_old.get(key), policies_new.get(key)
        policy = b if b is not None else a
        if policy is None:
            continue
        state = states_new.get(key) or states_old.get(key) or srv6.PolicyState()
        action = (
            'add'
            if a is None
            else 'delete'
            if b is None
            else 'replace'
            if a != b
            else 'state'
        )
        emit(
            PolicyEvent,
            device=name,
            action=action,
            color=key[0],
            endpoint=str(to_address(key[1], IPV6)),
            name=policy.name,
            owner=f'{policy.owner.name}:{policy.owner.instance}',
            active_path=state.active_path,
            status=state.status,
            reasons=state.reasons,
            programmed_version=state.programmed_version,
            basic_valid=state.basic_valid,
            strict_valid=state.strict_valid,
            first_valid=state.first_valid,
            programming=state.programming,
        )


def _interface_events(
    emit: Callable[..., None], odev: Any, ndev: Any, name: str, delta: StateDelta
) -> None:
    for iface, cfg_changed, oper_changed in delta.interface_changes(name):
        on = odev.interfaces.get(iface) if odev is not None else None
        nn = ndev.interfaces.get(iface)
        if nn is None:
            emit(ConfigEvent, device=name, interface=iface, action='removed')
            continue
        if on is None:
            emit(
                ConfigEvent,
                device=name,
                interface=iface,
                action='added',
                changes=_changes(None, nn.config),
            )
            continue
        if cfg_changed:
            changes = _changes(on.config, nn.config)
            if type(on) is not type(nn):
                changes = (('kind', type(on).__name__, type(nn).__name__),) + changes
            elif not changes and getattr(on, 'mac', None) != getattr(nn, 'mac', None):
                changes = (
                    (
                        'mac',
                        _fmt('mac', getattr(on, 'mac', None)),
                        _fmt('mac', getattr(nn, 'mac', None)),
                    ),
                )
            emit(
                ConfigEvent,
                device=name,
                interface=iface,
                action='changed',
                changes=changes,
            )
        if not oper_changed:
            continue
        oo, no = on.oper, nn.oper
        if (
            isinstance(nn, EthernetNode)
            and isinstance(on, EthernetNode)
            and oo.oper == no.oper
            and (
                oo.carrier_raw != no.carrier_raw
                or oo.carrier_effective != no.carrier_effective
            )
        ):
            emit(
                CarrierRawEvent,
                device=name,
                interface=iface,
                carrier_raw=no.carrier_raw,
                effective=no.carrier_effective,
            )
        if oo.oper != no.oper or oo.reason != no.reason:
            emit(
                InterfaceOperEvent,
                device=name,
                interface=iface,
                old=_oper_name(oo.oper),
                new=_oper_name(no.oper),
                reason=_reason_name(no.reason),
                carrier_raw=getattr(no, 'carrier_raw', None),
            )
        if isinstance(nn, PortChannelNode) and isinstance(on, PortChannelNode):
            active = tuple(sorted(n for n, m in no.members.items() if m.active))
            old_active = tuple(sorted(n for n, m in oo.members.items() if m.active))
            if (
                active != old_active
                or oo.bandwidth != no.bandwidth
                or oo.oper != no.oper
            ):
                emit(
                    LagEvent,
                    device=name,
                    portchannel=iface,
                    oper=_oper_name(no.oper),
                    reason=_reason_name(no.reason),
                    active_members=active,
                    bandwidth=no.bandwidth,
                )


def _route_events(
    emit: Callable[..., None], odev: Any, ndev: Any, name: str, delta: StateDelta
) -> None:
    for af in delta.ribs(name).keys:
        orib = odev.ribs.get(af) if odev is not None else None
        nrib = ndev.ribs.get(af)
        oshards = orib.shards if orib is not None else None
        nshards = nrib.shards if nrib is not None else None
        for plen in sorted(diff_pmap(oshards, nshards).keys):
            os_ = oshards.get(plen) if oshards is not None else None
            ns_ = nshards.get(plen) if nshards is not None else None

            def row_order(k):
                return (k[0], k[1], k[2].name, k[2].instance, repr(k[3]))

            for key in sorted(
                diff_pmap(os_, ns_, sort_key=row_order).keys, key=row_order
            ):
                orow = os_.get(key) if os_ is not None else None
                nrow = ns_.get(key) if ns_ is not None else None
                if orow is nrow or orow == nrow:
                    continue
                row = nrow if nrow is not None else orow
                assert row is not None
                emit(
                    RouteEvent,
                    device=name,
                    af=int(af),
                    prefix=_prefix_str((key[0], key[1]), int(af)),
                    source=key[2].name,
                    instance=key[2].instance,
                    distinguisher=repr(key[3]) if key[3] else '',
                    action='add'
                    if orow is None
                    else ('delete' if nrow is None else 'update'),
                    distance=row.distance,
                    metric=row.metric,
                    nexthops=tuple(_nexthop_str(nh, int(af)) for nh in row.nexthops)
                    if nrow is not None
                    else (),
                )


def _fib_events(
    emit: Callable[..., None], odev: Any, ndev: Any, name: str, delta: StateDelta
) -> None:
    for af in delta.fibs(name).keys:
        ofib = odev.fibs.get(af) if odev is not None else None
        nfib = ndev.fibs.get(af)
        if ofib is nfib:
            continue
        oshards = ofib.entries.shards() if ofib is not None else {}
        nshards = nfib.entries.shards() if nfib is not None else {}
        same_groups = (
            ofib is not None and nfib is not None and ofib.groups is nfib.groups
        )
        for plen in sorted(set(oshards) | set(nshards)):
            otab, ntab = oshards.get(plen), nshards.get(plen)
            if otab is ntab or (same_groups and otab == ntab):
                continue
            for net in sorted(set(otab or ()) | set(ntab or ())):
                oe = otab.get(net) if otab is not None else None
                ne = ntab.get(net) if ntab is not None else None
                prefix = _prefix_str((net, plen), int(af))
                if ne is None:
                    assert oe is not None
                    emit(
                        FibEvent,
                        device=name,
                        af=int(af),
                        prefix=prefix,
                        action='withdraw',
                        fib_action=_action_name(oe.action),
                        nexthops=(),
                    )
                    continue
                nexthops = _adjacency_names(nfib, ne, int(af))
                if (
                    oe is not None
                    and oe.action == ne.action
                    and _adjacency_names(ofib, oe, int(af)) == nexthops
                ):
                    continue  # same forwarding: a re-interned group or bookkeeping
                emit(
                    FibEvent,
                    device=name,
                    af=int(af),
                    prefix=prefix,
                    action='install' if oe is None else 'update',
                    fib_action=_action_name(ne.action),
                    nexthops=nexthops,
                )


# ---------------------------------------------------------------------------
# Timeline
# ---------------------------------------------------------------------------


class Timeline:
    """Append-only event log with bounded raw retention.

    ``events`` holds every extracted event and ``records`` one entry per
    commit, unless ``keep_events`` / ``keep_records`` budgets are set (then
    the oldest are dropped and counted in ``dropped_events`` /
    ``dropped_records``); ``deltas`` and ``roots`` are bounded deques
    (defaults keep the last 64 deltas and the last root of the last 256
    timestamps).
    """

    def __init__(
        self,
        *,
        keep_deltas: int = 64,
        keep_roots: int | None = 256,
        extract: bool = True,
        keep_reports: bool = False,
        keep_arrays: bool = True,
        keep_events: int | None = None,
        keep_records: int | None = None,
    ) -> None:
        self.events: list[Event] = []
        self.records: list[Record] = []
        self.keep_events = keep_events
        """Budget for ``events``: the oldest are dropped past it (``None``: unbounded)."""
        self.keep_records = keep_records
        """Budget for ``records`` (``None``: unbounded)."""
        self.dropped_events = 0
        self.dropped_records = 0
        self.deltas: deque[tuple[int, StateDelta]] = deque(maxlen=keep_deltas)
        self.roots: deque[tuple[float, NetworkState]] = deque(maxlen=keep_roots)
        self.extract = extract
        self.keep_reports = keep_reports
        self.keep_arrays = keep_arrays
        self.initializing = False
        """Set by ``Simulation`` around the initial convergence (origins ``init``)."""
        self._seq = 0
        self.round_of: Callable[[float], int] = lambda t: 0
        self.on_record: list[Callable[[Record, list[Event]], None]] = []
        """Streaming observers, called before retention eviction; do not mutate inputs."""

    # -- ingestion --------------------------------------------------------------

    def on_delta(self, time: float, origin_raw: Any, delta: StateDelta) -> None:
        self._seq += 1
        origin, gen = origin_from(origin_raw, initializing=self.initializing)
        round_ = gen if gen is not None else self.round_of(time)
        events = (
            extract_events(
                delta,
                self._seq,
                time,
                round_,
                origin,
                keep_report=self.keep_reports,
                keep_arrays=self.keep_arrays,
            )
            if self.extract
            else []
        )
        self.records.append(
            Record(self._seq, time, round_, origin, len(events), delta.new.version)
        )
        self.events.extend(events)
        for observe in self.on_record:
            observe(self.records[-1], events)
        self._trim()
        self.deltas.append((self._seq, delta))
        if self.roots and self.roots[-1][0] == time:
            self.roots[-1] = (time, delta.new)
        else:
            self.roots.append((time, delta.new))

    def _trim(self) -> None:
        """Enforce the budgets; trims in blocks so the cost is amortized O(1)."""
        for name, keep in (
            ('events', self.keep_events),
            ('records', self.keep_records),
        ):
            if keep is None:
                continue
            seq: list[Any] = getattr(self, name)
            excess = len(seq) - keep
            if excess > 0 and excess >= max(64, keep // 8):
                del seq[:excess]
                setattr(
                    self, f'dropped_{name}', getattr(self, f'dropped_{name}') + excess
                )

    def baseline(self, time: float, root: NetworkState) -> None:
        """Record the starting point: the root, and a placement event when
        the initial convergence committed none (an already-converged network)."""
        if not self.roots or self.roots[-1][1] is not root:
            self.roots.append((time, root))
        rep = root.placement
        if not self.extract or rep is None:
            return
        if not any(isinstance(e, PlacementEvent) for e in self.events):
            self._seq += 1
            origin = Origin('init', 'baseline')
            ev = PlacementEvent(
                seq=self._seq,
                idx=0,
                time=time,
                round=0,
                origin=origin,
                version=rep.version,
                delivered=rep.delivered_total,
                dropped=tuple(sorted(rep.dropped_by_reason.items())),
                max_utilization=rep.max_utilization(),
                oversubscribed=rep.oversubscribed_count(),
                offered=rep.offered if self.keep_arrays else None,
                carried=rep.carried if self.keep_arrays else None,
                capacity=rep.capacity if self.keep_arrays else None,
                demand_delivered=tuple(
                    (k, r.delivered) for k, r in rep.demands.sorted_items()
                ),
                report=rep if self.keep_reports else None,
            )
            self.records.append(Record(self._seq, time, 0, origin, 1, root.version))
            self.events.append(ev)
            for observe in self.on_record:
                observe(self.records[-1], [ev])

    # -- queries ----------------------------------------------------------------

    def select(
        self,
        *,
        kind: type | tuple[type, ...] | None = None,
        device: str | None = None,
        since: float | None = None,
        until: float | None = None,
        predicate: Callable[[Event], bool] | None = None,
    ) -> list[Event]:
        out = []
        for e in self.events:
            if kind is not None and not isinstance(e, kind):
                continue
            if device is not None and getattr(e, 'device', None) != device:
                continue
            if since is not None and e.time < since:
                continue
            if until is not None and e.time > until:
                continue
            if predicate is not None and not predicate(e):
                continue
            out.append(e)
        return out

    def at(self, time: float) -> list[Event]:
        return [e for e in self.events if e.time == time]

    def origins(self, time: float | None = None) -> list[Origin]:
        return [r.origin for r in self.records if time is None or r.time == time]

    def stage_names(self, time: float) -> list[str]:
        return [
            r.origin.name
            for r in self.records
            if r.time == time and r.origin.kind == 'stage'
        ]

    def rows(self, events: Iterable[Event] | None = None) -> list[dict[str, Any]]:
        return [e.row() for e in (self.events if events is None else events)]

    def to_csv(self, path: str, events: Iterable[Event] | None = None) -> int:
        """Write one row per event (tuples and dicts as ``repr``); returns the count.
        Columns are the union of the event fields, in first-seen order."""
        source = list(self.events if events is None else events)
        columns: list[str] = ['event', 'seq', 'idx', 'time', 'round', 'origin']
        seen: set[type] = set()
        for e in source:
            if type(e) not in seen:
                seen.add(type(e))
                for k in e.row():
                    if k not in columns:
                        columns.append(k)
        n = 0
        with open(path, 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=columns)
            w.writeheader()
            for e in source:
                w.writerow(
                    {
                        k: (repr(v) if isinstance(v, (tuple, dict)) else v)
                        for k, v in e.row().items()
                    }
                )
                n += 1
        return n

    def snapshot_at(self, time: float) -> NetworkState | None:
        best = None
        for t, root in self.roots:
            if t <= time:
                best = root
            else:
                break
        return best

    # -- series -----------------------------------------------------------------

    def placement_events(self) -> Iterator[PlacementEvent]:
        return (e for e in self.events if isinstance(e, PlacementEvent))

    def utilization_series(self, edge_id: int) -> list[tuple[float, float]]:
        """Settled utilization per timestamp for one directed edge (a step
        series; timestamps where the edge did not exist are skipped)."""
        out: dict[float, float] = {}
        for e in self.placement_events():
            u = e.utilization(edge_id)
            if u is not None:
                out[e.time] = u
        return sorted(out.items())

    def delivered_series(self, demand: str | None = None) -> list[tuple[float, float]]:
        """Delivered payload bit/s per timestamp, in total or for one demand
        (a demand's removal contributes one 0.0 sample)."""
        out: dict[float, float] = {}
        seen = False
        for e in self.placement_events():
            if demand is None:
                out[e.time] = e.delivered
                continue
            value = None
            for k, v in e.demand_delivered:
                if k == demand:
                    value = v
                    break
            if value is not None:
                seen = True
                out[e.time] = value
            elif seen:  # removed: one zero sample closes the step
                seen = False
                out[e.time] = 0.0
        return sorted(out.items())

    def interface_series(
        self, device: str, interface: str
    ) -> list[tuple[float, str, str]]:
        return [
            (e.time, e.new, e.reason)
            for e in self.select(kind=InterfaceOperEvent, device=device)
            if isinstance(e, InterfaceOperEvent) and e.interface == interface
        ]

    def summary(self, time: float | None = None) -> str:
        lines = []
        for e in self.events if time is None else self.at(time):
            r = e.row()
            for k in ('seq', 'idx', 'time', 'round', 'origin', 'event'):
                r.pop(k, None)
            lines.append(
                f'{e.time:>8g} r{e.round} {str(e.origin):<24} {e.kind:<18} '
                + ' '.join(f'{k}={v}' for k, v in r.items())
            )
        return '\n'.join(lines)


__all__ = [
    'Timeline',
    'Record',
    'Origin',
    'Event',
    'DeviceEvent',
    'AgentRunEvent',
    'LinkStateEvent',
    'LinkConfigEvent',
    'InterfaceOperEvent',
    'CarrierRawEvent',
    'LagEvent',
    'ConfigEvent',
    'RouteEvent',
    'FibEvent',
    'DemandEvent',
    'PlacementEvent',
    'extract_events',
    'origin_from',
]
