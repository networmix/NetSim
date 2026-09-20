"""Version 1 bounded SONiC CONFIG_DB interchange, with no optional dependencies.

Envelope: {"version": 1, "devices": {hostname: {table: {key: fields}}}}.
The static locator/My-SID vocabulary follows SONiC's srv6_static_config_hld;
SRV6_POLICY candidate_paths and explicit per-SID lengths are NetSim extensions.
Only the default table and pipe decapsulation are modeled. See README for the
complete subset. Unknown fields are errors, never silently discarded.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from contextlib import contextmanager
from copy import deepcopy
from ipaddress import IPv6Address, IPv6Network, ip_interface, ip_network
from pathlib import Path
from typing import Any, Iterator

from netsim.model import srv6 as sr
from netsim.model.addressing import AddressFamily, to_address
from netsim.model.contracts import STATIC
from netsim.model.interfaces import AdminState, EthernetNode, LoopbackNode
from netsim.model.network import Network
from netsim.model.routing import BLACKHOLE
from netsim.model.srv6_compress import compress


class SchemaError(ValueError):
    """Unsupported or malformed input, including its location in the document."""


@contextmanager
def _at(where: str) -> Iterator[None]:
    try:
        yield
    except (ValueError, KeyError, TypeError) as exc:
        if isinstance(exc, SchemaError):
            raise
        raise SchemaError(f'{where}: {exc}') from exc


def _object(
    value: Any,
    where: str,
    allowed: tuple[str, ...] | None = None,
    required: tuple[str, ...] = (),
) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(k, str) for k in value):
        raise SchemaError(f'{where}: expected an object with string keys')
    if allowed is not None and (unknown := value.keys() - set(allowed)):
        raise SchemaError(f'{where}: unsupported fields {sorted(unknown)}')
    if missing := set(required) - value.keys():
        raise SchemaError(f'{where}: missing fields {sorted(missing)}')
    return value


def _int(value: Any, where: str, low: int = 0, high: int = 0xFFFFFFFF) -> int:
    if isinstance(value, bool) or not (
        isinstance(value, int)
        or isinstance(value, str)
        and value.isascii()
        and value.isdigit()
    ):
        raise SchemaError(f'{where}: expected an integer')
    number = int(value)
    if not low <= number <= high:
        raise SchemaError(f'{where}: must be in {low}..{high}')
    return number


def _text(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value:
        raise SchemaError(f'{where}: expected a nonempty string')
    return value


def _array(value: Any, where: str) -> list[Any]:
    if not isinstance(value, list) or not value:
        raise SchemaError(f'{where}: expected a nonempty array')
    return value


def _choice(value: Any, choices: tuple[str, ...], where: str) -> str:
    if value not in choices:
        raise SchemaError(f'{where}: supported values are {choices}, got {value!r}')
    return value


def _structure(row: dict[str, Any], where: str) -> sr.SidStructure:
    return sr.SidStructure(
        *(
            _int(row[k], f'{where}.{k}', 0, 128)
            for k in ('block_len', 'node_len', 'func_len', 'arg_len')
        )
    )


def _lengths(structure: sr.SidStructure) -> dict[str, int]:
    return dict(
        zip(
            ('block_len', 'node_len', 'func_len', 'arg_len'),
            (structure.lbl, structure.lnl, structure.fl, structure.al),
            strict=True,
        )
    )


def _flavors(value: Any, where: str) -> int:
    if not isinstance(value, list) or len(value) != len(set(map(str, value))):
        raise SchemaError(f'{where}: expected an array of unique flavors')
    result = 0
    for name in value:
        _choice(name, ('usid', 'psp', 'usd'), where)
        result |= {'usid': sr.NEXT_CSID, 'psp': sr.PSP, 'usd': sr.USD}[name]
    return result


def _flavor_list(flags: int) -> list[str]:
    return [
        name
        for bit, name in ((sr.NEXT_CSID, 'usid'), (sr.PSP, 'psp'), (sr.USD, 'usd'))
        if flags & bit
    ]


def _literal(value: Any, where: str) -> sr.LiteralSid:
    if isinstance(value, str):
        return sr.LiteralSid(int(IPv6Address(value)))
    row = _object(
        value,
        where,
        ('sid', 'block_len', 'node_len', 'func_len', 'arg_len', 'flavors'),
        ('sid',),
    )
    structure = _structure(row, where) if any(k.endswith('_len') for k in row) else None
    return sr.LiteralSid(
        int(IPv6Address(_text(row['sid'], where))),
        structure,
        _flavors(row['flavors'], where) if 'flavors' in row else None,
    )


def _segment_json(segment: Any) -> Any:
    if isinstance(segment, int):
        return str(IPv6Address(segment))
    if not isinstance(segment, sr.LiteralSid):
        raise SchemaError(
            'dump: symbolic segments are outside the literal CONFIG_DB subset'
        )
    if segment.structure is None and segment.flavors is None:
        return str(IPv6Address(segment.address))
    return {
        'sid': str(IPv6Address(segment.address)),
        **(_lengths(segment.structure) if segment.structure else {}),
        **(
            {'flavors': _flavor_list(segment.flavors)}
            if segment.flavors is not None
            else {}
        ),
    }


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise SchemaError(f'JSON: duplicate key {key!r}')
        result[key] = value
    return result


def load(source: Mapping[str, Any] | str | Path) -> Network:
    """Load a mapping, JSON text, or Path into a new, unconnected device set.

    Parsing and model validation finish before the network is returned. Physical
    links are deliberately external to CONFIG_DB; add them with Network APIs.
    """
    if isinstance(source, Path):
        source = source.read_text()
    document = (
        json.loads(source, object_pairs_hook=_pairs)
        if isinstance(source, str)
        else deepcopy(dict(source) if isinstance(source, Mapping) else source)
    )
    root = _object(document, '$', ('version', 'devices'), ('version', 'devices'))
    if type(root['version']) is not int or root['version'] != 1:
        raise SchemaError('$.version: expected supported schema version 1')
    devices = _object(root['devices'], '$.devices')
    net = Network()
    meta: dict[str, Any] = {'source': document, 'sid_keys': {}, 'lists': {}}
    net.netsim_sonic = meta  # type: ignore[attr-defined]
    with net.batch():
        for name, tables in sorted(devices.items()):
            where = f'$.devices.{name}'
            try:
                _load_device(net, name, tables, where, meta)
            except (ValueError, KeyError, TypeError) as exc:
                if isinstance(exc, SchemaError):
                    raise
                raise SchemaError(f'{where}: {exc}') from exc
    meta['canonical'] = _encode(net)
    return net


def _load_device(
    net: Network, name: str, tables: Any, where: str, meta: dict[str, Any]
) -> None:
    tables = _object(
        tables,
        where,
        (
            'PORT',
            'INTERFACE',
            'LOOPBACK_INTERFACE',
            'STATIC_ROUTE',
            'SRV6_MY_LOCATORS',
            'SRV6_MY_SIDS',
            'SRV6_SID_LIST',
            'SRV6_POLICY',
        ),
    )
    for table, value in tables.items():
        _object(value, f'{where}.{table}')
    dev = net.add_device(name)
    addresses: dict[str, dict[str, list[str]]] = {}
    for table in ('INTERFACE', 'LOOPBACK_INTERFACE'):
        for key, row in sorted(tables.get(table, {}).items()):
            loc = f'{where}.{table}[{key}]'
            with _at(loc):
                _object(row, loc, ())
                parts = key.split('|')
                if len(parts) not in (1, 2):
                    raise SchemaError(
                        f'{loc}: expected interface or interface|address/prefix'
                    )
                iface = parts[0]
                entry = addresses.setdefault(iface, {'ipv4': [], 'ipv6': []})
                if len(parts) == 2:
                    address = ip_interface(parts[1])
                    entry[f'ipv{address.version}'].append(parts[1])
                if table == 'INTERFACE' and iface not in tables.get('PORT', {}):
                    raise SchemaError(f'{loc}: interface has no PORT entry')
                if table == 'LOOPBACK_INTERFACE' and iface in tables.get('PORT', {}):
                    raise SchemaError(f'{loc}: loopback conflicts with PORT')
    for iface, row in sorted(tables.get('PORT', {}).items()):
        loc = f'{where}.PORT[{iface}]'
        with _at(loc):
            _object(row, loc, ('speed', 'mtu', 'admin_status'))
            values = addresses.pop(iface, {})
            dev.add_ethernet(
                iface,
                speed=_int(row.get('speed', 10000), loc + '.speed', 1) * 1e6,
                mtu=_int(row.get('mtu', 1500), loc + '.mtu', 1280, 65535),
                admin=AdminState.UP
                if _choice(row.get('admin_status', 'up'), ('up', 'down'), loc) == 'up'
                else AdminState.DOWN,
                ipv4=values.get('ipv4', ()),
                ipv6=values.get('ipv6', ()),
            )
    for iface, values in sorted(addresses.items()):
        dev.add_loopback(iface, ipv4=values['ipv4'], ipv6=values['ipv6'])
    for key, row in sorted(tables.get('STATIC_ROUTE', {}).items()):
        loc = f'{where}.STATIC_ROUTE[{key}]'
        with _at(loc):
            _object(row, loc, ('nexthop', 'ifname', 'distance', 'blackhole'))
            ip_network(key)
            if 'blackhole' in row:
                _choice(row['blackhole'], ('true',), loc + '.blackhole')
                if 'nexthop' in row or 'ifname' in row:
                    raise SchemaError(f'{loc}: blackhole cannot have nexthop/ifname')
                hops: list[Any] = ['blackhole']
            else:
                hosts = (
                    _text(row.get('nexthop', ''), loc + '.nexthop').split(',')
                    if 'nexthop' in row
                    else []
                )
                ports = (
                    _text(row.get('ifname', ''), loc + '.ifname').split(',')
                    if 'ifname' in row
                    else []
                )
                if (
                    not hosts
                    and not ports
                    or hosts
                    and ports
                    and len(hosts) != len(ports)
                ):
                    raise SchemaError(
                        f'{loc}: nexthop/ifname must have equal, nonzero lengths'
                    )
                if any(p not in dev.node.interfaces for p in ports):
                    raise SchemaError(f'{loc}: unknown ifname')
                hops = (
                    [(p, h) for p, h in zip(ports, hosts, strict=True)]
                    if ports and hosts
                    else ports or hosts
                )
            dev.add_route(
                key,
                hops,
                distance=_int(row.get('distance', 1), loc + '.distance', 1, 255),
            )
    for key, row in sorted(tables.get('SRV6_MY_LOCATORS', {}).items()):
        loc = f'{where}.SRV6_MY_LOCATORS[{key}]'
        with _at(loc):
            _object(
                row,
                loc,
                ('prefix', 'block_len', 'node_len', 'func_len', 'arg_len', 'vrf'),
                ('prefix',),
            )
            _choice(row.get('vrf', 'default'), ('default',), loc + '.vrf')
            structure = _structure(
                {'block_len': 32, 'node_len': 16, 'func_len': 16, 'arg_len': 0, **row},
                loc,
            )
            prefix_text = _text(row['prefix'], loc + '.prefix')
            if '/' not in prefix_text:
                prefix_text += f'/{structure.lbl + structure.lnl}'
            prefix = IPv6Network(prefix_text)
            block = IPv6Network(
                (int(prefix.network_address), structure.lbl), strict=False
            )
            node_id = (
                (int(prefix.network_address) >> 80) & 0xFFFF
                if sr.is_csid(structure)
                else None
            )
            dev.add_locator(
                key, str(prefix), structure=structure, block=str(block), node_id=node_id
            )
    keys = meta['sid_keys'][name] = {}
    for key, row in sorted(tables.get('SRV6_MY_SIDS', {}).items()):
        loc = f'{where}.SRV6_MY_SIDS[{key}]'
        with _at(loc):
            try:
                _load_sid(dev, key, row, loc)
            except (ValueError, KeyError, TypeError) as exc:
                raise SchemaError(f'{loc}: {exc}') from exc
            value = int(IPv6Network(key.split('|')[1]).network_address)
            if value in keys:
                raise SchemaError(
                    f'{loc}: duplicate literal SID (also {keys[value]!r})'
                )
            keys[value] = key
    lists = meta['lists'][name] = {}
    for key, row in sorted(tables.get('SRV6_SID_LIST', {}).items()):
        loc = f'{where}.SRV6_SID_LIST[{key}]'
        with _at(loc):
            _object(row, loc, ('path',), ('path',))
            lists[key] = tuple(
                _literal(value, loc + '.path')
                for value in _array(row['path'], loc + '.path')
            )
    for key, row in sorted(tables.get('SRV6_POLICY', {}).items()):
        loc = f'{where}.SRV6_POLICY[{key}]'
        with _at(loc):
            _object(
                row,
                loc,
                ('color', 'endpoint', 'bsid', 'candidate_paths', 'fallback'),
                ('color', 'endpoint', 'candidate_paths'),
            )
            candidates = []
            for index, candidate in enumerate(
                _array(row['candidate_paths'], loc + '.candidate_paths')
            ):
                path = f'{loc}.candidate_paths[{index}]'
                _object(
                    candidate,
                    path,
                    (
                        'preference',
                        'name',
                        'protocol_origin',
                        'originator',
                        'discriminator',
                        'segment_lists',
                    ),
                    ('segment_lists',),
                )
                segments = []
                for ref in _array(candidate['segment_lists'], path + '.segment_lists'):
                    _object(ref, path + '.segment_lists', ('name', 'weight'), ('name',))
                    list_name = _text(ref['name'], path + '.name')
                    if list_name not in lists:
                        raise SchemaError(f'{path}: unknown segment list {list_name!r}')
                    segments.append(
                        sr.SegmentList(
                            lists[list_name],
                            _int(ref.get('weight', 1), path + '.weight', 1),
                            list_name,
                        )
                    )
                originator = candidate.get('originator', [0, 0])
                if not isinstance(originator, list) or len(originator) != 2:
                    raise SchemaError(f'{path}.originator: expected two integers')
                candidates.append(
                    sr.CandidatePath(
                        _int(candidate.get('preference', 100), path + '.preference'),
                        tuple(segments),
                        _text(candidate['name'], path + '.name')
                        if 'name' in candidate
                        else None,
                        _int(
                            candidate.get('protocol_origin', sr.ORIGIN_CLI),
                            path + '.protocol_origin',
                            0,
                            255,
                        ),
                        (_int(originator[0], path), _int(originator[1], path)),
                        _int(
                            candidate.get('discriminator', index),
                            path + '.discriminator',
                        ),
                    )
                )
            fallback = _choice(
                row.get('fallback', 'IGP'), ('IGP', 'DROP'), loc + '.fallback'
            )
            dev.policy_client().add(
                sr.SrPolicy(
                    STATIC,
                    _int(row['color'], loc + '.color'),
                    int(IPv6Address(_text(row['endpoint'], loc + '.endpoint'))),
                    name=key,
                    bsid=int(IPv6Address(_text(row['bsid'], loc + '.bsid')))
                    if 'bsid' in row
                    else None,
                    candidate_paths=tuple(candidates),
                    fallback=sr.FALLBACK_DROP
                    if fallback == 'DROP'
                    else sr.FALLBACK_IGP,
                )
            )


def _load_sid(dev: Any, key: str, row: Any, where: str) -> None:
    _object(
        row,
        where,
        (
            'action',
            'flavors',
            'interface',
            'adj',
            'decap_dscp_mode',
            'decap_vrf',
            'block_len',
            'node_len',
            'func_len',
            'arg_len',
        ),
        ('action',),
    )
    parts = key.split('|')
    if len(parts) != 2:
        raise SchemaError('key must be locator|IPv6-prefix')
    locator, prefix = parts[0], IPv6Network(parts[1])
    if dev.node.srv6_sids is None or locator not in dev.node.srv6_sids.locators:
        raise SchemaError(f'unknown locator {locator!r}')
    action = _choice(
        row['action'],
        ('End', 'End.X', 'End.DT46', 'uN', 'uA', 'uDT46'),
        where + '.action',
    )
    behavior = (
        sr.END
        if action in ('End', 'uN')
        else sr.END_X
        if action in ('End.X', 'uA')
        else sr.END_DT46
    )
    flags = _flavors(row.get('flavors', []), where + '.flavors') | (
        sr.NEXT_CSID if action.startswith('u') else 0
    )
    _choice(row.get('decap_dscp_mode', 'pipe'), ('pipe',), where + '.decap_dscp_mode')
    _choice(row.get('decap_vrf', 'default'), ('default',), where + '.decap_vrf')
    if 'decap_vrf' in row and behavior != sr.END_DT46:
        raise SchemaError('decap_vrf applies only to End.DT46/uDT46')
    if any(k.endswith('_len') for k in row):
        _object(row, where, required=('block_len', 'node_len', 'func_len', 'arg_len'))
        structure = _structure(row, where)
    elif flags & sr.NEXT_CSID:
        structure = (
            sr.F3216_GIB
            if behavior == sr.END and prefix.prefixlen == 48
            else sr.F3216_TERMINAL
            if behavior == sr.END_DT46 and prefix.prefixlen == 48
            else sr.F3216_LIB
            if prefix.prefixlen == 48
            else sr.F3216_COMPOSITE
        )
    else:
        structure = dev.node.srv6_sids.locators[locator].structure
    locator_structure = dev.node.srv6_sids.locators[locator].structure
    if sr.is_csid(structure) != sr.is_csid(locator_structure):
        raise SchemaError(
            'SID and locator must both use supported F3216 structures or both use classic structures'
        )
    if prefix.prefixlen != structure.installed_length:
        raise SchemaError(
            'SID prefix length must equal block_len + node_len + func_len'
        )
    dev.add_local_sid(
        behavior,
        structure=structure,
        flavors=flags,
        sid=int(prefix.network_address),
        interface=row.get('interface'),
        nexthop=row.get('adj'),
        locator=locator,
    )


def _restore(current: Any, previous: Any, source: Any) -> Any:
    """Retain source spelling/default omission only where model config agrees."""
    if current == previous:
        return deepcopy(source)
    if (
        isinstance(current, dict)
        and isinstance(previous, dict)
        and isinstance(source, dict)
    ):
        return {
            key: _restore(value, previous[key], source[key])
            if key in previous and key in source
            else value
            for key, value in current.items()
            if key in source or value != previous.get(key)
        }
    return current


def dump(net: Network, destination: str | Path | None = None) -> dict[str, Any]:
    """Export current config; retain unchanged source spellings and omissions.

    Oper state and derivation caches are excluded. Unsupported native model
    configuration raises instead of producing a misleading partial export.
    """
    current = _encode(net)
    meta = getattr(net, 'netsim_sonic', {})
    if 'canonical' in meta:
        current = _restore(current, meta['canonical'], meta['source'])
    if destination is not None:
        Path(destination).write_text(json.dumps(current, indent=2) + '\n')
    return current


def _encode(net: Network) -> dict[str, Any]:
    devices: dict[str, Any] = {}
    meta = getattr(net, 'netsim_sonic', {})
    for name, dev in net.state.devices.sorted_items():
        # Version 1 has no table for these forwarding-relevant device settings.
        # Check the live tree before restoring source spelling or writing a file.
        for field, default in (
            ('enabled', True),
            ('srv6_hop_limit', 64),
            ('srv6_source', None),
        ):
            value = getattr(dev.config, field)
            if value != default:
                raise SchemaError(
                    f'$.devices.{name}: unsupported device setting {field}={value!r}; '
                    f'version 1 requires {default!r}'
                )
        tables: dict[str, dict[str, Any]] = {
            key: {}
            for key in (
                'PORT',
                'INTERFACE',
                'LOOPBACK_INTERFACE',
                'STATIC_ROUTE',
                'SRV6_MY_LOCATORS',
                'SRV6_MY_SIDS',
                'SRV6_SID_LIST',
                'SRV6_POLICY',
            )
        }
        devices[name] = tables
        for iface, node in dev.interfaces.sorted_items():
            cfg = node.config
            if isinstance(node, EthernetNode):
                if (
                    cfg.metric != 1
                    or cfg.description
                    or cfg.forwarding_v4 is not None
                    or cfg.forwarding_v6 is not None
                    or cfg.carrier_delay_up
                    or cfg.carrier_delay_down
                    or not 1280 <= cfg.mtu <= 65535
                ):
                    raise SchemaError(f'{name}.{iface}: unsupported interface settings')
                if cfg.speed % 1e6 or not 1e6 <= cfg.speed <= 0xFFFFFFFF * 1e6:
                    raise SchemaError(f'{name}.{iface}: speed must be integer Mbit/s')
                if cfg.aggregate_id or cfg.unnumbered:
                    raise SchemaError(
                        f'{name}.{iface}: bundles/unnumbered are outside CONFIG_DB subset'
                    )
                tables['PORT'][iface] = {
                    'speed': str(int(cfg.speed / 1e6)),
                    'mtu': str(cfg.mtu),
                    'admin_status': 'up' if cfg.admin == AdminState.UP else 'down',
                }
                table = 'INTERFACE'
            elif isinstance(node, LoopbackNode):
                table = 'LOOPBACK_INTERFACE'
            else:
                raise SchemaError(f'{name}.{iface}: unsupported interface type')
            tables[table][iface] = {}
            for af in (4, 6):
                for host, length in getattr(cfg, f'ipv{af}'):
                    tables[table][
                        f'{iface}|{to_address(host, AddressFamily(af))}/{length}'
                    ] = {}
        for af, rib in dev.ribs.sorted_items():
            for route in rib.clients.get(STATIC, {}).values():
                if route.source != STATIC:
                    continue
                key = f'{to_address(route.prefix[0], AddressFamily(af))}/{route.prefix[1]}'
                if key in tables['STATIC_ROUTE'] or route.metric or route.distinguisher:
                    raise SchemaError(
                        f'{name}.STATIC_ROUTE[{key}]: unsupported multiple rows/metric/distinguisher'
                    )
                row: dict[str, Any] = {'distance': route.distance}
                if len(route.nexthops) == 1 and route.nexthops[0].special == BLACKHOLE:
                    row['blackhole'] = 'true'
                else:
                    for hop in route.nexthops:
                        if (
                            hop.special is not None
                            or hop.weight != 1
                            or hop.policy
                            or hop.srv6
                        ):
                            raise SchemaError(
                                f'{name}.STATIC_ROUTE[{key}]: unsupported nexthop'
                            )
                    hosts = [
                        str(to_address(h.address, AddressFamily(h.af or af)))
                        for h in route.nexthops
                        if h.address is not None
                    ]
                    ports = [
                        h.interface for h in route.nexthops if h.interface is not None
                    ]
                    if (
                        hosts
                        and len(hosts) != len(route.nexthops)
                        or ports
                        and len(ports) != len(route.nexthops)
                    ):
                        raise SchemaError(
                            f'{name}.STATIC_ROUTE[{key}]: mixed nexthop forms'
                        )
                    if hosts:
                        row['nexthop'] = ','.join(hosts)
                    if ports:
                        row['ifname'] = ','.join(ports)
                tables['STATIC_ROUTE'][key] = row
        db = dev.srv6_sids
        if db:
            for key, loc in db.locators.sorted_items():
                tables['SRV6_MY_LOCATORS'][key] = {
                    'prefix': str(IPv6Network(loc.prefix)),
                    **_lengths(loc.structure),
                }
            for sid, local in db.sids.sorted_items():
                if local.owner != STATIC:
                    raise SchemaError(
                        f'{name}: CONFIG_DB supports static SID ownership only'
                    )
                key = meta.get('sid_keys', {}).get(name, {}).get(sid)
                if key is None:
                    loc = sr.select_locator(db, local.structure, None, sid)
                    key = f'{loc.name}|{IPv6Address(sid)}/{local.length}'
                row = {
                    'action': sr.behavior_name(local.behavior),
                    **_lengths(local.structure),
                    'flavors': _flavor_list(local.flavors),
                    'decap_dscp_mode': 'pipe',
                }
                if local.interface is not None:
                    row['interface'] = local.interface
                if local.nexthop is not None:
                    row['adj'] = str(IPv6Address(local.nexthop))
                tables['SRV6_MY_SIDS'][key] = row
        lists = dict(meta.get('lists', {}).get(name, {}))
        policies = dev.srv6_policies
        if policies:
            if policies.steering:
                raise SchemaError(
                    f'{name}: steering tables are outside CONFIG_DB subset'
                )
            assigned: dict[str, tuple[Any, ...]] = {}
            for _, policy in policies.policies.sorted_items():
                if policy.owner != STATIC:
                    raise SchemaError(
                        f'{name}: CONFIG_DB supports static policy ownership only'
                    )
                key = policy.name or f'{policy.color}|{IPv6Address(policy.endpoint)}'
                if key in tables['SRV6_POLICY']:
                    raise SchemaError(f'{name}: duplicate policy name {key!r}')
                if not policy.candidate_paths:
                    raise SchemaError(
                        f'{name}.{key}: nonempty candidate_paths required'
                    )
                candidates = []
                for pi, candidate in enumerate(policy.candidate_paths):
                    if not candidate.segment_lists:
                        raise SchemaError(
                            f'{name}.{key}: nonempty segment_lists required'
                        )
                    refs = []
                    for li, segment_list in enumerate(candidate.segment_lists):
                        if not segment_list.segments or segment_list.weight <= 0:
                            raise SchemaError(
                                f'{name}.{key}: nonempty list and positive weight required'
                            )
                        list_name = segment_list.name or f'{key}:{pi}:{li}'
                        if (
                            list_name in assigned
                            and assigned[list_name] != segment_list.segments
                        ):
                            raise SchemaError(
                                f'{name}: conflicting list name {list_name!r}'
                            )
                        assigned[list_name] = lists[list_name] = segment_list.segments
                        refs.append({'name': list_name, 'weight': segment_list.weight})
                    candidates.append(
                        {
                            'preference': candidate.preference,
                            **(
                                {'name': candidate.name}
                                if candidate.name is not None
                                else {}
                            ),
                            'protocol_origin': candidate.protocol_origin,
                            'originator': list(candidate.originator),
                            'discriminator': candidate.discriminator,
                            'segment_lists': refs,
                        }
                    )
                tables['SRV6_POLICY'][key] = {
                    'color': policy.color,
                    'endpoint': str(IPv6Address(policy.endpoint)),
                    **(
                        {'bsid': str(IPv6Address(policy.bsid))}
                        if policy.bsid is not None
                        else {}
                    ),
                    'fallback': 'DROP'
                    if policy.fallback == sr.FALLBACK_DROP
                    else 'IGP',
                    'candidate_paths': candidates,
                }
        tables['SRV6_SID_LIST'] = {
            key: {'path': [_segment_json(s) for s in segments]}
            for key, segments in sorted(lists.items())
        }
    return {'version': 1, 'devices': devices}


def appl_db(net: Network) -> dict[str, Any]:
    """APPL_DB-style SID-list tables, in wire forwarding order, per device.

    This is an encoding export, not a claim of installed policy validity.
    Only explicit literal metadata is used; scoped bare SIDs are never inferred
    from an address match at another device (RFC 9800 section 6.2).
    """
    config = _encode(net)
    result = {}
    for name, tables in config['devices'].items():
        table = {}
        for key, row in tables['SRV6_SID_LIST'].items():
            segments = [_literal(value, key) for value in row['path']]
            wire = compress(
                [(s.address, s.structure, s.flavors or 0) for s in segments]
            )
            table[key] = {'path': ','.join(str(IPv6Address(s)) for s in wire)}
        result[name] = {'SRV6_SID_LIST_TABLE': table}
    return result
