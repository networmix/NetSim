"""The bounded CONFIG_DB schema preserves literal SR records."""

import json
from copy import deepcopy
from dataclasses import replace
from ipaddress import IPv6Address

import pytest

from netsim.adapters import sonic
from netsim.model import srv6 as sr
from netsim.model.contracts import STATIC
from netsim.model.network import Network
from netsim.model.state import tree_equal


def config():
    return {
        'version': 1,
        'devices': {
            'R1': {
                'PORT': {'Ethernet0': {'speed': '100000', 'mtu': '1500'}},
                'INTERFACE': {'Ethernet0|2001:db8::1/127': {}},
                'LOOPBACK_INTERFACE': {'Loopback0|2001:db8:ffff::1/128': {}},
                'SRV6_MY_LOCATORS': {
                    'loc': {
                        'prefix': '5F00:0:1::/48',
                        'block_len': 32,
                        'node_len': 16,
                        'func_len': 16,
                        'arg_len': 64,
                    }
                },
                'SRV6_MY_SIDS': {
                    'loc|5F00:0:E001::/48': {
                        'action': 'uA',
                        'interface': 'Ethernet0',
                        'decap_dscp_mode': 'pipe',
                    },
                    'loc|5f00:0:1:e002::/64': {
                        'action': 'uA',
                        'interface': 'Ethernet0',
                        'decap_dscp_mode': 'pipe',
                    },
                    'loc|5f00:0:e800::/48': {
                        'action': 'uDT46',
                        'decap_vrf': 'default',
                        'decap_dscp_mode': 'pipe',
                    },
                },
                'SRV6_SID_LIST': {'path': {'path': ['5F00:0:E001::', '5f00:0:e800::']}},
                'SRV6_POLICY': {
                    'p': {
                        'color': 10,
                        'endpoint': '2001:db8:ffff::1',
                        'candidate_paths': [
                            {
                                'preference': 200,
                                'segment_lists': [{'name': 'path', 'weight': 3}],
                            }
                        ],
                        'fallback': 'DROP',
                    }
                },
            }
        },
    }


def test_round_trip():
    source = config()
    net = sonic.load(source)
    assert sonic.dump(net) == source


def test_literals_structures_flavors_survive_json_and_convergence(tmp_path):
    source = config()
    bare = source['devices']['R1']['SRV6_MY_SIDS']['loc|5F00:0:E001::/48']
    bare['flavors'] = ['psp', 'usd']
    net = sonic.load(json.dumps(source))
    sids = net['R1'].node.srv6_sids.sids
    a, b = (sids[int(IPv6Address(s))] for s in ('5f00:0:e001::', '5f00:0:1:e002::'))
    assert a.sid != b.sid and a.length == 48 and b.length == 64
    assert a.structure == sr.F3216_LIB and b.structure == sr.F3216_COMPOSITE
    assert a.flavors == sr.NEXT_CSID | sr.PSP | sr.USD
    net.converge()
    assert sonic.dump(net, tmp_path / 'config.json') == source
    assert sonic.dump(sonic.load(tmp_path / 'config.json')) == source
    assert tree_equal(sonic.load(source).state, sonic.load(source).state)


def test_dump_reads_current_tree_and_forks_are_detached():
    source = config()
    net = sonic.load(source)
    source['devices'].clear()
    fork = net.fork()
    policy = next(iter(fork['R1'].node.srv6_policies.policies.values()))
    candidate = replace(policy.candidate_paths[0], preference=42)
    fork['R1'].policy_client().replace(replace(policy, candidate_paths=(candidate,)))
    fork['R1']['Ethernet0'].configure(mtu=9000)
    exported = sonic.dump(fork)
    assert exported['devices']['R1']['PORT']['Ethernet0']['mtu'] == '9000'
    assert (
        exported['devices']['R1']['SRV6_POLICY']['p']['candidate_paths'][0][
            'preference'
        ]
        == 42
    )
    assert sonic.dump(net) == config()
    assert sonic.dump(sonic.load(exported)) == exported
    exported['devices'].clear()
    assert sonic.dump(fork)['devices']


def test_plain_tables_and_literal_classic_sid():
    source = {
        'version': 1,
        'devices': {
            'a': {
                'PORT': {'Ethernet0': {'admin_status': 'down'}},
                'INTERFACE': {'Ethernet0': {}, 'Ethernet0|10.0.0.1/31': {}},
                'LOOPBACK_INTERFACE': {'lo': {}, 'lo|10.255.0.1/32': {}},
                'STATIC_ROUTE': {
                    '10.1.0.0/16': {
                        'nexthop': '10.0.0.0',
                        'ifname': 'Ethernet0',
                        'distance': '2',
                    },
                    '10.2.0.0/16': {'ifname': 'Ethernet0'},
                    '10.3.0.0/16': {'nexthop': '10.0.0.0'},
                    '10.4.0.0/16': {'blackhole': 'true'},
                },
                'SRV6_MY_LOCATORS': {
                    'classic': {
                        'prefix': '2001:db8:1:1::/64',
                        'block_len': 48,
                        'node_len': 16,
                        'func_len': 16,
                        'arg_len': 48,
                    }
                },
                'SRV6_MY_SIDS': {
                    'classic|2001:db8:1:1:abcd::/80': {'action': 'End.DT46'}
                },
            }
        },
    }
    net = sonic.load(source)
    assert (
        net['a'].node.srv6_sids.sids[int(IPv6Address('2001:db8:1:1:abcd::'))].structure
        == sr.UNCOMPRESSED
    )
    net.converge()
    assert sonic.dump(net) == source
    assert len(net['a'].rib(4).clients[STATIC]) == 4


def test_wire_export_hand_checked_compression_and_unknown_literals():
    source = config()
    source['devices']['R1']['SRV6_SID_LIST']['path']['path'] = [
        {
            'sid': '5f00:0:e001::',
            'block_len': 32,
            'node_len': 0,
            'func_len': 16,
            'arg_len': 80,
            'flavors': ['usid'],
        },
        {
            'sid': '5f00:0:e800::',
            'block_len': 32,
            'node_len': 0,
            'func_len': 16,
            'arg_len': 0,
        },
        '2001:db8:dead::1',
    ]
    net = sonic.load(source)
    assert sonic.dump(net) == source
    assert sonic.appl_db(net) == {
        'R1': {
            'SRV6_SID_LIST_TABLE': {
                'path': {'path': '5f00:0:e001:e800::,2001:db8:dead::1'}
            }
        }
    }


@pytest.mark.parametrize(
    'table,key,field,value,match',
    [
        ('PORT', 'Ethernet0', 'vrf', 'blue', 'unsupported fields'),
        ('PORT', 'Ethernet0', 'speed', True, 'integer'),
        ('PORT', 'Ethernet0', 'mtu', 12, 'must be in'),
        ('PORT', 'Ethernet0', 'admin_status', 'testing', 'supported values'),
        ('SRV6_MY_LOCATORS', 'loc', 'vrf', 'blue', 'supported values'),
        ('SRV6_MY_LOCATORS', 'loc', 'block_len', -1, 'must be in'),
        ('SRV6_MY_SIDS', 'loc|5F00:0:E001::/48', 'action', 'uB6', 'supported values'),
        (
            'SRV6_MY_SIDS',
            'loc|5F00:0:E001::/48',
            'action',
            'End.DT4',
            'supported values',
        ),
        (
            'SRV6_MY_SIDS',
            'loc|5F00:0:E001::/48',
            'action',
            'End.DX6',
            'supported values',
        ),
        (
            'SRV6_MY_SIDS',
            'loc|5F00:0:E001::/48',
            'flavors',
            ['usp'],
            'supported values',
        ),
        (
            'SRV6_MY_SIDS',
            'loc|5F00:0:E001::/48',
            'flavors',
            ['usid', 'usid'],
            'unique flavors',
        ),
        (
            'SRV6_MY_SIDS',
            'loc|5F00:0:E001::/48',
            'decap_dscp_mode',
            'uniform',
            'supported values',
        ),
        (
            'SRV6_MY_SIDS',
            'loc|5F00:0:E001::/48',
            'decap_vrf',
            'blue',
            'supported values',
        ),
        (
            'SRV6_MY_SIDS',
            'loc|5F00:0:E001::/48',
            'decap_vrf',
            'default',
            'applies only',
        ),
        (
            'SRV6_MY_SIDS',
            'loc|5F00:0:E001::/48',
            'interface',
            'missing',
            'bound interface',
        ),
        (
            'SRV6_MY_SIDS',
            'loc|5f00:0:e800::/48',
            'flavors',
            ['psp'],
            'UNSUPPORTED_FLAVOR',
        ),
        ('SRV6_POLICY', 'p', 'encap', 'H.Insert', 'unsupported fields'),
        ('SRV6_POLICY', 'p', 'candidate_paths', [], 'nonempty array'),
        ('SRV6_POLICY', 'p', 'endpoint', True, 'nonempty string'),
        ('SRV6_POLICY', 'p', 'color', 2**32, 'must be in'),
    ],
)
def test_schema_rejections_have_table_and_key(table, key, field, value, match):
    source = config()
    source['devices']['R1'][table][key][field] = value
    with pytest.raises(sonic.SchemaError, match=match) as exc:
        sonic.load(source)
    assert f'{table}[{key}]' in str(exc.value)


@pytest.mark.parametrize(
    'document,match',
    [
        ({'version': 2, 'devices': {}}, 'version'),
        ({'version': True, 'devices': {}}, 'version'),
        ({'version': 1, 'devices': [], 'extra': {}}, 'unsupported fields'),
        ({'version': 1, 'devices': []}, 'object'),
        ({'version': 1}, 'missing fields'),
        ({'version': 1, 'devices': {'r': {'BGP_NEIGHBOR': {}}}}, 'unsupported fields'),
        ('{"version":1,"version":1,"devices":{}}', 'duplicate key'),
    ],
)
def test_envelope_rejections(document, match):
    with pytest.raises(sonic.SchemaError, match=match):
        sonic.load(document)


def test_candidate_lists_weights_and_explicit_literal_metadata():
    source = config()
    policy = source['devices']['R1']['SRV6_POLICY']['p']
    policy['candidate_paths'].append(
        {
            'name': 'standby',
            'preference': 100,
            'protocol_origin': 20,
            'originator': [0, 2],
            'discriminator': 7,
            'segment_lists': [{'name': 'path', 'weight': 1}],
        }
    )
    policy['bsid'] = '5F00:0:F001::'
    net = sonic.load(source)
    saved = next(iter(net['R1'].node.srv6_policies.policies.values()))
    assert [p.preference for p in saved.candidate_paths] == [200, 100]
    assert saved.candidate_paths[0].segment_lists[0].weight == 3
    assert saved.bsid == int(IPv6Address('5f00:0:f001::'))
    assert sonic.dump(net) == source
    for replacement, match in [
        ([{'name': 'missing'}], 'unknown segment list'),
        ([{'name': 'path', 'weight': 0}], 'must be in'),
    ]:
        bad = deepcopy(source)
        bad['devices']['R1']['SRV6_POLICY']['p']['candidate_paths'][0][
            'segment_lists'
        ] = replacement
        with pytest.raises(sonic.SchemaError, match=match):
            sonic.load(bad)


def test_native_export_and_unsupported_native_configs():
    net = Network()
    dev = net.add_device('r')
    dev.add_locator('loc', structure=sr.F3216_GIB)
    terminal = dev.add_local_sid(sr.END_DT46, structure=sr.F3216_TERMINAL)
    dev.policy_client().add(
        sr.SrPolicy(
            STATIC,
            2,
            1,
            candidate_paths=(
                sr.CandidatePath(
                    segment_lists=(sr.SegmentList((sr.LiteralSid(terminal.sid),)),)
                ),
            ),
        )
    )
    exported = sonic.dump(net)
    imported = sonic.load(exported)
    assert sonic.dump(imported) == exported
    policy = next(iter(dev.node.srv6_policies.policies.values()))
    dev.policy_client().set_steering((sr.SteeringRule('r', policy.key),))
    with pytest.raises(sonic.SchemaError, match='steering'):
        sonic.dump(net)


def test_sonic_locator_defaults_and_address_form_prefix():
    source = config()
    source['devices']['R1']['SRV6_MY_LOCATORS']['loc'] = {'prefix': '5F00:0:1::'}
    source['devices']['R1']['SRV6_MY_SIDS'] = {}
    net = sonic.load(source)
    loc = net['R1'].node.srv6_sids.locators['loc']
    assert loc.prefix == (int(IPv6Address('5f00:0:1::')), 48)
    assert loc.structure == sr.SidStructure(32, 16, 16, 0)
    assert sonic.dump(net) == source


def test_mapping_input_and_invalid_scalar_input():
    from types import MappingProxyType

    source = config()
    assert sonic.dump(sonic.load(MappingProxyType(source))) == source
    with pytest.raises(sonic.SchemaError, match='object'):
        sonic.load(12)


def test_dump_rejects_unrepresentable_interface_changes():
    net = sonic.load(config())
    net['R1']['Ethernet0'].configure(speed=123456789)
    with pytest.raises(sonic.SchemaError, match='speed'):
        sonic.dump(net)


@pytest.mark.parametrize('action', ['End.DT6', 'End.DX4', 'End.B6.Encaps'])
def test_remaining_gate_b_exclusions(action):
    source = config()
    source['devices']['R1']['SRV6_MY_SIDS']['loc|5f00:0:e800::/48']['action'] = action
    with pytest.raises(sonic.SchemaError, match='supported values'):
        sonic.load(source)


def test_duplicate_alias_sids_and_bad_lengths_rejected():
    source = config()
    sids = source['devices']['R1']['SRV6_MY_SIDS']
    sids['loc|5f00:0:e001::/48'] = dict(sids['loc|5F00:0:E001::/48'])
    with pytest.raises(sonic.SchemaError, match='duplicate literal SID'):
        sonic.load(source)
    del sids['loc|5f00:0:e001::/48']
    sids['loc|5F00:0:E001::/48'].update(
        block_len=32, node_len=0, func_len=32, arg_len=64
    )
    with pytest.raises(sonic.SchemaError, match='prefix length'):
        sonic.load(source)


def test_device_set_keeps_bare_sids_scoped_and_literal_composites_distinct():
    source = config()
    second = deepcopy(source['devices']['R1'])
    second['SRV6_MY_LOCATORS']['loc']['prefix'] = '5f00:0:2::/48'
    second['SRV6_MY_SIDS']['loc|5f00:0:2:e002::/64'] = second['SRV6_MY_SIDS'].pop(
        'loc|5f00:0:1:e002::/64'
    )
    second['LOOPBACK_INTERFACE'] = {'Loopback0|2001:db8:ffff::2/128': {}}
    second['INTERFACE'] = {'Ethernet0|2001:db8::/127': {}}
    source['devices']['R2'] = second
    net = sonic.load(source)
    net.add_link(('R1', 'Ethernet0'), ('R2', 'Ethernet0'))
    net.converge()
    bare = int(IPv6Address('5f00:0:e001::'))
    assert net['R1'].node.srv6_sids.sids[bare].adjacency_up
    assert net['R2'].node.srv6_sids.sids[bare].adjacency_up
    assert sonic.dump(net) == source


def test_default_classic_locator_does_not_reinterpret_usid_structure():
    source = config()
    source['devices']['R1']['SRV6_MY_LOCATORS']['loc'] = {'prefix': '5f00:0:1::'}
    with pytest.raises(sonic.SchemaError, match='both use supported F3216'):
        sonic.load(source)


def test_dump_rejects_invalid_policy_lists_and_unmodeled_port_settings():
    net = sonic.load(config())
    policy = next(iter(net['R1'].node.srv6_policies.policies.values()))
    net['R1'].policy_client().replace(replace(policy, candidate_paths=()))
    with pytest.raises(sonic.SchemaError, match='nonempty candidate'):
        sonic.dump(net)
    net['R1'].policy_client().replace(policy)
    net['R1']['Ethernet0'].configure(metric=9)
    with pytest.raises(sonic.SchemaError, match='interface settings'):
        sonic.dump(net)


@pytest.mark.parametrize(
    'field,unsupported,default',
    [
        ('enabled', False, True),
        ('srv6_hop_limit', 7, 64),
        ('srv6_source', 1, None),
    ],
)
@pytest.mark.parametrize('imported', [False, True])
def test_device_settings_cannot_be_silently_defaulted_on_export(
    field,
    unsupported,
    default,
    imported,
    tmp_path,
):
    if imported:
        net = sonic.load(config())
    else:
        net = Network()
        net.add_device('R1')
    original = sonic.dump(net)
    assert getattr(sonic.load(original)['R1'].node.config, field) == default
    net['R1'].configure(**{field: unsupported})
    before = net.state
    target = tmp_path / 'config.json'
    target.write_text('existing file')
    with pytest.raises(sonic.SchemaError, match=rf'R1.*{field}'):
        sonic.dump(net, target)
    assert net.state is before
    assert target.read_text() == 'existing file'
    net['R1'].configure(**{field: default})
    assert sonic.dump(net) == original
    assert getattr(sonic.load(sonic.dump(net))['R1'].node.config, field) == default


@pytest.mark.parametrize(
    'field,value',
    [
        ('enabled', False),
        ('srv6_hop_limit', 7),
        ('srv6_source', 1),
    ],
)
def test_device_settings_are_outside_version_one_input_schema(field, value):
    source = config()
    source['devices']['R1'][field] = value
    with pytest.raises(sonic.SchemaError, match=rf'R1.*{field}'):
        sonic.load(source)
