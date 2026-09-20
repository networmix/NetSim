"""C2R: detached query inputs and interface-incarnation registration identity."""

from collections.abc import Mapping
from dataclasses import fields, is_dataclass, replace

import pytest

from netsim import Environment
from netsim.model import nht
from netsim.model import srv6 as sr
from netsim.model.addressing import MacAddress
from netsim.model.contracts import STATIC, NhtKey
from netsim.model.derive import DeviceContext
from netsim.model.entities import Device
from netsim.model.network import Network
from netsim.model.routing import Nexthop, ResolutionPolicy
from netsim.model.state import DeviceState, NetworkState, validate_immutable
from netsim.runtime import Simulation
from tests.model.test_network import A, build_diamond
from tests.model.test_policies import diamond, policy


def test_detached_self_covering_candidate_and_locality():
    net, routers = build_diamond()
    dev = routers['R1']
    row = dev.add_route('192.0.2.0/24', ['192.0.2.1'])
    key = NhtKey(STATIC, 4, A('192.0.2.1'))
    local = nht.local_context(dev.node)
    validate_immutable(local)

    def check(value):
        assert not isinstance(value, (NetworkState, DeviceState, Device))
        if is_dataclass(value):
            for field in fields(value):
                check(getattr(value, field.name))
        elif isinstance(value, Mapping):
            for k, v in value.items():
                check(k)
                check(v)
        elif isinstance(value, (tuple, frozenset)):
            for item in value:
                check(item)

    check(local)
    assert not hasattr(local, 'state') and not hasattr(local, 'dev')
    answer = nht.resolve(
        local, ResolutionPolicy(), key, exclude_rows=frozenset({row.key}), input_epoch=7
    )
    assert not answer.eligible and answer.reason == 'SELF_COVERED'
    assert answer.via_prefix == row.prefix and answer.input_epoch == 7
    # The detached snapshot remains usable after the live device disappears.
    net.remove_device(dev.name)
    assert (
        nht.resolve(
            local,
            ResolutionPolicy(),
            key,
            exclude_rows=frozenset({row.key}),
            input_epoch=7,
        )
        == answer
    )


@pytest.mark.parametrize('af,address', [(4, '10.0.0.2'), (6, '2001:db8:99::2')])
def test_detached_query_and_installed_match_device_context(af, address):
    net, routers = build_diamond()
    dev = routers['R1']
    if af == 6:
        dev['Po1'].configure(forwarding_v6=True)
        dev.add_route(address + '/128', [('Po1', '10.1.12.1')])
    net.converge()
    local = nht.local_context(dev.node)
    key = NhtKey(STATIC, af, A(address))
    epoch = dev.node.resolver_input_epoch[af]
    expected = nht.resolve(
        DeviceContext(net.state, dev.name), ResolutionPolicy(), key, input_epoch=epoch
    )
    assert expected.eligible
    assert nht.resolve(local, ResolutionPolicy(), key, input_epoch=epoch) == expected
    assert dev.nht_client().resolve(key) == expected
    assert nht.installed(local, key) == dev.nht_client().installed(key)
    assert nht.installed(local, key) == nht.installed(net.state, dev.name, key)
    assert local.interface_generation('absent') is None
    assert not local.interface_exists('absent') and not local.l3_usable('absent', af)


def test_detached_installed_retains_programmed_epoch_during_delay():
    net, routers = build_diamond()
    dev = routers['R1']
    sim = Simulation(Environment(), net)
    dev.configure(fib_delay=2)
    sim.run_until(3)
    key = NhtKey(STATIC, 4, A('192.0.2.1'))
    dev.add_route('192.0.2.1/32', [('Po1', '10.1.12.1')])
    local = nht.local_context(dev.node)
    epoch = dev.node.resolver_input_epoch[4]
    assert nht.resolve(local, ResolutionPolicy(), key, input_epoch=epoch).eligible
    answer = nht.installed(local, key)
    assert answer.status == 'PENDING' and answer.processed_epoch < epoch
    assert answer.prefix is None
    sim.run_until(5)
    assert dev.nht_client().installed(key).status == 'INSTALLED'
    assert dev.nht_client().installed(key, ctx=local) == answer
    assert nht.installed(local, key) == answer
    captured = dev.nht_client().resolve(key, ctx=local)
    dev.rib_client().sync(())
    assert not dev.nht_client().resolve(key).eligible
    assert dev.nht_client().resolve(key, ctx=local) == captured


def test_detached_query_keeps_local_compiled_policy_programs():
    net, routers = diamond()
    p = policy(routers)
    dev = routers['R1']
    dev.add_route('192.0.2.1/32', [Nexthop(policy=sr.PolicyRef(*p.key))])
    net.converge()
    key = NhtKey(STATIC, 4, A('192.0.2.1'))
    epoch = dev.node.resolver_input_epoch[4]
    expected = nht.resolve(
        DeviceContext(net.state, dev.name), ResolutionPolicy(), key, input_epoch=epoch
    )
    assert expected.eligible and expected.legs
    assert (
        nht.resolve(
            nht.local_context(dev.node), ResolutionPolicy(), key, input_epoch=epoch
        )
        == expected
    )


def scoped_pair():
    net = Network()
    a, b = net.add_device('a'), net.add_device('b')
    net.add_p2p(a, 'e', b, 'e', unnumbered=True)
    net.converge()
    address = MacAddress(b.node.interfaces['e'].mac).link_local_int()
    return net, a, NhtKey(STATIC, 6, address, interface='e')


def recreate_scope(net, dev):
    dev.remove_interface('e')
    net.converge()
    dev.add_ethernet('e', unnumbered=True)
    net.add_link(('a', 'e'), ('b', 'e'))
    net.converge()


def test_omitted_registration_generation_is_bound_until_explicit_replacement():
    net, dev, request = scoped_pair()
    client = dev.nht_client()
    bound = client.register(request)
    assert bound == replace(request, interface_generation=dev['e'].generation)
    assert tuple(dev.node.nht.registrations) == (bound,)
    assert client.result(bound).eligible
    assert client.result(request) is client.result(bound)
    assert nht.register(net.state, dev.name, request) is net.state
    recreate_scope(net, dev)
    stale = client.result(bound)
    assert not stale.eligible and stale.reason == 'SCOPE_STALE'
    assert client.result(request) is stale
    assert client.installed(bound).adjacencies == ()
    assert client.register(bound) is bound
    assert nht.register(net.state, dev.name, bound) is net.state
    # An unbound one-shot query deliberately asks about the current interface.
    assert client.resolve(request).eligible
    assert client.resolve(bound).reason == 'SCOPE_STALE'
    client.unregister(request)
    assert client.result(bound) is None
    replacement = client.register(request)
    assert replacement.interface_generation == dev['e'].generation
    assert replacement != bound and client.result(replacement).eligible


def test_pure_register_binds_generation_and_unbound_unregister_after_removal():
    net, dev, request = scoped_pair()
    root = nht.register(net.state, dev.name, request)
    (bound,) = root.devices[dev.name].nht.registrations
    assert bound.interface_generation == dev['e'].generation
    assert nht.register(root, dev.name, request) is root
    without = replace(
        root.devices[dev.name], interfaces=dev.node.interfaces.remove('e')
    )
    root = replace(root, devices=root.devices.set(dev.name, without))
    root = nht.refresh(root, dev.name)
    assert root.devices[dev.name].nht.registrations[bound].reason == 'SCOPE_STALE'
    removed = nht.unregister(root, dev.name, request)
    assert not removed.devices[dev.name].nht.registrations
    assert nht.unregister(removed, dev.name, request) is removed


def test_ambiguous_unbound_key_requires_explicit_generation():
    net, dev, request = scoped_pair()
    client = dev.nht_client()
    old = client.register(request)
    recreate_scope(net, dev)
    new = client.register(request)
    assert old != new and client.result(old).reason == 'SCOPE_STALE'
    assert client.result(new).eligible
    for operation in (client.result, client.unregister):
        with pytest.raises(ValueError, match='generation-bound key'):
            operation(request)
    client.unregister(old)
    assert client.result(request) is client.result(new)


def test_empty_local_context_and_absent_registration():
    dev = DeviceState('empty', 1)
    ctx = nht.local_context(dev)
    key = NhtKey(STATIC, 4, A('192.0.2.1'))
    assert ctx.neighbor_mac('missing', key.address) is None
    assert ctx.peer_mac('missing') is None
    assert not nht.resolve(ctx, ResolutionPolicy(), key, input_epoch=0).eligible
    assert nht.installed(ctx, key).status == 'PENDING'
    assert nht.registered_key(dev, key) is key
