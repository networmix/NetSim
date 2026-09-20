"""Learned SR claims never fall back to the oracle's remote state."""

from dataclasses import replace

import pytest

from netsim.model import srv6 as sr
from netsim.model.contracts import AgentNode, ClientId, RemoteSid, SrDbView
from netsim.model.state import PMap
from tests.model.test_policies import diamond, policy, state_of


def publish(net, view):
    def apply(state):
        dev = state.devices['R1']
        old = dev.agents.get('ls')
        agent = (
            replace(old, srdb_view=view)
            if old
            else AgentNode('ls', 1000, ClientId('ls'), srdb_view=view)
        )
        return replace(
            state,
            devices=state.devices.set(
                'R1', replace(dev, agents=dev.agents.set('ls', agent))
            ),
        )

    net.update(apply)


def claims(routers):
    sids, locators = [], []
    from netsim.model.srv6 import _PolicyValidator

    oracle = _PolicyValidator(routers['R1'].network.state, 'R1')
    for name, dev in routers.items():
        locators.extend(
            (name, loc.prefix) for loc in dev.node.srv6_sids.locators.values()
        )
        locators.extend(
            (name, (addr, 128))
            for node in dev.node.interfaces.values()
            for addr, _ in node.config.ipv6
        )
        for sid in dev.node.srv6_sids.sids.values():
            sids.append(
                RemoteSid(
                    sid.sid,
                    sid.length,
                    sid.behavior,
                    sid.flavors,
                    sid.structure,
                    name,
                    sid.adjacency_up,
                    oracle.peer(name, sid.interface) if sid.interface else None,
                    sid.interface,
                )
            )
    return SrDbView(tuple(sids), tuple(locators))


def literal_policy(routers):
    terminal = next(
        s
        for s in routers['R4'].node.srv6_sids.sids.values()
        if s.behavior == sr.END_DT46
    )
    return policy(routers, lists=(sr.SegmentList((terminal.sid,)),)), terminal


def test_empty_agent_view_does_not_use_oracle():
    net, routers = diamond(compressed=False)
    p, _ = literal_policy(routers)
    net.converge()
    assert state_of(routers, p).status == sr.POLICY_UP
    publish(net, SrDbView())
    routers['R1'].configure(srdb_source=('agent', 'ls'))
    net.converge()
    assert state_of(routers, p).status == sr.POLICY_DOWN
    assert (0, 0, 'SID_UNKNOWN_IN_VIEW') in state_of(routers, p).reasons


def test_unsupported_and_missing_agent_rejected():
    net, routers = diamond(compressed=False)
    for source in (('bgp', 'x'), ('agent', 'missing'), 'oracle', ('agent',)):
        with pytest.raises(ValueError, match='srdb_source'):
            routers['R1'].configure(srdb_source=source)
        dev = routers['R1'].node
        invalid = replace(
            net.state,
            devices=net.state.devices.set(
                'R1', replace(dev, config=replace(dev.config, srdb_source=source))
            ),
        )
        assert any('srdb_source' in issue for issue in sr.validate(invalid))
        with pytest.raises(ValueError, match='srdb_source'):
            sr.derive_policy_states(invalid, 'R1')


@pytest.mark.parametrize('compressed', [False, True])
def test_view_advertisements_and_adjacency_down(compressed):
    net, routers = diamond(compressed=compressed)
    p = policy(routers)
    view = claims(routers)
    publish(net, view)
    routers['R1'].configure(srdb_source=('agent', 'ls'))
    net.converge()
    status = state_of(routers, p)
    assert status.status == sr.POLICY_UP and status.strict_valid == ((0, 0),)
    old_epoch = routers['R1'].node.resolver_input_epoch
    down = replace(
        view,
        sids=tuple(
            replace(sid, adjacency_up=False)
            if sid.owner == 'R2' and sid.behavior == sr.END_X
            else sid
            for sid in view.sids
        ),
    )
    publish(net, down)
    assert routers['R1'].node.resolver_input_epoch[6] > old_epoch[6]
    assert state_of(routers, p).status == sr.POLICY_DOWN
    net.converge()
    publish(net, view)
    net.converge()
    assert state_of(routers, p).status == sr.POLICY_UP


def test_stale_view_stays_valid_when_oracle_rejects_remote_shadowing():
    from ipaddress import IPv6Address

    from netsim.model.routing import Nexthop

    net, routers = diamond(compressed=False)
    p, terminal = literal_policy(routers)
    publish(net, claims(routers))
    routers['R1'].configure(srdb_source=('agent', 'ls'))
    net.converge()
    assert state_of(routers, p).status == sr.POLICY_UP
    # A remote more-specific RIB row shadows its SID. The agent has not learned it.
    routers['R4'].add_route(f'{IPv6Address(terminal.sid)}/128', [Nexthop.blackhole()])
    net.converge()
    assert state_of(routers, p).status == sr.POLICY_UP
    learned = routers['R1'].node.srv6_policies
    routers['R1'].configure(srdb_source=None)
    net.converge()
    assert state_of(routers, p).status == sr.POLICY_DOWN
    assert learned.states[p.key].status == sr.POLICY_UP


def test_agent_validation_never_reads_remote_rib_or_interfaces(monkeypatch):
    from netsim.model.derive import DeviceContext

    net, routers = diamond(compressed=False)
    p, _ = literal_policy(routers)
    publish(net, claims(routers))
    routers['R1'].configure(srdb_source=('agent', 'ls'))
    init = DeviceContext.__init__

    def local_only(self, state, device):
        assert device == 'R1', 'remote resolver access'
        init(self, state, device)

    monkeypatch.setattr(DeviceContext, '__init__', local_only)
    # Remove remote nodes entirely in a pure query, preserving local observations
    # and the view. Validation must not use remote handles or the global inventory.
    state = replace(net.state, devices=PMap({'R1': routers['R1'].node}))
    table = sr.derive_policy_states(state, 'R1')
    assert table.states[p.key].status == sr.POLICY_UP
    assert all(kind != 'reach' for _, kind, _, _ in table.states[p.key].dependencies)


def test_missing_locator_and_endpoint_claims_are_unknown():
    net, routers = diamond(compressed=False)
    p, _ = literal_policy(routers)
    view = claims(routers)
    publish(net, replace(view, locators=()))
    routers['R1'].configure(srdb_source=('agent', 'ls'))
    net.converge()
    assert state_of(routers, p).status == sr.POLICY_DOWN
    # Advertise the endpoint but omit the SID owner's locator.
    publish(net, replace(view, locators=(('R4', (p.endpoint, 128)),)))
    net.converge()
    assert (0, 0, 'LOCATOR_UNKNOWN_IN_VIEW') in state_of(routers, p).reasons


def test_relaxed_local_resolution_never_upgrades_unknown_agent_claim():
    from netsim.model.routing import ResolutionPolicy

    net, routers = diamond(compressed=False)
    p, _ = literal_policy(routers)
    publish(net, SrDbView())
    routers['R1'].configure(
        srdb_source=('agent', 'ls'),
        resolution_policy=ResolutionPolicy(validate_all_sids=False),
    )
    net.converge()
    status = state_of(routers, p)
    assert status.first_valid == ((0, 0),)
    assert status.strict_valid == () and status.status == sr.POLICY_DOWN


def test_unpublished_view_and_nht_registration_do_not_advance_epochs():
    from netsim.model.contracts import STATIC, NhtKey
    from tests.model.test_network import A

    net, routers = diamond(compressed=False)
    p, _ = literal_policy(routers)
    publish(net, None)
    routers['R1'].configure(srdb_source=('agent', 'ls'))
    net.converge()
    assert state_of(routers, p).status == sr.POLICY_DOWN
    epoch = routers['R1'].node.resolver_input_epoch
    client = routers['R1'].nht_client()
    k = NhtKey(STATIC, 4, A('10.0.0.2'))
    client.register(k)
    assert routers['R1'].node.resolver_input_epoch is epoch
    client.unregister(k)
    assert routers['R1'].node.resolver_input_epoch is epoch


def test_configuration_rejects_invalid_view_and_new_device_mode():
    net, routers = diamond(compressed=False)
    publish(net, ())
    with pytest.raises(ValueError, match='SrDbView'):
        routers['R1'].configure(srdb_source=('agent', 'ls'))
    with pytest.raises(ValueError, match='srdb_source'):
        dev = routers['R1'].node
        net.update(
            lambda state: replace(
                state,
                devices=state.devices.set(
                    'R1',
                    replace(dev, config=replace(dev.config, srdb_source=('bgp', 'x'))),
                ),
            )
        )
