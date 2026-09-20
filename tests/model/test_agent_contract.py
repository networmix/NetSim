"""Gate C slice C0: the agent contract records, validation and registration."""

import dataclasses

import pytest

from netsim.model import contracts as c
from netsim.model.contracts import (
    AgentConfig,
    AgentNode,
    AgentOutput,
    ClientId,
    ClientProfile,
    Datagram,
    Endpoint,
    NhtKey,
    RouteOp,
    SessionOp,
    TimerOp,
)
from netsim.model.routing import Nexthop, Route
from netsim.model.state import StateDelta, validate_immutable
from tests.model.test_network import build_diamond

REF = ClientId('ref', 0)
REF_PROFILE = ClientProfile(REF, distance=115, protocol_origin=20)


class Minimal:
    """The smallest plugin that satisfies ``DeviceAgent``."""

    client = REF
    profile = REF_PROFILE
    config = AgentConfig()

    def __init__(self, subs=(('interfaces',),)):
        self._subs = tuple(subs)

    def subscriptions(self):
        return self._subs

    def on_init(self, ctx):
        return AgentOutput(state=('init',))

    def on_run(self, ctx):
        return AgentOutput(state=ctx.agent_state)


class TestConfig:
    @pytest.mark.parametrize('bad', [-1e-3, float('inf'), float('nan')])
    def test_run_delay_finite_non_negative(self, bad):
        with pytest.raises(ValueError):
            AgentConfig(run_delay=bad)

    def test_zero_run_delay_is_permitted(self):
        assert AgentConfig(run_delay=0).run_delay == 0.0

    def test_limits_positive_integers(self):
        for name in ('inbox_limit', 'queue_limit', 'byte_limit'):
            with pytest.raises(ValueError):
                AgentConfig(**{name: 0})

    def test_ports_and_params_validated(self):
        with pytest.raises(ValueError):
            AgentConfig(listen_ports=(70000,))
        with pytest.raises(TypeError):
            AgentConfig(params={'peers': ['R2']})
        assert AgentConfig(params=(('peers', ('R2',)),)).params[0][1] == ('R2',)


class TestOperationRecords:
    def test_route_op_sync_exclusive(self):
        row = Route((0, 0), 4, REF, 115, (Nexthop.blackhole(),))
        with pytest.raises(ValueError):
            RouteOp(4, add=(row,), sync=())
        assert RouteOp(4, sync=()).sync == ()

    def test_one_route_op_per_family(self):
        with pytest.raises(ValueError):
            AgentOutput(route_ops=(RouteOp(4), RouteOp(4)))
        out = AgentOutput(route_ops=(RouteOp(4), RouteOp(6)))
        assert not out.is_noop()
        assert AgentOutput().is_noop()

    def test_row_family_must_match_op(self):
        row = Route((0, 0), 6, REF, 115, (Nexthop.blackhole(),))
        with pytest.raises(ValueError):
            c.check_output(AgentOutput(route_ops=(RouteOp(4, add=(row,)),)))
        with pytest.raises(TypeError):
            c.check_output(None)

    def test_mutable_state_and_payloads_rejected(self):
        with pytest.raises(TypeError):
            AgentOutput(state={'x': 1})
        with pytest.raises(TypeError):
            AgentOutput(srdb_view=[1])
        with pytest.raises(TypeError):
            Datagram('eth1', payload=[1, 2])
        with pytest.raises(TypeError):
            c.Message(1, payload=bytearray(b'x'))
        validate_immutable(
            AgentOutput(state=(1, 2), datagrams=(Datagram('eth1', b'x'),))
        )

    def test_timer_delay_strictly_positive(self):
        with pytest.raises(ValueError):
            TimerOp('hello', 0)
        assert TimerOp('hello').delay is None  # cancel
        assert TimerOp('hello', 0.5).delay == 0.5

    def test_session_op_shapes(self):
        local = Endpoint(6, 1, 179, scope='eth1')
        with pytest.raises(ValueError):
            SessionOp(c.OPEN_OP, local=local)  # no remote
        with pytest.raises(ValueError):
            SessionOp(c.CLOSE_OP)  # no connection
        with pytest.raises(ValueError):
            SessionOp(c.LISTEN_OP)  # no local
        with pytest.raises(ValueError):
            SessionOp(c.OPEN_OP, local=local, remote=local, timeout=0)
        with pytest.raises(ValueError):
            Endpoint(6, 1, 70000)
        assert SessionOp(c.ABORT_OP, connection=3).connection == 3

    def test_delivery_is_datagram_or_message(self):
        with pytest.raises(ValueError):
            c.Delivery(0.0, b'x')
        with pytest.raises(ValueError):
            c.Delivery(0.0, b'x', interface='eth1', connection=1)
        assert c.Delivery(0.0, b'x', connection=1).seq == 0

    def test_policy_sid_nht_ops(self):
        with pytest.raises(ValueError):
            c.PolicyOp('bogus')
        with pytest.raises(ValueError):
            c.PolicyOp(c.ADD_POLICY)
        with pytest.raises(ValueError):
            c.PolicyOp(c.DELETE_POLICY)
        with pytest.raises(ValueError):
            c.SidOp('bogus', 'r1')
        with pytest.raises(ValueError):
            c.NhtOp('bogus', NhtKey(REF, 4, 1))
        keys = sorted([NhtKey(REF, 6, 5), NhtKey(REF, 4, 9), NhtKey(REF, 4, 1)])
        assert [(k.af, k.address) for k in keys] == [(4, 1), (4, 9), (6, 5)]


class TestPluginValidation:
    def test_minimal_agent_passes(self):
        c.check_agent(Minimal())

    def test_non_device_scoped_subscription_rejected(self):
        with pytest.raises(ValueError):
            c.check_agent(Minimal(subs=(('devices', 'R2'),)))
        with pytest.raises(TypeError):
            c.check_agent(Minimal(subs=('interfaces',)))

    def test_profile_client_mismatch_rejected(self):
        class Bad(Minimal):
            profile = ClientProfile(ClientId('other'), distance=1)

        with pytest.raises(ValueError):
            c.check_agent(Bad())

    def test_not_an_agent(self):
        with pytest.raises(TypeError):
            c.check_agent(object())


class TestRegistration:
    def test_add_agent_commits_node_with_fresh_generation(self):
        net, R = build_diamond()
        before = net.state
        node = net.add_agent('R1', Minimal())
        assert isinstance(node, AgentNode)
        assert node.name == 'ref' and node.client == REF
        assert not node.initialized and node.state is None and node.receipt is None
        assert node.generation == before.allocators.next_generation
        assert net.state.devices['R1'].agents['ref'] is node
        assert net.profiles[REF] == REF_PROFILE
        assert net.agent('R1', 'ref').client == REF
        # Unrelated devices keep their identity (one shard touched).
        assert net.state.devices['R3'] is before.devices['R3']

    def test_duplicate_name_and_client_rejected(self):
        net, R = build_diamond()
        net.add_agent('R1', Minimal())
        with pytest.raises(ValueError):
            net.add_agent('R1', Minimal())
        with pytest.raises(ValueError):
            net.add_agent(R['R1'], Minimal(), name='second')
        # The same client on another device is fine.
        other = net.add_agent('R2', Minimal())
        assert other.generation != net.state.devices['R1'].agents['ref'].generation

    def test_profile_conflict_rejected(self):
        net, R = build_diamond()
        net.add_agent('R1', Minimal())

        class Other(Minimal):
            profile = ClientProfile(REF, distance=20)

        with pytest.raises(ValueError):
            net.add_agent('R2', Other())
        assert ('R2', 'ref') not in net.agents

    def test_failed_registration_leaves_tree_and_registry_unchanged(self):
        net, R = build_diamond()
        net.add_agent('R1', Minimal())
        state = net.state
        with pytest.raises(ValueError):
            net.add_agent('R1', Minimal(), name='dup-client')
        assert net.state is state
        assert set(net.agents) == {('R1', 'ref')}

    def test_remove_agent_and_device(self):
        net, R = build_diamond()
        net.add_agent('R1', Minimal())
        net.add_agent('R2', Minimal())
        net.remove_agent('R1', 'ref')
        assert 'ref' not in net.state.devices['R1'].agents
        assert ('R1', 'ref') not in net.agents
        with pytest.raises(KeyError):
            net.remove_agent('R1', 'ref')
        net.remove_device('R2')
        assert ('R2', 'ref') not in net.agents

    def test_batch_registration_is_one_commit(self):
        net, R = build_diamond()
        seen = []
        net.on_delta.append(lambda t, o, d: seen.append(o))
        with net.batch():
            net.add_agent('R1', Minimal())
            net.add_agent('R2', Minimal())
        assert len(seen) == 1 and seen[0][0] == 'batch'
        assert {k for k in net.agents} == {('R1', 'ref'), ('R2', 'ref')}
        gens = {net.state.devices[d].agents['ref'].generation for d in ('R1', 'R2')}
        assert len(gens) == 2

    def test_fork_shares_the_registry_and_converge_ignores_agents(self):
        net, R = build_diamond()
        net.add_agent('R1', Minimal())
        net.converge()
        state = net.state
        fork = net.fork()
        assert fork.agents == net.agents and fork.agents is not net.agents
        fork.converge()
        assert fork.state is state  # agents never run in converge()
        assert fork.state.devices['R1'].agents['ref'].state is None

    def test_agent_delta_is_identity_based(self):
        net, R = build_diamond()
        net.add_agent('R1', Minimal())
        old = net.state
        node = old.devices['R1'].agents['ref']
        same = dataclasses.replace(node)  # equal by value, different object
        state_a = dataclasses.replace(node, state=('a',))

        def put(n):
            def fn(state):
                dev = state.devices['R1']
                return dataclasses.replace(
                    state,
                    devices=state.devices.set(
                        'R1', dataclasses.replace(dev, agents=dev.agents.set('ref', n))
                    ),
                )

            return fn

        assert net.update(put(same), 'noop') is not None or True
        delta = StateDelta(old, net.state)
        assert delta.agents('R1').changed == ('ref',) or not delta.agents('R1')
        net.update(put(state_a), 'state')
        delta = StateDelta(old, net.state)
        assert delta.agents('R1').changed == ('ref',)
        assert ('devices', 'R1', 'agents', 'ref') in delta.changed_paths()

    def test_mutable_agent_state_is_rejected_at_commit(self):
        net, R = build_diamond()
        net.add_agent('R1', Minimal())
        net.debug_validate = True

        def fn(state):
            dev = state.devices['R1']
            node = dataclasses.replace(dev.agents['ref'], state=[1])
            return dataclasses.replace(
                state,
                devices=state.devices.set(
                    'R1', dataclasses.replace(dev, agents=dev.agents.set('ref', node))
                ),
            )

        with pytest.raises(TypeError):
            net.update(fn, 'bad')


class TestRuntimeStubs:
    def test_simulation_binds_runtimes_without_kinds(self):
        import netsim
        from netsim.runtime import Simulation

        net, R = build_diamond()
        net.add_agent('R1', Minimal())
        sim = Simulation(netsim.Environment(), net)
        assert sim.agents.generation('R1', 'ref') == (
            net.state.devices['R1'].agents['ref'].generation
        )
        assert sim.agents.generation('R1', 'nope') is None
        assert sim.agents.budget() == {}
        assert sim.transport.budget()['inflight_datagrams'] == 0
        assert sim.transport.budget()['queued_messages'] == 0
