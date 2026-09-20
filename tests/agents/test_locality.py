"""Reject oracle access during plugin callbacks, even for correct oracle rows."""

import ast
from pathlib import Path

import pytest

from netsim import Environment
from netsim.agents import ReferenceAgent
from netsim.model import contracts as c
from netsim.model.network import Network
from netsim.runtime import Simulation
from netsim.runtime.agents import AgentBatchError
from tests.agents.fixtures import assert_equivalent, topology, twins


def guard(monkeypatch, sim):
    assert sim.agents.active is None
    state_get = Network.state.fget
    view, device = Network.view, Network.device

    def forbid():
        assert sim.agents.active is None, 'agent attempted nonlocal model access'

    def state(self):
        forbid()
        return state_get(self)

    def guarded_view(self, *args, **kwargs):
        forbid()
        return view(self, *args, **kwargs)

    def guarded_device(self, *args, **kwargs):
        forbid()
        return device(self, *args, **kwargs)

    monkeypatch.setattr(Network, 'state', property(state))
    monkeypatch.setattr(Network, 'view', guarded_view)
    monkeypatch.setattr(Network, 'device', guarded_device)


def test_no_oracle_imports_or_network_state_reads():
    import netsim.agents

    for path in Path(netsim.agents.__file__).parent.glob('*.py'):
        tree = ast.parse(path.read_text())
        forbidden = (
            'netsim.model.igp',
            'netsim.model.network',
            'netsim.model.entities',
        )
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert not any(
                    alias.name.startswith(forbidden) for alias in node.names
                ), path
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ''] + [
                    f'{node.module}.{alias.name}' for alias in node.names
                ]
                assert not any(name.startswith(forbidden) for name in names), path
            elif isinstance(node, ast.Name):
                assert node.id != 'NetworkState', path
            elif isinstance(node, ast.Attribute):
                assert node.attr != 'NetworkState', path


def test_reference_callbacks_are_local_on_discovery_failure_restart(monkeypatch):
    sim, oracle = twins()
    guard(monkeypatch, sim)
    sim.run_until(2)
    assert_equivalent(sim, oracle)
    next(iter(sim.network.links.values())).fail()
    next(iter(oracle.links.values())).fail()
    oracle.converge()
    sim.run_until(4)
    sim.reset_agent('r0', 'ref')
    sim.run_until(6)
    assert sim.agents.active is None
    assert_equivalent(sim, oracle)


@pytest.mark.parametrize('access', ['state', 'view', 'device'])
def test_cheating_plugin_fails_even_when_its_routes_match_oracle(monkeypatch, access):
    from dataclasses import replace

    from netsim.model.igp import shortest_path_routes
    from tests.agents.fixtures import normalized_rows

    net = topology(agent=False)
    oracle = topology(agent=False)
    oracle.converge()
    net.sources.clear()

    class Cheater(ReferenceAgent):
        def on_init(self, ctx):
            if access == 'view':
                net.view(ctx.device)
            elif access == 'device':
                net.device(ctx.device)
            rows = tuple(
                replace(row, source=self.client, distance=115)
                for row in shortest_path_routes(net.state, ctx.device, 4)
            )
            return c.AgentOutput(route_ops=(c.RouteOp(4, sync=rows),))

    net.add_agent('r0', Cheater())
    sim = Simulation(Environment(), net)
    sim.settle()
    assert normalized_rows(net, 'r0', c.ClientId('ref')) == {
        row for row in normalized_rows(oracle, 'r0', c.IGP) if row[0] == 4
    }
    sim.reset_agent('r0', 'ref')
    guard(monkeypatch, sim)
    with pytest.raises(AgentBatchError, match='nonlocal model access'):
        sim.settle()
    assert sim.agents.active is None  # including exceptional callbacks
