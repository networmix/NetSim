"""A Simulation is a fresh runtime: agents start from configuration.

Design (Gate C, studies with agents): each iteration constructs a fresh
runtime and performs deterministic warm-up; a model fork is not a warm
protocol restart. A forked network carries initialized agent nodes whose
inboxes, timers and sessions never existed in the new runtime, so binding
restarts them with a fresh generation and runs ``on_init`` again.
"""

import netsim
from netsim.model import contracts as c
from netsim.runtime import Simulation
from tests.model.test_network import build_diamond

REF = c.ClientId('ref', 0)


class Counter:
    client = REF
    profile = c.ClientProfile(REF, distance=115)
    config = c.AgentConfig(run_delay=0.001)

    def subscriptions(self):
        return (('interfaces',),)

    def on_init(self, ctx):
        ctx.stats.add('init')
        return c.AgentOutput(state=('init', ctx.now), timers=(c.TimerOp('t', 0.5),))

    def on_run(self, ctx):
        return c.AgentOutput(state=('run', ctx.now, ctx.agent_state))


def running_network():
    net, R = build_diamond()
    net.add_agent('R1', Counter())
    net.add_agent('R2', Counter())
    sim = Simulation(netsim.Environment(), net)
    sim.run_until(2)
    return net, sim


def test_fork_bound_to_a_new_simulation_restarts_agents_cold():
    net, sim = running_network()
    warm = {d: net.state.devices[d].agents['ref'] for d in ('R1', 'R2')}
    assert all(n.initialized and n.runs >= 2 for n in warm.values())
    fork = net.fork()
    assert fork.state is net.state  # nothing copied
    env2 = netsim.Environment()
    sim2 = Simulation(env2, fork)
    fresh = {d: fork.state.devices[d].agents['ref'] for d in ('R1', 'R2')}
    for d in ('R1', 'R2'):
        assert fresh[d].generation != warm[d].generation
        assert not fresh[d].initialized and fresh[d].state is None
        assert fresh[d].receipt is None and fresh[d].runs == 0
    # The original runtime is untouched.
    assert net.state.devices['R1'].agents['ref'] is warm['R1']
    assert sim2.agents.budget()['pending_runs'] == 2
    sim2.run_until(0.6)
    restarted = fork.state.devices['R1'].agents['ref']
    assert restarted.initialized and restarted.runs == 2
    assert restarted.state[0] == 'run' and restarted.state[2] == ('init', 0)
    # The first runtime keeps running independently.
    sim.run_until(3)
    assert net.state.devices['R1'].agents['ref'].generation == warm['R1'].generation


def test_a_network_without_initialized_agents_is_not_rewritten():
    net, R = build_diamond()
    net.add_agent('R1', Counter())
    before = net.state
    sim = Simulation(netsim.Environment(), net)
    # Only derivations touched the tree: the agent node is the registered one.
    assert net.state.devices['R1'].agents['ref'] is before.devices['R1'].agents['ref']
    sim.settle()
    assert net.state.devices['R1'].agents['ref'].initialized


def test_restart_is_deterministic_across_forks():
    net, sim = running_network()

    def outcome():
        fork = net.fork()
        sim2 = Simulation(netsim.Environment(), fork)
        sim2.run_until(1.2)
        return tuple(
            (d, fork.state.devices[d].agents['ref'].state) for d in ('R1', 'R2')
        )

    assert outcome() == outcome()
