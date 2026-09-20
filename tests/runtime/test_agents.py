"""Gate C1: receipt publication, retry, locality and scheduling."""

from dataclasses import dataclass, replace

import pytest

from netsim import Environment
from netsim.model import contracts as c
from netsim.model import routing
from netsim.model.network import Network
from netsim.model.routing import Nexthop, Route
from netsim.runtime.simulation import Simulation


@dataclass(frozen=True)
class Plugin:
    client: c.ClientId = c.ClientId('test')
    config: c.AgentConfig = c.AgentConfig(run_delay=0)
    paths: tuple = ()
    callback: object = None

    @property
    def profile(self):
        return c.ClientProfile(self.client, 115)

    def subscriptions(self):
        return self.paths

    def on_init(self, ctx):
        return self.on_run(ctx)

    def on_run(self, ctx):
        if self.callback:
            return self.callback(ctx)
        return c.AgentOutput(state=(ctx.agent_state or 0) + 1)


def fixture(*plugins, **kwargs):
    net = Network()
    net.add_device('r')
    for plugin in plugins:
        net.add_agent('r', plugin)
    return Simulation(Environment(), net, **kwargs)


def row(client, af=4):
    return Route((0, 0), af, client, 115, (Nexthop.blackhole(),))


def test_initial_run_and_receipt():
    sim = fixture(Plugin())
    sim.settle()
    node = sim.state.devices['r'].agents['test']
    assert node.initialized and node.state == 1 and node.runs == 1
    assert node.receipt.status == 'PUBLISHED'


def test_registration_after_binding_indexes_before_commit_dispatch():
    sim = fixture()
    sim.network.add_agent('r', Plugin(paths=(('config',),)))
    sim.settle()
    assert sim.state.devices['r'].agents['test'].runs == 1


def test_partial_failure_retry_and_captured_prefix(monkeypatch):
    from netsim.runtime.agents import AgentBatchError
    from netsim.runtime.timeline import AgentRunEvent

    calls = []
    sent = []
    bad = [True]

    def callback(ctx):
        calls.append((ctx.agent, ctx.inbox, ctx.rng.random(), ctx.rib_view(4)))
        ctx.stats.add(ctx.agent)
        if ctx.agent == 'b' and bad[0]:
            return c.AgentOutput(
                state=9,
                route_ops=(c.RouteOp(4, add=(row(c.ClientId('wrong')),)),),
                messages=(c.Message(1, 'bad'),),
            )
        return c.AgentOutput(
            state=1,
            route_ops=(c.RouteOp(4, add=(row(ctx.client),)),),
            messages=(c.Message(1, ctx.agent),),
        )

    sim = fixture(
        Plugin(c.ClientId('b'), callback=callback),
        Plugin(c.ClientId('a'), callback=callback),
    )
    monkeypatch.setattr(sim.transport, 'send_message', lambda *args: sent.append(args))
    first = c.Delivery(0, 'first', connection=1)
    later = c.Delivery(0, 'later', connection=1)
    sim.agents.deliver('r', 'b', first)
    with pytest.raises(AgentBatchError) as failure:
        sim.settle()
    assert failure.value.rejections[0].agent == 'b'
    a, b = (sim.state.devices['r'].agents[x] for x in ('a', 'b'))
    assert a.runs == 1 and b.runs == 0 and b.rng is None and b.state is None
    assert b.receipt.status == 'REJECTED'
    assert len(sent) == 1 and sent[0][-1].payload == 'a'
    assert sim.stats.counters == {'a': [(0, 1)]}
    assert len([r for r in sim.timeline.records if r.origin.name == 'agent']) == 1
    assert len(sim.timeline.select(kind=AgentRunEvent)) == 2
    assert calls[0][0] == 'a' and calls[1][0] == 'b'
    assert not calls[1][3]  # both prepared against the same input
    sim.agents.deliver('r', 'b', later)
    # Drain successful FIB work; no implicit retry on a fresh arrival.
    sim.settle()
    assert len([call for call in calls if call[0] == 'b']) == 1
    bad[0] = False
    sim.retry()
    sim.settle()
    retries = [call for call in calls if call[0] == 'b']
    assert retries[1][1] == (first,) and retries[1][2] == retries[0][2]
    assert retries[2][1] == (later,)
    assert sim.agents.budget()['inbox_entries'] == 0
    assert sim.state.devices['r'].ribs[4].rows_of(c.ClientId('a'))
    assert sim.state.devices['r'].ribs[4].rows_of(c.ClientId('b'))


def test_observer_failure_consumes_receipt_and_outbox(monkeypatch):
    calls = []
    sent = []

    def callback(ctx):
        calls.append(ctx.inbox)
        return c.AgentOutput(
            messages=(c.Message(1, 'hello'),), timers=(c.TimerOp('hello', 1),)
        )

    sim = fixture(Plugin(callback=callback))
    monkeypatch.setattr(sim.transport, 'send_message', lambda *args: sent.append(args))

    def observer(_time, origin, _delta):
        if origin[:2] == ('kind', 'agent'):
            raise ValueError('observer')

    sim.network.on_delta.insert(0, observer)
    sim.agents.deliver('r', 'test', c.Delivery(0, 1, connection=1))
    with pytest.raises(ValueError, match='observer'):
        sim.settle()
    assert len(calls) == len(sent) == 1
    assert sim.agents.budget()['armed_timers'] == 1
    assert sim.agents.budget()['inbox_entries'] == 0
    sim.retry()
    sim.settle()
    assert len(calls) == len(sent) == 1


def test_messages_only_output_is_published_and_transport_rejects_explicitly():
    def callback(ctx):
        return c.AgentOutput(
            state=ctx.inbox, messages=() if ctx.inbox else (c.Message(1, 'x'),)
        )

    sim = fixture(Plugin(callback=callback))
    sim.settle()
    node = sim.state.devices['r'].agents['test']
    assert node.runs == 2
    # A message on a connection that does not exist is an explicit rejection
    # delivered to the sender's inbox, never a silent drop.
    assert node.state[0].reason == 'NOT_ESTABLISHED'
    assert node.state[0].connection == 1
    assert not sim.agents.budget()['inbox_entries']


def test_subscriptions_refine_fields_families_and_ancestors():
    from dataclasses import replace

    from netsim.model.state import StateDelta
    from netsim.runtime.subscriptions import SubscriptionIndex

    sim = fixture()
    r = sim.network.device('r')
    r.add_ethernet('eth1')
    sim.settle()
    index = SubscriptionIndex()
    for agent, path in [
        ('oper', ('interfaces', 'eth1', 'oper')),
        ('config', ('interfaces', 'eth1', 'config')),
        ('all', ('interfaces',)),
        ('v4', ('ribs', '4')),
    ]:
        index.add('r', agent, (path,))
    before = sim.state.devices['r']
    r['eth1'].configure(description='new')
    after = sim.state.devices['r']
    assert set(index.affected('r', before, after)) == {'config', 'all'}
    iface = after.interfaces['eth1']
    newer = replace(
        after,
        interfaces=after.interfaces.set(
            'eth1', replace(iface, oper=replace(iface.oper, oper=1))
        ),
    )
    assert set(index.affected('r', after, newer)) == {'oper', 'all'}
    empty = replace(newer, interfaces=newer.interfaces.remove('eth1'))
    assert set(index.affected('r', newer, empty)) == {'oper', 'config', 'all'}
    six = replace(empty, ribs=empty.ribs.set(6, routing.RibState.empty(6)))
    assert not index.affected('r', empty, six)
    index.remove('r', 'oper')
    assert 'oper' not in index.affected('r', newer, empty)
    plugin = Plugin(paths=(('interfaces', 'eth1', 'oper'),))
    sim.network.add_agent('r', plugin)
    sim.settle()
    old = sim.state
    r['eth1'].configure(description='again')
    assert not sim.agents.affected(StateDelta(old, sim.state), sim.state)


def test_context_is_detached_read_only_and_immutable_projections():
    import dataclasses
    import random

    from netsim.model.entities import Device
    from netsim.model.network import _View
    from netsim.model.state import NetworkState, PMap, validate_immutable

    contexts = []
    sim = fixture(
        Plugin(
            paths=(('fibs',),),
            callback=lambda ctx: contexts.append(ctx) or c.AgentOutput(),
        )
    )
    sim.network.device('r').add_loopback('lo', ipv4=('10.0.0.1/32',))
    sim.settle()
    assert contexts[0].lookup(4, 0x0A000001).status == 'PENDING'
    ctx = contexts[-1]
    assert ctx.router_id == 0x0A000001
    assert ctx.interfaces['lo'].l3_usable_v4
    assert ctx.lookup(4, 0x0A000001).prefix == (0x0A000001, 32)
    assert any(row.selected for row in ctx.rib_view(4))
    with pytest.raises(dataclasses.FrozenInstanceError):
        ctx.device = 'elsewhere'
    stack, seen = [ctx], set()
    while stack:
        obj = stack.pop()
        if id(obj) in seen:
            continue
        seen.add(id(obj))
        assert not isinstance(obj, (Device, _View, NetworkState))
        if isinstance(obj, (random.Random, c.AgentStats)):
            continue
        if dataclasses.is_dataclass(obj):
            for field in dataclasses.fields(obj):
                child = getattr(obj, field.name)
                if obj is ctx and field.name not in ('rng', 'stats'):
                    validate_immutable(child)
                stack.append(child)
        elif isinstance(obj, PMap):
            stack.extend(obj.keys())
            stack.extend(obj.values())
        elif isinstance(obj, (tuple, list, dict)):
            stack.extend(obj)
        elif hasattr(obj, '__slots__'):
            stack.extend(
                getattr(obj, slot) for slot in obj.__slots__ if hasattr(obj, slot)
            )


def test_timers_reset_removal_and_stale_generation(monkeypatch):
    cancelled = []
    sim = fixture(
        Plugin(
            callback=lambda ctx: c.AgentOutput(
                state=ctx.inbox, timers=(c.TimerOp('t', 2),)
            )
        )
    )
    monkeypatch.setattr(
        sim.transport, 'cancel_agent', lambda *args: cancelled.append(args)
    )
    sim.settle()
    old = sim.agents.generation('r', 'test')
    sim.run_until(1)
    sim.reset_agent('r', 'test')
    new = sim.agents.generation('r', 'test')
    assert new != old and cancelled == [('r', 'test', old)]
    assert not sim.agents.deliver('r', 'test', c.TimerFired(1, 'stale', generation=old))
    sim.settle()
    sim.run_until(2)
    assert sim.state.devices['r'].agents['test'].runs == 1
    sim.run_until(3)
    assert sim.state.devices['r'].agents['test'].state == (c.TimerFired(3, 't', new),)
    sim.network.remove_agent('r', 'test')
    assert sim.agents.budget()['agents'] == 0
    assert sim.agents.budget()['armed_timers'] == 0
    assert not sim.agents.deliver('r', 'test', c.TimerFired(3, 'gone'))
    sim.run_until(10)
    assert sim.agents.budget()['scheduled_timer_events'] == 0


def test_same_time_loop_and_positive_delay_event_budget():
    from netsim.runtime.pipeline import ConvergenceError
    from netsim.runtime.simulation import BudgetExceeded

    loop = Plugin(paths=(('agents', 'test', 'state'),))
    sim = fixture(loop, max_rounds_per_timestamp=3)
    with pytest.raises(ConvergenceError) as failure:
        sim.settle()
    assert failure.value.rounds == 4
    slow = replace(loop, config=c.AgentConfig(run_delay=0.25))
    sim = fixture(slow, max_rounds_per_timestamp=3, event_budget=20)
    with pytest.raises(BudgetExceeded) as failure:
        sim.run_until(100)
    assert failure.value.dispatched == 20 and sim.env.now > 0
    with pytest.raises(BudgetExceeded):
        sim.run_derivations()


def test_inbox_limit_and_float_deadlines():
    from netsim.runtime.agents import AgentBatchError

    sim = fixture(Plugin(config=c.AgentConfig(run_delay=1, inbox_limit=1)))
    sim.settle()
    assert sim.agents.deliver('r', 'test', c.TimerFired(0, 'first'))
    assert not sim.agents.deliver('r', 'test', c.TimerFired(0, 'overflow'))
    sim.run_until(1)
    assert not sim.agents.budget()['inbox_entries']
    sim = fixture(
        Plugin(callback=lambda ctx: c.AgentOutput(timers=(c.TimerOp('t', 1e-300),)))
    )
    sim.env._now = 1e20
    with pytest.raises(AgentBatchError, match='advance'):
        # Drive the kind explicitly at the large representable timestamp.
        sim.agents._run(sim.state, sim.env.now, [('r', 'test')])
        sim.agents._after_run(sim.env.now, [('r', 'test')])


def test_noop_route_operations_and_sync_isolation():
    sim = fixture()
    net = sim.network
    client = c.ClientId('a')
    other = c.ClientId('b')
    first = sim.agents._apply(
        sim.state,
        'r',
        client,
        c.AgentOutput(route_ops=(c.RouteOp(4, add=(row(client),)),)),
    )
    second = sim.agents._apply(
        first, 'r', other, c.AgentOutput(route_ops=(c.RouteOp(4, add=(row(other),)),))
    )
    assert (
        sim.agents._apply(
            second,
            'r',
            client,
            c.AgentOutput(route_ops=(c.RouteOp(4, add=(row(client),)),)),
        )
        is second
    )
    assert (
        sim.agents._apply(
            second,
            'r',
            client,
            c.AgentOutput(route_ops=(c.RouteOp(4, delete=((1, 32, client, ()),)),)),
        )
        is second
    )
    net.update(lambda _: second)
    assert (
        net.update(
            lambda state: sim.agents._apply(
                state,
                'r',
                client,
                c.AgentOutput(route_ops=(c.RouteOp(4, add=(row(client),)),)),
            )
        )
        is None
    )
    synced = sim.agents._apply(
        second, 'r', client, c.AgentOutput(route_ops=(c.RouteOp(4, sync=()),))
    )
    assert synced.devices['r'].ribs[4].rows_of(other) == (row(other),)
    assert not synced.devices['r'].ribs[4].rows_of(client)


def test_successor_round_order_and_one_delta_per_round():
    def callback(ctx):
        # The first FIB notification changes the route after FIB has run.
        metric = 2 if ctx.rib_view(4) else 1
        return c.AgentOutput(
            route_ops=(c.RouteOp(4, add=(replace(row(ctx.client), metric=metric),)),)
        )

    sim = fixture(Plugin(callback=callback))
    sim.settle()
    records = [rec for rec in sim.timeline.records if rec.origin.kind == 'stage']
    assert [r.origin.name for r in records][:4] == ['agent', 'fib', 'agent', 'fib']
    agents = [r for r in records if r.origin.name == 'agent']
    assert len({r.round for r in agents}) == len(agents)
    assert records[2].round > records[1].round


@pytest.mark.parametrize(
    'bad', ['owner', 'family', 'mutable', 'callback', 'allocation']
)
def test_invalid_output_rolls_back_only_its_journal(bad, monkeypatch):
    from netsim.model import srv6
    from netsim.runtime.agents import AgentBatchError

    def callback(ctx):
        ctx.rng.random()
        if ctx.agent == 'a':
            return c.AgentOutput(
                sid_ops=(
                    c.SidOp(
                        c.REQUEST_SID,
                        'sid',
                        srv6.END,
                        (('sid', '2001:db8:0:0:1::'), ('structure', srv6.UNCOMPRESSED)),
                    ),
                )
            )
        routes = (c.RouteOp(4, add=(row(ctx.client),)),)
        if bad == 'owner':
            routes = (c.RouteOp(4, add=(row(c.ClientId('wrong')),)),)
        elif bad == 'family':
            routes = (c.RouteOp(4, add=(row(ctx.client, 6),)),)
        elif bad == 'mutable':
            return c.AgentOutput(state=[])
        elif bad == 'callback':
            raise RuntimeError('bad callback')
        return c.AgentOutput(
            state='bad',
            route_ops=routes,
            messages=(c.Message(1, 'bad'),),
            sid_ops=(
                c.SidOp(
                    c.REQUEST_SID,
                    'sid',
                    srv6.END,
                    (('sid', '2001:db8:0:0:1::'), ('structure', srv6.UNCOMPRESSED)),
                ),
            )
            if bad == 'allocation'
            else (),
        )

    sim = fixture(
        Plugin(c.ClientId('a'), callback=callback),
        Plugin(c.ClientId('b'), callback=callback),
    )
    sim.network.device('r').add_locator('loc', '2001:db8::/64')
    sent = []
    monkeypatch.setattr(sim.transport, 'send_message', lambda *args: sent.append(args))
    with pytest.raises(AgentBatchError) as failure:
        sim.settle()
    assert (
        len(failure.value.rejections) == 1 and failure.value.rejections[0].agent == 'b'
    )
    dev = sim.state.devices['r']
    assert dev.agents['a'].runs == 1
    assert dev.agents['b'].rng is dev.agents['b'].state is None
    assert not dev.ribs[4].rows_of(c.ClientId('b')) and not sent
    assert len(dev.srv6_sids.sids) == 1
    assert next(iter(dev.srv6_sids.sids.values())).owner == c.ClientId('a')


def test_nht_sid_results_routes_causes_and_purge():
    from netsim.model import srv6
    from netsim.model.state import PMap

    contexts = []

    def callback(ctx):
        contexts.append(ctx)
        return c.AgentOutput(
            route_ops=(
                c.RouteOp(4, sync=(row(ctx.client),)),
                c.RouteOp(6, sync=(row(ctx.client, 6),)),
            ),
            nht_ops=(c.NhtOp(c.REGISTER_NHT, c.NhtKey(ctx.client, 4, 1)),),
            sid_ops=(
                c.SidOp(
                    c.REQUEST_SID,
                    'sid',
                    srv6.END,
                    (('sid', '2001:db8:0:0:1::'), ('structure', srv6.UNCOMPRESSED)),
                ),
            ),
        )

    sim = fixture(Plugin(callback=callback))
    sim.network.device('r').add_locator('loc', '2001:db8::/64')
    sim.settle()
    assert contexts[-1].sid_results[0].request_id == 'sid'
    assert any(cause.kind == c.CAUSE_SIDS for ctx in contexts for cause in ctx.causes)
    assert any(cause.kind == c.CAUSE_ROUTES for ctx in contexts for cause in ctx.causes)
    node = sim.state.devices['r'].agents['test']
    key = c.NhtKey(node.client, 4, 1)

    def results(state):
        dev = state.devices['r']
        table = replace(
            dev.nht, registrations=PMap({key: c.NhtResult(True, 1, cost=5)})
        )
        return replace(state, devices=state.devices.set('r', replace(dev, nht=table)))

    sim.network.update(results)
    sim.settle()
    assert contexts[-1].nht[key].cost == 5
    assert c.Cause(c.CAUSE_NHT) in contexts[-1].causes
    # Retain on reset, then purge all client-owned resources before on_init.
    sim.reset_agent('r', 'test')
    assert sim.state.devices['r'].nht.registrations
    sim.reset_agent('r', 'test', purge=True)
    dev = sim.state.devices['r']
    assert not dev.ribs[4].rows_of(node.client) and not dev.ribs[6].rows_of(node.client)
    assert not dev.nht.registrations and not dev.srv6_sids.sids
    assert dev.agents['test'].runs == 0 and dev.agents['test'].receipt is None


def test_timer_rearm_cancel_and_compaction():
    outputs = [c.TimerOp('t', 10)]
    inboxes = []
    sim = fixture(
        Plugin(
            callback=lambda ctx: inboxes.append(ctx.inbox)
            or c.AgentOutput(timers=tuple(outputs))
        )
    )
    sim.settle()
    # Re-arm repeatedly while the original events remain in the future.
    for index in range(100):
        outputs[:] = [c.TimerOp('t', 20 + index)]
        sim.agents.deliver('r', 'test', c.Delivery(0, index, connection=1))
        sim.settle()
    budget = sim.agents.budget()
    assert budget['armed_timers'] == 1
    assert budget['scheduled_timer_events'] <= 64
    outputs[:] = [c.TimerOp('t')]
    sim.agents.deliver('r', 'test', c.Delivery(0, 'cancel', connection=1))
    sim.settle()
    assert sim.agents.budget()['armed_timers'] == 0
    sim.run_until(200)
    assert not any(
        isinstance(entry, c.TimerFired) for inbox in inboxes for entry in inbox
    )


def test_registration_rollback_and_observer_failure():
    sim = fixture()
    client = c.ClientId('rolled-back')
    with pytest.raises(RuntimeError):
        with sim.network.batch():
            sim.network.add_agent('r', Plugin(client))
            raise RuntimeError('abort')
    assert not sim.network.agents and client not in sim.network.profiles
    plugin = Plugin()
    sim.network.add_agent('r', plugin)
    with pytest.raises(ValueError):
        sim.network.add_agent('r', replace(plugin, paths=(('config',),)))
    assert sim.network.agents['r', 'test'] is plugin
    sim.settle()

    def fail(*args):
        raise ValueError('observer')

    sim.network.on_delta.append(fail)
    with pytest.raises(ValueError):
        sim.network.remove_agent('r', 'test')
    assert not sim.network.agents and not sim.agents.budget()['agents']


def test_positive_run_delay_must_advance_clock():
    sim = fixture(Plugin(config=c.AgentConfig(run_delay=1e-300)))
    sim.settle()
    sim.run_until(1e20)
    with pytest.raises(ValueError, match='advance'):
        sim.agents.deliver('r', 'test', c.TimerFired(sim.env.now, 'now'))
    assert sim.agents.budget()['inbox_entries'] == 0


def test_device_removal_cancels_registered_agents():
    sim = fixture(
        Plugin(callback=lambda ctx: c.AgentOutput(timers=(c.TimerOp('t', 5),)))
    )
    sim.settle()
    sim.network.remove_device('r')
    assert sim.agents.budget()['agents'] == sim.agents.budget()['armed_timers'] == 0
    sim.run_until(10)


def test_policy_ops_and_purge_keep_other_client():
    from netsim.model import srv6
    from netsim.model.srv6 import ipv6

    sim = fixture(Plugin())
    sim.settle()
    client = c.ClientId('test')
    other = c.ClientId('other')
    policy = srv6.SrPolicy(client, 10, ipv6('2001:db8::1'))
    peer = srv6.SrPolicy(other, 20, ipv6('2001:db8::2'))
    rules = (srv6.SteeringRule('s', policy.key),)
    output = c.AgentOutput(
        policy_ops=(
            c.PolicyOp(c.ADD_POLICY, policy=policy),
            c.PolicyOp(c.SET_STEERING, rules=rules),
        )
    )
    sim.network.update(lambda state: sim.agents._apply(state, 'r', client, output))
    sim.network.update(
        lambda state: sim.agents._apply(
            state,
            'r',
            other,
            c.AgentOutput(policy_ops=(c.PolicyOp(c.ADD_POLICY, policy=peer),)),
        )
    )
    sim.reset_agent('r', 'test', purge=True)
    table = sim.state.devices['r'].srv6_policies
    assert policy.key not in table.policies and peer.key in table.policies
    assert not table.steering
    sim.network.update(
        lambda state: sim.agents._apply(
            state,
            'r',
            other,
            c.AgentOutput(
                policy_ops=(
                    c.PolicyOp(c.REPLACE_POLICY, policy=replace(peer, name='changed')),
                )
            ),
        )
    )
    assert sim.state.devices['r'].srv6_policies.policies[peer.key].name == 'changed'
    sim.network.update(
        lambda state: sim.agents._apply(
            state,
            'r',
            other,
            c.AgentOutput(policy_ops=(c.PolicyOp(c.DELETE_POLICY, key=peer.key),)),
        )
    )
    assert not sim.state.devices['r'].srv6_policies.policies


def test_link_neighbor_and_connection_projections(monkeypatch):
    from netsim.model.addressing import MacAddress
    from netsim.model.state import PMap

    net = Network()
    a, b = net.add_device('a'), net.add_device('b')
    net.add_p2p(
        a, 'eth', b, 'eth', ipv4=('10.0.0.0/31', '10.0.0.1/31'), unnumbered=True
    )
    contexts = []
    net.add_agent(
        'a', Plugin(callback=lambda ctx: contexts.append(ctx) or c.AgentOutput())
    )
    sim = Simulation(Environment(), net)
    generation = sim.agents.generation('a', 'test')
    conn = c.ConnectionState(
        1,
        'a',
        'test',
        c.Endpoint(6, 1, 100),
        generation,
        'b',
        'test',
        c.Endpoint(6, 2, 100),
        900,
        state=c.ESTABLISHED,
        a_to_b_reachable=True,
    )
    unrelated = replace(conn, id=2, a_agent='else')
    inbound = replace(
        conn,
        id=3,
        a_device='b',
        b_device='a',
        b_generation=generation,
        a_generation=900,
        b_to_a_reachable=True,
    )
    net.update(
        lambda state: replace(
            state,
            transport=c.TransportState(
                connections=PMap({1: conn, 2: unrelated, 3: inbound})
            ),
        )
    )
    monkeypatch.setattr(
        sim.transport,
        'budget',
        # The transport reports one counter dict per direction (a->b, b->a);
        # side a of connection 1 sees its own sending direction.
        lambda: {
            'connections': {
                1: ({'messages': 2, 'bytes': 50}, {'messages': 9, 'bytes': 99})
            }
        },
    )
    sim.settle()
    ctx = contexts[0]
    interface = ctx.interfaces['eth']
    assert interface.link.index == net.state.links[next(iter(net.state.links))].index
    assert interface.config is net.state.devices['a'].interfaces['eth'].config
    assert interface.link_local == MacAddress(interface.mac).link_local_int()
    assert interface.l3_usable_v4 and interface.l3_usable_v6
    assert {n.interface for n in ctx.neighbors} == {'eth'}
    assert ctx.lookup(4, 0x0A000001, scope='nope').adjacencies == ()
    assert set(ctx.connections) == {1, 3}
    assert (
        ctx.connections[1].queued_messages == 2
        and ctx.connections[1].queued_bytes == 50
    )
    assert ctx.connections[1].initiator and not ctx.connections[3].initiator


def test_outbox_error_is_published_continues_and_never_replays(monkeypatch):
    calls = []
    sim = fixture(
        Plugin(
            callback=lambda ctx: c.AgentOutput(
                messages=(c.Message(1, 'bad'), c.Message(1, 'good')),
                datagrams=(c.Datagram('eth', 'hello'),),
                sessions=(c.SessionOp(c.ABORT_OP, connection=1),),
            )
        )
    )

    def message(*args):
        calls.append(args[-1].payload)
        if args[-1].payload == 'bad':
            raise ValueError('send error')

    monkeypatch.setattr(sim.transport, 'send_message', message)
    monkeypatch.setattr(
        sim.transport, 'send_datagram', lambda *args: calls.append('datagram')
    )
    monkeypatch.setattr(
        sim.transport, 'session_op', lambda *args: calls.append('session')
    )
    with pytest.raises(ValueError, match='send error'):
        sim.settle()
    assert calls == ['datagram', 'bad', 'good', 'session']
    assert sim.state.devices['r'].agents['test'].runs == 1
    sim.retry()
    sim.settle()
    assert len(calls) == 4


def test_remove_agent_and_device_in_aborted_batch_restore_registries():
    sim = fixture(Plugin())
    original = sim.network.agents.copy()
    for remove in (
        lambda: sim.network.remove_agent('r', 'test'),
        lambda: sim.network.remove_device('r'),
    ):
        with pytest.raises(ValueError):
            with sim.network.batch():
                remove()
                raise ValueError('abort')
        assert sim.network.agents == original
        assert sim.agents.generation('r', 'test') is not None


def test_retry_at_later_time_captures_same_prefix_with_fresh_due_work():
    from netsim.runtime.agents import AgentBatchError

    seen = []
    bad = [True]

    def callback(ctx):
        seen.append((ctx.agent, ctx.now, ctx.inbox))
        if ctx.agent == 'a' and bad[0]:
            raise ValueError('correct me')
        return c.AgentOutput()

    sim = fixture(Plugin(c.ClientId('a'), callback=callback))
    first = c.TimerFired(0, 'first')
    sim.agents.deliver('r', 'a', first)
    with pytest.raises(AgentBatchError):
        sim.settle()
    sim.run_until(5)
    bad[0] = False
    sim.network.add_agent('r', Plugin(c.ClientId('b'), callback=callback))
    sim.retry()
    sim.settle()
    assert seen == [('a', 0, (first,)), ('a', 5, (first,)), ('b', 5, ())]


@pytest.mark.parametrize('method', ['settle', 'run_derivations', 'run'])
def test_event_budget_all_drivers(method):
    from netsim.runtime.simulation import BudgetExceeded

    sim = fixture(Plugin(), event_budget=0)
    with pytest.raises(BudgetExceeded):
        getattr(sim, method)()
    assert sim.events_dispatched == 0


def test_replacement_invalidates_descendant_subscription_even_equal_values():
    from netsim.runtime.subscriptions import SubscriptionIndex

    sim = fixture()
    sim.network.device('r').add_ethernet('eth')
    old = sim.state.devices['r']
    interface = old.interfaces['eth']
    new = replace(
        old,
        interfaces=old.interfaces.set(
            'eth', replace(interface, generation=interface.generation + 10)
        ),
    )
    index = SubscriptionIndex()
    index.add('r', 'test', (('interfaces', 'eth', 'oper', 'oper'),))
    assert index.affected('r', old, new) == {
        'test': {('interfaces', 'eth', 'oper', 'oper')}
    }


def test_all_inbox_prefixes_are_captured_before_any_callback():
    seen = []

    def callback(ctx):
        seen.append((ctx.agent, ctx.inbox))
        if ctx.agent == 'a':
            sim.agents.deliver('r', 'b', c.TimerFired(0, 'during-run'))
        return c.AgentOutput()

    sim = fixture(
        Plugin(c.ClientId('a'), callback=callback),
        Plugin(c.ClientId('b'), callback=callback),
    )
    sim.settle()
    assert seen == [('a', ()), ('b', ()), ('b', (c.TimerFired(0, 'during-run'),))]


def test_immediate_partial_retry_uses_successor_round():
    from netsim.runtime.agents import AgentBatchError

    bad = [True]

    def callback(ctx):
        if ctx.agent == 'b' and bad[0]:
            raise ValueError('reject')
        return c.AgentOutput()

    sim = fixture(
        Plugin(c.ClientId('a'), callback=callback),
        Plugin(c.ClientId('b'), callback=callback),
    )
    with pytest.raises(AgentBatchError):
        sim.settle()
    bad[0] = False
    sim.retry()
    sim.settle()
    records = [rec for rec in sim.timeline.records if rec.origin.name == 'agent']
    assert len(records) == 2 and records[0].round < records[1].round
    assert sim.state.devices['r'].agents['a'].runs == 1
    assert sim.state.devices['r'].agents['b'].runs == 1


def test_readding_to_empty_rib_preserves_version_lineage():
    sim = fixture()
    client = c.ClientId('test')
    add = c.AgentOutput(route_ops=(c.RouteOp(4, add=(row(client),)),))
    state = sim.agents._apply(sim.state, 'r', client, add)
    state = sim.agents._apply(
        state, 'r', client, c.AgentOutput(route_ops=(c.RouteOp(4, sync=()),))
    )
    version = state.devices['r'].ribs[4].version
    state = sim.agents._apply(state, 'r', client, add)
    assert state.devices['r'].ribs[4].version == version + 1
