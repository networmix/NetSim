"""C2F seams exercised through immutable plugins and real agent publication."""

from dataclasses import dataclass, replace
from ipaddress import IPv6Address

from netsim import Environment
from netsim.model import contracts as c
from netsim.model import srv6 as sr
from netsim.model.routing import Nexthop, Route
from netsim.model.state import validate_immutable
from netsim.runtime import Simulation
from tests.model.test_network import A, build_diamond
from tests.model.test_nht import route
from tests.model.test_nht_context import scoped_pair
from tests.model.test_policies import diamond, state_of
from tests.model.test_srdb_source import claims, literal_policy


@dataclass(frozen=True)
class Tracker:
    address: int
    client: c.ClientId = c.ClientId('watch')
    config: c.AgentConfig = c.AgentConfig(run_delay=0)
    announce: tuple[Route, ...] = ()

    @property
    def profile(self):
        return c.ClientProfile(self.client, 115, link_state=bool(self.announce))

    @property
    def key(self):
        return c.NhtKey(self.client, 4, self.address)

    def subscriptions(self):
        return ()  # NHT causes must work without a broad tree subscription.

    def on_init(self, ctx):
        return c.AgentOutput(
            state=(),
            nht_ops=(c.NhtOp(c.REGISTER_NHT, self.key),),
            route_ops=(c.RouteOp(4, add=self.announce),) if self.announce else (),
        )

    def on_run(self, ctx):
        sample = (
            tuple(cause.kind for cause in ctx.causes),
            ctx.nht[self.key],
            ctx.lookup(4, self.address),
        )
        return c.AgentOutput(state=ctx.agent_state + (sample,))


def tracking(address='192.0.2.1', *, present=True):
    net, routers = build_diamond()
    head = routers['R1']
    row = route(address + '/32', metric=7)
    if present:
        head.rib_client(c.IGP).add_routes([row])
    plugin = Tracker(A(address))
    net.add_agent(head, plugin)
    sim = Simulation(Environment(), net)
    return net, head, plugin, sim, row


def samples(head, plugin):
    return head.node.agents[plugin.client.name].state


def test_registration_has_first_answer_in_its_initial_publication():
    net, head, plugin, sim, _ = tracking()
    publications = []

    def first_observer(_time, origin, delta):
        if origin[:2] == ('kind', 'agent'):
            node = delta.new.devices[head.name]
            # This observer runs before the pipeline can react to publication.
            answer = node.nht.registrations[plugin.key]
            assert answer is not None and answer.eligible and answer.cost == 7
            publications.append((node.agents['watch'].runs, answer))

    net.on_delta.insert(0, first_observer)
    sim.settle()
    assert [runs for runs, _ in publications] == [1, 2]
    assert publications[0][1] is publications[1][1]
    seen = samples(head, plugin)
    assert len(seen) == 1 and seen[0][0] == (c.CAUSE_NHT,)
    assert seen[0][1] is publications[0][1]
    assert head.node.agents['watch'].receipt.status == c.RECEIPT_PUBLISHED
    validate_immutable(head.node.agents['watch'].state)


def test_cost_only_change_wakes_agent_with_identical_fib():
    _, head, plugin, sim, row = tracking()
    sim.settle()
    before = samples(head, plugin)
    fib = head.fib(4)
    head.rib_client(c.IGP).add_routes([replace(row, metric=23)])
    sim.settle()
    seen = samples(head, plugin)
    assert head.fib(4) is fib
    assert len(seen) == len(before) + 1
    causes, answer, installed = seen[-1]
    assert causes == (c.CAUSE_NHT,)
    assert answer is not before[-1][1]
    assert answer.eligible and answer.cost == 23 and answer.cost_source == c.IGP
    assert installed.fib_version == fib.version


def test_failed_resolved_withdrawn_and_recovered_answers_wake_agent():
    _, head, plugin, sim, row = tracking(present=False)
    sim.settle()
    assert not samples(head, plugin)[-1][1].eligible
    for add in (True, False, True):
        before = samples(head, plugin)
        if add:
            head.rib_client(c.IGP).add_routes([row])
        else:
            head.rib_client(c.IGP).delete_routes([row.key])
        sim.settle()
        seen = samples(head, plugin)
        assert len(seen) == len(before) + 1
        assert seen[-1][0] == (c.CAUSE_NHT,)
        assert seen[-1][1].eligible is add
        assert seen[-1][1] is not before[-1][1]


def test_same_device_unrelated_interface_epochs_do_not_wake_agent():
    _, head, plugin, sim, _ = tracking()
    sim.settle()
    agent = head.node.agents['watch']
    answer = samples(head, plugin)[-1][1]
    epoch = head.node.resolver_input_epoch[4]
    head.add_loopback('unrelated', ipv4=['198.51.100.7/32'])
    sim.settle()
    assert head.node.resolver_input_epoch[4] > epoch
    assert head.node.nht.input_epochs[4] == head.node.resolver_input_epoch[4]
    assert head.node.nht.registrations[plugin.key] is answer
    assert head.node.agents['watch'] is agent
    assert sim.env.peek() == float('inf')


def test_agent_route_output_preserves_link_state_cost_profile():
    net, routers = build_diamond()
    client = c.ClientId('learned')
    row = route('192.0.2.1/32', source=client, metric=37, distance=115)
    plugin = Tracker(row.prefix[0], client=client, announce=(row,))
    net.add_agent(routers['R1'], plugin)
    sim = Simulation(Environment(), net)
    sim.settle()
    answer = samples(routers['R1'], plugin)[-1][1]
    assert answer.eligible and answer.cost == 37 and answer.cost_source == client
    assert client in routers['R1'].rib(4).link_state_sources


@dataclass(frozen=True)
class ScopedTracker:
    key: c.NhtKey
    config: c.AgentConfig = c.AgentConfig(run_delay=0)

    @property
    def client(self):
        return self.key.owner

    @property
    def profile(self):
        return c.ClientProfile(self.client, 115)

    def subscriptions(self):
        return ()

    def on_init(self, ctx):
        return c.AgentOutput(state=(), nht_ops=(c.NhtOp(c.REGISTER_NHT, self.key),))

    def on_run(self, ctx):
        return c.AgentOutput(
            state=ctx.agent_state
            + (
                (
                    tuple(cause.kind for cause in ctx.causes),
                    tuple(ctx.nht.sorted_items()),
                ),
            )
        )


def test_agent_registration_binds_scope_and_never_rebinds_after_recreation():
    net, head, request = scoped_pair()
    plugin = ScopedTracker(replace(request, owner=c.ClientId('scope-watch')))
    net.add_agent(head, plugin)
    sim = Simulation(Environment(), net)
    publications = []

    def observe(_time, origin, delta):
        if origin[:2] == ('kind', 'agent'):
            publications.append(delta.new.devices[head.name].nht.registrations)

    net.on_delta.insert(0, observe)
    sim.settle()
    ((bound, answer),) = publications[0].items()
    assert bound == replace(plugin.key, interface_generation=head['e'].generation)
    assert answer.eligible  # Already bound and answered in the initial publication.
    assert samples(head, plugin)[-1][0] == (c.CAUSE_NHT,)
    head.remove_interface('e')
    sim.settle()
    stale = head.node.nht.registrations[bound]
    assert stale.reason == 'SCOPE_STALE' and not stale.eligible
    assert samples(head, plugin)[-1][0] == (c.CAUSE_NHT,)
    agent = head.node.agents[plugin.client.name]
    head.add_ethernet('e', unnumbered=True)
    net.add_link(('a', 'e'), ('b', 'e'))
    sim.settle()
    assert head['e'].generation != bound.interface_generation
    assert head.node.nht.registrations[bound] is stale
    assert head.node.agents[plugin.client.name] is agent
    assert head.nht_client(plugin.client).resolve(plugin.key).eligible
    # Explicit purge/re-registration is a new scope, not resurrection of the old one.
    sim.reset_agent(head.name, plugin.client.name, purge=True)
    sim.settle()
    ((fresh, answer),) = head.node.nht.registrations.items()
    assert fresh.interface_generation == head['e'].generation and fresh != bound
    assert answer.eligible


@dataclass(frozen=True)
class ViewPublisher:
    views: tuple[c.SrDbView, ...]
    client: c.ClientId = c.ClientId('learned-sr')
    config: c.AgentConfig = c.AgentConfig(run_delay=0)

    @property
    def profile(self):
        return c.ClientProfile(self.client, 115)

    def subscriptions(self):
        return ()

    def publish(self, index):
        return c.AgentOutput(
            state=index,
            srdb_view=self.views[index],
            timers=(c.TimerOp('next-view', 1),) if index + 1 < len(self.views) else (),
        )

    def on_init(self, ctx):
        return self.publish(0)

    def on_run(self, ctx):
        assert c.CAUSE_TIMER in {cause.kind for cause in ctx.causes}
        return self.publish(ctx.agent_state + 1)


def test_runtime_published_sr_views_and_stale_view_trap(monkeypatch):
    net, routers = diamond(compressed=False)
    policy, terminal = literal_policy(routers)
    advertised = claims(routers)
    withdrawn = replace(
        advertised, sids=tuple(sid for sid in advertised.sids if sid.owner != 'R4')
    )
    plugin = ViewPublisher((c.SrDbView(), advertised, withdrawn, advertised))
    net.add_agent(routers['R1'], plugin)
    routers['R1'].configure(srdb_source=('agent', plugin.client.name))

    def forbidden(*_args, **_kwargs):
        raise AssertionError('learned SR validation consulted the oracle')

    # Guard the oracle inventory/topology methods as well as the oracle factory.
    with monkeypatch.context() as guard:
        oracle = sr._PolicyValidator
        for method in ('symbolic', 'literal', 'peer', 'reaches', 'endpoint_owned'):
            guard.setattr(oracle, method, forbidden)
        original = oracle.forwarding

        def local_forwarding(self, device, address):
            assert isinstance(self, sr._AgentPolicyValidator)
            assert device == self.head
            return original(self, device, address)

        guard.setattr(oracle, 'forwarding', local_forwarding)
        guard.setattr(sr, '_PolicyValidator', forbidden)
        sim = Simulation(Environment(), net)
        sim.settle()
        assert state_of(routers, policy).status == sr.POLICY_DOWN
        assert (0, 0, 'SID_UNKNOWN_IN_VIEW') in state_of(routers, policy).reasons
        sim.run_until(1)
        assert state_of(routers, policy).status == sr.POLICY_UP
        assert routers['R1'].node.agents[plugin.client.name].srdb_view is advertised
        # Remote forwarding changes without an advertisement. Learned validity
        # stays UP although the oracle would reject this newly shadowed SID.
        routers['R4'].add_route(
            f'{IPv6Address(terminal.sid)}/128', [Nexthop.blackhole()]
        )
        sim.settle()
        assert state_of(routers, policy).status == sr.POLICY_UP
        sim.run_until(2)
        assert state_of(routers, policy).status == sr.POLICY_DOWN
        sim.run_until(3)
        assert state_of(routers, policy).status == sr.POLICY_UP
        assert routers['R1'].node.agents[plugin.client.name].runs == 4

    routers['R1'].configure(srdb_source=None)
    sim.settle()
    assert state_of(routers, policy).status == sr.POLICY_DOWN
