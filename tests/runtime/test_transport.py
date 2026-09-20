"""C3 channel regressions, with the C0/C1 inbox seam kept real."""

import pytest

import netsim
from netsim.model import contracts as c
from netsim.model.addressing import MacAddress
from netsim.model.interfaces import OperState
from netsim.model.network import Network
from netsim.runtime import Simulation
from tests.model.test_agent_contract import Minimal
from tests.model.test_network import build_diamond


class Fake:
    def __init__(self, sim):
        self.sim = sim
        self.entries = []
        self.full = False
        self.blocked = set()

    def generation(self, device, agent):
        dev = self.sim.state.devices.get(device)
        node = dev.agents.get(agent) if dev else None
        return node.generation if node else None

    def deliver(self, device, agent, entry):
        if (
            self.full
            or (device, agent) in self.blocked
            or self.generation(device, agent) is None
        ):
            return False
        self.entries.append((device, agent, entry))
        return True


def register(net, device, name='ref', **kwargs):
    agent = Minimal()
    agent.client = c.ClientId(name)
    agent.profile = c.ClientProfile(agent.client, distance=115)
    # These fixtures assert exact wire timings: the link delay alone decides
    # delivery unless a test asks for a processing delay explicitly.
    kwargs.setdefault('processing_delay', 0.0)
    agent.config = c.AgentConfig(listen_ports=(179,), **kwargs)
    return net.add_agent(device, agent, name=name).generation


def bind(net):
    sim = Simulation(netsim.Environment(), net)
    sim.agents = Fake(sim)
    return sim


def pair(delay=0.125, **config):
    net = Network(seed=17)
    a, b = net.add_device('R1'), net.add_device('R2')
    net.add_p2p(a, 'e1', b, 'e2', unnumbered=True, delay=delay)
    ga = register(net, 'R1', **config)
    gb = register(net, 'R2', **config)
    return bind(net), ga, gb


def deliveries(sim):
    return [(d, a, e) for d, a, e in sim.agents.entries if isinstance(e, c.Delivery)]


def send(sim, gen, payload, interface='e1', device='R1', **kw):
    return sim.transport.send_datagram(
        device, 'ref', gen, c.Datagram(interface, payload, port=179, **kw)
    )


def test_cold_start_scoped_identity_and_port_fanout():
    sim, ga, gb = pair()
    register(sim.network, 'R2', 'other')
    assert not sim.state.devices['R1'].fibs[6].entries
    assert send(sim, ga, b'hello') is None
    assert send(sim, gb, b'reply', interface='e2', device='R2') is None
    sim.run_until(0.125)
    got = deliveries(sim)
    assert [(d, a, e.payload) for d, a, e in got] == [
        ('R2', 'other', b'hello'),
        ('R2', 'ref', b'hello'),
        ('R1', 'ref', b'reply'),
    ]
    entry = got[0][2]
    address = MacAddress(sim.state.devices['R1'].interfaces['e1'].mac).link_local_int()
    assert entry.sender == c.Sender('e2', c.Endpoint(6, address, 179, 'e2'))
    assert entry.interface == 'e2'


def test_failed_wire_during_carrier_delay_and_inflight_drop():
    sim, ga, _ = pair(delay=1)
    for device, iface in [('R1', 'e1'), ('R2', 'e2')]:
        sim.network.devices[device][iface].configure(carrier_delay_down=1)
    sim.settle()
    link = next(iter(sim.network.links.values()))
    sim.at(9.5, lambda: send(sim, ga, 'inflight'))
    sim.at(10, link.fail)
    sim.run_until(10.2)
    assert sim.state.devices['R1'].interfaces['e1'].oper.oper == OperState.UP
    assert send(sim, ga, 'failed') is None
    assert not sim.agents.entries
    sim.run_until(11)
    assert not deliveries(sim)
    assert sim.transport.budget()['datagrams_dropped'] == 2
    assert sim.transport.budget()['datagrams_lost_physical'] == 2


def test_fifo_with_shorter_later_delay():
    sim, ga, _ = pair(delay=1)
    send(sim, ga, 'U1')
    sim.run_until(0.125)
    next(iter(sim.network.links.values())).configure(delay=0.125)
    send(sim, ga, 'U2')
    sim.run_until(1)
    assert [(e.time, e.payload) for _, _, e in deliveries(sim)] == [
        (1, 'U1'),
        (1, 'U2'),
    ]
    assert sim.transport.budget()['inflight_datagrams'] == 0


def test_bundle_uses_minimum_active_delay():
    net, _ = build_diamond()
    ga = register(net, 'R1')
    register(net, 'R2')
    net.links['R1:eth1--R2:eth1'].configure(delay=0.5)
    net.links['R1:eth2--R2:eth2'].configure(delay=0.25)
    sim = bind(net)
    send(sim, ga, 'bundle', interface='Po1', af=4)
    sim.run_until(0.25)
    assert deliveries(sim)[0][2].interface == 'Po1'


def test_zero_or_unrepresentable_delay_rejected_before_scheduling():
    sim, ga, _ = pair(delay=0)
    with pytest.raises(ValueError, match='future'):
        send(sim, ga, 'zero')
    next(iter(sim.network.links.values())).configure(delay=0.125)
    sim.env._now = float(2**54)
    with pytest.raises(ValueError, match='future'):
        send(sim, ga, 'rounded')
    assert not deliveries(sim)


@pytest.mark.parametrize(
    'change',
    ['receiver_generation', 'sender_cancel', 'receiver_cancel', 'interface_recreate'],
)
def test_old_datagram_incarnations_are_never_delivered(change):
    sim, ga, gb = pair()
    send(sim, ga, 'old')
    if change == 'receiver_generation':
        sim.network.remove_agent('R2', 'ref')
        register(sim.network, 'R2')
    elif change == 'sender_cancel':
        sim.transport.cancel_agent('R1', 'ref', ga)
        assert sim.transport.budget()['inflight_datagrams'] == 0
    elif change == 'receiver_cancel':
        sim.transport.cancel_agent('R2', 'ref', gb)
    else:
        device = sim.network.devices['R2']
        device.remove_interface('e2')
        device.add_ethernet('e2', unnumbered=True)
        sim.network.add_link(('R1', 'e1'), ('R2', 'e2'), delay=0.125)
    sim.run_until(1)
    assert not deliveries(sim)
    assert sim.transport.budget()['datagrams_dropped'] == 1


def test_receiver_physical_down_before_derived_oper_is_silent_loss():
    sim, ga, _ = pair()
    sim.network.devices['R2']['e2'].admin_down()
    assert sim.state.devices['R1'].interfaces['e1'].oper.oper == OperState.UP
    assert send(sim, ga, 'no') is None
    assert sim.transport.budget()['datagrams_lost_physical'] == 1
    sim.settle()
    assert send(sim, ga, 'no').reason == 'INTERFACE_DOWN'


def test_missing_port_and_full_inbox_are_counted():
    sim, ga, _ = pair()
    sim.transport.send_datagram('R1', 'ref', ga, c.Datagram('e1', 'missing', port=999))
    sim.agents.blocked.add(('R2', 'ref'))
    send(sim, ga, 'full')
    sim.run_until(0.125)
    assert sim.transport.budget()['datagrams_dropped'] == 2
    assert sim.transport.budget()['inbox_rejections'] == 1
    assert sim.agents.entries[-1][2].reason == 'INBOX_FULL'


def test_channel_delivery_does_not_iterate_channel_registry():
    class NoScan(dict):
        def __iter__(self):
            raise AssertionError('delivery scanned the channels')

        def items(self):
            raise AssertionError('delivery scanned the channels')

        def values(self):
            raise AssertionError('delivery scanned the channels')

    sim, ga, _ = pair()
    for i in range(100):
        send(sim, ga, i)
    sim.transport._channels = NoScan(sim.transport._channels)
    sim.run_until(0.125)
    assert len(deliveries(sim)) == 100


def test_processing_delay_and_payload_validation():
    sim, ga, _ = pair(delay=0, processing_delay=0.25)
    send(sim, ga, ('immutable', 1))
    sim.run_until(0.25)
    assert deliveries(sim)[0][2].time == 0.25
    with pytest.raises(TypeError):
        c.Datagram('e1', [])


def measure_channels(channels, per_channel=10, repeats=5):
    """Manual size-vs-time experiment, outside pytest and outside coverage."""
    from statistics import median
    from time import perf_counter

    net = Network(seed=17)
    with net.batch():
        a, b = net.add_device('R1'), net.add_device('R2')
        for i in range(channels):
            net.add_p2p(a, f'e{i}', b, f'e{i}', unnumbered=True, delay=1)
        ga = register(net, 'R1')
        register(net, 'R2')
    net.converge()
    sends, receives = [], []
    for _ in range(repeats):
        sim = bind(net.fork())
        datagrams = tuple(
            c.Datagram(f'e{i}', (i, j), port=179)
            for j in range(per_channel)
            for i in range(channels)
        )
        start = perf_counter()
        for datagram in datagrams:
            sim.transport.send_datagram('R1', 'ref', ga, datagram)
        sends.append(perf_counter() - start)
        assert sim.transport.budget()['inflight_datagrams'] == len(datagrams)
        start = perf_counter()
        sim.run_until(1)
        receives.append(perf_counter() - start)
        assert len(deliveries(sim)) == len(datagrams)
        assert sim.transport.budget()['inflight_datagrams'] == 0
    count = channels * per_channel
    return {
        'channels': channels,
        'datagrams': count,
        'repeats': repeats,
        'send_seconds': median(sends),
        'deliver_seconds': median(receives),
        'send_us_each': median(sends) / count * 1e6,
        'deliver_us_each': median(receives) / count * 1e6,
    }


def test_ipv4_unnumbered_channel_does_not_invent_a_source_address():
    sim, ga, _ = pair()
    rejection = send(sim, ga, 'v4', af=4)
    assert isinstance(rejection, c.Rejection)
    assert rejection.reason == 'NO_SOURCE'
    assert sim.transport.budget()['inflight_datagrams'] == 0
