"""Independent protocol/oracle twins and forwarding-normalized comparisons."""

from netsim import Environment
from netsim.agents import ReferenceAgent, ReferenceConfig
from netsim.model import contracts as c
from netsim.model.addressing import MacAddress
from netsim.model.igp import oracle_igp
from netsim.model.network import Network
from netsim.runtime import Simulation


def topology(
    edges=None,
    *,
    size=4,
    numbered=True,
    families=(4, 6),
    agent=True,
    config=None,
    fib_delay=0,
    demands=True,
    sr=False,
):
    if edges is None:
        edges = ((0, 1), (0, 2), (1, 3), (2, 3))
    net = Network(seed=71)
    devices = [net.add_device(f'r{i}', fib_delay=fib_delay) for i in range(size)]
    for i, dev in enumerate(devices):
        dev.configure(router_id=i + 1)
        dev.add_loopback(
            'lo',
            ipv4=[f'10.0.0.{i + 1}/32'] if 4 in families else (),
            ipv6=[f'2001:db8::{i + 1}/128'] if 6 in families else (),
        )
        if sr:
            from netsim.model import srv6

            dev.add_locator('sr', prefix=f'2001:db8:{i + 1}::/64')
            dev.add_local_sid(srv6.END_DT46, structure=srv6.UNCOMPRESSED)
    for k, (a, b) in enumerate(edges):
        net.add_p2p(
            devices[a],
            f'e{b}',
            devices[b],
            f'e{a}',
            delay=0.015625,
            ipv4=(f'10.1.{k}.0/31', f'10.1.{k}.1/31')
            if numbered and 4 in families
            else None,
            ipv6=(f'2001:db8:ffff:{k}::/127', f'2001:db8:ffff:{k}::1/127')
            if numbered and 6 in families
            else None,
            unnumbered=not numbered,
        )
        for dev, name in ((devices[a], f'e{b}'), (devices[b], f'e{a}')):
            dev[name].configure(
                forwarding_v4=4 in families, forwarding_v6=6 in families
            )
    if demands:
        for af in families:
            dst = f'10.0.0.{size}' if af == 4 else f'2001:db8::{size}'
            net.add_demand(f'flow{af}', 'r0', dst, 1000)
    if agent:
        for dev in devices:
            net.add_agent(dev.name, ReferenceAgent(config))
        assert not net.sources
    else:
        net.add_source(oracle_igp)
    return net


def twins(**kwargs):
    net = topology(**kwargs)
    oracle = topology(agent=False, **kwargs)
    oracle.converge()
    sim = Simulation(Environment(), net, keep_records=10000, keep_deltas=10000)
    return sim, oracle


def counter(sim, name):
    return sum(value for _, value in sim.stats.counters.get(name, ()))


def state(sim, name='r0'):
    return sim.state.devices[name].agents['ref'].state


def normalized_rows(net, name, source):
    dev = net.state.devices[name]
    rows = set()
    for af, rib in dev.ribs.items():
        for row in rib.rows_of(source):
            hops = []
            for nh in row.nexthops:
                if nh.address is None:
                    address = None
                else:
                    # Resolve both global and scoped link-local identities through
                    # this device's neighbor table, then use the peer's LL address.
                    mac = dev.neighbors.mac(nh.interface, nh.address)
                    if mac is None and nh.af == 6:
                        peer = dev.neighbors.peer_mac(nh.interface)
                        if (
                            peer is not None
                            and MacAddress(peer).link_local_int() == nh.address
                        ):
                            mac = peer
                    assert mac is not None, (name, row, nh)
                    address = MacAddress(mac).link_local_int()
                hops.append((nh.interface, address))
            rows.add((af, row.prefix, row.metric, frozenset(hops)))
    return rows


def normalized_fibs(net, name):
    dev = net.state.devices[name]
    entries = set()
    for af, fib in dev.fibs.items():
        for _, _, entry in fib.entries.items():
            group = fib.group(entry)
            legs = (
                frozenset((leg.interface, leg.mac) for leg in group.adjacencies)
                if group
                else frozenset()
            )
            entries.add((af, entry.prefix, entry.action, legs))
    return entries


def assert_equivalent(sim, oracle):
    assert not sim.network.sources
    for name in sorted(sim.state.devices):
        assert normalized_rows(sim.network, name, c.ClientId('ref')) == normalized_rows(
            oracle, name, c.IGP
        ), name
        assert normalized_fibs(sim.network, name) == normalized_fibs(oracle, name), name
    if sim.network.placement is None or oracle.placement is None:
        assert sim.network.placement is oracle.placement
        return
    assert sim.network.placement.delivered_total == oracle.placement.delivered_total
    assert sim.network.placement.dropped_by_reason == oracle.placement.dropped_by_reason
    assert tuple(sim.network.placement.carried) == tuple(oracle.placement.carried)
    assert tuple(sim.network.placement.offered) == tuple(oracle.placement.offered)


def measure_ring(size):
    """Reproducible size-vs-cost qualification; setup/oracle time is separate.

    Dual-stack unnumbered ring, two loopback prefixes/router, no SR, one
    agent/router, two discovered peers/router, two end-to-end demands.
    Retain four roots, no deltas/events, 128 records. Periodic hello=4 s,
    hold=12 s, refresh=40 s, max_age=120 s; timed link delay=1/64 s.
    Wall intervals include binding/cold start and failure events respectively,
    excluding topology construction and oracle calculation. No speedup claim.
    """
    import time

    edges = tuple(sorted({tuple(sorted((i, (i + 1) % size))) for i in range(size)}))
    config = ReferenceConfig(
        hello_interval=4, hold_time=12, max_age=120, refresh_interval=40
    )
    net = topology(edges=edges, size=size, numbered=False, config=config)
    oracle = topology(edges=edges, size=size, numbered=False, agent=False)
    oracle.converge()
    expected = {
        name: normalized_rows(oracle, name, c.IGP) for name in oracle.state.devices
    }

    def converged(sim):
        return all(
            normalized_rows(sim.network, name, c.ClientId('ref')) == rows
            for name, rows in expected.items()
        )

    start = time.perf_counter()
    sim = Simulation(
        Environment(),
        net,
        keep_roots=4,
        keep_deltas=0,
        keep_records=128,
        extract_events=False,
        keep_arrays=False,
        event_budget=200000,
    )
    for step in range(1, 81):
        sim.run_until(step / 16)
        if converged(sim):
            break
    else:
        raise AssertionError('ring did not converge in 5 simulated seconds')
    cold_wall = time.perf_counter() - start
    cold_time = sim.env.now
    cold_events = sim.events_dispatched
    cold_spf = counter(sim, 'spf')
    cold_messages = counter(sim, 'messages_sent')
    cold_hellos = counter(sim, 'hellos_sent')
    assert_equivalent(sim, oracle)
    sim.run_until(10)
    key = sorted(net.links)[0]
    oracle.links[key].fail()
    oracle.converge()
    expected = {
        name: normalized_rows(oracle, name, c.IGP) for name in oracle.state.devices
    }
    before_events = sim.events_dispatched
    before_spf = counter(sim, 'spf')
    before_messages = counter(sim, 'messages_sent')
    start = time.perf_counter()
    net.links[key].fail()
    for step in range(1, 33):
        sim.run_until(10 + step / 16)
        if converged(sim):
            break
    else:
        raise AssertionError('ring did not reconverge in 2 simulated seconds')
    failure_wall = time.perf_counter() - start
    failure_events = sim.events_dispatched - before_events
    assert_equivalent(sim, oracle)
    return {
        'devices': size,
        'links': size,
        'families': 2,
        'prefixes_per_origin': 2,
        'lsdb_per_agent': 3 * size,
        'rows_per_agent': 2 * (size - 1),
        'sessions': size,
        'cold_simulated': cold_time,
        'cold_wall': cold_wall,
        'cold_events': cold_events,
        'cold_messages': cold_messages,
        'cold_hellos': cold_hellos,
        'cold_spf': cold_spf,
        'failure_simulated': sim.env.now - 10,
        'failure_wall': failure_wall,
        'failure_events': failure_events,
        'failure_us_per_event': failure_wall / failure_events * 1e6,
        'failure_messages': counter(sim, 'messages_sent') - before_messages,
        'failure_spf': counter(sim, 'spf') - before_spf,
    }
