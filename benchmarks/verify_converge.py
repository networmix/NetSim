"""Live-tree convergence scaling, no-op converge, and Simulation construction on a converged network."""

import time

import netsim
from netsim.model.igp import oracle_igp
from netsim.model.lpm import PrefixTable
from netsim.model.network import Network
from netsim.runtime import Simulation


def timed(fn):
    t = time.perf_counter()
    v = fn()
    return time.perf_counter() - t, v


def ring(n):
    """Ring with chords (degree 4), unnumbered, oracle IGP, a few demands; public API."""
    net = Network()
    devs = [net.add_device(f'r{i:05}') for i in range(n)]
    for i, d in enumerate(devs):
        d.add_loopback(
            'lo', ipv4=[f'10.{(i >> 16) & 255}.{(i >> 8) & 255}.{i & 255}/32']
        )
    step = max(2, int(n**0.5))
    pairs = sorted(
        {
            tuple(sorted((i, (i + off) % n)))
            for i in range(n)
            for off in (1, step)
            if i != (i + off) % n
        }
    )
    for a, b in pairs:
        net.add_p2p(devs[a], f'e{b}', devs[b], f'e{a}', speed=10e9, unnumbered=True)
    net.add_source(oracle_igp)
    for k in range(8):
        i = (k * 7919) % n
        net.add_demand(
            f'd{k}',
            devs[i].name,
            f'10.{((n - 1) >> 16) & 255}.{((n - 1) >> 8) & 255}.{(n - 1) & 255}',
            1e6,
        )
    return net


for n in (32, 64, 128, 256):
    t_build, net = timed(lambda n=n: ring(n))
    t_init, _ = timed(net.converge)
    t_noop, _ = timed(net.converge)
    env = netsim.Environment()
    t_sim, sim = timed(lambda env=env, net=net: Simulation(env, net))
    lid = sorted(net.links)[3]
    sim.at(1, net.links[lid].fail)
    t_fail, _ = timed(lambda sim=sim: sim.run_until(2))
    rows = sum(
        len(list(sh.values()))
        for d in net.state.devices.values()
        for r in d.ribs.values()
        for sh in r.shards.values()
    )
    print(
        f'n={n:>4}: build {t_build:6.2f}s | initial converge {t_init:6.2f}s | no-op converge {t_noop:6.2f}s | Simulation() on converged {t_sim:6.2f}s | one failure {t_fail:6.2f}s | rib rows {rows}'
    )
# R5 with the real API
t = PrefixTable(32)
name = [m for m in ('insert', 'add', 'put', 'set') if hasattr(t, m)]
print('PrefixTable write method:', name)
