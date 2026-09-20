"""Clos fabric builder shared by tests and benchmarks (unnumbered, oracle IGP)."""

from netsim.model.igp import oracle_igp
from netsim.model.network import Network


def build_clos(
    leaves: int = 8,
    spines: int = 4,
    speed: float = 100e9,
    demand_rate: float = 1e9,
    seed: int = 0,
) -> Network:
    net = Network(seed=seed)
    leaf = [net.add_device(f'leaf{i}') for i in range(leaves)]
    spine = [net.add_device(f'spine{j}') for j in range(spines)]
    for i, d in enumerate(leaf):
        d.add_loopback('lo0', ipv4=[f'10.1.{i // 256}.{i % 256}/32'])
    for j, d in enumerate(spine):
        d.add_loopback('lo0', ipv4=[f'10.2.{j // 256}.{j % 256}/32'])
    for i, lf in enumerate(leaf):
        for j, s in enumerate(spine):
            net.add_p2p(lf, f'eth{j}', s, f'eth{i}', speed=speed, unnumbered=True)
    net.add_source(oracle_igp)
    for i in range(leaves):
        for k in range(leaves):
            if i != k:
                net.add_demand(
                    f'd{i}-{k}', f'leaf{i}', f'10.1.{k // 256}.{k % 256}', demand_rate
                )
    return net
