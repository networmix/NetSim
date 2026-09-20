"""Idle bounded-state locality qualification (no routes, messages or timers).

The timings isolate affected() on a computed one-device delta, not SR consumer
invalidation or the whole commit. Run with -s to retain machine measurements.
"""

import gc
from dataclasses import replace
from statistics import median
from time import perf_counter

import pytest

from netsim import Environment
from netsim.model import contracts as c
from netsim.model.network import Network
from netsim.model.state import Allocators, DeviceState, NetworkState, PMap, StateDelta
from netsim.runtime.simulation import Simulation
from tests.runtime.test_agents import Plugin


def idle_fixture(count):
    net = Network()
    plugins = tuple(
        Plugin(c.ClientId(f'a{i}'), paths=(('config', 'enabled'),)) for i in range(4)
    )
    devices = {}
    generation = 1
    for i in range(count // 4):
        name = f'r{i:05}'
        device_generation = generation
        generation += 1
        nodes = {}
        for plugin in plugins:
            nodes[plugin.client.name] = c.AgentNode(
                plugin.client.name,
                generation,
                plugin.client,
                plugin.config,
                initialized=True,
            )
            generation += 1
            net.agents[name, plugin.client.name] = plugin
        devices[name] = DeviceState(name, device_generation, agents=PMap(nodes))
    net.update(
        lambda _: NetworkState(
            devices=PMap(devices), allocators=Allocators(next_generation=generation)
        )
    )
    sim = Simulation(
        Environment(), net, extract_events=False, keep_deltas=0, keep_roots=0
    )
    assert sim.agents.budget()['agents'] == count
    assert not sim.agents.kind().pending
    old = sim.state
    device = old.devices['r00000']
    new = replace(
        old,
        devices=old.devices.set(
            'r00000', replace(device, config=replace(device.config, enabled=False))
        ),
    )
    delta = StateDelta(old, new)
    delta.devices()  # commit dispatch already computes this shared lazy section
    return sim, delta


@pytest.mark.timeout(120)
def test_idle_twenty_thousand_agents_one_device_only(monkeypatch):
    measurements = []
    for size in (1_000, 20_000):
        sim, delta = idle_fixture(size)
        examined = []
        original = sim.agents.subscriptions.affected

        def tracked(device, old, new, examined=examined, original=original):
            examined.append(device)
            return original(device, old, new)

        monkeypatch.setattr(sim.agents.subscriptions, 'affected', tracked)
        expected = {('r00000', f'a{i}') for i in range(4)}
        assert sim.agents.affected(delta, delta.new) == expected
        assert examined == ['r00000']
        monkeypatch.setattr(sim.agents.subscriptions, 'affected', original)
        samples = []
        gc.collect()
        was_enabled = gc.isenabled()
        gc.disable()
        try:
            for _ in range(7):
                start = perf_counter()
                for _ in range(500):
                    assert sim.agents.affected(delta, delta.new) == expected
                samples.append((perf_counter() - start) / 500)
        finally:
            if was_enabled:
                gc.enable()
        measurements.append(median(samples))
        print(
            f'AGENT affected {size}: median {median(samples) * 1e6:.2f} us/change; '
            f'range {min(samples) * 1e6:.2f}..{max(samples) * 1e6:.2f}; wakes=4; examined devices=1'
        )
        assert (
            not sim.agents.kind().pending
        )  # affected prepares causes; pipeline schedules
    # Generous timing guard for loaded CI; exact locality is asserted above.
    assert measurements[1] < measurements[0] * 3 + 10e-6
