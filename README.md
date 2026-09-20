# NetSim

[![CI](https://github.com/networmix/NetSim/actions/workflows/python-test.yml/badge.svg?branch=main)](https://github.com/networmix/NetSim/actions/workflows/python-test.yml)

Discrete-event simulation engine. Zero dependencies. Python 3.11+, including
free-threaded builds.

## Install

```bash
pip install netsim
```

## Quick Start

```python
import netsim

env = netsim.Environment()

def producer(env, store):
    for i in range(5):
        yield store.put(i)
        yield env.timeout(1)

def consumer(env, store):
    while True:
        item = yield store.get()
        print(f't={env.now}: got {item}')

store = netsim.Store(env)
env.process(producer(env, store))
env.process(consumer(env, store))
env.run()
```

## Core Primitives

| Type | Purpose |
|------|---------|
| `Environment` | Simulation clock and event scheduler |
| `Event` | Something that may happen; pending → triggered → processed |
| `Timeout` | Event that auto-triggers after a delay |
| `Process` | Generator-based coroutine; is itself an Event |
| `AllOf` / `AnyOf` | Composite events (`event_a & event_b`, `event_a \| event_b`) |

## Resources

| Type | Purpose |
|------|---------|
| `Store` | FIFO queue with optional capacity |
| `FilterStore` | Store with filtered get requests |
| `PriorityStore` | Retrieves items in priority order |
| `Resource` | Mutual exclusion with capacity slots |
| `PriorityResource` | Resource with priority-ordered requests |
| `PreemptiveResource` | Higher-priority requests preempt lower-priority users |
| `Container` | Bulk quantity tracking (put/get amounts) |

## Key Patterns

### Process waits for process

A process is an event. `yield` it to wait for completion:

```python
def sub_task(env):
    yield env.timeout(5)
    return 42

def main(env):
    result = yield env.process(sub_task(env))
    assert result == 42
```

### Interrupts

```python
def worker(env):
    try:
        yield env.timeout(100)
    except netsim.Interrupt as e:
        print(f'Interrupted at t={env.now}: {e.cause}')

def interruptor(env, target):
    yield env.timeout(5)
    target.interrupt('stop')

env = netsim.Environment()
proc = env.process(worker(env))
env.process(interruptor(env, proc))
env.run()
```

### Resource contention

```python
def user(env, resource):
    with resource.request() as req:
        yield req
        yield env.timeout(1)  # hold the resource
```

### Parallel simulations without the GIL

An `Environment` is single-threaded and must not be shared between threads.
Separate environments share nothing, so on a free-threaded Python build
(`python3.14t`) independent simulations run in parallel threads:

```python
from concurrent.futures import ThreadPoolExecutor

def replicate(seed):
    env = netsim.Environment()
    ...
    env.run()
    return env.now

with ThreadPoolExecutor() as pool:
    results = list(pool.map(replicate, range(8)))
```

## Network layer

`netsim.model` and `netsim.runtime` simulate IP networks on top of the
engine: devices with a RIB and a FIB, Loopback / Ethernet / PortChannel
interfaces (min-links, carrier delay), IPv4 and IPv6 addressing, static
and shortest-path routes with recursive resolution and ECMP, hashed or
fluid flow placement with per-link utilization, and packet probes. State
lives in one immutable tree; derivations run as ordered kinds either
clock-free (`Network.converge()`) or on the DES clock (`Simulation`).

```python
import netsim
from netsim.model.network import Network
from netsim.runtime import Simulation

net = Network()
r1, r2 = net.add_device('R1'), net.add_device('R2')
r1.add_loopback('lo0', ipv4=['10.0.0.1/32'])
r2.add_loopback('lo0', ipv4=['10.0.0.2/32'])
net.add_p2p(r1, 'eth1', r2, 'eth1', ipv4=('10.1.12.0/31', '10.1.12.1/31'), speed=10e9)
r1.add_route('10.0.0.2/32', [('eth1', '10.1.12.1')])
net.add_demand('d1', 'R1', '10.0.0.2', rate=4e9)

env = netsim.Environment()
sim = Simulation(env, net)                 # initial convergence at t=0
sim.at(10, net.links['R1:eth1--R2:eth1'].fail)
sim.run_until(20)
report = sim.timeline.snapshot_at(10).placement
print(report.dropped_by_reason)            # {'LINK_DOWN': 4e9} until routing reacts
```

Every committed change is one timeline record (`seq`, `time`, `round`,
`origin`) plus flat, typed events extracted from it: link state,
interface oper transitions with RFC 2863 names and reasons, carrier
debounce, bundle membership, RIB rows, FIB entries with their next hops,
demands and placement summaries. Nothing in an event needs the state
tree, so post-analysis is filtering and tabulating:

```python
tl = sim.timeline
print(tl.summary(10))                                  # one line per event at t=10
tl.select(kind=FibEvent, device='R1', since=10)        # typed, filterable
tl.interface_series('R1', 'eth1')                      # [(0, 'UP', 'UP'), (10, 'DOWN', 'CARRIER')]
tl.utilization_series(net.links['R1:eth1--R2:eth1'].edge('R1'))
tl.to_csv('events.csv')                                # or tl.rows() for pandas
```

Two device settings decide what a short outage costs. `fib_delay` is the
control plane's reaction time: until it elapses, the FIB still carries the
dead leg and flows hashed to it drop with `EGRESS_DOWN`. `fast_failover`
is data-plane pruning: a next-hop group member whose port has no link is
skipped at selection time and its flows re-hash over the live members, so a
slow control plane loses nothing (both behaviours exist on real platforms).

Raw deltas and roots are kept in bounded deques (`keep_deltas`,
`keep_roots` on `Simulation`); series come from the per-edge arrays the
placement events carry, so a long run does not retain every tree.

The design document behind this layer (state tree, rounds, RIB/FIB
resolution, SRv6 plan, placement semantics) is kept with the project
plans; Gate A ships plain IP, Gate B adds SRv6, Gate C protocol agents.

## Development

```bash
make dev          # create venv, install deps, set up pre-commit
make check        # pre-commit + tests + lint
make test         # tests with coverage
make qt           # quick tests (no coverage)
make lint         # ruff + pyright
make venv-ft      # free-threaded (no GIL) venv, needs uv or python3.14t
make check-ft     # lint + tests on the free-threaded venv
```

## Requirements

- Python 3.11+ (CI runs 3.11 to 3.14 and the free-threaded 3.14t build)
- No runtime dependencies

## License

MIT
