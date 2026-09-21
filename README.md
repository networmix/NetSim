# NetSim

[![CI](https://github.com/networmix/NetSim/actions/workflows/python-test.yml/badge.svg?branch=main)](https://github.com/networmix/NetSim/actions/workflows/python-test.yml)

Discrete-event simulation for IP networks. Zero dependencies. Python 3.11+,
including free-threaded builds.

Two layers:

- `netsim` is a small SimPy-style engine: an environment, events, processes
  and resources.
- `netsim.model`, `netsim.runtime`, `netsim.agents` and `netsim.study` model
  routers with a RIB and a FIB, interfaces and links that fail and recover,
  static and shortest-path routing with ECMP, SRv6 policies, flow placement
  with per-link utilization, protocol agents that learn routes by exchanging
  messages, and failure studies that measure what an outage costs while
  routing reacts.

## Install

```bash
pip install netsim
```

## Engine

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

| Type | Purpose |
|------|---------|
| `Environment` | Clock and event scheduler |
| `Event`, `Timeout` | Something that may happen; a timeout triggers after a delay |
| `Process` | Generator-based coroutine; is itself an event, so processes can wait for each other |
| `AllOf`, `AnyOf` | Composite events (`a & b`, `a \| b`) |
| `Store`, `FilterStore`, `PriorityStore` | Queues (FIFO, filtered get, priority order) |
| `Resource`, `PriorityResource`, `PreemptiveResource` | Capacity slots, with priorities and preemption |
| `Container` | Bulk quantities |

A process interrupts another with `proc.interrupt(cause)`; the target sees
`netsim.Interrupt`. Resources are used with `with resource.request() as req:
yield req`. An `Environment` is single-threaded and shares nothing with
other environments, so independent simulations run in parallel threads on a
free-threaded build.

## Network layer

```python
import netsim
from netsim.model.network import Network
from netsim.runtime import Simulation

net = Network()
r1, r2 = net.add_device('R1'), net.add_device('R2')
r1.add_loopback('lo0', ipv4=['10.0.0.1/32'], ipv6=['2001:db8::1/128'])
r2.add_loopback('lo0', ipv4=['10.0.0.2/32'], ipv6=['2001:db8::2/128'])
net.add_p2p(r1, 'eth1', r2, 'eth1', ipv4=('10.1.12.0/31', '10.1.12.1/31'), speed=10e9)
r1.add_route('10.0.0.2/32', [('eth1', '10.1.12.1')])
net.add_demand('d1', 'R1', '10.0.0.2', rate=4e9)

sim = Simulation(netsim.Environment(), net)   # converges at t=0
sim.at(10, net.links['R1:eth1--R2:eth1'].fail)
sim.run_until(20)
print(sim.timeline.snapshot_at(10).placement.dropped_by_reason)   # {'NO_ROUTE': 4e9}
```

All simulated state is one immutable tree. Every change commits a new root,
and derivations (carrier debounce, LAG membership, L3 neighbors, routes, FIB
resolution, placement) run in dependency order either without a clock
(`net.converge()`, `net.place()`) or on the simulation clock, where device
settings such as `fib_delay` and `carrier_delay_down` decide how long a dead
leg keeps carrying traffic.

- Bulk edits go in `with net.batch():` and commit once.
- `sim.timeline` records every commit as typed events: `summary(t)`,
  `select(kind=..., device=...)`, `interface_series()`,
  `utilization_series(edge)`, `rows()`, `to_csv(path)`.
- `net.add_source(netsim.model.igp.oracle_igp)` installs shortest-path ECMP
  routes without a protocol.

## Segment routing

Locators and local SIDs (End, End.X, End.DT46 and their NEXT-C-SID forms
uN, uA, uDT46; PSP and USD flavors), SR policies with weighted candidate
paths, BSID and per-flow steering, RFC 9800 compression, and H.Encaps or
H.Encaps.Red at the headend. Placement follows the encapsulated packet hop
by hop, so wire load includes the outer headers.

```python
from netsim.model import srv6 as sr
from netsim.model.contracts import STATIC
from netsim.model.igp import oracle_igp

net = Network()
with net.batch():
    r1, r2 = net.add_device('R1'), net.add_device('R2')
    net.add_p2p(r1, 'eth1', r2, 'eth1', unnumbered=True)
    for i, r in enumerate((r1, r2), 1):
        r['eth1'].configure(forwarding_v6=True)
        r.add_loopback('lo0', ipv6=[f'2001:db8::{i}/128'])
        r.add_locator('loc', structure=sr.F3216_GIB, node_id=i)
        r.add_local_sid(sr.END_DT46, structure=sr.F3216_TERMINAL)
        r.add_local_sid(sr.END_X, structure=sr.F3216_LIB, flavors=sr.NEXT_CSID, interface='eth1')
    path = sr.CandidatePath(preference=200, segment_lists=(
        sr.SegmentList((sr.AdjSeg('R1', 'eth1'), sr.TermSeg('R2'))),))
    policy = r1.policy_client().add(sr.SrPolicy(STATIC, 10, r2['lo0'].node.config.ipv6[0][0],
                                                candidate_paths=(path,)))
net.add_source(oracle_igp)                    # locator reachability
net.converge()
print(r1.node.srv6_policies.states[policy.key].status)   # UP
```

Policy state reports validity, the selected path, programming status and
observed delivery separately. See `netsim/model/srv6.py` for the records and
`docs/reference.md` for the SONiC CONFIG_DB interchange format.

## Protocol agents

An agent runs on one device and sees only that device: its interfaces,
neighbors, RIB, installed forwarding, timers and delivered messages. It
returns route, policy, SID and next-hop-tracking operations plus datagrams,
session operations and timers; the runtime applies them in one delta per
round and isolates a rejected output from its peers. Datagrams travel over
link channels that honour physical failure, sessions are ordered reliable
streams with timeouts.

`netsim.agents.reference` is a small link-state protocol built on that
contract: hello discovery, flooding, SPF, learned SRv6 SIDs. It converges to
the same routes as the oracle without reading remote state.

```python
from netsim.agents.reference import ReferenceAgent, ReferenceConfig
from netsim.model.addressing import to_network
from netsim.model.contracts import ClientId

net = Network()
r1, r2 = net.add_device('R1'), net.add_device('R2')
r1.add_loopback('lo0', ipv4=['10.0.0.1/32'])
r2.add_loopback('lo0', ipv4=['10.0.0.2/32'])
net.add_p2p(r1, 'eth1', r2, 'eth1', unnumbered=True)
for name in ('R1', 'R2'):
    net.add_agent(name, ReferenceAgent(ReferenceConfig(hello_interval=0.5, hold_time=1.5)))
sim = Simulation(netsim.Environment(), net)
sim.run_until(5)
print([str(to_network(*row.prefix, 4)) for row in r1.rib(4).rows_of(ClientId('ref'))])   # ['10.0.0.2/32']
```

## Failure studies

```python
from netsim.runtime import Draws, Process, Schedule
from netsim.study import Study

study = Study(net)
result = study.enumerate('links')                 # every single link, k=2 for pairs
result = study.process(Process({('link', lid): {'mtbf': 100, 'mttr': 2}}, seed=42),
                       horizon=10_000)            # renewal process
result = study.iterations(draws, warmup=2, horizon=3, stability='all', quiet=0.5)
rows = result.rows()                              # per flow and failure pattern
```

Each iteration forks the converged baseline (or, with agents, warms up a
fresh runtime), applies a failure at `t0`, and reports convergence status,
transient loss, downtime and event counts alongside the placement. With
NetGraph installed, `Study.from_scenario(scenario)`, `Draws.from_policy(...)`
and the `NetSimStudy` workflow step reuse NetGraph scenarios, failure
policies, seeds and the `results.json` format:

```sh
netsim run scenario.yaml --results results.json
```

## Adapters

- `netsim.adapters.ngraph`: NetGraph scenarios and networks in, per-link
  utilization and NetGraph-shaped results out.
- `netsim.adapters.core`: NetGraph-Core as an SPF accelerator and an
  independent cross-check for plain-IP placement.
- `netsim.adapters.sonic`: SONiC CONFIG_DB load and dump for a bounded schema.

Details of these formats and the study options are in
[docs/reference.md](docs/reference.md).

## Development

```bash
make dev          # venv, dependencies, pre-commit hooks
make check-ci     # lint, types, tests with coverage
make qt           # quick tests
make venv-ft      # free-threaded venv (python3.14t)
make check-ft     # the same checks without the GIL
```

CI runs Python 3.11 to 3.14 and the free-threaded 3.14t build.

## License

MIT
