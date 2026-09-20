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

For bulk construction or a group of timed edits, use a scoped transaction:

```python
with net.batch() as b:
    a = b.add_device('A')
    z = b.add_device('Z')
    b.add_p2p(a, 'eth0', z, 'eth0', unnumbered=True)
    a.add_route('192.0.2.0/24', ['eth0'])
```

The block stages device, interface, link, route and demand edits in private
builders, then validates and commits once. Hooks and the runtime receive one
delta with origin `('batch', n_ops)` at the clock time when the block exits.
`n_ops` counts primitive builder/update calls, including no-ops; `add_p2p`
counts as three. An empty or content-equivalent batch commits nothing. Route
edits are folded into one `rib_apply` per changed device and address family
at each freeze, submitting only changed rows and withdrawals. Frozen route
operations are consumed, so later reads do not replay them. Existing
single-operation calls retain their immediate commit behavior.

An exception escaping the block restores the original tree and all allocator
counters. Handles created for provisional entities are permanently stale after
an abort (`exists` is false), even if a later entity reuses their name and
generation; their equality/hash incarnation also stays distinct. Handles for
existing entities remain valid. Nested batches raise `RuntimeError`. As with
`update()`, a hook exception after publication does not undo a commit.

Reads see staged edits: `state`, device `node`, and `fork()` publish immutable
snapshots, and explicit `update(fn)` passes an immutable snapshot to `fn`.
These reads/updates are freeze boundaries and can reduce the bulk-construction
benefit if performed on every iteration: changed route indexes still need to
be frozen. Plain reads preserve the epoch baseline; edits after an intermediate
`converge()` invalidate its processed inputs when the batch commits. `update()`
returns `None` inside a batch because no delta is committed yet. Non-tree settings (client profiles,
route sources, capacity model and adapter metadata) are outside the transaction.

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

## Failure and availability studies

`netsim.study.Study` adds transient measurements to the network model. The
core, explicit schedules, enumerations and renewal processes need no runtime
packages beyond Python. NetGraph supplies scenario YAML and policy selection
when installed separately.

```python
from netsim.runtime import Draws, Process, Schedule
from netsim.study import Study

study = Study(network)  # Private, converged baseline; network is unchanged.
result = study.enumerate('links')  # Every single link; k=2 for all link pairs.
result = study.enumerate('devices')
rows = result.rows()  # One row per flow/pattern, with occurrence_count.
step_document = result.to_ngraph()

# Per-entity renewal: MTBF is healthy time before failure; MTTR is repair time.
source = Process({('link', link_id): {
    'mtbf': 100, 'mttr': 2, 'ttf': 'exponential',
    'ttr': {'kind': 'lognormal', 'sigma': 0.5},
}}, seed=42)
availability = study.process(source, horizon=10_000)

# Explicit (entities, start seconds, duration seconds or None) rows.
schedule = Schedule([
    ([('device', 'R2')], 1.0, 3.0),
    ([('link', link_id)], 2.0, None),
])
# On an existing simulation: sim.failures(schedule); sim.run_until(10).
```

Entities use `('device', name)`, `('link', id)`, or `('risk_group', name)`.
For native networks, pass a group mapping to
`sim.failures(schedule, risk_groups={'rack': (('device', 'R2'),)})` on the
first call. Nested groups resolve once; unknown groups and cycles are rejected.
All sources on a simulation share leases: an entity fails on its first lease
and returns to its original state on its last release. A pre-disabled entity
stays disabled. A `None` duration holds the lease permanently. Direct manual
fail/restore calls during active leases are outside this ownership contract.

Renewal distributions are `exponential`, `lognormal`, `weibull`, or `constant`.
Their arithmetic means come from `mtbf`/`mttr`; lognormal `sigma` is log-space
standard deviation and Weibull `shape` determines its shape. Both default to
1.0. An explicit duration number means constant seconds; a duration dictionary
uses `{'kind': ..., 'mean': ...}`. Renewal starts healthy and samples the next
TTF after repair. Each entity has an independent stream derived with NetGraph's
SHA-256 seed formula and components `('netsim', kind, name)`.

```python
from netsim.runtime import Draws, Process
from netsim.study import Study

study = Study.from_scenario(scenario, demand_set='traffic', capacity_unit=1e9)
draws = Draws.from_policy(
    scenario.network, scenario.failure_policy_set,
    policy='single_link_failure', iterations=50, seed=42,
)
result = study.iterations(draws, t0=1, settle=1, restore=True, parallelism=1)
result = study.replay('results.json', 'tm_placement', select=['failure_id_here'])

source = Process.from_policy(
    scenario.network, scenario.failure_policy_set,
    policy='single_link_failure', rate=0.1,
    duration={'kind': 'exponential', 'mean': 5}, seed=42,
)
result = study.process(source, horizon=1000)
```

Policy draws use `FailureManager.compute_exclusions` with `effective_seed + i`,
including NetGraph's fallback to the policy seed. Empty/no-rule policies produce
no failure iterations. Identical patterns run once with their multiplicity in
`occurrence_count`; replay preserves that weight. Failure IDs use NetGraph's
BLAKE2s formula, with `""` for empty patterns. `Schedule.replay(results, step,
select, start=1, dwell=1)` instead lays the selected unique patterns onto one
timeline with timed repairs. Link IDs in results remain NetGraph IDs.

`settle` is a minimum observation window after failure and repair. Iterations
drain any longer pending derivations before sampling/restoring; process runs
stop at their exact horizon. Only serial `parallelism=1` is implemented.
`data.netsim` contains settle/recovery times, transient bits lost, the full-window
loss integral (bits), per-demand downtime (seconds) and unavailability,
drop reasons (bit/s), and event counts. Process results additionally expose the
concurrent leased-entity histogram as seconds at each count. Loss integrates
the timeline's left-constant delivered samples, including the final interval;
multiple transitions at one timestamp contribute no elapsed time.

Flow rates in `to_ngraph()` use the scenario's capacity unit (default Gbit/s),
or bit/s for native networks; loss is always bits. The baseline and failure
records follow NetGraph's `FlowIterationResult` shape. This is format and
failure-pattern compatibility: the existing NetSim adapter's demand expansion
and capacity model remain unchanged. In particular, the included square-mesh
scenario offers 144 units through NetSim's per-pair expansion versus 12 in
NetGraph's workflow result. Do not treat their placed totals as equivalent
without first aligning demand expansion and placement models.

`Study(network, keep={...})` accepts `roots` and `deltas` (both default 0),
`arrays`, `reports`, and `timeline` (default false). Metrics always retain
placement samples for the current iteration/run. `arrays=True` exports edge
utilization series; `timeline=True` exports event rows. Events/records grow with
the run; bounded root/delta retention is not a bounded total-history guarantee.
An undefined utilization is exported as JSON `null`.

Importing `netsim.adapters.ngraph` registers the optional `NetSimStudy` workflow
step. Timing and interface overrides live in node/link/risk-group `attrs.netsim`:
`mtbf`, `mttr`, `ttf`, `ttr`, node `fib_delay`/`fast_failover`, link
`carrier_delay_down`/`carrier_delay_up`, `source_interface`/`target_interface`,
and node `loopback_ipv4`/`loopback_ipv6`. Renewal parameters are imported for
entities with both `mtbf` and `mttr`. `Process.from_network(imported_network)`
reads those parameters. Risk-group members, including nested groups, are
resolved from the expanded scenario.

```yaml
workflow:
  - type: NetSimStudy
    name: transients
    demand_set: traffic
    failure_policy: single_link_failure
    iterations: 50
    seed: 42
    mode: iterations
    addressing: unnumbered
    capacity_unit: 1000000000
    keep: {roots: 0, deltas: 0, arrays: false, timeline: false}
```

`mode: process` accepts `horizon`, `rate` and `duration`; omit `failure_policy`
for renewal from entity attributes. `mode: replay` accepts `results_json`,
`step`, and optional `select` failure IDs; omit `results_json` to read an earlier
step in the same scenario. The step also accepts `t0`, `settle`, `restore`, and
individual `keep_roots`, `keep_deltas`, `keep_arrays`, `keep_reports`,
`keep_timeline` options (entries in `keep` take precedence).

```sh
netsim run scenario.yaml --results results.json
# Equivalent module entry point:
python -m netsim run scenario.yaml --results results.json
```

The command requires NetGraph, registers the step before parsing, runs
`Scenario.from_yaml(text).run()`, and writes the complete NetGraph results
including all workflow steps. Without `--results`, it writes `results.json` in
the current directory. Core imports and `netsim --help` work without NetGraph.

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
