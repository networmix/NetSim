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
`update()`, hook exceptions after publication never undo a commit or starve
other observers. Every hook runs in registration order, then the first
exception is re-raised.

Reads see staged edits: `state`, device `node`, and `fork()` publish immutable
snapshots, and explicit `update(fn)` passes an immutable snapshot to `fn`.
These reads/updates are freeze boundaries and can reduce the bulk-construction
benefit if performed on every iteration: changed route indexes still need to
be frozen. Plain reads preserve the epoch baseline; `update(fn)` accounts for
resolver inputs changed by both preceding builders and the callback itself.
`update()` returns `None` inside a batch because no delta is committed yet.
A batch contains builder and pure update operations only: `converge()` and
`place()` raise `RuntimeError` inside the block. Commit the batch first, then
run derivations on the committed root. Non-tree settings (client profiles,
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

## Segment routing

Gate B's configuration contract supports H.Encaps/H.Encaps.Red, End and
End.X with PSP/USD, End.DT46, and NEXT-C-SID (uN, uA, uDT46). B6/uB6,
nested encapsulation, DT4/DT6/DX4/DX6, USP and VRFs are rejected. Bare local
SIDs (`block:function::/48`) and composite SIDs
(`block:node:function::/64`) keep their literal addresses and structures.
They have different scopes and are never converted into one another.

SR policy derivation, forwarding and FLUID/HASH placement are integrated.
The following diamond example configures two candidates, steering, and a
bundle failure/repair, then prints adjacency state and selected path.
Policy validity is derived from current inputs; installed FIB programs stay
in use until `fib_delay` elapses. Placement records their actual delivery
and losses, including losses during that programming interval.

```python
import netsim
from netsim.model import srv6 as sr
from netsim.model.contracts import STATIC
from netsim.model.igp import oracle_igp
from netsim.model.network import Network
from netsim.runtime import Simulation

net = Network(seed=7)
with net.batch():
    routers = [net.add_device(f'R{i}') for i in range(1, 5)]
    r1, r2, r3, r4 = routers
    for i, router in enumerate(routers, 1):
        router.add_loopback('lo0', ipv4=[f'10.255.0.{i}/32'],
                            ipv6=[f'2001:db8:ffff::{i}/128'])
        router.add_locator('loc', structure=sr.F3216_GIB, node_id=i)
        router.add_local_sid(sr.END, structure=sr.F3216_GIB,
                             flavors=sr.NEXT_CSID)
        router.add_local_sid(sr.END_DT46, structure=sr.F3216_TERMINAL)
    net.add_lag(r1, 'Po1', ['e12a', 'e12b'],
                r2, 'Po1', ['e21a', 'e21b'],
                min_links=2, speed=1e9, unnumbered=True)
    for a, ai, b, bi in ((r1, 'e13', r3, 'e31'),
                         (r2, 'e24', r4, 'e42'),
                         (r3, 'e34', r4, 'e43')):
        net.add_p2p(a, ai, b, bi, speed=1e9, unnumbered=True)
    for router, interface in ((r1, 'Po1'), (r1, 'e13'),
                              (r2, 'Po1'), (r2, 'e24'),
                              (r3, 'e31'), (r3, 'e34'),
                              (r4, 'e42'), (r4, 'e43')):
        router[interface].configure(forwarding_v6=True)
        router.add_local_sid(sr.END_X, structure=sr.F3216_LIB,
                             flavors=sr.NEXT_CSID, interface=interface)
    endpoint = r4['lo0'].node.config.ipv6[0][0]
    primary = sr.CandidatePath(preference=200, name='via-R2',
        segment_lists=(sr.SegmentList((sr.AdjSeg('R1', 'Po1'),
            sr.AdjSeg('R2', 'e24'), sr.TermSeg('R4'))),))
    backup = sr.CandidatePath(preference=100, name='via-R3', discriminator=1,
        segment_lists=(sr.SegmentList((sr.AdjSeg('R1', 'e13'),
            sr.AdjSeg('R3', 'e34'), sr.TermSeg('R4'))),))
    policy = r1.policy_client().add(sr.SrPolicy(STATIC, 10, endpoint,
        name='diamond', candidate_paths=(primary, backup),
        fallback=sr.FALLBACK_DROP))
    r1.policy_client().set_steering((sr.SteeringRule('class-10', policy.key,
                                                  dscp=10),))
    net.add_demand('video', 'R1', '10.255.0.4', rate=100e6,
                   payload_size=1000, dscp=10, steer=sr.PolicyRef(10, endpoint))
net.add_source(oracle_igp)
sim = Simulation(netsim.Environment(), net)
member = net.link(r1['e12a'].node.link)
sim.at(10, member.fail)
sim.at(20, member.restore)
for time in (0, 10, 20):
    sim.run_until(time)
    sid = next(s for s in r1.node.srv6_sids.sids.values() if s.interface == 'Po1')
    state = r1.node.srv6_policies.states.get(policy.key)
    print(time, sid.adjacency_up, state.status if state else 'UNCOMPUTED',
          state.active_path if state else None)
```

Candidate index 0 (preference 200) carries the demand;
at t=10 the bundle falls below `min_links=2` and candidate index 1 takes over;
at t=20 candidate 0 returns. Removing the backup gives `POLICY_DOWN` while
Po1 is down because fallback is DROP. Policy validity, programmed version,
and observed delivery must be inspected separately when `fib_delay` is nonzero.

The Gate B accounting targets for a 100 Mbit/s **IP payload** demand with
1000-byte payloads, IPv4 inner headers and Ethernet framing are:

| Encapsulation on the transmitted hop | Bytes per frame | Wire Mbit/s |
|---|---:|---:|
| One compressed container, reduced encapsulation without SRH | 1000 + 20 + 40 + 14 = 1074 | 107.4 |
| Outer IPv6 plus one-entry SRH | 1000 + 20 + 40 + 24 + 14 = 1098 | 109.8 |
| Inner packet after decapsulation | 1000 + 20 + 14 = 1034 | 103.4 |

The primary path carries all 100 Mbit/s of payload on Po1 (50 per member)
and R2→R4; these wire rates are calculated from the actual headers on each
hop. They are acceptance targets, not a delivery measurement from the
intermediate branch. The compressor adopts RFC 9800 §6.2 S01–S16;
H.Encaps.Red omits the SRH only for one segment with no flags, tag or TLVs
(RFC 8986 §5.2). Reduced SRH permits SL = LE + 1 (RFC 8754 §4.3.1).

The zero-dependency SONiC adapter exposes `sonic.load(mapping_or_json_or_Path)`,
`sonic.dump(network, optional_path)` and `sonic.appl_db(network)`. The envelope
is `{"version": 1, "devices": {"R1": {"TABLE": {"key": {"field": "value"}}}}}`.
It builds an unconnected device set; physical links are added separately with
`net.add_link((device, interface), (device, interface))`. This is a bounded
interchange schema, not a full SONiC image configuration. It uses the
[SONiC static SRv6 table vocabulary](https://github.com/sonic-net/SONiC/blob/master/doc/srv6/srv6_static_config_hld.md)
and [SID-list APPL_DB table shape](https://github.com/sonic-net/SONiC/blob/master/doc/srv6/srv6_hld.md).

| Table | Version 1 fields |
|---|---|
| `PORT` | Interface key; optional `speed` in Mbit/s (default 10000), `mtu` (1500), `admin_status` (`up`/`down`) |
| `INTERFACE`, `LOOPBACK_INTERFACE` | Interface or `interface\|address/prefix` key; empty object values |
| `STATIC_ROUTE` | Canonical IP prefix key; `nexthop` and/or `ifname` comma-separated equal-length vectors, or `blackhole: "true"`; optional `distance` (1) |
| `SRV6_MY_LOCATORS` | Locator name key; required `prefix` (address or prefix); optional `block_len`, `node_len`, `func_len`, `arg_len` (defaults 32/16/16/0), and `vrf: "default"` |
| `SRV6_MY_SIDS` | `locator\|IPv6-prefix` key; required `action` (`End`, `End.X`, `End.DT46`, `uN`, `uA`, `uDT46`); optional `flavors: ["usid", "psp", "usd"]`, `interface`, `adj`, `decap_dscp_mode: "pipe"`, `decap_vrf: "default"`; four explicit length fields may override structure inference |
| `SRV6_SID_LIST` | List name key; `path` array of literal IPv6 addresses, or objects with `sid`, optional four length fields and `flavors` |
| `SRV6_POLICY` | Policy name key; required `color`, IPv6 `endpoint`, `candidate_paths`; optional literal `bsid`, `fallback` (`IGP`/`DROP`, default IGP) |

Each candidate has a required nonempty `segment_lists` array of
`{"name": "list-name", "weight": 1}` references, and optional `preference`
(100), `name`, `protocol_origin` (30), `originator` (two integers, `[0, 0]`),
and `discriminator` (candidate index). Candidate fields, explicit literal
metadata and the versioned envelope are NetSim extensions. Weights must be
positive. Integers may be JSON integers or decimal strings, except `version`.
Unknown tables/fields, duplicate JSON keys, unsupported flavors/actions,
uniform decapsulation and non-default VRFs produce `SchemaError` with the
entry path. `decap_vrf` is allowed only on DT46. Gate B combinations are
also checked by the shared model validator. F3216 SIDs require an F3216
locator (for example, explicit lengths 32/16/16/64); the legacy locator
defaults 32/16/16/0 describe a classic structure in the shared contract.

Load→dump preserves the input exactly modulo object key order, including
address spelling, omitted defaults, standalone unused lists, and bare versus
composite prefixes. Dump reads current model configuration; changed rows are
serialized from that configuration. Import spelling/list metadata is copied
on fork. Configurations with native features outside this subset (such as
bundles, unnumbered interfaces or symbolic segments) cannot be dumped through
this adapter. Version 1 also requires device `enabled=True`,
`srv6_hop_limit=64`, and `srv6_source=None`; export rejects other values with
the device and field in the error before writing any output. Restoring these
defaults makes the device exportable again. APPL_DB output is keyed by device,
then `SRV6_SID_LIST_TABLE`, then list name, with comma-separated **forwarding-order** wire SIDs. Explicit
list metadata enables compression; addresses without metadata stay literal.
APPL_DB export does not certify policy reachability or installation.

`from_scenario(scenario, srv6=True)` enables an IPv6 underlay and allocates
one F3216 GIB locator, uN and terminal uDT46 per device. GIB allocation uses a
`Random(scenario.seed)` permutation of sorted device names. Each L3 link end
gets one uA, with bundles represented by their PortChannel. A pairwise demand
with exactly one explicit `StaticPath(nodes=...)` or `StaticPath(links=...)`
becomes one candidate containing an `AdjSeg` for every hop and a target
`TermSeg`; `Demand.steer` selects it and fallback is DROP. Strict mode rejects
`TE_WCMP_UNLIM`, `TE_ECMP_UP_TO_256_LSP`, and `TE_ECMP_16_LSP`, even with a pin:
the adapter does not reproduce their capacity admission or translate payload
capacity into wire capacity. For example, Core admits 100 on a 100-capacity
pin offered 200; NetSim's default UNCONSTRAINED model delivers 200. The legacy
`strict=False` escape hatch accepts the routing translation without promising
Core admission semantics; it does not change the capacity model.

Node hops choose the cheapest enabled link, breaking ties by link ID. Both
node-form and link-form paths pin that individual link in Core. If its imported
interface is a bundle member, NetSim rejects the pin, including under
`strict=False`. To deliberately approximate it with the whole bundle, set
`TrafficDemand.attrs={"netsim": {"allow_bundle_pins": True}}`. This boolean
opt-in applies to both path forms and is also honored by `demands_from` and
`NetSimStudy`; it does not enable TE presets in strict mode. The approximation
shares traffic across members and can survive failure of the selected member
when `min_links` remains satisfied, whereas the Core pin drops. Multi-route
pins, non-pairwise pins, broad selectors, loops and disconnected hops remain
unsupported. `NetSimStudy` accepts the same `srv6: true` setting.

Study results expose each failure snapshot's `data.netsim.policies` and a
step-level `data.netsim.policy_iterations` list in `to_ngraph()`. Each policy
reports basic validity, headend first-entry resolution, strict validity,
selected path, programming status (`PENDING` or `INSTALLED`), programmed
version, and observed delivery. Missing derived state is `UNCOMPUTED` with
null validity/version. Delivery covers every steering form actually encountered
by placement, including policies on transit and decapsulating nodes. Rates use
the study's capacity unit; delivery is null without an observation and never
infers validity. Recovery metrics do not overwrite the failure snapshot.

`StudyResult.rows()` adds `policy_` columns to existing flow rows;
`to_csv(path)` writes them. Existing columns remain `failure_id`,
`occurrence_count`, `source`, `destination`, `priority`, `demand`, `placed`,
`dropped`, `cost_distribution`, and `data`. New columns are `policy_device`,
`policy_color`, `policy_endpoint`, `policy_name`, `policy_status`,
`policy_basic_valid`, `policy_strict_valid`, `policy_basic_valid_lists`,
`policy_strict_valid_lists`, `policy_active_path`, `policy_programmed_version`,
`policy_reasons`, `policy_delivered`, and `policy_delivery_scope`. The policy
rows also expose `first_valid`, `first_valid_lists`, `programming`, and
`observed_delivery` (per-demand delivery and drops); their `policy_` columns
follow the same naming convention. `strict_valid_lists` contains all passing
`(candidate, list)` indices, including standby paths, independently of whether
the strict profile is enabled. Destination labels, original NetGraph priorities,
and imported capacity units are retained.

With `Study(..., keep={"timeline": True})`, `rows(events=True)` and
`to_csv(path, events=True)` export retained `PolicyEvent`/`SidEvent` rows.
Common event columns are `failure_id`, `occurrence_count`, `event`, `seq`,
`idx`, `time`, `round`, `origin`, `device`, `action`, and `owner`.
Policy events add `color`, `endpoint`, `name`, `active_path`, `status`,
`reasons`, `programmed_version`, `basic_valid`, `first_valid`, `strict_valid`,
`programming`; SID events add `sid`, `behavior`, `flavors`,
`interface`, `adjacency_up`. CSV columns are sorted, structured cells are
JSON, and null scalars are blank. Event retention limits still apply;
these exports do not synthesize evicted events.


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
Each acquisition or release stages its member transitions in one network
batch. An aborted batch changes neither the tree nor the leases; if a
post-commit observer raises, the matching lease transition remains recorded
and the exception propagates. `registry.active_leases` exposes live tokens for
recovery; retrying a consumed release is harmless. Scheduled committed faults
arm their repair even when an observer raises.

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
multiple transitions at one timestamp contribute no elapsed time. A single
shortfall tolerance applies to flow drops, dropped-flow counts, downtime and
all loss integrals: residuals at or below `max(1e-12 bit/s, 1e-9 * offered)` are
roundoff. Comparisons use each demand's payload bit/s before output-unit
conversion; tolerated residuals export as zero dropped with placed equal to
offered. This does not alter the model's placement records.

Flow rates in `to_ngraph()` use the scenario's capacity unit (default Gbit/s),
or bit/s for native networks; loss is always bits. The baseline and failure
records follow NetGraph's `FlowIterationResult` shape. This is format and
failure-pattern compatibility. The adapter splits pairwise volume like
NetGraph: the included square-mesh scenario offers 12 units in both systems.
Placed totals still depend on the capacity/placement model; NetSim's default
UNCONSTRAINED model does not enforce NetGraph's lossless admission semantics.

`Study(network, keep={...})` accepts `roots` and `deltas` (both default 0),
`arrays`, `reports`, and `timeline` (default false), plus `events` and `records`
(default `None`, unbounded). `keep_events` and `keep_records` are accepted as
aliases inside `keep`; conflicting aliases are rejected. These budgets pass
through to `Simulation` and use its amortized trimming policy. Streaming
per-demand accumulators preserve full-run loss, downtime and settle times even
when history is evicted. `arrays=True` exports retained edge utilization series;
`timeline=True` exports retained event rows. `event_counts` counts retained
events; `dropped_events`/`dropped_records` disclose evictions, and `commits`
includes evicted records. An undefined utilization is exported as JSON `null`.
The lease event trace/history still grows with faults; these options do not
promise a bound on total study memory.

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

NetGraph demand semantics carried by the adapter: `pairwise` splits the
volume over the expanded pairs; `combine` announces one anycast prefix on
every target and sends an even share from every source (the nearest target
wins, as with NetGraph's pseudo sink; a source that cannot reach any target
drops its share, whereas NetGraph re-splits the volume over the reachable
sources per iteration); NetGraph priorities (lower served first) map to
NetSim priorities (higher served first) and the original value is exported.
Static paths, group modes and WCMP or TE flow policies are not translated:
`from_scenario(..., strict=True)` (the default) rejects them.

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
