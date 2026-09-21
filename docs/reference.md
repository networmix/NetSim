# Reference

Formats and options that are too detailed for the README.

## Batches

`with net.batch():` stages device, interface, link, route and demand edits
and commits them once; hooks and the runtime see one delta with origin
`('batch', n_ops)`. An exception inside the block restores the tree and the
allocators; handles of provisional entities become stale. Reads inside the
block (`state`, a device's `node`, `fork()`) see the staged edits.
`converge()` and `place()` are not allowed inside a batch. Client profiles,
route sources and the capacity model are outside the transaction.

## Timeline

Every commit is one record (`seq`, `time`, `round`, `origin`) with typed
events: link state, interface oper transitions (RFC 2863 names and reasons),
carrier debounce, bundle membership, RIB rows, FIB entries, SIDs, policies,
demands, placement summaries, agent runs. `Simulation(keep_roots=,
keep_deltas=, keep_events=, keep_records=)` bound what is retained; series
come from the arrays the placement events carry.

## Failure studies

Entities are `('device', name)`, `('link', id)` or `('risk_group', name)`.
All sources on one simulation share leases: an entity fails on its first
lease and recovers on its last release. Renewal distributions are
`exponential`, `lognormal`, `weibull` and `constant`, parameterized by
`mtbf` and `mttr` (means), `sigma` for lognormal and `shape` for Weibull.

`Study.iterations`, `enumerate`, `process` and `replay` accept:

| Option | Meaning |
|---|---|
| `t0`, `settle`, `restore` | failure time, minimum observation window, restore after the window |
| `warmup` | simulated time run on a fresh runtime before `t0` (needed with agents) |
| `horizon` | observation window after the failure (`settle` is its alias) |
| `event_budget` | maximum engine events per iteration; exceeding reports `budget_exceeded` |
| `stability`, `quiet` | predicate (`routing`, `programming`, `delivery`, `all`) that must hold for `quiet` seconds to report `converged` |
| `keep` | retention: `roots`, `deltas`, `events`, `records`, `arrays`, `reports`, `timeline` |

Each iteration's `data.netsim` reports `status` (`converged`,
`deadline_exceeded`, `budget_exceeded`), `converged_at`, transient and
full-window loss in bits, per-demand downtime, drop reasons, engine events
and rounds; with agents, the baseline is prepared on a warmed runtime and
carries `baseline_complete`. `StudyResult.rows()` has one row per flow and
failure pattern (with `occurrence_count`), plus `policy_*` columns for SR
policies; `to_ngraph()` follows NetGraph's `FlowIterationResult` shape.

## NetGraph adapter

`from_scenario(scenario, capacity_unit=1e9, addressing='unnumbered',
srv6=False, strict=True)` maps nodes to devices with loopbacks, links to
Ethernets (parallel links tagged `lag` to a PortChannel), costs to metrics,
`demands` to demand sets and `failures` to draws. `pairwise` demands split
the volume over the pairs; `combine` demands become one anycast prefix.
NetGraph priorities (lower first) map to NetSim priorities (higher first).
With `srv6=True` every device gets an F3216 locator, uN and uDT46, every
L3 link end a uA, and a pairwise demand with one explicit `StaticPath`
becomes an SR policy. Strict mode rejects WCMP and TE flow policies.

Timing overrides live in `attrs.netsim` on nodes, links and risk groups:
`mtbf`, `mttr`, `ttf`, `ttr`, `fib_delay`, `fast_failover`,
`carrier_delay_down`, `carrier_delay_up`, `source_interface`,
`target_interface`, `loopback_ipv4`, `loopback_ipv6`.

Importing `netsim.adapters.ngraph` registers the `NetSimStudy` workflow step:

```yaml
workflow:
  - type: NetSimStudy
    name: transients
    demand_set: traffic
    failure_policy: single_link_failure
    iterations: 50
    seed: 42
    mode: iterations          # or process (rate, duration, horizon) or replay (results_json, step, select)
    addressing: unnumbered
    capacity_unit: 1000000000
    keep: {roots: 0, deltas: 0, arrays: false, timeline: false}
```

`netsim run scenario.yaml --results results.json` runs the scenario with
NetGraph and writes the complete results.

## SONiC adapter

`sonic.load(mapping_or_json_or_path)`, `sonic.dump(network, path=None)` and
`sonic.appl_db(network)` use the envelope
`{"version": 1, "devices": {"R1": {"TABLE": {"key": {"field": "value"}}}}}`.
Devices are unconnected on load; add links with
`net.add_link((device, interface), (device, interface))`.

| Table | Fields |
|---|---|
| `PORT` | `speed` (Mbit/s, default 10000), `mtu` (1500), `admin_status` |
| `INTERFACE`, `LOOPBACK_INTERFACE` | keys `interface` and `interface\|address/prefix` |
| `STATIC_ROUTE` | `nexthop` and/or `ifname` vectors, or `blackhole: "true"`; `distance` |
| `SRV6_MY_LOCATORS` | `prefix`; `block_len`, `node_len`, `func_len`, `arg_len` (32/16/16/0) |
| `SRV6_MY_SIDS` | key `locator\|prefix`; `action` (`End`, `End.X`, `End.DT46`, `uN`, `uA`, `uDT46`), `flavors`, `interface`, `adj` |
| `SRV6_SID_LIST` | `path`: literal IPv6 addresses or objects with `sid`, lengths and `flavors` |
| `SRV6_POLICY` | `color`, `endpoint`, `candidate_paths` (`segment_lists` references with `weight`, `preference`, `name`), `bsid`, `fallback` |

Unknown tables or fields, unsupported behaviours and non-default VRFs raise
`SchemaError` with the entry path. Load followed by dump preserves the input
except key order. Configurations using bundles, unnumbered interfaces or
symbolic segments cannot be dumped. APPL_DB output lists forwarding-order
wire SIDs per SID list.
