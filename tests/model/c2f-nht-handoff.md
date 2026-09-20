# C2F — runtime seam tests and scoped SR invalidation

Base: `main` at `2ab9d7d`, merged by fast-forward into `gatec/c2-nht`.
The integration commit was read before work. Its NHT refresh, profile propagation
and result-identity wakeup seams are used unchanged. No other worktree or main
was edited, and no runtime implementation or existing SR test was changed.

## End-to-end evidence

`tests/model/test_nht_agent.py` defines frozen real plugins registered with
`Network.add_agent` and driven by `Simulation`:

- The initial `AgentOutput(nht_ops=(NhtOp(REGISTER_NHT, key),))` publication
  already contains a non-None, eligible result. An observer ahead of pipeline
  dispatch inspects that first committed delta, before any follow-up derivation
  can fill it in. The next agent callback receives exactly `CAUSE_NHT` and the
  same result object.
- An IGP-row metric change from 7 to 23 produces one NHT wakeup with the new
  cost while the device's `Fib` remains the same object.
- A miss becomes resolved, is withdrawn, and recovers; each transition produces
  exactly one NHT wakeup with a different result object.
- An unrelated loopback addition on the same device advances resolver epochs
  but retains the answer and agent node identities, with no callback or pending
  event. The tracker subscribes to no tree paths, so none of these NHT wakeups
  can be explained by a broad subscription.
- A second plugin writes its own link-state route and registration in one
  output; the runtime's `profile=` seam supplies cost provenance correctly.
- A timer-driven plugin publishes empty, advertised, withdrawn and advertised
  `SrDbView`s through `AgentOutput.srdb_view`, flipping a policy DOWN/UP/DOWN/UP.
  Oracle inventory, symbolic resolution, adjacency, ownership and reachability
  methods are guarded against calls; shared RIB queries are allowed only at
  the headend. A remote route shadows the terminal SID while the advertised
  view is retained: learned validity remains UP. Switching explicitly to oracle
  mode rejects the same policy. The fixture supplies immutable advertised
  snapshots; the plugin does not discover remote state through model access.

## Safe scope of the optimization

`sr.consumers_affected` now uses the device components of the existing
`PolicyState.dependencies` for changes to RIBs, neighbors and interfaces where
all changed old/new interface nodes are loopbacks. Positive and negative
queries are both included. A consumer's own device always invalidates it;
a missing policy-state record or empty dependencies keeps the fallback.

Global invalidation remains for device creation/removal/configuration, links,
non-loopback interfaces, SR-DB inventory and policy inputs. The reason is
specific: literal SID lookup scans all owners, including owners absent from
the previous match, while peer/bundle lookup reads both link endpoints without
recording every remote field. The current records do not justify safely
narrowing those categories. No string parsing, mutable dependency cache, new
contract field, or changed dependency representation was introduced.

`tests/model/test_srv6_invalidation.py` first reproduced eight unnecessary
validations on the old ring implementation and one on a disconnected-device
fixture. The fixed tests assert no validations and unchanged headend epochs /
policy identities. Additional cases prove endpoint withdrawal/recovery,
negative RIB queries, more-specific SID shadowing, global SID-inventory
fallback and missing-record fallback still invalidate correctly.

This is device-level invalidation, not a prefix-level incremental validator.
The consumer set and dependency tuples are still scanned. Later IGP-derived
changes to a headend's own RIB legitimately invalidate that headend; the
benchmark below measures the unrelated input commit itself, not a complete
oracle convergence or protocol workload.

## Before/after measurement

Command (same workload script in both revisions):

```sh
venv/bin/python -m tests.model.test_srv6_invalidation --benchmark
```

The baseline was measured on the merged `2ab9d7d` implementation before changing
`srv6.py`; the after measurement used the scoped implementation. Both ran
sequentially on this Apple M4 Max / macOS arm64 machine with CPython 3.14.5;
this worker launched no concurrent checks or benchmarks during the timing.

Workload: 64 devices, 64 uniform-metric unnumbered ring links, 12,288 total
IPv4/IPv6 RIB rows, 8 headends with classic terminal-SID policies to nodes two
clockwise hops away, no demands, agents, sessions or timers. Device `r63` is
outside every recorded validation path. Each trial forks the same converged
immutable root and adds one unrelated IPv4 loopback on `r63`. Topology setup,
convergence and fork construction are excluded. Three warm-ups precede 25
measured commits; GC is disabled only inside each timed region. Both versions
use the same validation-call counter wrapper.

| Implementation | Median per commit | Min–max | Policy validations per commit |
|---|---:|---:|---:|
| `2ab9d7d`, conservative | 1.4190 ms | 1.3279–1.5960 ms | 8 |
| Scoped dependencies | 0.1237 ms | 0.1207–0.2418 ms | 0 |

The median fell by 91.3% (11.47x for this specific commit workload). This does
not claim the same improvement for end-to-end simulation, global fallbacks,
or updates that touch a recorded path.

## Checks and integration

- Setup succeeded with `bash .superset/workspace.sh setup`.
- `make check-ci`: Ruff formatting/lint and Pyright clean; all tests passed.
- `make check-ft`: same checks on free-threaded CPython 3.14.5 with GIL disabled.
- Both suites: 1,411 passed, 10 skipped, 95.27% coverage, including unchanged
  `tests/test_fingerprints.py` and every existing SR test.
- New focused tests: 12 passed (6 runtime seam cases and 6 invalidation cases).
- `git diff --check`: clean.

No contract additions and no changes required in C1/C3. Their merged seams are
now covered by the real-plugin tests. No known C2F blocker. Local checks do not
replace CI.
