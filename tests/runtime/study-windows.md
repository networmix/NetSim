# Gate C / C5 study windows and qualification

Initial qualification base: `9032ea8` (revision 16 / C0). Integration update:
main `6ff26e9` was merged after C5 commit `e003e4a`; see the update below.
C5's own changes do not modify model, routing, forwarding, placement or
protocol implementations. Local checks do not replace CI.

## API and clock rules

`Study.iterations`, `enumerate`, `replay` and `process` accept `warmup`,
`event_budget`, `stability` and `quiet`; the first three also accept `horizon`.
`process` retains its required observation horizon. NetGraph's NetSimStudy
passes these options through for all three workflow modes. Its omitted horizon
still means 100 seconds for process mode and the legacy default for iterations.

- With no new options and no agents, iterations retains its minimum `settle`
  duration followed by `run_derivations()`, including delayed FIB work. Existing
  `settle_time` and transient-loss fields retain their meanings in this path.
- Explicit window options, or registered agents, select bounded observation:
  `horizon` is the duration after the failure; if absent, `settle` is its alias
  and upper bound. An explicit horizon takes precedence over settle. No work
  after the deadline is drained. Recovery gets a separate window of the same
  duration. All events exactly at the deadline are included.
- Warm-up runs on the fresh runtime from **-warmup to 0**, preserving the existing
  failure time `t0` (default 1) and process source times. Events during [0, t0)
  are additional pre-failure runtime work. Positive windows must advance the
  finite float clock. Warm-up and pre-failure loss are excluded from observations.
- One event budget covers warm-up, pre-failure events, failure observation and
  recovery, including NORMAL timers and stale scheduled events. Reaching the
  budget is allowed; needing another event before the deadline yields
  `budget_exceeded`, without dispatching it. Empty clock advancement consumes
  no synthetic engine event. Ordinary callback errors still propagate.
- Agent iterations create a new Environment, Simulation, AgentRuntime and
  TransportRuntime and clear transport state. Simulation's **restart_all()**
  owns agent restart: initialized nodes get a fresh generation and registration
  configuration; nodes not yet initialized keep their registration generation.
  C5 leaves agent nodes intact before binding so the runtime can recognize
  initialized nodes. Registered plugin configuration is reused. Existing client
  rows remain until sync, matching **reset_agent(purge=False)**. A model fork
  cannot restore protocol timers, inboxes, connections or RNG progress.
  The oracle path shares the converged immutable baseline as before.

`Stability` is a string enum exported by `netsim.study`: ROUTING, PROGRAMMING,
DELIVERY, ALL. `None` selects all. Routing compares device FIBs; programming
requires matching resolver input/processed epochs and INSTALLED policy
programming; delivery compares aggregate delivered rate and per-reason drop
rates. Selected outputs must satisfy the predicate for the **final continuous
quiet interval**, so a later change invalidates an earlier quiet interval.
Unrelated commits and periodic liveness events do not reset it.

Metrics under each iteration's `data.netsim` include status, `converged_at`
(the start of the confirmed quiet interval), `convergence_time`,
`recovery_status`, `recovery_converged_at`, `engine_events`, timeline event
counts and committed `rounds`. No convergence field is inferred from the last
commit. With recovery enabled, status covers both phases (budget exhaustion
wins), while converged_at/convergence_time describe the failure phase and the
recovery fields describe recovery. A selected routing/delivery predicate alone
can converge while programming remains pending; use all to require all three.

`loss_integral`, `bits_lost_by_reason` and per-demand downtime cover the full
observation, including recovery. With explicit windows,
`bits_lost_transient` / `bits_lost_transient_by_reason` stop at each confirmed
quiet interval's beginning. A phase that cannot converge reports its full
partial loss. A permanently disconnected but settled network can therefore
have zero transient loss and positive full-window loss. Converged means quiet
under the selected predicate, not full demand delivery.

`StudyResult.costs` reports aggregate wall `warmup_seconds` and `warmup_events`;
per-iteration deterministic warmup_events also appear in metrics. Wall costs
are deliberately excluded from rows()/to_ngraph(), preserving exact repeatability.
For agent studies, these aggregate costs also include the separate fault-free
baseline preparation described in the C5R update below.

## Streaming and ownership

The only change in a C1-owned file is a six-line additive Timeline hook:
`timeline.on_record: list[Callable[[Record, list[Event]], None]]`. The timeline
calls each observer with the new record and events **before retention eviction**,
also for baseline publication. Callbacks must not mutate these values. C1 can
use the same hook when adding agent events. **No contracts.py additions.**

Study counters and placement/loss accumulators consume that stream. A logical
observation baseline is seeded from current state, independently of constructor
history retention. Quiet-window loss checkpoints keep scalar totals and
per-reason accumulators, not roots or event history. Lease transition histograms
are also aggregated; completed lease history and trace lists do not accumulate.
Rounds count distinct committed stage (time, round) pairs; no-op engine events
are reflected in engine_events rather than invented timeline records.

Budgets for events/records preserve the timeline's existing amortized trimming
allowance (up to 63 extra retained entries). Optional timeline rows and edge
series remain retention-dependent; scalar metrics do not. Initial fault
schedules are still materialized by the existing Process/Schedule API, so
pending scheduled work scales with the supplied fault workload. Returned study
rows scale with the requested output size; bounded history is not a claim of
constant memory for an arbitrarily large returned document.

## Verification

Two initial regression reproducers failed on C0: horizon was an unsupported
keyword, and a 40-fault process with keep_events=0 reported only one retained
PlacementEvent versus 81 plus all the other events in the unbounded run.

The new window tests cover the 1 ms self-rearming NORMAL timer over a 5-second
horizon; delayed FIB deadlines and partial loss; exact-budget completion and
budget exhaustion in warm-up, failure and recovery; fresh agent runtime state;
quiet predicates; late output changes; ignored unrelated commits; warm-up
exclusion; retention-independent counters/loss/rounds; NetGraph kwargs;
Workload; and seeded repeatability. Existing study and SRv6 tests are unchanged.

The pinned sorted-JSON SHA-256, shared by normal and free-threaded interpreters:
`8f7451ae26a6e619312ffe14786c079bc7478fc5bf583e5de7c240e5f8e41ecf`.

Final command results and measured workload tables are recorded below.

## Measurement method

The following timing and memory tables describe C5 commit `e003e4a` against
its C0 base, before the main integration update. They are preserved as the
original qualification evidence, not refreshed timings for main `6ff26e9`.

CPython 3.14.5, macOS 26.6.1 arm64, GC enabled. Builds and convergence are
outside timed regions. Default-path A/B/A loads the C0 study module using
`git show 9032ea8:netsim/study.py` into an isolated Python module, without
checking out or modifying another worktree. Both versions use identical
immutable baselines, four lexically first distinct link-failure draws, default
`iterations(draws)` arguments and retention, with recovery enabled. Each phase
runs 30 calls / 120 iterations. Result summaries are compared for equality.
GC runs before each phase. Both paths share the same runtime implementation;
the additive timeline hook has no registered callback in the old Study path.

Three quiet checks, two seconds apart, precede each fixture. A separate monitor
samples other Python processes every 250 ms. A fixture's whole A/B/A is rejected
and repeated if another Python process exceeds 10% CPU. Normal desktop apps
remain open; this is a local serial-overhead measurement, not a throughput SLA.
The clean measurements below do not include the rejected runs.

Explicit-window benchmarks are reproducible with:

```sh
venv/bin/python -m pytest tests/test_benchmarks.py -k study_windows \
  --benchmark-enable --benchmark-min-rounds=5 -o addopts='' -q
```

Each benchmark call performs four unique iterations with failure and recovery,
`horizon=0.25`, `quiet=0.125`, and event/record budgets of 1. Divide pytest's
per-call timing by four for per-iteration cost. The workload passport is attached
to pytest-benchmark's extra_info (use --benchmark-json to save it).

### Default path: final A/B/A

| Fixture | A1 old ms/iteration | B new ms/iteration | A2 old ms/iteration | B vs old mean |
|---|---:|---:|---:|---:|
| Diamond | 2.660842 | 2.553590 | 2.494627 | -0.94% |
| Clos 8×4 | 22.016005 | 22.031999 | 22.276628 | -0.52% |
| Clos 16×4 | 70.786207 | 71.952859 | 72.020993 | +0.77% |

All B values lie inside the surrounding old-code range. Clos 16×4's +0.77%
versus the old mean is smaller than its 1.73% A1/A2 variation; this measurement
does not resolve a default-path regression or establish a speedup. The study
keeps shortfall samples between checkpoints, checks programming only on changed
devices, and reuses baseline address strings to keep the extra observations
within the existing iteration cost.

Peak competing-Python CPU in the accepted runs: 0.0%, 0.1%, and 5.7%
respectively. Two Clos 8×4 attempts and one Clos 16×4 attempt were rejected
for competing Python activity above 10%. Earlier development timings are not
used in this table. Each row represents 360 total measured iterations across
its three phases, with identical old/new result summaries.

### Warm-up cost: Clos 8×4

Forty iterations per row, four distinct link draws repeated ten times, with
`horizon=0.25`, `quiet=0.125`, recovery enabled, keep_events=keep_records=1.

| Runtime fixture | warmup (simulated s) | ms/iteration | warmup wall ms/iteration | warmup events/iteration |
|---|---:|---:|---:|---:|
| Oracle | 0 | 21.884510 | 0 | 0 |
| Oracle | 1 | 21.628474 | 0.000867 | 0 |
| Fresh registered agent + NORMAL timer | 0 | 23.357394 | 0 | 0 |
| Fresh registered agent + NORMAL timer | 1 | 23.912761 | 0.544567 | 999 |

The registered-agent fixture uses C0's Minimal registration and injects a
self-rearming 1 ms sim.at timer. It measures runtime creation and liveness
execution; it **does not measure a C1/C4 routing protocol**. The 999 warm-up
firings reflect accumulated floating-point 1 ms deadlines; the next firing is
strictly after zero. Oracle warm-up has no engine events. Peak competing Python
CPU during these measurements and the memory experiment was 0.3%.

Both Clos 8×4 passports have 12 devices, 32 links, 12 distinct RIB prefixes,
144 RIB rows, 56 demands, 8 placement classes, zero sessions, and 0/1 agents.
Declared external liveness event_rate is 0/1000 per second, respectively.
Observation plus recovery duration is 0.5 s; t0 remains 1 s. Retention is
roots=0, deltas=0, events=1, records=1, arrays/reports/timeline=False.

### Memory: 200 process studies

After building/converging the diamond baseline and one untimed warm run,
start tracemalloc and run 200 calls to
`study.process(Schedule([((('link', 'R1:eth1--R2:eth1'),), .125, .125)]), .5, quiet=.125)`.
Use keep_events=keep_records=1, default zero retained roots/deltas, and release
each returned result. GC remains enabled; explicitly collect at checkpoints.
Every call reports converged. Total instrumented wall time: 2.469290 s.

| Completed iterations | Current traced bytes after GC | Peak traced bytes |
|---|---:|---:|
| 1 | 8,321 | 143,823 |
| 50 | 1,824 | 1,034,075 |
| 100 | 1,971 | 1,143,573 |
| 200 | 2,054 | 1,143,573 |

The peak plateaus at **1.09 MiB**; completed simulations and their histories
are released. These figures exclude the already-built baseline and do not
claim constant memory if the caller keeps all 200 result documents or supplies
an unbounded future fault schedule. Separate tests exercise 80 faults in one
process study: full and tiny-retention scalar metrics agree, and completed
lease history/trace lists stay empty.

### Explicit-window workload benchmarks

The enabled benchmark tests passed: **3 passed, 9 deselected**, 4.27 s total.
The monitor detected 0.0% competing Python CPU. Per-iteration figures divide
pytest-benchmark's four-iteration call measurements by four.

| Fixture | Devices | Links | Prefixes | RIB rows | Demands | Classes | Mean ms/iteration | Median ms/iteration | Measured calls |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Diamond | 4 | 5 | 20 | 29 | 1 | 1 | 2.614897 | 2.569094 | 93 |
| Clos 8×4 | 12 | 32 | 12 | 144 | 56 | 8 | 21.864962 | 21.717323 | 11 |
| Clos 16×4 | 20 | 64 | 20 | 400 | 240 | 16 | 66.515117 | 65.720375 | 5 |

All are oracle fixtures with zero agents and sessions. Prefixes count distinct
(AF, network, length) RIB keys, including connected/local prefixes; RIB rows
count every device/client copy. Classes are the baseline placement's compiled
classes, not the number of demands. Duration is 0.5 s of failure/recovery
observation. Event rate is unknown unless explicitly declared to describe();
it is not guessed from the baseline. Retention is identical to the warm-up
experiment. These small topologies define the measured envelope; they make no
claim about flat-domain large-scale protocol state, LSDBs or transport queues.

## Integration handoff

- No shared C0 records, validation semantics, network lifecycle, agent scheduler,
  transport scheduler or derivation kinds were changed.
- `Timeline.on_record` is the one additive C1-file change; preserve that hook
  when integrating C1's agent events. C5 consumes it before history eviction.
- Agent warm-up runs ordinary runtime events. The original measurements used
  C0 seams and recurring NORMAL timers. The main integration update now also
  tests real C1 initialized-agent restarts and C3 datagram delivery during
  warm-up; full C4 reference-protocol qualification remains separate.
- Preserve negative initial runtime time for warm-up, the non-purging restart
  rule, and the separation of wall costs from deterministic exports.
- Future fault schedules and returned result documents retain their explicit
  workload size; only streaming observation and completed-history memory are
  bounded independently of run length.

## Initial C5 checks (before main integration)

- `bash .superset/workspace.sh setup`: passed; worktree venv configured without
  replacing shared hooks. This branch was first fast-forwarded from its older
  checkout to the requested `9032ea8` base.
- `make check-ci`: format, Ruff and pyright passed; **1,317 passed, 14 skipped**,
  **94.85%** coverage, 16.98 s pytest time.
- `make venv-ft`: installed free-threaded CPython 3.14.5 and verified the GIL
  disabled. `make check-ft`: format, Ruff and pyright passed;
  **1,317 passed, 14 skipped**, **94.85%** coverage, 16.28 s pytest time.
  Both include the existing plain-IP FIB and placement fingerprint tests.
- `PYTHONPATH=. /Users/networmix/ws/NetGraph/venv/bin/python -m pytest
  tests/adapters tests/runtime/test_study_windows.py -q -p no:cacheprovider
  -o addopts=''`: **172 passed**, 1.62 s (134 adapter tests plus 38 window tests).
- Enabled workload benchmark command above: **3 passed, 9 deselected**.
- `git diff --check`: passed.

C0 contracts are unchanged. The new public records are local to netsim.study:
Stability and Workload; StudyResult adds the defaulted costs field. No new
runtime dependency is introduced. CI after integration is still required.

## Main integration update: 6ff26e9

Merged main `6ff26e9` after the initial C5 commit. Its real C1/C2/C3
implementations and fresh-runtime rules are now included in this branch.
The merge preserves the additive Timeline.on_record hook.

A regression first demonstrated that C5's local AgentNode reset prevented
Simulation's restart_all() from detecting previously initialized nodes: the
new-generation assertion failed. C5 now leaves those nodes intact and lets
AgentRuntime.restart_all() assign their fresh generations before convergence.
The study still clears transport state on its private fork; client-owned routes,
policies, SIDs and NHT registrations follow the runtime's non-purging rule.
No C1/C2/C3-owned implementation required an additional edit for this correction.

The existing fresh-runtime fixture now pins both a changed generation and equal
restart generations across independent forks. Two added integration tests run
actual runtime agents: an initialized agent's on_init runs again during negative-
time warm-up, and agents exchange datagrams across a zero-delay link using
main's default processing_delay=0.001. Delivery occurs strictly after send time.
Original runtimes stay untouched and repeated study exports remain identical.
C5 does not override processing_delay; existing exact-wire-timing fixtures on
main retain their explicit zero override.

Post-merge verification:

- Focused study/SRv6/fresh-runtime tests: **76 passed, 1 skipped**.
- `make check-ci`: format, lint and pyright pass; **1,441 passed, 14 skipped**,
  **95.26%** coverage, 20.83 s pytest time.
- `make check-ft`: format, lint and pyright pass; **1,441 passed, 14 skipped**,
  **95.26%** coverage, 19.70 s pytest time. Includes the pinned study fingerprint
  and the updated idle-agent scale fixture initialized through the real runtime.
- Real NetGraph adapter and window tests: **174 passed**, 1.75 s.
- Enabled workload benchmarks: **3 passed, 9 deselected**, 4.11 s.
- `git diff --check`: pass.

The timing/memory tables above remain the pre-merge qualification record.
This integration update makes no new performance claim. C5 adds no further
shared contract fields or validation changes; processing_delay's new default
and the restart ownership/generation rule come from main.

Main advanced again during verification. Merged `f2dae73`, including the
additional C1/C2 integration checks and obsolete-listener replacement on agent
restart. No further C5 code changes were needed. Final merged-state checks:

- `make check-ci`: format, lint and pyright pass; **1,465 passed, 14 skipped**,
  **95.29%** coverage, 22.58 s pytest time.
- `make check-ft`: format, lint and pyright pass; **1,465 passed, 14 skipped**,
  **95.29%** coverage, 20.25 s pytest time.
- Real NetGraph adapter and window tests: **174 passed**, 1.70 s.
- `git diff --check`: pass.

## C5R: prepared agent baseline (review round 2, finding 2)

Review base `a2370a0` exported the synchronously converged configuration as the
baseline, although synchronous convergence cannot run agents. Replayed the
review's `probe_study_baseline.py` before editing: both iterations and process
exported **baseline total_placed=0**, **no-fault total_placed=2000**, and
`converged`, with 104 warm-up events. New regression tests failed on that code
(17 failures, two oracle-path checks passed).

Agent study calls now prepare one additional **fresh, fault-free runtime**
from the same frozen configuration as their iterations. Preparation runs
[-warmup, 0] and then through `t0` for iterations (through zero for process),
including events at the preparation deadline. The exported record is captured
before any faults; only that detached record is returned, with no baseline
runtime or root retained in StudyResult. Initialized agents are restarted by
Simulation, preserving main's generation and non-purging rules. An empty draw
set still prepares the requested agent baseline. Oracle baselines continue to
export the synchronously converged tree without constructing a baseline runtime.

The preparation has its own `event_budget` allowance, equal to each iteration's
allowance and independent of the iteration's dispatch count. The aggregate
`costs.warmup_seconds` and `costs.warmup_events` include baseline warm-up once
plus each iteration's warm-up. As before, work during [0, t0] is preparation,
not part of the negative-time warm-up cost. No wall time enters deterministic
exports.

Agent baseline records add these fields under `baseline.data.netsim`:

- `baseline_complete`: whether the requested preparation schedule finished.
- `preparation_status`: `complete` or `budget_exceeded`.
- `preparation_end` and `preparation_deadline`: actual and requested simulated
  endpoints, exposing partial preparation even when the partial placed total
  is zero.
- `engine_events` and `warmup_events`: dispatched preparation and warm-up counts.

Completeness does **not** claim protocol convergence. With no warm-up and t0=0,
the complete requested baseline can still precede routing startup; later
observation output is never substituted for it. Consuming exactly the budget is
allowed if no further event is due by the preparation deadline. Exhausting
baseline preparation neither raises nor spends an iteration's budget.

Regression coverage includes both entry points with cold input, partially
initialized input (t=0.03125) and fully initialized input (t=0.5); equal placed
totals and repeatable exports after warm-up; unchanged caller roots/runtimes;
pre-failure t0 work without warm-up; empty draws; immediate faults; incomplete
preparation; exact budgets and separately accounted wall costs. Existing runtime
count assertions now include the baseline runtime. Oracle no-extra-runtime and
existing export fingerprint tests remain unchanged in behavior.

The review reproducer after the fix reports **baseline=2000**, **no-fault=2000**,
`converged`, and **208 aggregate warm-up events** in each mode (104 for baseline
plus 104 for the observation runtime). Merged main `7d51a18` during the fix,
preserving its `validate_admitted` implementation and agent admission call sites.
No shared contract additions, runtime implementation edits, dependencies or new
performance claims are part of C5R. Earlier timing tables remain historical.

C5R verification (with main `7d51a18`):

- `make check-ci`: format, Ruff and pyright pass; **1,613 passed, 14 skipped**,
  **95.29%** coverage, 38.86 s pytest time.
- `make check-ft`: format, Ruff and pyright pass; **1,613 passed, 14 skipped**,
  **95.29%** coverage, 33.58 s pytest time; GIL-disabled interpreter confirmed.
- `PYTHONPATH=. /Users/networmix/ws/NetGraph/venv/bin/python -m pytest
  tests/adapters tests/runtime/test_study_windows.py -q -p no:cacheprovider
  -o addopts=''`: **197 passed**, 3.20 s.
- Review reproducer: baseline/no-fault totals 2,000/2,000 and warm-up events 208
  in both entry points on both interpreters.
- `git diff --check`: passed.
