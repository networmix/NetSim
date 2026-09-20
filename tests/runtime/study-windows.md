# Gate C / C5 study windows and qualification

Base: `9032ea8` (revision 16 / C0). No model, routing, forwarding,
placement or protocol implementation is changed. Local checks do not replace CI.

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
  TransportRuntime, reset AgentNode to its registration configuration and clear
  transport state. Registered plugin configuration is reused. Existing client
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
- Agent warm-up runs ordinary runtime events. C1/C3 are seams on this base;
  real reference-protocol/transport convergence remains an integration check
  after those slices land. C5 tests intentionally use recurring NORMAL timers
  instead of implementing an agent scheduler.
- Preserve negative initial runtime time for warm-up, the non-purging restart
  rule, and the separation of wall costs from deterministic exports.
- Future fault schedules and returned result documents retain their explicit
  workload size; only streaming observation and completed-history memory are
  bounded independently of run length.

## Final checks

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
