# T6 validation (2026-09-20)

Base: `c2d658e` (the worktree's original `network-layer-gate-a` base).
Scope: failure sources/leases, serial studies, NetGraph integration and CLI.
No routing, forwarding or placement derivation was changed.

## Correctness

- `make check-ci`: lint/format and pyright pass; **446 passed, 2 skipped**;
  **92.92%** coverage on CPython 3.14.5.
- `make venv-ft`, then `make check-ft`: lint/format and pyright pass;
  **446 passed, 2 skipped**, **92.92%** coverage on free-threaded 3.14.5
  (setup verified the GIL was disabled).
- `PYTHONPATH="$PWD" /Users/networmix/ws/NetGraph/venv/bin/python -m pytest
  tests/adapters -p no:cacheprovider -o addopts=''`: **14 passed**.
  Includes an actual 50-draw `TrafficMatrixPlacement` run using a verbatim copy
  of NetGraph's `scenarios/square_mesh.yaml` fixture, failure ID and count
  comparison, real results.json replay, `FlowIterationResult`/`FlowEntry`/
  `FlowSummary` construction, all three workflow modes, and console CLI smoke.
- Zero-dependency imports and the missing-NetGraph CLI error are tested in
  subprocesses with `python -S`; optional NetGraph tests skip in the NetSim venv.
- Lease union invariants cover 100 overlapping random faults, nested groups,
  permanent faults, zero-duration faults and pre-disabled baseline entities.
  Renewal availability covers all four distributions with 100,000 simulated
  seconds per source; entity unavailability is within 0.015 of
  `MTTR / (MTBF + MTTR)`. Independent stream ordering and seed reproducibility
  are pinned. Transient loss has an exact 50 Mbit/s × 0.05 s oracle.
- Before implementation, two regression reproducers failed on the base code:
  `test_failure_schedules_share_overlapping_leases` restored a link at 5 s
  despite its second active lease lasting until 6 s; and
  `test_existing_schedule_matches_policy_seed_fallback` used zero instead of
  the policy seed. Both now pass.

Local checks do not replace integrator CI.

## Performance experiment

Command: `venv/bin/python -m tests.runtime.benchmark_study`.
CPython 3.14.5 with the GIL, macOS 26.6.1 arm64, 14 logical CPUs, GC enabled.
One interpreter; A/B/A order; collection before each block. The old approach
is a manual `Network.fork` + `Simulation.at` loop. The new approach is
`Study.iterations`. Both run identical failure/recovery windows and disable
root/delta/edge-array retention. Construction/convergence is outside timing.
Study additionally builds result records and integrates transient metrics.
Fifty distinct link draws from `build_clos(16, 4)` avoid a deduplication discount.

| Phase | Total for 50 iterations | ms/iteration |
|---|---:|---:|
| A1: manual | 4.212984 | 84.259686 |
| B: Study | 4.311429 | 86.228577 |
| A2: manual | 4.192380 | 83.847607 |

All three delivered-placement result sequences have SHA-256
`52bf9a701c037e4886f4eba2920653a9a72abebff5bbe70aba616a4255b137b3`.

The timed run started after three quiet observations two seconds apart. A
monitor sampled process CPU usage every second during A/B/A and detected no
other Python worker above 10% CPU; no local tests ran concurrently. Normal
desktop applications remained open. Study cost is **2.59% above** the mean
manual-loop cost on this workload; A1/A2 differ by 0.49%. These numbers describe
serial overhead, not a claim that NetSim competes with NetGraph static Monte
Carlo or that larger workloads have the same cost.

## Integration semantics

- Core `netsim.Process` remains the DES coroutine; the failure source is
  `netsim.runtime.Process`. `Study` and `StudyResult` live in `netsim.study`.
- `Simulation.failures` shares one registry; `keep_arrays`/`keep_reports` are
  new optional simulation arguments. Existing defaults are unchanged.
- Adapter `from_scenario` adds optional `demand_set` filtering. The existing
  `FailureSchedule` API now uses leases and the policy seed fallback matches
  NetGraph. Empty/no-rule policies have no failure iterations, like NetGraph.
- `parallelism=1` only; other values are rejected by `Study.iterations`.
  `settle` is a minimum observation window; delayed derivations are drained.
  Process horizons truncate observation exactly, including unrepaired leases.
- Flow rates use the imported capacity unit; loss integrals use bits. Empty
  failure patterns use NetGraph's empty-string ID. Replays preserve sampling
  multiplicities. Histograms count leased entities, not independent leases.
- NetGraph compatibility covers failure patterns and result format. The
  integrated adapter splits pairwise demand volume: the square-mesh baseline
  offers 12 units in both NetSim and NetGraph. Placed totals still depend on
  their different capacity/placement models.
- `keep` accepts root/delta budgets plus `events`/`records` budgets (aliases
  `keep_events`/`keep_records`), passed through to Simulation. Streaming
  per-demand accumulators preserve complete integrals with bounded history.
  Event rows, edge series and typed event counts describe retained history;
  eviction counts are explicit. Lease history remains proportional to faults.
  No bound on total study memory or process parallelism is claimed.

## T6B adversarial follow-up (2026-09-20)

Merged `network-layer-gate-a` first (fast-forward to `7d4976a`). Baseline
`make check-ci`: **555 passed, 3 skipped, 94.07%** coverage. Twelve targeted
regressions then failed on that code before any fix: healthy 15/7-bit/s demands
counted a dropped flow, quadratic tuple visits, unavailable keep options,
partial multi-entity observer transitions, corrupt acquire/release state after
pre-commit aborts, and missing scheduled repairs after post-commit errors.

Changes:

- One residual policy everywhere: ignore <= `max(1e-12 bit/s, 1e-9 * offered)`
  per demand, before capacity-unit conversion. Tolerated residuals normalize
  exported placed to offered and dropped to zero; physical model records stay
  untouched. Tests cover relative and absolute thresholds, real shortfalls,
  bit/s versus Gbit/s export, and a small failed demand beside a large healthy
  one (aggregate tolerances must not hide the small demand's loss).
- Integrate all delivered values together. The study streams into per-demand
  accumulators with O(D) storage; offline timeline reduction visits each sample
  tuple once. Same-timestamp replacements, missing values and the final interval
  are covered. History budgets do not truncate full-run availability or settle
  times. Event counts and optional series intentionally describe retained
  history, with explicit eviction counters.
- Lease changes stage in one `Network.batch()`. Registry changes roll back when
  the root did not commit; post-commit exceptions propagate with matching
  registry state preserved. Active tokens remain discoverable; consumed
  releases are idempotent. Scheduled faults arm repairs in `finally` only when
  their lease survived. Tests include existing overlaps, acquire/release errors,
  nested-batch rejection, and pre-disabled entities.
- Corrected the stale demand-volume documentation: integrated square-mesh
  demand is 12 in both systems. Added events/records budget pass-through and
  their `keep_events`/`keep_records` aliases to `Study(keep=...)`.

Metrics A/B/A command:
`venv/bin/python -m tests.runtime.benchmark_study_metrics --old 7d4976a`.
One CPython 3.14.5 process, GC enabled, 20 repetitions per block, 16 identical
placement samples, setup excluded. Old and new per-demand results and total
loss agree on the benchmark's exactly representable rates.

| Demands | A1 old ms | B new ms | A2 old ms | One-sample tuple visits old → new |
|---|---:|---:|---:|---:|
| 100 | 0.962210 | 0.273244 | 0.981579 | 5,050 → 100 |
| 1,000 | 64.972983 | 2.892677 | 65.658633 | 500,500 → 1,000 |

The run began after three quiet observations two seconds apart; a monitor
sampled every second and found no other Python worker above 10% CPU during
A/B/A. No local tests ran concurrently; normal desktop applications remained
open. These timings isolate metrics extraction, not end-to-end simulation.
The operation-count regressions establish linear tuple visitation without
relying on machine timing.

Separate integration lead, outside T6B: a native single demand of `1e-8` bit/s
reproduced `ZeroDivisionError` in `model/flows.py` source-share normalization
when `Fraction.limit_denominator` rounded the class total to zero. This is a
placement-layer issue, unchanged here; the absolute-floor test uses `1e-4`
bit/s to exercise the study tolerance without depending on that defect.

Final T6B verification:

- `make check-ci`: format/lint/types pass; **581 passed, 3 skipped**;
  **94.14%** coverage on CPython 3.14.5.
- `make check-ft`: format/lint/types pass; **581 passed, 3 skipped**;
  **94.14%** coverage on free-threaded 3.14.5.
- `PYTHONPATH="$PWD" /Users/networmix/ws/NetGraph/venv/bin/python -m pytest
  tests/adapters -p no:cacheprovider -o addopts='' -q`: **19 passed**.
- `git diff --check`: clean. No public model/routing/placement API was changed;
  `LeaseRegistry.active_leases` is the new recovery inspection property, and
  consumed release tokens are now safe to retry. Local checks do not replace CI.
