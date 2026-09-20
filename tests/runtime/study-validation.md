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
- NetGraph compatibility covers failure patterns and result format. Existing
  demand/placement semantics are preserved: the square-mesh adapter baseline
  offers 144 units while NetGraph's placement workflow reports 12. The test
  pins this known difference; aligning demand expansion is outside T6.
- Root/delta retention is bounded by `keep`; event/record history still grows
  with the current run. The study drops each simulation after extracting its
  metrics. No new total-history budget or process parallelism is claimed.
