# T4 deadline heap verification

Measured on 2026-09-20 against baseline `c2d658e`, using the same local
CPython 3.14.5 executable (`venv/bin/python`) for separate A/B/A processes.
A is the baseline pipeline; B is the ticketed heap. No other repository
implementation was changed for these measurements.

The scheduler's quadratic pending scan is removed. **The original carrier
script's flat-per-deadline acceptance target is not met:** its dominant
cost is L3 derivation over all interfaces at every deadline. This change
does not modify those derivations or broaden T4 into incremental L3.

## Reproduction

```sh
git show c2d658e:netsim/runtime/pipeline.py > /tmp/t4-pipeline-old.py
venv/bin/python tests/runtime/verify_pending.py --pipeline-source /tmp/t4-pipeline-old.py
venv/bin/python tests/runtime/verify_pending.py
venv/bin/python tests/runtime/verify_pending.py --pipeline-source /tmp/t4-pipeline-old.py
```

Repeat the three benchmark commands with `--carrier --repeats 1` for the
original workload. The harness loads the saved pipeline in its own process;
it does not change the checkout. It emits raw samples and fingerprints as
JSON lines. Both workloads include 250, 500, 1,000, 2,000 and 4,000 deadlines.

## Scheduler isolation

One no-op kind, a real `Network`, distinct deadlines, timed `env.run()`;
enqueueing is outside the timed region. Seven samples per size after three
1,000-deadline warmups, medians below. This is a separate diagnostic workload,
not a substitute for the original carrier acceptance workload.

| Deadlines | Old A1 (ms) | Heap B (ms) | Old A2 (ms) | Heap per deadline (us) |
| ---: | ---: | ---: | ---: | ---: |
| 250 | 0.701 | 0.411 | 0.684 | 1.644 |
| 500 | 2.202 | 0.883 | 2.167 | 1.766 |
| 1,000 | 7.555 | 1.894 | 7.347 | 1.894 |
| 2,000 | 26.316 | 4.091 | 26.221 | 2.045 |
| 4,000 | 96.755 | 8.537 | 97.506 | 2.134 |

The new heap work is O(n log n), including heap operations, with amortized
compaction. The per-deadline cost still rises slightly; these numbers do
not establish strict linearity or a slope flat within measurement noise.
The structural regression test counts pending-map visits: the old scan
visits 8,002,000 entries at n=4,000, versus zero with the heap in the same
no-reschedule case.

Other Python workers were visible during the first measurement pass.
The isolated A/B/A above was repeated after they stopped; process sampling
then showed no competing Python workload. Normal desktop applications
remained running. Local tests and benchmark processes were run sequentially.

## Original carrier workload

Two devices, n parallel unnumbered links, distinct down-debounce delays on
one side; all links fail at t=1. Construction and initial convergence are
untimed. The timed region is exactly the original `sim.run_until(...)`,
including the individual link failures and their derived updates. One sample
per size per A/B/A process. The initial pass overlapped other workers at
some points, so small timing differences are not treated as a speedup.

| Deadlines | Old A1 (s) | Heap B (s) | Old A2 (s) | Heap per deadline (ms) |
| ---: | ---: | ---: | ---: | ---: |
| 250 | 0.222 | 0.216 | 0.217 | 0.866 |
| 500 | 0.748 | 0.745 | 0.744 | 1.490 |
| 1,000 | 2.803 | 2.752 | 2.763 | 2.752 |
| 2,000 | 11.394 | 11.111 | 11.069 | 5.555 |
| 4,000 | 51.885 | 52.041 | 51.553 | 13.010 |

All three runs have identical SHA-256 fingerprints of the complete final
state (including FIBs) and the full timeline record sequence at every size;
record counts are respectively 753, 1,503, 3,003, 6,003 and 12,003. This
workload has no demands; placement equivalence is covered by the existing
simulation and packet-loss tests, not by this fingerprint.

A separate cProfile of only `Simulation.run_until` at n=1,000 on the heap
implementation took 8.350 profiled seconds (profiling overhead included):

- `derive_l3`: 6.088 s cumulative, including `_neighbors` 4.593 s and
  `_connected_rows` 1.063 s. Both helpers traverse all device interfaces.
- `fail_all`: 1.325 s cumulative, including `Network.links` 0.765 s, which
  constructs the link-handle map for each of the n calls to `net.links[id]`.
- Deadline claiming (`Kind._claim_due`): 0.010 s cumulative over 4,004 calls.

These times overlap within call trees and must not be added together.
They explain why replacing the pending scan alone cannot satisfy the
full-workload scaling target.

## Semantics and checks

`Kind.pending[entity]` now stores `(deadline, ticket)`. Tickets are per kind,
monotonically increasing, independent of round generations, and used both
for heap ties and invalidation. Retry bookkeeping retains failed tickets
so it cannot resurrect completed/cancelled newer work. Method signatures
and round scheduling semantics are unchanged.

Compaction rebuilds only the kind heap when stale entries exceed live
entries. Stale `StageEvent`s remain in the engine queue until consumed;
generation and pending-deadline checks prevent stale work from running.
Only the original exact-key consumption paths remove `scheduled` and
`round_end_scheduled` entries.

- New regression suite on baseline: 7 failures, 17 passes (scan complexity,
  heap/ticket invariants, and completed/cancelled newer work resurrected by retry).
- `make check-ci`: lint and pyright pass; 432 passed, 1 skipped; 94.69% coverage.
- `make venv-ft`, then `make check-ft`: Python 3.14.5 free-threaded, GIL disabled;
  lint and pyright pass; 432 passed, 1 skipped; 94.69% coverage.

Local checks do not replace CI. The integrator should retain the original
carrier benchmark as an open performance acceptance item for incremental L3
and the workload's repeated link-handle lookup, rather than claiming T4 alone
makes it linear.
