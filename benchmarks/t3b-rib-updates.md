# T3B: deletion ownership and incremental RIB updates

Baseline: integrated `network-layer-gate-a` at `7d4976a`, merged before
changes. The required baseline `make check-ci` passed: 555 passed, 3 skipped,
94.07% coverage.

## Reproduction and changes

Eight ownership cases failed on the baseline because `delete_routes` did not
raise: BGP versus STATIC, different STATIC instances, foreign-only and mixed
valid/foreign keys, each inside and outside `Network.batch()`. The fix first
materializes and validates every key, then calls `_apply`. Invalid calls never
stage an operation or delete an owned row, including when the foreign key does
not exist. Valid batched deletions still succeed.

Six instrumented baseline cases counted 1,001/10,001 `Route.key` evaluations
for a one-row add or delete at 1,000/10,000 rows, and 1,002/10,002 evaluations
for replacement. The client-tuple reconstruction caused that walk, and the
prefix update also copied the entire length table. The new implementation:

- Stores client rows in `PMap[ClientId, PMap[RowKey, Route]]` and edits them
  through builders. Add/delete/replace do not call `rows_of`, materialize the
  client's other rows, or sort client keys.
- Stores each frozen prefix-length index in a PMap. Each touched map shard is
  copied once per `rib_apply`; publication uses `_owned` without a second copy.
  Only the outer length dictionary (bounded by address width) is copied whole.
- Keeps ranked prefix tuples, existing row storage shards, sync semantics,
  no-op canonicalization and deterministic RouteEvents. `rows_of` still returns
  a tuple sorted by row key; sorting now occurs only when this query is called.
- Preserves FrozenPrefixTable's constructor/freeze copying, read-only views,
  lookup/count semantics and content equality between dict and PMap layouts.

The corresponding new tests pass with 1/1/2 row-key evaluations for
add/delete/replace at both sizes. They also require exactly one changed shard
out of 256 in **each** of row storage, client index and prefix index. Retained
snapshots and unchanged client maps remain shared and unchanged.

These are incremental updates using the integrated PMap, not a claim of
asymptotic O(log N): PMap documents O(S + N/S), S=256, with a one-time promotion
above 512 entries. Ranking still costs in proportion to the candidates in the
changed prefix. A full sync must inspect the client's old keys and supplied
rows; `rows_of` materialization is O(C log C) for C client rows.

## A/B/A measurements

Measured 2026-09-20 on Apple M4 Max / macOS arm64, CPython 3.14.5 with the GIL,
`PYTHONHASHSEED=0`, fresh processes and serial A/B/A execution. Each value is
the median of seven samples after warm-up. Each single-row sample performs
100 independent mutations of the same immutable 10,000-row base; each sync
sample performs one call. Route construction is outside timing. GC is disabled
during timing and collected before each sample. Raw samples and source paths
are in `t3b_rib_updates_results.json`.

| Operation | A1: integrated baseline | B: incremental indexes | A2: integrated baseline |
| --- | ---: | ---: | ---: |
| Add one new /32 | 1,517.703 µs | 16.781 µs | 1,515.103 µs |
| Delete one existing /32 | 1,502.111 µs | 16.515 µs | 1,508.117 µs |
| Replace one row's metric | 1,516.272 µs | 13.167 µs | 1,509.927 µs |
| Sync 10,000 rows into empty RIB | 14.682 ms | 20.918 ms | 14.708 ms |
| Sync 10,000 metric replacements into populated RIB | 19.157 ms | 24.198 ms | 18.740 ms |

Bulk sync is slower because it constructs/updates the additional persistent
maps; this trade-off is reported, not a claimed bulk-write improvement.
No tests or other benchmarks from this worker ran during these measurements.
Timing sequences that overlapped another worker were discarded. The reported
sequence followed a two-second idle window and was monitored every 0.2 seconds;
no other Python process above 1% CPU was observed during any leg. The monitor
and normal desktop applications remained running, so this is not a dedicated
isolated host. The two baselines bracket drift.

FIB, placement and ordered RouteEvent fingerprints matched across all three
legs for both the diamond and `build_clos(8, 4)`, including initial convergence,
one failure and restoration. The original golden fingerprint tests also pass.

```sh
mkdir -p build/t3b-baseline
git archive 7d4976a netsim | tar -x -C build/t3b-baseline
PYTHONHASHSEED=0 venv/bin/python benchmarks/t3b_rib_updates.py --source build/t3b-baseline --label A1
PYTHONHASHSEED=0 venv/bin/python benchmarks/t3b_rib_updates.py --label B
PYTHONHASHSEED=0 venv/bin/python benchmarks/t3b_rib_updates.py --source build/t3b-baseline --label A2
```

## Checks and integration

- `make check-ci`: lint/format/pyright pass; 570 passed, 3 skipped;
  94.09% coverage.
- `make check-ft`: lint/format/pyright pass on CPython 3.14.5t with GIL disabled;
  570 passed, 3 skipped; 94.09% coverage.
- `git diff --check`: passed. These are local checks; CI has not run.

No public method signatures or timeline schemas changed. The internal
`RibState.clients` values are now PMaps rather than cached tuples, and its
frozen prefix-length tables are PMaps rather than dicts. `rows_of` preserves
contents and order, but no longer promises tuple identity between calls.
The original T3 report describes the previous representation and timings;
this report supersedes its client-index and mutation-cost notes.
