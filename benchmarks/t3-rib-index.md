# T3: RIB prefix and client indexes

Baseline: `c2d658e` (`network-layer-gate-a`). Measured on 2026-09-20,
Apple M4 Max / macOS arm64, CPython 3.14.5 with the GIL enabled. Each
A/B/A leg used the same interpreter in a fresh process. Benchmarks ran
serially, without this worker's tests or other benchmarks running alongside
them. This is a shared desktop, not an isolated benchmark host; background
desktop activity was not disabled. The two baseline legs bracket drift.

| Measurement | A1: baseline | B: indexed | A2: baseline |
| --- | ---: | ---: | ---: |
| 200 `rows(prefix)` calls, 10,000 /32 rows | 108.283 ms | 0.041 ms | 110.536 ms |
| One `sync` of 10,000 rows into an empty RIB | 8.373 ms | 11.734 ms | 8.660 ms |
| 256-device ring, initial convergence | 4.73 s | 3.86 s | 4.80 s |
| 256-device ring, no-op convergence | 1.14 s | 1.16 s | 1.16 s |
| 256-device ring, `Simulation()` after convergence | 1.15 s | 1.15 s | 1.15 s |
| 256-device ring, one link failure | 1.55 s | 1.35 s | 1.55 s |

Microbenchmarks report the median of seven samples after one warm-up. Route
creation is outside the timed region, garbage collection runs before each
sample and is disabled during it. Lookups visit 200 distinct prefixes. The
ring script reports one run per leg, at its original 0.01 s precision; all
256-device runs end with 65,536 RIB rows. The lookup target of <2 ms is met.
Maintaining the additional indexes increases the cost of the empty-RIB sync;
this change does not claim a write-speed improvement or accelerate oracle SPF.

The copied `verify_converge.py` has the same workloads as the supplied script;
only formatting, unused imports and explicit lambda captures were adjusted.
Initial convergence at the smaller ring sizes was:

| Devices | A1 | B | A2 |
| --- | ---: | ---: | ---: |
| 32 | 0.07 s | 0.06 s | 0.07 s |
| 64 | 0.24 s | 0.22 s | 0.25 s |
| 128 | 1.06 s | 0.92 s | 1.07 s |

## Reproduction

The baseline is an archive inside this worktree; no other worktree is used.

```sh
mkdir -p build/t3-baseline
git archive c2d658e netsim | tar -x -C build/t3-baseline

venv/bin/python benchmarks/t3_rib_index.py --source build/t3-baseline --label A1
PYTHONPATH="$PWD/build/t3-baseline" venv/bin/python benchmarks/verify_converge.py
venv/bin/python benchmarks/t3_rib_index.py --label B
PYTHONPATH="$PWD" venv/bin/python benchmarks/verify_converge.py
venv/bin/python benchmarks/t3_rib_index.py --source build/t3-baseline --label A2
PYTHONPATH="$PWD/build/t3-baseline" venv/bin/python benchmarks/verify_converge.py
```

The microbenchmark also outputs the imported source path, interpreter/GIL
metadata, all samples, and FIB, placement and RouteEvent SHA-256 digests. All
three digests matched in A1/B/A2 for both the diamond and `build_clos(8, 4)`,
across initial convergence, one link failure and restoration. These baseline
digests are pinned in `tests/model/test_routing.py` (including FIB dependencies,
groups/weights, placement arrays, demand/class results and ordered RouteEvents).

## Representation and integration

- Prefix-length `shards` remain `PMap[int, PMap[RowKey, Route]]`; `len(rib)`
  still counts rows. No resolver or network API signatures changed.
- As specified by T3, `prefixes` values change from counts to ranked
  `tuple[Route, ...]`. `rows(prefix)` now returns rank order, the same ordering
  previously returned by `candidates(prefix)`. Both reads return the stored
  tuple directly. Use `len(rib.rows(prefix))` when a count is needed.
- `clients` maps `ClientId` to tuples sorted by row key; `rows_of(client)`
  returns that tuple directly. Both client name and instance, and every
  distinguisher, remain in the row key.
- A batch stages all changes privately. It copies each changed storage shard
  once, copies each changed prefix-length index dict once, and rebuilds only
  touched prefix/client tuples. Unchanged prefix-length dicts and tuple values
  remain shared read-only. Unchanged adds/syncs and absent deletions preserve
  the original RIB object and existing route identities.
- Index updates use `FrozenPrefixTable._owned` with a fresh outer dict and
  fresh changed inner dicts, so publication does not copy them again. Public
  constructor and `freeze()` copying behavior is unchanged.
- Reads are O(1), including `rows_of`; writes still copy affected length
  shards/index dicts and sort affected prefix/client rows. They are not O(1).
- Timeline extraction uses prefix tuple identity to skip untouched prefixes,
  retaining the prior length/network/client/distinguisher ordering and schema.

## Validation

Before the implementation, the new indexed-read, rank-order and IPv4/IPv6
index tests produced four failures on the baseline. The read reproducer
rejects storage scans/sorts rather than relying on a timing threshold.
Additional tests cover metric/distance replacement, competing clients and
instances, distinguishers, withdrawal of the final row, combined
sync/delete/add ordering, no-op identity, failed-sync isolation, and 80 seeded
mixed batches checked against a flat reference with all snapshots retained.

- `bash .superset/workspace.sh setup`: passed.
- `make check-ci`: lint and pyright passed; 416 passed, 1 skipped;
  coverage 94.68%.
- `make venv-ft`: created CPython 3.14.5t; confirmed the GIL is disabled.
- `make check-ft`: lint and pyright passed; 416 passed, 1 skipped;
  coverage 94.68%.
- Existing constructor/freeze immutability and timeline tests pass.
- `git diff --check`: passed.

These are local checks; CI has not run.
