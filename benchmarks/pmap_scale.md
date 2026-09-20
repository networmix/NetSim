# T1: persistent map sharding

Base: `network-layer-gate-a`, commit `c2d658e67e62a8df1fe99aa3cae5a1f9f3656630`.
Candidate implementation: `0659d1e50162ce9f8e1a708f4b3d9ea062331a71`.
Measured on 2026-09-20, Apple M4 Max (14 CPU cores), macOS 26.6.1 arm64,
CPython 3.14.5 with the GIL enabled. All A/B comparisons use the same
worktree interpreter, separate serial processes, and `PYTHONHASHSEED=1`.

## Representation and compatibility

- Maps with at most 512 entries use the original one-slot flat dictionary
  representation. Larger maps promote to 256 shards at construction or
  builder publication; they do not demote on removal.
- Storage selection uses `hash((key,)) & (shard_count - 1)`. The tuple hash
  mixes aligned integer keys as well as string hashes. Hashes never order
  output: shard-local insertion ordinals preserve dictionary insertion order,
  including removal/reinsertion, across promotion and Python hash seeds.
  Value-only edits share all order metadata. `sorted_items()` remains the
  canonical key order, and `keys()` retains set-like equality and operations.
- `set` copies one data shard and the vector; builders copy each touched shard
  once before publication and detach afterward. Key additions/removals also
  copy the corresponding order shard. Empty maps still occupy 104 bytes
  including their empty dictionary on this interpreter.
- `diff_pmap`, equality, `tree_equal`, epoch bumps, state-delta sections and
  paths skip shared shards. Flat/promoted or different-size layouts fall back
  to content comparison. `tree_equal` remains separate from canonicalization;
  equality and `canon` still include timestamps and epochs.
- All PMaps participate, including `allocators.next_ifindex`, RIB row maps,
  flow classes and dependency maps. Timeline RIB extraction and placement
  cache comparisons use the same diff boundary.
- The existing PMap API is preserved. `diff_pmap` adds an optional keyword-only
  `sort_key` for the timeline's existing route-key ordering. The promoted
  representation is a private PMap subclass; callers should keep using the
  API and `isinstance`, not private `_d` storage or exact-type assumptions.

For S shards and q changed shards, mutations cost O(S + N/S), and comparisons
cost O(S + q*N/S). These are fixed-shard bounds, not a claim of asymptotically
sublinear updates. Full insertion-order iteration merges the sorted ordinal
runs in O(N log S), with O(N) temporary storage. Sorting output remains
O(N log N). Insertion-order metadata adds space to the baseline map but is
shared across value-only snapshots.

## Benchmark method

`pmap_scale.py` builds 100,000 distinct empty devices before starting timers.
An edit increments device `r0`'s configuration seed; the full commit includes
`Network.update`, resolver epoch maintenance and delta checking. Set timings
replace the device with a distinct object, rather than timing an identity no-op.

Each timing is the median of seven rounds after warmup. A round uses 25
epoch/delta/commit operations, 50 sets, or 500 batches of 1,000 lookups. GC is
disabled only during timing. Both prebuilt-key and formatted-key lookups are
reported; the latter matches the supplied prototype's lookup workload.
Coverage and pytest instrumentation are absent.

Retained memory is `tracemalloc`'s live allocation increase after retaining
64 successive committed roots plus the original baseline, followed by GC.
The baseline tree is created before tracing starts. Values are decimal MB,
not peak RSS or total network footprint. Debug tree validation and observers
are disabled, as in the supplied empty-device reproducer.

To reproduce, archive the base into a temporary directory, then run the same
interpreter and script in A/B/A order. `--repo` explicitly selects the source
tree ahead of the editable installation:

```sh
baseline_dir="$(mktemp -d)"
git archive c2d658e67e62a8df1fe99aa3cae5a1f9f3656630 | tar -x -C "$baseline_dir"
PYTHONHASHSEED=1 venv/bin/python benchmarks/pmap_scale.py --repo "$baseline_dir" --label A1
PYTHONHASHSEED=1 venv/bin/python benchmarks/pmap_scale.py --label B --shards 256
PYTHONHASHSEED=1 venv/bin/python benchmarks/pmap_scale.py --repo "$baseline_dir" --label A2
```

`--shards 512` permits the planned alternative-layout comparison. It is a
benchmark-only class override; there is no new application configuration API.

## Results

Final monitored A/B/A, with the selected 256-shard implementation:

| Metric | A: base before | B: sharded | A: base after |
|---|---:|---:|---:|
| One-device commit, ms | 21.147222 | **0.074895** | 23.122595 |
| Fresh `StateDelta.is_empty()`, ms | 11.440705 | **0.027093** | 12.402573 |
| `bump_epochs`, ms | 8.208227 | 0.032638 | 8.598050 |
| Changed-value `set`, ms | 0.674232 | 0.002363 | 0.712185 |
| Prebuilt-key `get`, ns/key | 64.020166 | **116.881750** | 64.184416 |
| Formatted-key `get`, ns/key | 91.684668 | 154.139750 | 91.143834 |
| 64 retained roots, extra MB | 246.115384 | **1.020216** | 246.115384 |
| Empty map including dict, bytes | 104 | 104 | 104 |

Lookup is 1.823x the mean of the surrounding prebuilt-key baselines (1.686x
including key formatting). The commit meets both the <0.5 ms acceptance
limit and 0.1 ms target; delta time is below 0.1 ms and retained memory is
below 10 MB. The local baseline differs from the supplied 16.5 ms result;
the table uses this interpreter's fresh before/after measurements.

The final series waited for other benchmark work to become idle, then sampled
process CPU usage every 0.5 seconds throughout all three runs. No other Python
process above 20% CPU was observed. The monitor would discard an overlapping
run. Normal desktop processes remained active; the two baseline commit
medians differ by 9.3%, so these are local measurements, not exact timing
guarantees. No tests or other workloads were launched by this worker during
the series.

The earlier serial layout trials were `A1 / B256 / A2 / B512 / A3 / B256-repeat
/ A4`; other workers were active around that exploratory series. Both layouts
were bracketed by fresh base runs:

| Metric | A1 | B256 | A2 | B512 | A3 | B256 repeat | A4 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Commit, ms | 22.789803 | 0.075213 | 23.291627 | 0.061317 | 24.560345 | 0.076218 | 22.900373 |
| Delta, ms | 14.165747 | 0.027265 | 12.224415 | 0.020310 | 13.154767 | 0.027063 | 13.098677 |
| Prebuilt-key get, ns | 61.583666 | 120.478832 | 62.495000 | 125.323750 | 63.002418 | 117.703082 | 63.312084 |
| Retained extra MB | 246.115384 | 1.020216 | 246.115384 | 0.738868 | 246.115384 | 1.020216 | 246.115384 |

256 shards meet all targets and keep shorter vectors and fewer empty buckets
on promotion. The 512-shard trial reduced commit and retention costs, but its
prebuilt lookup was already near the 2x limit. The default remains 256.
Full precision and interpreter metadata for every run are in
[`pmap_scale_results.json`](pmap_scale_results.json).

## Correctness evidence

Before implementation, the one-device regression on 10,000 keys failed with
10,003 key probes. The placement-cache regression failed on the base with
20,000 probes, and the timeline test caught whole-row-map enumeration. All
three pass with shard-scoped comparisons. Tests also cover shard sharing,
aligned integer/string distribution, builder detachment and copy count,
promotion, layout fallback, identity-versus-value semantics, set-like views,
arbitrary incomparable keys, hash-seed-independent iteration, retained roots
against a dictionary reference, and the per-device allocator map.

`tests/model/test_scale_fingerprints.json` was generated on the base before
editing product code. It pins sorted FIB entries and groups, all four placement
arrays, edge labels and per-demand results for the diamond and `build_clos(8, 4)`.
Both include FLUID and HASH demands and initial/failure/restoration phases.
The tests compare both default promotion and forced sharding of the small maps
to those same base fingerprints.

- `make check-ci`: 431 passed, 1 optional NetGraph integration test skipped;
  Ruff and pyright passed; coverage 94.78% (state.py 98%).
- `make venv-ft`, then `make check-ft`: same results on CPython 3.14.5t,
  with `sys._is_gil_enabled()` verified false.
- Local checks do not replace CI. These measurements concern empty-device
  commits, not full-network convergence or a claim of 100k routed-device capacity.
