# T2: scoped batches and NetGraph construction

Base: `c2d658e67e62a8df1fe99aa3cae5a1f9f3656630` (`network-layer-gate-a`).
Worktree/branch: `scale/t2-batch`. No other worktree was modified.

## Behavior

- `with net.batch() as b:` yields the same Network. Existing builder methods
  stage into owned PMapBuilders for devices, interfaces, links, demands and
  per-device interface-index allocation. Nothing mutable enters a state root.
- A successful content-changing exit performs one epoch update, optional debug
  immutability validation, delta and hook dispatch. Origin is `('batch', n_ops)`.
  Primitive calls, including no-ops, count once; `add_p2p` counts as three.
  Hook time comes from the clock at commit. Configuration timestamps retain the
  time of their operation. Empty/content-equivalent batches retain the old root.
- Nested batches raise. Exceptions escaping the block discard staged edits and
  restore all tree allocators. Each primitive also journals its writes so a
  caught validation error cannot leave a half-linked interface or consumed index.
- Provisional handles are explicitly invalidated on abort, removed from the
  handle cache, and retain a distinct equality/hash incarnation from later
  entities reusing their generations. Only handles acquired during the batch
  are visited on abort; existing handles remain valid. Escaped RibClients bind
  to their device handle and cannot mutate a replacement after an abort.
- Reads of `state`, device `node`, and `fork()` produce immutable snapshots of
  staged work. Explicit `update(fn)` freezes a snapshot and returns `None` until
  the enclosing commit. These are deliberate freeze boundaries: avoid them in
  every iteration of a bulk build. Leaf interface/link reads do not freeze maps.
- `register_client`, `add_source`, `set_capacity_model`, and adapter metadata are
  non-tree settings and remain outside the transaction. Hook exceptions occur
  after commit and retain the published tree, matching existing `update()`.
- All NetGraph entry points (`from_network`, `from_scenario`, `demands_from`)
  commit once per call. `from_scenario` includes topology and demands together.

## Integration

T1: staging uses the public PMap/PMapBuilder API only, with no backing-dict or
sharding assumptions. This includes `allocators.next_ifindex`.

T3: `routing.py` is untouched. Route add/delete/sync operations are folded in
order, independently per device/AF, then passed to the existing
`rib_apply(rib, add=all_final_rows, delete=withdrawn_keys)` once per device/AF at
freeze. This preserves competing clients, duplicate-row last-write behavior,
sync withdrawal and canonical unchanged rows without rebuilding a prefix index
for every imported route. Existing prefix-length `rib.shards` storage is used
to collect the starting rows. Explicit snapshot reads can cause extra freezes.

## Reproduction and performance

The initial reproducer measured 10,000 individual device additions at 6.98 s
and 10,000 commits. The 127-device/756-link/6-demand scenario import made 2,528
commits. The first batch/equivalence regression tests failed on the base with
`AttributeError: 'Network' object has no attribute 'batch'`.

Final A/B/A measurements used separate, sequential subprocesses on this Mac,
CPython 3.14.5 with the GIL enabled, `PYTHONHASHSEED=0`, three fresh builds per
measurement, and no overlapping test/benchmark jobs. The base came from a
`git archive` in `/tmp`, not another worktree. A1 and A2 are the unmodified base;
B is this implementation. Values below are medians in seconds. All raw samples,
interpreter/source paths and fingerprints are in [t2-results.json](t2-results.json).

| Workload | A1 old | B batch | A2 old | Commits old → batch |
|---|---:|---:|---:|---:|
| 1,000 add_device | 0.077646 | 0.008561 | 0.077911 | 1,000 → 1 |
| 5,000 add_device | 1.701770 | 0.045755 | 1.705787 | 5,000 → 1 |
| 10,000 add_device | 6.777861 | 0.092969 | 6.929715 | 10,000 → 1 |
| backbone_clos.yml import | 0.152524 | 0.026560 | 0.154993 | 2,528 → 1 |

The 10,000-device result is below the 0.5 s acceptance threshold. Device build
cost scales approximately linearly over the measured batch sizes. Import timing
excludes package imports, YAML expansion and convergence. The scenario runs used
`/Users/networmix/ws/NetGraph/venv/bin/python` as requested.

Reproduce each A/B/A leg using the same interpreter and the desired revision's
absolute path (the script prints the actual imported Network source):

```sh
PYTHONPATH=<revision> <worktree>/venv/bin/python <worktree>/benchmarks/batch.py devices --count 10000 --repeat 3
PYTHONPATH=<revision> /Users/networmix/ws/NetGraph/venv/bin/python <worktree>/benchmarks/batch.py scenario --repeat 3
PYTHONPATH=<revision> /Users/networmix/ws/NetGraph/venv/bin/python <worktree>/benchmarks/batch.py scenario --repeat 1 --fingerprint
```

The real scenario's construction tree, converged FIBs and full placement report
have equal old/new SHA-256 fingerprints. Normalization follows `tree_equal`'s
bookkeeping exclusions (version/timestamp/epoch fields); maps and prefix tables
have deterministic ordering. In particular:

- FIBs: `875d0fc58856fcc648cf64e5fb355e1a30d3d3dad915a49b423592033d9ee3b5`
- Placement: `0f4cec99ec7e48d0a3d1e35cc1fe5152affb8e2c3d1cd7192f3c05feaaa1384b`

## Checks and remaining scope

- `bash .superset/workspace.sh setup`: passed; shared hooks were not replaced.
- `make check-ci`: lint and pyright passed; 429 passed, 1 skipped; 95.18% coverage.
- `make venv-ft` and `make check-ft`: free-threaded 3.14.5, GIL disabled;
  lint and pyright passed; 429 passed, 1 skipped; 95.18% coverage.
- Real NetGraph adapter suite, with its interpreter and this worktree's
  `PYTHONPATH`: 9 passed (including the integration test skipped without ngraph).
- Regression coverage includes equal sequential/batched trees before and after
  convergence; one hook/validation; runtime integration; commit-time clock;
  no persistent set/freeze per builder operation; one bulk RIB apply per AF;
  route add/sync/delete ordering; immutable staged snapshots; nested rejection;
  abort/reused generations/escaped clients; caught partial-operation failure;
  no-op canonicalization; pure update and convergence within a batch; and
  exceptions during final validation or post-commit observation.

Existing out-of-scope issue, independently reproduced on the base: setting
Historical note: at the time of this measurement, `net.debug_validate = True` followed by adding a route raised `TypeError: unrecognized object FrozenPrefixTable`; the validator now accepts frozen prefix tables and rejects the mutable builder (fixed on the integration branch).

Local checks do not replace CI. Integration with the other scale branches still
needs the integrator's combined test run.
