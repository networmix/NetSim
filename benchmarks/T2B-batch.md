# T2B: convergence boundaries and incremental RIB snapshots

The convergence-in-batch contract below is historical and superseded by
[T2C](T2C-batch.md): `converge()` and `place()` now reject active batches.
The incremental RIB snapshot behavior remains in place.

Base: `7d4976ad79693663cb3b604f7e0fef1727f812d2`, after the requested
`git merge network-layer-gate-a`. The merge fast-forwarded this worktree and
included all integrated worker changes. Initial `make check-ci` passed:
555 passed, 3 skipped, 94.07% coverage.

## Reproductions

The reported sequence reproduced exactly: after adding a route, converging
inside a batch, adding a second route, and converging after exit, the second
route remained in the RIB but its FIB lookup was `None`, its status was
`NOT_INSTALLED/SHADOWED`, and both input and processed epochs were 3.

The new IPv4/IPv6 clock-free and timed regression cases failed on that code.
Cases ending with another in-batch convergence already passed and guard against
invalidating a result that has actually consumed all final inputs.

Snapshot regressions also failed: a one-row update resubmitted unchanged rows,
and ordinary add/delete folding enumerated the existing RIB. The benchmark
independently recorded 803 RIB applications submitting 4,015,000 rows for just
201 mutations across two devices and repeated reads.

## Fix and epoch semantics

This uses the review's alternative of comparing against the **last staged
inputs**, instead of changing the global `derive.bump_epochs` guard.

`_Edits.base` is the immutable result of the last explicit staged `update()` or
`converge()`. Snapshot reads preserve that reference. At batch exit, when this
reference has advanced beyond the entry root, pending edits are epoch-checked
against it before the usual single outer commit. Thus a route added after an
intermediate convergence changes epoch 3 to 4; the existing FIB gate sees the
pending epoch and resolves it on either `converge()` or the timed pipeline.
Both address families retain the existing cross-family invalidation contract.

If the last operation was convergence, there are no later changed inputs and
no additional invalidation is needed. Simply treating every processed epoch
as consumed while comparing against the much older batch-entry root would
also invalidate these already-correct results. The existing derivation-level
idempotence contract therefore stays intact; `derive.py` is unchanged.

Ordinary batches with no intermediate update retain their existing single
epoch pass. Hooks still receive one delta, and abort still discards the tree
and allocator changes. There are no public API changes.

## Snapshot semantics and cost

Pending RIB operations are folded only for touched `(device, af)` pairs:

- Add/delete use keyed lookups and retain only the final operation for each key.
- A sync discards earlier operations for its client; only clients being synced
  need their existing rows enumerated through `rows_of(client)`.
- Equal upserts and nonexistent deletes are removed from the resulting delta.
- `rib_apply(add=changed_rows, delete=withdrawn_keys)` is called only for a
  nonempty delta. Neither `routing.py` nor its public API changed.
- The frozen RIB is published into the private staging builder and its pending
  operations are consumed. Repeated reads, unrelated-device edits and another
  AF's edits do not replay it. Previously returned snapshots remain immutable.
- Returning to the baseline row content restores its RIB identity/version,
  including restoring an absent RIB after an add/delete cycle across reads.
  No timestamp/epoch-ignoring `tree_equal` is used for canonicalization.

The folding layer costs O(pending rows/keys + existing rows of synced clients),
plus keyed lookups. This is not a claim that every snapshot is O(changes):
`rib_apply` still copies affected prefix-length index tables and rebuilds the
changed client's row index; snapshot publication/canonicalization also has
persistent-map costs. Reading after every route in a growing single-client
RIB can therefore still produce quadratic total work. Bulk edits without
intermediate reads remain preferable. T2B removes full-RIB materialization,
unchanged-row resubmission, and replay of already-frozen operations.

## A/B/A measurement

[batch_snapshots.py](batch_snapshots.py) uses two devices with 5,000 initial
IPv4 routes each, one mutation on B, then 200 one-row edits on A, with two
explicit state reads after each edit. Setup, imports, GC and the final
fingerprint are outside the timer. All runs use the same normal CPython 3.14.5
interpreter on this Mac, `PYTHONHASHSEED=0`, three fresh forks per leg, and
sequential subprocesses with no overlapping test or benchmark jobs. The base
was extracted with `git archive` under `/tmp`; no other worktree was touched.

| Leg | Median seconds | rib_apply calls | Submitted add rows |
|---|---:|---:|---:|
| A1 integrated base | 5.929465 | 803 | 4,015,000 |
| B T2B | 0.188860 | 201 | 201 |
| A2 integrated base | 6.082024 | 803 | 4,015,000 |

All three final RIB fingerprints match. Raw samples, interpreter/source paths,
and the SHA-256 digest are in [t2b-results.json](t2b-results.json).

Reproduce each leg with the same interpreter and the desired revision:

```sh
PYTHONPATH=<revision> PYTHONHASHSEED=0 <worktree>/venv/bin/python <worktree>/benchmarks/batch_snapshots.py --rows 5000 --edits 200 --repeat 3
```

## Validation

Regression coverage includes edits before/after intermediate convergence under
both clock-free and timed execution; IPv4 and IPv6; a final in-batch convergence;
repeated convergence followed by withdrawal; matching sequential add/delete/sync
behavior across clients with and without intervening snapshots; selective row
submission; no replay on repeated reads; immutable earlier snapshots; and
canonical reversion across read boundaries. Existing scale/FIB/placement
fingerprint fixtures remain part of the full suite.

Final `make check-ci` and `make check-ft` both passed lint, pyright and the
full suite: 571 passed, 3 skipped, 94.11% coverage. The latter used free-threaded
CPython 3.14.5. The real NetGraph interpreter also passed all 19 tests in
`tests/adapters` with this worktree on `PYTHONPATH`. Local checks do not replace
CI or the next integrated branch run.
