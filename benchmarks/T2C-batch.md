# T2C: committed-root derivations and complete observer dispatch

Base: `2b028cc`, after the requested `git merge network-layer-gate-a`.
The initial `make check-ci` passed with 627 tests, 4 skipped and 94.22% coverage.

## Reproductions on the integrated base

- After an intermediate batch convergence, `update(fn)` added a second route
  using `rib_apply`. After exit and another convergence, the RIB contained it,
  but its FIB lookup was `None` and its status was `NOT_INSTALLED/SHADOWED`.
  Both input and processed epochs were 4.
- Restricting the diamond's ECMP policy to one path, converging inside the
  batch, then restoring the policy left the route `PENDING` with one leg after
  `Simulation.run()`.
- Failing a link, converging inside the batch, then restoring the link left
  its interface `DOWN` after `Simulation.run()`.
- A user observer registered before `Simulation()` raised on link failure.
  The committed outage produced zero link events; the interface remained `UP`
  because the runtime observer never received the delta.

Before source changes, the replacement/new regression cases produced 18
failures: ten derivation rejection/abort cases, four callback epoch cases,
two observer dispatch cases and two timed outage cases. Reverted inputs
without derivation already behaved correctly and remain covered.

## Contract and implementation

A batch is one commit of builder and pure update operations. `converge()` and
`place()` reject active batches with `RuntimeError`, respectively:

```text
converge() inside batch(): commit the batch first
place() inside batch(): commit the batch first
```

The check happens before either method reads or derives staged state. Catching
the error within the block permits further edits; letting it escape aborts
the batch and invalidates provisional handles. Run derivations after commit.
The old T2B tests permitting intermediate convergence have been replaced.

Pure `update(fn)` remains supported. Epoch maintenance still precedes the
callback so it sees pending builder inputs. It now also runs on the callback's
result against the staging base, before adopting the result as the next base.
The existing idempotent epoch guard prevents a double advance. Both address
families remain invalidated when either RIB changes. No changes to `derive.py`,
`routing.py`, or the runtime scheduling API are needed.

After a commit, every observer in the registration-order snapshot runs, even
if another raises. Exceptions are collected locally; the first is re-raised
after all observers finish and the nested-update guard is cleared. Observer
failures never roll back the root or starve subsequent observers. This covers
single operations and batch publication, including the runtime's pipeline
and timeline hook.

T2B's incremental RIB snapshot folding is retained. Explicit snapshots still
freeze affected persistent maps/indexes, so per-operation reads/updates can
reduce bulk construction benefits. The callback fix adds an epoch check over
changed devices at each explicit update boundary. This correctness change
makes no new performance claim; no new A/B/A timing comparison was run.

## Validation

Focused model/batch/timeline tests: 89 passed. They cover IPv4 and IPv6,
clock-free and timed installation, both-family `fib_affected` scheduling,
delayed FIB completion, no-op policy/link reversions, caught/uncaught derivation
errors, first-error precedence, committed-state visibility, and runtime outage
recording despite a preceding failing observer.

`make check-ci` and `make check-ft` both passed formatting, lint, pyright and
the full suite: **639 passed, 4 skipped, 94.23% coverage**. The free-threaded
interpreter is CPython 3.14.5, with `Py_GIL_DISABLED=1` and
`sys._is_gil_enabled() == False`. `git diff --check` also passed.
These local checks do not replace CI or the next integrated branch run.
