# C2R — detached queries and generation-bound NHT scopes

Merged `main` into `gatec/c2-nht` before changes. Main had advanced from the
requested `89677eb` to `32563fb` (C5 merge); the merge was a fast-forward.
Read the adversarial review and both NHT probes, the integrator contract commit
`f2afd16`, and revision 16 of the design. C4's scoped EUI-64 resolution change
is retained. No other worktree or main was modified; nothing was pushed.

## Reproduction and regression evidence

Both review probes reproduced on the merged baseline:

- `probe_nht_context.py`: the external NhtClient query reported SELF_COVERED,
  but the runtime Context could not provide a prospective query (`rib` absent).
- `probe_nht_scope.py`: generation 3 was replaced by generation 6, yet the
  registration retained generation None and became eligible again.

Eight initial regression cases failed before the implementation: seven
detached-context/direct-registration cases and one real-agent registration
case. Two additional cases cover empty local snapshots and ambiguous unbound
lookups. All ten new cases pass, along with the existing NHT suite.

The original scope probe now prints:

```text
before: 3 True
after replacement: 6 False SCOPE_STALE
registration generation: 3
```

The detached-context tests cover SELF_COVERED with exclusions, recursive
inspection rejecting Device/DeviceState/NetworkState anywhere in the snapshot,
immutability validation, snapshot use after device deletion, IPv4/IPv6 parity
with DeviceContext, delayed FIB PENDING/processed epochs, retained snapshots
after live changes, and policy-bearing next hops using local compiled SR
programs. The runtime test proves generation binding and the first answer are
present in the initial agent publication; removal wakes the agent with
CAUSE_NHT/SCOPE_STALE, recreation preserves both result and agent identities,
and explicit purge/re-registration binds the replacement incarnation.

## API handoff to C1R

`nht.local_context(dev: DeviceState) -> LocalContext` implements the existing
ResolutionContext protocol, including `interface_generation(name)`. Its frozen
records contain projected interface generations/effective AF usability, local
RIBs, neighbors/peer MACs, config, FIBs, outcomes, epochs and local policies.
They retain no root, DeviceState, handle, raw carrier/link endpoints or callback.

Capture it from the run's immutable input device before entering the plugin;
wrap these pure calls for `ctx.resolve` and `ctx.installed`:

```python
local = nht.local_context(input_device)
answer = nht.resolve(
    local,
    local.config.resolution_policy or ResolutionPolicy(),
    key,
    exclude_rows=exclude_rows,
    input_epoch=local.resolver_input_epoch.get(key.af, 0),
)
programmed = nht.installed(local, key)
```

The existing `nht.installed(state, device, key)` form remains supported.
`NhtClient.resolve(key, *, exclude_rows=..., ctx=None)` and
`NhtClient.installed(key, *, ctx=None)` accept a captured LocalContext; without
one, they construct it from the current device. These handle-backed convenience
clients are not objects to expose to plugins. C1R still owns the runtime
Context wrappers; this change deliberately does not edit that worker's files.

The resolver accepts `ctx.srv6_policies` directly and retains its previous
DeviceContext fallback. This preserves policy resolution without placing a
DeviceState in the detached context. No resolution algorithm was duplicated.

## Registration identity

- `nht.register(state, device, key) -> NetworkState` preserves its existing
  state-returning API. It stores a canonical key with the current interface
  generation when a scoped request omits it. `registration_key(dev, key)`
  exposes the same validation/binding step.
- `NhtClient.register(key) -> NhtKey` now returns that canonical key. Agent
  callers receive the canonical key in `ctx.nht`; the runtime's existing NhtOp
  path already calls `nht.register`, so it required no source change.
- A retained registration never switches interface incarnations. An existing
  explicit key remains an identity no-op on re-register, including while stale.
  Unregister the old key and register again to replace it; runtime purge/reset
  follows the same rule. Registering a new generation can coexist with a stale
  registration because generation is part of the key identity.
- For compatibility, result/unregister also accept an original unbound request
  if exactly one stored incarnation matches. They find the stored key, never
  bind the lookup to the replacement interface. Multiple matches raise an
  explicit ValueError requiring a generation-bound key.
- Pure one-shot resolve/installed calls with no generation intentionally query
  the current interface. Pass the returned canonical key for incarnation-bound
  answers. Result identity canonicalization and epoch-only suppression are
  unchanged, and registration does not advance resolver input epochs.

No additions or changes to `contracts.py`. New public model records/helpers:
`LocalInterface`, `LocalContext`, `local_context`, `registration_key`, and
`registered_key`. Files changed are confined to C2 model ownership and its tests.

## Validation

- `bash .superset/workspace.sh setup`: passed.
- Focused NHT suites: 48 passed in 0.42 s.
- `make check-ci`: formatting/lint and Pyright clean; 1,544 passed, 14 skipped,
  1 existing expected failure in 38.19 s; 95.38% coverage.
- `make check-ft`: same clean checks on free-threaded CPython 3.14.5, GIL
  disabled; 1,544 passed, 14 skipped, 1 existing expected failure in 35.86 s;
  95.38% coverage.
- `git diff --check`: clean.

The existing expected failure is the C1 remote endpoint scope projection case
in `tests/runtime/test_transport_integration.py`; it is outside this slice.
The plain-IP fingerprints and every existing SR test passed unchanged.
No performance claim or new benchmark is made in this correctness follow-up.
