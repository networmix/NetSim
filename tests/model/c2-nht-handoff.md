# Gate C slice C2 — local query, NHT and learned SR validation

Base: `9032ea8` (C0, design revision 16). Worktree branch: `gatec/c2-nht`.
The supplied worktree started at `02c9fbb` and was fast-forwarded to the
requested C0 base before implementation. No other worktree or main was edited.

## APIs for integration

- `nht.resolve(ctx, policy, key, *, exclude_rows=frozenset(), input_epoch=...)`
  is a pure RIB question. Exclude the prospective candidate's own `RowKey`
  together with withdrawn rows. Its own exclusive covering prefix produces
  `SELF_COVERED`; excluding a recursive dependency instead produces
  `UNRESOLVED`. Loops produce `LOOP_DETECTED`. Default permission comes from
  `NhtKey`, independently of device policy. Connected-only includes connected,
  local and explicit interface next hops.
- `routing.resolve_candidate(...)` shares the existing resolver's group,
  recursion, adjacency, substitution and weight logic. Its exclusion overlay
  probes the existing prefix index without copying or scanning the RIB.
- `nht.register`, `unregister`, `refresh` and `remove_client` are pure tree
  operations. A raw registration initially stores `None`: C1 should call
  `nht.refresh(staging_root, device)` after applying its NHT operations, before
  publication. `Device.nht_client(client).register(key)` already does this.
  The next FIB run also resolves new registrations even when its programming
  epoch has not changed. No C1 pipeline files were edited.
- Resolver input commits refresh existing registrations immediately, after SR
  policy validation, so delayed FIB programming does not delay RIB answers.
  FIB derivation recomputes registrations for the consumed family as well.
  NHT-only edits never advance resolver epochs or mark unrelated rows pending.
- `Device.nht_client(client)` exposes `register`, `unregister`, `result`,
  `resolve` (including exclusions), and `installed`. `nht.installed` is also a
  pure helper for constructing a `LookupView`; it reports the actual FIB
  version, processed epoch, installed legs and `PENDING` independently of the
  RIB answer. Supply `interface_generation` for incarnation-bound scopes.
- **Notification identity:** compare individual registration results by `is`.
  Cost, eligibility, source, legs, reason and dependency changes replace the
  result. An epoch-only refresh preserves it. `NhtTable.input_epochs[af]`
  records the latest checked epoch; a retained `NhtResult.input_epoch` records
  when that semantic answer was produced. A direct `nht.resolve` always stamps
  the supplied epoch. The table itself can therefore change without waking a
  registration whose answer is identical.
- **Metric provenance:** `IGP` is recognized directly. Other link-state clients
  use `ClientProfile(link_state=True)`. `RibClient` persists this designation in
  the RIB, including inside batches. C1's direct calls to `rib_apply` must pass
  `profile=agent.profile` to persist custom metric provenance. Only the queried
  resolving row's metric is returned, never a sum. Mixed-source recursive
  resolution remains eligible but reports `COST_UNAVAILABLE`.
- `nht.affected_by(delta, state, device, af)` checks recorded successful and
  failed lookup ranges in either family, consulted interfaces (including failed
  adjacency attempts), and scoped neighbors. The FIB kind still conservatively
  recomputes all registrations of a dirty device/family. It does not yet use
  this helper as a selective scheduler.
- `nht.remove_client(state, device, client)` purges just that client's
  registrations for C1 reset/purge. Retention remains C1's lifecycle decision.

## Additive contract changes

No C0 field was removed or renamed; existing contract validation is unchanged.

| Record | Added field | Default / purpose |
|---|---|---|
| `contracts.ClientProfile` | `link_state: bool` | `False`; marks metrics as IGP costs |
| `contracts.NhtResult` | `interfaces: tuple[str, ...]` | `()`; positive and failed interface dependencies |
| `contracts.NhtTable` | `input_epochs: PMap[int, int]` | empty; checked epochs independent of notification identity |
| `contracts.RemoteSid` | `interface: str | None` | `None`; advertised name for symbolic `AdjSeg` |
| `routing.RibState` | `link_state_sources: frozenset[ClientId]` | empty; immutable metric provenance |

Also added `routing.CandidateResolution`, `routing.resolve_candidate`,
`rib_apply(profile=None)`, and `DeviceContext.interface_generation`.

## Agent SR-DB mode

`None` retains oracle validation. `('agent', name)` requires an existing
agent; an unpublished view means an empty view. Unsupported sources, missing
agents and malformed view types are explicit configuration/validation errors.

The learned validator resolves remote SID ownership, behavior, adjacency peers
and reachability only from `SrDbView`. It shares the SID packet interpreter
with oracle validation, but never queries remote RIBs or interfaces. Local
first-entry resolution remains mandatory and does not prove remote claims.
Unknown claims invalidate the list even with `validate_all_sids=False`.
Advertise owner locator prefixes and endpoint host prefixes in `locators`;
adjacency claims need `peer`, plus `interface` for symbolic paths. Literal
adjacency SIDs can use `peer` without a symbolic interface name. The headend's
own SID state remains local knowledge; peer identities must still be advertised.

A stale view intentionally remains valid when a remote RIB starts shadowing a
SID: the oracle rejects that same policy. Advertised validity is not a claim
of current physical end-to-end delivery. Updating the selected agent's view
invalidates its headend's policy inputs without depending on FIB identity.

## Verification and performance

The new SR tests were first run against C0 and reproduced both defects:
an empty view incorrectly left the policy UP; unsupported/missing sources
were accepted. Re-running the new files against an isolated `git archive
9032ea8` confirmed those two failures and the missing NHT module. The final
matrix also includes IPv4/IPv6, scope generations, default/connected controls,
self-cover and loops, same-prefix fallback, cost-only identity changes, failed
lookups, cross-AF recursion, delayed FIB, purge and withdrawal/recovery.

`tests/runtime/test_srv6_events.py` has one minimal fixture adaptation outside
the C2 model files: its existing `srdb_source=('agent', 'test')` invalidation
case now registers that agent before configuring the source. Its original
assertions remain unchanged. No other worker-owned implementation was edited.

Final checks: `make check-ci` and `make check-ft` (CPython 3.14.5 / 3.14.5t,
GIL disabled on the latter). Both include lint, pyright, all existing SR tests,
plain-IP FIB/placement fingerprints and coverage:

- `make check-ci`: 1,322 passed, 10 skipped, 94.98% coverage; test phase 17.08 s.
- `make check-ft`: 1,322 passed, 10 skipped, 94.98% coverage; test phase 16.07 s.
- Both: Ruff formatting/lint clean; Pyright 0 errors / 0 warnings.
- New exit matrix: 32 NHT cases and 10 agent-SR cases.
- `git diff --check`: clean. Local checks do not replace CI.

Reproducible size comparison:
`venv/bin/python -m tests.model.test_nht --benchmark`.
Apple M4 Max, macOS 26.6.1 arm64, CPython 3.14.5. No concurrent checks or
benchmark jobs were launched during measurement. Each case refreshes 1,000
registrations on one device, direct IGP /32 rows, one egress, no policy
consumers, agents, timers or demands. Construction is excluded; seven measured
runs follow warm-up; GC is disabled during timing. These are local model
microbenchmarks, not end-to-end protocol scale qualification.

| RIB rows | First answers, median | Per registration | Unchanged refresh, median | Per registration |
|---:|---:|---:|---:|---:|
| 1,000 | 19.128 ms | 19.128 us | 21.251 ms | 21.251 us |
| 10,000 | 19.606 ms | 19.606 us | 21.125 ms | 21.125 us |
| 100,000 | 19.318 ms | 19.318 us | 21.249 ms | 21.249 us |

At 10,000 rows the seven-run ranges were 19.000–19.658 ms for first answers
and 20.921–21.355 ms for unchanged refresh. The instrumented 10k-row test
allows at most seven index operations for two hits and a miss, and fails on
any RIB iteration. The query cost is bounded by populated prefix lengths and
matched candidate/recursion work, not total RIB rows. No old/new speedup is
claimed: C0 has no NHT implementation to time.

Open integration work belongs to C1: call the pure NHT operations/refresh in
publication, compare result identities for notifications, and pass the agent
profile to direct RIB writes. There are no known C2 blockers.
