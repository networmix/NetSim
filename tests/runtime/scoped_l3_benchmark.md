# T7: interface-scoped L3

Baseline: integration commit `34c031d`, including T1–T6. The first merge at
`5770482` passed `make check-ci` (515 tests); the subsequent T1 merge kept
both sides' tests. The only merge conflict was an import in `derive.py`;
both the new L3 imports and T1's `diff_pmap` import were retained.

## Derivation contract

`derive_l3(state, now, targets)` still accepts device names or `None` for
full-device derivation. It additionally accepts `(device, frozenset(names))`.
Overlapping scopes are unioned, a full-device cause takes precedence, and
devices and interfaces are processed in sorted order. The L3 runtime kind
queues individual `(device, interface)` entities and combines their scopes
in one update; device-level configuration changes request the full path.

Each device owns an immutable `l3_interfaces` map of contributions:
CONNECTED/LOCAL rows, neighbor addresses/MACs, point-to-point peer MAC, and
whether the interface is a loopback. This is ownership information in the
tree, not a runtime cache. A missing initial index selects full derivation.

- Row ownership is the interface name in `Route.distinguisher`, so overlapping
  connected prefixes remain independent and LOCAL/RECEIVE rows do not need an
  interface next hop. Previous contributions identify withdrawals after address
  or interface removal. Scoped updates use `rib_apply(add=..., delete=...)`;
  untouched rows retain identity. The full path retains CONNECTED/LOCAL syncs.
- Neighbor replacement touches only the selected interfaces' old/new keys.
  The existing `(interface, address)` key contract is unchanged; the interface
  provides IPv6 link-local scope. Peer configuration, link removal, and bundle
  membership changes invalidate both affected L3 owners.
- Router ID is recomputed for full-device causes or changed/removed loopbacks.
  Ethernet-only scoped work does not enumerate loopbacks.
- Carrier invalidation uses direct interface/peer lookups. LAG invalidation
  selects the affected bundles, rather than every bundle on the device.
  Bundle member discovery uses an immutable `interface_index`: its input-map
  identity certifies membership, and changed interface shards update it at the
  existing `bump_epochs` commit boundary. Pure uncommitted candidates derive a
  correct temporary index from the changed shards too. LAG still processes all
  members of the affected connected bundle component, as required by its rules.

Both new index fields are bookkeeping for `StateDelta.changed_paths` and
remain transitively immutable. No runtime dependencies, module-level caches,
round semantics, FIB/placement APIs, or neighbor key shapes changed.

## Benchmark method

The original `verify_pending.py` does `net.links[id]` inside a loop of n
failures. `Network.links` materializes **all** link handles on every access;
that setup callback is independently quadratic. The default harness now
captures handles once before timing. `--legacy-link-lookup` preserves the
exact old callback. This correction applies equally to A, B and A.

The harness also records the deadline phase, starting with a NORMAL callback
at the first carrier deadline (before its stage event). This excludes failure
injection and the initial zero-delay work without changing the simulation's
outputs. Construction and initial convergence remain outside all timed regions.

```sh
git show 34c031d:netsim/model/derive.py > /tmp/t7-derive-old.py
git show 34c031d:netsim/runtime/pipeline.py > /tmp/t7-pipeline-old.py
venv/bin/python tests/runtime/verify_pending.py --carrier --repeats 1 --derive-source /tmp/t7-derive-old.py --pipeline-source /tmp/t7-pipeline-old.py
venv/bin/python tests/runtime/verify_pending.py --carrier --repeats 3
venv/bin/python tests/runtime/verify_pending.py --carrier --repeats 1 --derive-source /tmp/t7-derive-old.py --pipeline-source /tmp/t7-pipeline-old.py
```

Separate processes, same CPython 3.14.5 executable, sequential A/B/A on this
machine. No local test suite ran during these final measurements; process
sampling showed no other Python workload, with normal desktop applications
still running. Saved baseline file hashes match the corresponding Git blobs.
The baseline functions are loaded over the same integrated package and public
state representation; old derivations leave the additional index fields unset.

## A/B/A results

Total timed run with handles captured once (seconds; B is the median of three
samples, A1/A2 one sample each):

| Interfaces | Old A1 (s) | Scoped B (s) | Old A2 (s) | Scoped total per deadline (ms) |
| ---: | ---: | ---: | ---: | ---: |
| 250 | 0.207 | 0.091 | 0.219 | 0.364 |
| 500 | 0.668 | 0.210 | 0.703 | 0.420 |
| 1,000 | 4.465 | 0.357 | 4.657 | 0.357 |
| 2,000 | 18.355 | 0.717 | 18.505 | 0.358 |
| 4,000 | 89.774 | 1.460 | 87.073 | 0.365 |

Deadline phase only (milliseconds per deadline):

| Interfaces | Old A1 | Scoped B median | Old A2 | Scoped B sample range |
| ---: | ---: | ---: | ---: | ---: |
| 250 | 0.724 | 0.282 | 0.768 | 0.281–0.286 |
| 500 | 1.192 | 0.323 | 1.248 | 0.316–0.323 |
| 1,000 | 3.510 | 0.269 | 3.655 | 0.267–0.273 |
| 2,000 | 7.189 | 0.275 | 7.233 | 0.270–0.280 |
| 4,000 | 17.014 | 0.278 | 16.546 | 0.277–0.279 |

The 250-to-4,000 endpoint cost is flat: total 0.364 to 0.365 ms/deadline;
deadline phase 0.282 to 0.278 ms/deadline. At 500 interfaces there is a
repeatable 0.420 ms total / 0.323 ms deadline-phase bump, consistent with
the small-map regime below PMap's 512-entry promotion threshold. This is
larger than sample noise, so strict flatness at **every** size is not claimed.
The former growth with total interface count is removed.

Literal old callback, with `--legacy-link-lookup --sizes 250 1000`, seconds:

| Interfaces | Old A1 | Scoped B | Old A2 |
| ---: | ---: | ---: | ---: |
| 250 | 0.232 | 0.108 | 0.227 |
| 1,000 | 4.886 | 0.766 | 4.901 |

The literal callback retains its separate quadratic handle-map construction;
it is not evidence of an L3 scan remaining. Its deadline phase matches the
cached-handle workload, and the legacy and cached modes have matching final
state/timeline fingerprints at the common sizes.

All A/B/A state/timeline fingerprints match at every measured size. Only the
two internal ownership/membership indexes are excluded from this digest;
FIBs, RIBs, interface state, versions, timestamps and timeline records are
included. The unchanged fingerprint suites separately cover placement.
Raw samples, exact interpreter details, configuration and complete hashes:
[scoped_l3_results.json](scoped_l3_results.json).


## Verification and boundaries

- Seeded randomized config/oper sequences on a device with 50 Ethernet
  interfaces compare scoped L3 with full L3 using `tree_equal`, and require a
  subsequent full pass to return the identical root.
- Runtime sequences verify the same full-pass fixed point after address,
  admin, device-enable and link changes. Additional tests cover overlapping
  prefix ownership, exact neighbor replacement, link removal, loopback
  add/remove, peer bundle changes/member moves, mixed scopes and full-device
  invalidation, untouched contribution identity, and indexed bundle discovery.
- Saved pre-T7 derivations fail 11 of the 15 new regression cases.
- `make check-ci`: 553 passed, 2 skipped, 94.07% coverage; lint and pyright pass.
- `make check-ft`: 553 passed, 2 skipped, 94.07% coverage on Python 3.14.5 with
  the GIL disabled; lint and pyright pass. Both fingerprint suites pass unchanged.

Scope costs still include the underlying persistent-map updates and RIB index
maintenance in `rib_apply`. Full-device causes and clock-free convergence may
walk the device, and a router-ID change must consider all loopbacks. The timing
workload is the specified unnumbered carrier-debounce scenario, not a claim of
constant time for arbitrary large addressed RIB mutations. Local checks do not
replace CI.
