# C4 reference protocol handoff

Implemented on `gatec/c4-reference`, merged with main through `f2dae73`.
No runtime dependencies. No push. Revision 16 is the design of record.

## API and protocol decisions

- Register `Network.add_agent(device, ReferenceAgent(ReferenceConfig(...)))`.
  The client is `ClientId('ref', 0)`, distance 115, protocol origin 20,
  `link_state=True`. Configuration is frozen in `AgentConfig.params`.
- Defaults: port 6999, hello 1 s, hold 3 s, refresh 10 s, max age 30 s,
  session timeout 5 s, run/processing delay 1 ms. Router IDs must be unique.
- Hellos use scoped IPv6 link control even when only IPv4 forwarding is
  enabled. The receiving interface and advertised router ID identify the
  neighbor; the peer must echo our ID before adjacency is usable. Interface
  DOWN removes it on the subscribed run; physical-send rejection does not.
- Both peers initially open sessions. The lower-router-ID initiator wins;
  new retained sessions synchronize the full LSDB. Floods batch records in
  `LsUpdate`, over sessions only. A rejected session send aborts the session;
  discovery retries the lower-ID open and performs full synchronization.
- Frozen `Lsa`, `Hello`, `LsUpdate`, `ReferenceState` and body records live
  in `netsim.agents.reference`. Three LSA kinds per origin: ADJACENCY,
  PREFIX, SID. Adjacency advertisements carry interface **indices**, never
  remote interface names; they also carry AF eligibility. Prefix metrics
  are zero, matching the oracle's cost-to-origin semantics.
- Sequences seed from simulated time on initialization, then increment.
  A newer self LSA triggers fightback above its sequence, including after a
  restart in the same timestamp. An empty body is a seq+1 purge. Absolute
  `originated_at + max_age` expiry removes records, including purges;
  duplicates and forwarding do not extend expiry. Expired in-flight copies
  are rejected even after their record has been removed.
- SPF compares semantic bodies, ignoring versions/timestamps. It requires
  reciprocal, family-eligible edges and computes ECMP. Hello refreshes keep
  the exact state identity. LSA refreshes retain routes and SR view identity.
- Initial/restart route output syncs both AFs, including empty families.
  Subsequent equality emits no operation. At most 8 changed row operations
  (delete count + add count, configurable `delta_limit`) use delete/add;
  larger changes sync. A replacement counts as two operations.
- Learned `SrDbView` owners/peers are **decimal router-ID strings**. Locators,
  IPv6 endpoint /128 claims and local SID behavior/structure/flavors/oper
  arrive through SID LSAs. Interface names stay local, so learned policies
  use literal SIDs, not remote interface-name symbols. No oracle fallback.

## Cross-slice changes

Only these four files outside C4 ownership changed relative to merged main:

1. `model/contracts.py`: one defaulted field,
   **`Datagram.link_local: bool = False`**. True requires AF 6, uses scoped
   EUI-64 source identity, and permits either locally usable L3 family.
   Existing positional arguments and default datagram semantics are unchanged.
2. `runtime/transport.py`: implements that opt-in at send and delivery,
   retaining physical availability, incarnation checks and FIFO. No session
   coalescing was added to transport.
3. `runtime/agents.py`: public **`AgentRuntime.active`**, `(device, agent)`
   only during the plugin callback, otherwise `None`; reset in `finally`.
   Projection construction and publication are outside this scope.
4. `model/routing.py`: narrowly resolve an IPv4 route's explicitly scoped
   IPv6 next hop against the already-known local peer MAC when no ND entry
   exists. The address must equal that peer's exact EUI-64 link-local address;
   arbitrary link-local addresses remain unresolved. No remote read and no
   additional work/rounds for ordinary plain-IP routes.

The fresh-runtime test independently reproduced the stale listener race.
Main's `f2dae73` now supplies that fix; C4 carries no separate listener edit.

## Acceptance and regression evidence

`tests/agents/test_reference.py` and `test_locality.py` cover:

- Independent protocol/oracle networks; no RouteSource on the protocol twin.
  IPv4, IPv6, dual stack; numbered/unnumbered diamond; seeded connected
  6/8/10-device graphs, including metric changes, failure and ECMP.
  Compare normalized prefixes/costs/scoped next hops, installed FIB groups,
  delivered/drop totals and per-edge offered/carried placement arrays.
- Three-way discovery, lower-ID session selection, DOWN at exactly t=10
  with zero batching delay; dead wire with oper UP and hold-time withdrawal;
  partition, expiry and heal; reset and new-runtime fork; self-sequence
  fightback; duplicate/out-of-order/expired LSAs and absolute age; seq+1
  purge withdrawal; metric-only changes without adjacency/session churn.
- Heartbeat-only and version-refresh intervals: no SPF, FIB or placement
  deltas; exact heartbeat state identity and learned-view identity.
- Initial sync, small route additions/deletions and large sync; one AGENT
  delta per round with successful receipts; delayed FIB PENDING with an
  observable transient placement drop; learned SID/locator validation and
  adjacency SID withdrawal.
- Static forbidden-import/root-access checks and dynamic guards on
  Network.state/view/device only during callbacks. A cheating closure
  produces matching oracle rows unguarded, then fails each access guard.
- New transport opt-in tests first failed with TypeError on the old contract.
  The direct IPv4-only RFC 8950 resolver test failed with no installed FIB
  entry before the resolver fix; a wrong EUI-64 remains rejected. Locality
  tests failed without the runtime indicator. The fresh-fork protocol test
  reproduced ADDRESS_IN_USE/REFUSED before the now-integrated listener fix.

## Scale qualification

Reproduce with `tests.agents.fixtures.measure_ring(n)` on n=16,32,64.
Three sequential trials per size, with no test/check process running in this
worktree. CPython 3.14.5, macOS 26.6.1 arm64, 14 logical CPUs. Desktop remained
active: one-minute system load 3.30 before, 4.23 after. These are same-machine
size-versus-time qualification results, not an old/new speedup claim or an
isolated-machine CPU benchmark.

Workload: one area, N devices and N unnumbered ring links; dual stack, two
loopback prefixes/router, no SR or policies; one agent/router, two peers,
N retained sessions; 3N LSAs and 2(N-1) remote rows per router; two 1000 bit/s
end-to-end demands/classes. Link delay 1/64 s, run/processing delay 1 ms,
hello/hold 4/12 s, refresh/max-age 40/120 s. Four roots, zero deltas/events,
128 records retained. Event budget 200000. Flood payloads are batched;
modeled payload admission size is 64 + 128 bytes per LSA in each message.

Convergence is checked every 1/16 s against oracle rows, then all normalized
FIBs and placements are verified. Simulated times below are upper bounds at
that sampling resolution. Wall intervals include polling comparisons and
runtime work; cold includes binding, excludes fixture/oracle construction.
Failure excludes the oracle recomputation. Event costs are wall interval /
dispatched events, not an isolated scheduler dispatch measurement.

| Routers | Cold simulated s | Cold wall median s (range) | Session messages / hellos | SPF | Events |
| --- | ---: | ---: | ---: | ---: | ---: |
| 16 | 0.2500 | 0.3921 (0.3810–0.4052) | 288 / 96 | 160 | 493 |
| 32 | 0.3750 | 2.1486 (2.1437–2.4419) | 1088 / 192 | 576 | 1479 |
| 64 | 0.6250 | 14.0504 (13.9788–14.4795) | 4224 / 384 | 2176 | 4857 |

Fail the first sorted link at t=10:

| Routers | Reconvergence simulated s | Wall median s (range) | Events | us/event median (range) | Session messages / SPF |
| --- | ---: | ---: | ---: | ---: | ---: |
| 16 | 0.1250 | 0.07434 (0.07055–0.07486) | 93 | 799 (759–805) | 30 / 16 |
| 32 | 0.3125 | 0.29872 (0.29327–0.31345) | 198 | 1509 (1481–1583) | 70 / 36 |
| 64 | 0.5625 | 1.08785 (1.05692–1.14493) | 366 | 2972 (2888–3128) | 126 / 64 |

Message/SPF/event counts and simulated horizons were identical in all three
trials. The 64-router acceptance is marked slow with an explicit 180 s test
timeout. It makes no wall-time threshold assertion.

This teaching implementation does not claim a larger scale envelope,
authenticated peers, Open/R compatibility, or runtime checkpoint/restore.
Local checks do not replace CI.

## Final checks

- `bash .superset/workspace.sh setup`: passed (per-worktree Python 3.14.5).
- `make check-ci`: lint and pyright passed; **1470 passed, 10 skipped**,
  **95.33% coverage**, 42.25 s. Existing fingerprints remain green.
- `make venv-ft`: created Python 3.14.5t; verified GIL disabled.
- `make check-ft`: lint and pyright passed; **1470 passed, 10 skipped**,
  **95.33% coverage**, 39.58 s. The same 44 C4 acceptance/regression cases pass
  in both suites, including the slow 64-router test.
- `git diff --check`: passed. No remaining known C4 acceptance failures.
