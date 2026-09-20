"""SR-DB allocation, ownership and immutable-tree operations."""

from dataclasses import replace
from ipaddress import IPv6Address, IPv6Network

import pytest

from netsim.model import derive, srv6
from netsim.model.addressing import IPV6
from netsim.model.contracts import SRV6_LOCAL, STATIC, ClientId, ClientProfile
from netsim.model.igp import oracle_igp
from netsim.model.network import Network
from netsim.model.routing import SRV6_LOCAL_NH, UNREACHABLE
from netsim.model.srv6 import END, END_X, F3216_GIB, F3216_LIB, NEXT_CSID
from netsim.model.state import PMap, validate_immutable


def test_allocator_is_atomic_and_fork_local():
    net = Network()
    a = net.add_device('a')
    locator = a.add_locator('loc', structure=F3216_GIB)
    port = a.add_ethernet('p', unnumbered=True)
    sid = a.add_local_sid(END_X, flavors=NEXT_CSID, structure=F3216_LIB, interface='p')
    before = net.state
    with pytest.raises(ValueError):
        a.add_local_sid(END, structure=F3216_LIB, sid=sid.sid)
    assert net.state is before
    fork = net.fork()
    b = fork.add_device('b')
    assert b.add_locator('loc', structure=F3216_GIB).node_id == locator.node_id + 1
    assert 'b' not in net.state.devices
    assert port.index > 0


def address(text):
    return int(IPv6Address(text))


def f3216(net, name='r', **kw):
    dev = net.add_device(name)
    dev.add_locator('loc', structure=F3216_GIB, **kw)
    return dev


def test_subranges_bare_composite_and_wide():
    net = Network()
    dev = f3216(net)
    port = dev.add_ethernet('p', unnumbered=True)
    node = dev.add_local_sid(END, flavors=NEXT_CSID, structure=F3216_GIB)
    adj = dev.add_local_sid(
        END_X, flavors=NEXT_CSID, structure=F3216_LIB, interface=port
    )
    term = dev.add_local_sid(srv6.END_DT46, structure=srv6.F3216_TERMINAL)
    client = dev.sid_client().request_sid(
        'request', srv6.END_DT46, {'structure': srv6.F3216_TERMINAL}
    )
    comp = dev.add_local_sid(
        END_X, flavors=NEXT_CSID, structure=srv6.F3216_COMPOSITE, interface='p'
    )
    wide = dev.add_local_sid(
        END_X, flavors=NEXT_CSID, structure=srv6.F3216_WLIB, interface='p'
    )
    assert adj.sid >> 80 & 0xFFFF == srv6.LIB_RANGE[0] + port.index
    assert wide.sid >> 96 == srv6.DEFAULT_BLOCK[0] >> 96
    assert wide.sid >> 80 & 0xFFFF == srv6.WLIB_RANGE[0]
    assert len({x.sid for x in (node, adj, term, client, comp, wide)}) == 6
    pools = [
        srv6.function_range(F3216_LIB, srv6.SidRanges(), p)
        for p in ('adjacency', 'terminal', 'bsid', 'client')
    ]
    assert all(a[1] < b[0] for a, b in zip(pools, pools[1:], strict=False))
    assert pools[1][0] <= term.sid >> 80 & 0xFFFF <= pools[1][1]
    assert pools[3][0] <= client.sid >> 80 & 0xFFFF <= pools[3][1]
    assert net.validate() == []
    validate_immutable(net.state)


def test_multi_block_node_exhaustion_and_explicit_collision_atomic():
    net = Network()
    tiny = srv6.SidRanges(gib=(1, 2))
    a = f3216(net, 'a', ranges=tiny)
    b = f3216(net, 'b', ranges=tiny)
    c = net.add_device('c')
    before = net.state
    with pytest.raises(ValueError, match='exhausted'):
        c.add_locator('bad', structure=F3216_GIB, ranges=tiny)
    assert net.state is before
    with pytest.raises(ValueError, match='unique'):
        c.add_locator('bad', structure=F3216_GIB, ranges=tiny, node_id=1)
    assert net.state is before
    other = c.add_locator('second', structure=F3216_GIB, block='5f01::/32', ranges=tiny)
    assert other.node_id == 1
    assert a.node.srv6_sids.locators['loc'].node_id == 1
    assert b.node.srv6_sids.locators['loc'].node_id == 2
    c.add_local_sid(END, flavors=NEXT_CSID, structure=F3216_GIB)
    assert net.validate() == []


def test_function_exhaustion_fallback_and_multiblock():
    net = Network()
    tiny = srv6.SidRanges(gib=(1, 10), lib=(0xE000, 0xE003), wlib=(0xFFFE, 0xFFFF))
    dev = f3216(net, ranges=tiny)
    dev.add_ethernet('p', unnumbered=True)
    adj = dev.add_local_sid(END_X, structure=F3216_LIB, interface='p')
    assert adj.sid >> 80 & 0xFFFF == 0xE000  # index does not fit; cursor fallback
    dev.add_ethernet('q', unnumbered=True)
    before = net.state
    with pytest.raises(ValueError, match='exhausted'):
        dev.add_local_sid(END_X, structure=F3216_LIB, interface='q')
    assert net.state is before
    dev.add_locator('two', structure=F3216_GIB, block='5f01::/32', ranges=tiny)
    other = dev.add_local_sid(END_X, structure=F3216_LIB, interface='q', locator='two')
    assert other.sid != adj.sid
    assert net.validate() == []


def test_recreated_interface_and_device_do_not_reuse_ids():
    net = Network()
    dev = f3216(net)
    old_id = dev.node.srv6_sids.locators['loc'].node_id
    old_port = dev.add_ethernet('p', unnumbered=True)
    old_index = old_port.index
    old = dev.add_local_sid(END_X, structure=F3216_LIB, interface='p')
    dev.remove_interface('p')
    assert old.sid not in dev.node.srv6_sids.sids
    new_port = dev.add_ethernet('p', unnumbered=True)
    new = dev.add_local_sid(END_X, structure=F3216_LIB, interface='p')
    assert new_port.index > old_index
    assert new.sid != old.sid
    net.remove_device('r')
    replacement = f3216(net)
    assert replacement.node.srv6_sids.locators['loc'].node_id > old_id


def test_batch_rollback_and_failed_operation_inside_batch():
    net = Network()
    dev = f3216(net)
    before = net.state
    with pytest.raises(ValueError):
        with net.batch():
            dev.add_local_sid(srv6.END_DT46, structure=srv6.F3216_TERMINAL)
            dev.add_local_sid(srv6.END_DT4, structure=srv6.F3216_TERMINAL)
    assert net.state is before
    with net.batch():
        sid = dev.add_local_sid(srv6.END_DT46, structure=srv6.F3216_TERMINAL)
        snapshot = net.state
        with pytest.raises(ValueError):
            dev.add_local_sid(srv6.END_DT4, structure=srv6.F3216_TERMINAL)
        assert net.state is snapshot
        dev.add_ethernet('p', unnumbered=True)
        dev.add_local_sid(END_X, structure=F3216_LIB, interface='p')
    assert len(dev.node.srv6_sids.sids) == 2
    clone = net.fork()
    clone['r'].remove_local_sid(sid.sid)
    assert sid.sid in dev.node.srv6_sids.sids
    assert sid.sid not in clone['r'].node.srv6_sids.sids


@pytest.mark.parametrize(
    'behavior,flavors',
    [(b, 0) for b in (4, 5, 6, 7, 8)]
    + [
        (END, srv6.USP),
        (END, 16),
        (srv6.END_DT46, srv6.PSP),
        (srv6.END_DT46, srv6.USD),
    ],
)
def test_gate_b_rejections_are_atomic(behavior, flavors):
    net = Network()
    dev = f3216(net)
    old = net.state
    with pytest.raises(ValueError, match='UNSUPPORTED'):
        dev.add_local_sid(behavior, flavors=flavors, structure=F3216_LIB)
    assert net.state is old


@pytest.mark.parametrize('options', [{'vrf': 'blue'}, {'table': 0}, {'table': 42}])
def test_table_selection_is_rejected(options):
    net = Network()
    dev = f3216(net)
    with pytest.raises(ValueError, match='VRF/table'):
        dev.add_local_sid(END, structure=F3216_GIB, **options)
    with pytest.raises(ValueError, match='VRF/table'):
        dev.add_locator('other', structure=F3216_GIB, **options)
    with pytest.raises(ValueError, match='VRF/table'):
        dev.policy_client().add(srv6.SrPolicy(STATIC, 1, 1), **options)


def test_requests_are_scoped_idempotent_and_owned():
    net = Network()
    dev = f3216(net)
    owner = ClientId('controller', 7)
    net.register_client(ClientProfile(owner, 20))
    args = {'structure': srv6.F3216_TERMINAL}
    first = dev.sid_client().request_sid('x', srv6.END_DT46, args)
    snapshot = net.state
    assert dev.sid_client().request_sid('x', srv6.END_DT46, args) is first
    assert net.state is snapshot
    other = dev.sid_client(owner).request_sid('x', srv6.END_DT46, args)
    assert first.sid != other.sid
    with pytest.raises(ValueError, match='different arguments'):
        dev.sid_client(owner).request_sid('x', END, args)
    with pytest.raises(ValueError, match='another owner'):
        dev.remove_local_sid(other.sid)
    with pytest.raises(ValueError, match='unregistered'):
        dev.sid_client(ClientId('missing'))
    request = dev.node.srv6_sids.requests[srv6.request_key(owner, 'x')]
    assert request.result.sid == other.sid
    dev.sid_client(owner).remove_sid(other.sid)
    assert srv6.request_key(owner, 'x') not in dev.node.srv6_sids.requests
    assert dev.sid_client(owner).request_sid('x', srv6.END_DT46, args).sid != other.sid


def test_policy_ownership_bsid_and_client_steering():
    net = Network()
    dev = f3216(net)
    other = ClientId('controller')
    net.register_client(ClientProfile(other, 20))
    client, remote = dev.policy_client(), dev.policy_client(other)
    p = srv6.SrPolicy(STATIC, 10, 1, bsid=address('5f00:0:efff::'))
    client.add(p)
    snapshot = net.state
    client.add(p)
    assert net.state is snapshot
    for fn in (remote.add, remote.replace):
        with pytest.raises(ValueError, match='owner'):
            fn(replace(p, owner=other))
    with pytest.raises(ValueError, match='owner'):
        remote.delete(p.key)
    with pytest.raises(ValueError, match='BSID collision'):
        remote.add(replace(p, owner=other, color=20))
    client.set_steering((srv6.SteeringRule('same-id', p.key),))
    q = replace(p, owner=other, color=20, bsid=address('5f00:0:effe::'))
    remote.add(q)
    remote.set_steering((srv6.SteeringRule('same-id', q.key, dscp=3),))
    assert dev.node.srv6_policies.steering[0].policy == q.key
    client.set_steering(())
    assert len(dev.node.srv6_policies.steering) == 1
    client.replace(replace(p, name='new'))
    client.delete(p.key)
    assert q.key in dev.node.srv6_policies.policies
    remote.delete(q.key)
    assert not dev.node.srv6_policies.bsids
    assert not dev.node.srv6_policies.steering
    validate_immutable(net.state)


def test_candidate_keys_have_one_owner_and_unsupported_terms_rejected():
    net = Network()
    dev = f3216(net)
    other = ClientId('pcep')
    net.register_client(ClientProfile(other, 20))
    path = srv6.CandidatePath(segment_lists=(srv6.SegmentList((srv6.TermSeg('r'),)),))
    dev.policy_client().add(srv6.SrPolicy(STATIC, 1, 1, candidate_paths=(path,)))
    with pytest.raises(ValueError, match='candidate key'):
        dev.policy_client(other).add(
            srv6.SrPolicy(other, 2, 1, candidate_paths=(path,))
        )
    bad_path = replace(
        path,
        segment_lists=(srv6.SegmentList((srv6.TermSeg('r', srv6.END_B6_ENCAPS),)),),
    )
    with pytest.raises(ValueError, match='UNSUPPORTED_BEHAVIOR'):
        dev.policy_client().replace(
            srv6.SrPolicy(STATIC, 1, 1, candidate_paths=(bad_path,))
        )


def test_validation_ranges_functions_classic_and_interface_collisions():
    with pytest.raises(ValueError, match='disjoint'):
        srv6.SidRanges(gib=(1, 0xE001))
    net = Network()
    dev = f3216(net)
    before = net.state
    with pytest.raises(ValueError, match='outside LIB'):
        dev.add_local_sid(END, structure=F3216_LIB, sid='5f00:0:2::')
    with pytest.raises(ValueError, match='covers interface'):
        dev.add_loopback('bad', ipv6=['5f00:0:1::1/128'])
    assert net.state is before
    other = net.add_device('other')
    other.add_loopback('lo', ipv6=['2001:db8:100::1/128'])
    with pytest.raises(ValueError, match='covers interface'):
        other.add_locator('classic', '2001:db8:100:1::/64')
    classic = other.add_locator('classic', '2001:db8:200:1::/64')
    with pytest.raises(ValueError, match='overlap'):
        dev.add_locator('classic', classic.prefix)
    sid = other.add_local_sid(srv6.END_DT46, structure=srv6.UNCOMPRESSED)
    assert srv6.contains(classic.prefix, sid.sid)
    with pytest.raises(ValueError, match='covers interface'):
        other['lo'].configure(ipv6=['2001:db8:200::1/128'])
    assert net.validate() == []


def test_network_validate_catches_direct_bad_records():
    net = Network()
    f3216(net, 'a')
    b = f3216(net, 'b')
    # A deliberately malformed SR-DB models validation of imported records.
    row = srv6.LocalSid(
        b.node.srv6_sids.locators['loc'].prefix[0], 48, END, structure=F3216_LIB
    )

    def bad(state):
        dev = state.devices['a']
        db = replace(dev.srv6_sids, sids=PMap({row.sid: row}))
        return replace(
            state, devices=state.devices.set('a', replace(dev, srv6_sids=db))
        )

    net.update(bad)
    issues = net.validate()
    assert any('covers b uN' in issue for issue in issues)
    assert any('outside LIB' in issue for issue in issues)


@pytest.mark.parametrize('numbered', [False, True])
def test_oracle_originates_locator_before_policies(numbered):
    net = Network()
    a, b = f3216(net, 'a'), f3216(net, 'b')
    net.add_p2p(
        a,
        'p',
        b,
        'p',
        unnumbered=not numbered,
        ipv6=('2001:db8::1/64', '2001:db8::2/64') if numbered else None,
    )
    net.add_source(oracle_igp)
    net.converge()
    prefix = b.node.srv6_sids.locators['loc'].prefix
    assert any(row.prefix == prefix for row in a.rib(IPV6).rows_of(ClientId('igp')))
    assert a.fib(IPV6).entries.lookup(prefix[0]) is not None
    before = net.state
    net.converge()
    assert net.state is before


def test_unknown_ranges_are_exact_and_l3_canonical():
    net = Network()
    dev = f3216(net)
    node = dev.add_local_sid(END, flavors=NEXT_CSID, structure=F3216_GIB)
    net.converge()
    rows = dev.rib(IPV6).rows_of(SRV6_LOCAL)
    assert any(
        r.prefix == (node.sid, 48) and r.nexthops[0].special == SRV6_LOCAL_NH
        for r in rows
    )
    drops = [r for r in rows if r.nexthops[0].special == UNREACHABLE]
    expected = IPv6Network('5f00:0:e000::/35')
    assert [r.prefix for r in drops] == [(int(expected.network_address), 35)]
    assert derive.derive_l3(net.state, 0) is net.state
    custom = srv6.SidRanges(gib=(1, 100), lib=(0xE001, 0xE00F), wlib=(0xFFFE, 0xFFFF))
    db = replace(dev.node.srv6_sids, ranges=PMap({srv6.DEFAULT_BLOCK: custom}))
    cover = srv6.unknown_prefixes(db)
    for function in range(65536):
        covered = any(
            srv6.contains(prefix, srv6.DEFAULT_BLOCK[0] | function << 80)
            for prefix in cover
        )
        assert covered == (0xE001 <= function <= 0xE00F or 0xFFFE <= function <= 0xFFFF)
    assert srv6.unknown_prefixes(replace(db, drop_unknown_local=False)) == ()


def test_auto_bsid_uses_reserved_pool_and_is_canonical():
    net = Network()
    dev = f3216(net)
    configured = srv6.SrPolicy(STATIC, 10, 1)
    stored = dev.policy_client().add(configured)
    assert stored.bsid is not None
    lo, hi = srv6.function_range(F3216_LIB, srv6.SidRanges(), 'bsid')
    assert lo <= stored.bsid >> 80 & 0xFFFF <= hi
    before = net.state
    assert dev.policy_client().add(configured) is stored
    assert net.state is before
    assert (
        dev.policy_client().replace(replace(configured, name='renamed')).bsid
        == stored.bsid
    )
    dev.policy_client().delete(stored.key)
    replacement = dev.policy_client().add(configured)
    assert replacement.bsid != stored.bsid
    with pytest.raises(ValueError, match='BSID'):
        dev.add_local_sid(END, structure=F3216_LIB, sid=replacement.bsid)


def test_repeated_locator_and_adjacency_configuration_canonicalizes():
    net = Network()
    dev = f3216(net)
    loc = dev.node.srv6_sids.locators['loc']
    before = net.state
    assert dev.add_locator('loc', structure=F3216_GIB) is loc
    assert net.state is before
    dev.add_ethernet('p', unnumbered=True)
    sid = dev.add_local_sid(END_X, structure=F3216_LIB, interface='p')
    before = net.state
    assert dev.add_local_sid(END_X, structure=F3216_LIB, interface='p') is sid
    assert net.state is before


def test_default_purpose_ranges_have_independent_expected_boundaries():
    assert [
        srv6.function_range(F3216_LIB, srv6.SidRanges(), purpose)
        for purpose in ('adjacency', 'terminal', 'bsid', 'client')
    ] == [(0xE000, 0xE7FC), (0xE7FD, 0xEFFA), (0xEFFB, 0xF7F8), (0xF7F9, 0xFFF6)]


def test_explicit_reservation_collision_and_removed_link_cleanup():
    net = Network()
    a, b = f3216(net, 'a'), f3216(net, 'b')
    link = net.add_p2p(a, 'p', b, 'p', unnumbered=True)
    a.add_local_sid(END, structure=F3216_LIB, sid='5f00:0:e001::')
    before = net.state
    with pytest.raises(ValueError, match='collision'):
        a.add_local_sid(END_X, structure=F3216_LIB, interface='p')
    assert net.state is before
    a.remove_interface('p')
    assert not link.exists
    assert b['p'].node.link is None
    net.converge()
    assert net.validate() == []


def test_policy_index_tracks_fork_batch_and_device_deletion():
    net = Network()
    dev = f3216(net)
    assert not net.state.srv6_consumers
    policy = dev.policy_client().add(srv6.SrPolicy(STATIC, 1, 1))
    assert net.state.srv6_consumers == frozenset({'r'})
    fork = net.fork()
    with fork.batch():
        fork['r'].policy_client().delete(policy.key)
    assert not fork.state.srv6_consumers
    assert net.state.srv6_consumers == frozenset({'r'})
    net.remove_device('r')
    assert not net.state.srv6_consumers
