"""SR header chains, tunnel boundaries and hand-counted wire sizes."""

from dataclasses import replace

import pytest

from netsim.model import packets as pk
from netsim.model.hashing import LoadBalancer, flow_label_for
from netsim.model.srv6 import H_ENCAPS, H_ENCAPS_RED
from netsim.model.state import validate_immutable


def inner_packet(af=4, ttl=64):
    return pk.PacketTemplate(
        af, 1, 2, sport=1234, dport=80, dscp=42, ttl=ttl, hop_limit=ttl
    ).to_packet()


def encap(inner, entries=(11, 22), behavior=H_ENCAPS_RED, transit=False, **kw):
    return pk.encapsulate(
        inner,
        entries,
        behavior=behavior,
        source=99,
        hop_limit=64,
        flow_label=123,
        transit=transit,
        **kw,
    )


def test_srh_default_keeps_positional_constructors():
    srh = pk.SRH((22, 11), 1, 1, 0, 0)
    assert srh.next_header == pk.PROTO_UDP
    assert srh.validate() is None


@pytest.mark.parametrize('af', [4, 6])
@pytest.mark.parametrize('behavior', [H_ENCAPS, H_ENCAPS_RED])
@pytest.mark.parametrize('count', [1, 2, 3])
def test_encapsulation_chain_and_wire_sizes(af, behavior, count):
    inner = inner_packet(af)
    entries = (11, 22, 33)[:count]
    outer = encap(inner, entries, behavior)
    assert outer.payload is inner  # originated: no copy or decrement
    assert (outer.src, outer.dst, outer.hop_limit) == (99, 11, 64)
    assert (outer.traffic_class, outer.flow_label) == (42 << 2, 123)
    protocol = 4 if af == 4 else 41
    if behavior == H_ENCAPS_RED and count == 1:
        assert outer.srh is None and outer.next_header == protocol
        overhead = 40
    else:
        assert outer.next_header == 43
        srh = outer.srh
        assert srh is not None and srh.next_header == protocol
        expected = entries[::-1] if behavior == H_ENCAPS else entries[:0:-1]
        assert srh.entries == expected
        assert srh.segments_left == count - 1
        assert srh.last_entry == len(expected) - 1
        assert pk.srh_bytes(srh) == 8 + 16 * len(expected)
        overhead = 40 + 8 + 16 * len(expected)
    assert pk.validate_chain(outer) is None
    assert pk.decapsulate(outer) is inner
    assert pk.outer_stack_bytes(inner) == 0
    assert pk.outer_stack_bytes(outer) == overhead
    assert pk.ip_bytes(outer) == overhead + (20 if af == 4 else 40) + 1000
    assert pk.frame_bytes(outer) == pk.ip_bytes(outer) + 14
    validate_immutable(outer)


@pytest.mark.parametrize('af', [4, 6])
def test_transit_decrements_inner_once_and_decap_preserves_identity(af):
    inner = inner_packet(af, 9)
    outer = encap(inner, transit=True)
    expected = replace(inner, **({'ttl': 8} if af == 4 else {'hop_limit': 8}))
    assert outer.payload == expected and outer.payload is not inner
    assert pk.decapsulate(outer) is outer.payload
    assert outer.hop_limit == 64
    assert (inner.ttl if af == 4 else inner.hop_limit) == 9


@pytest.mark.parametrize('af', [4, 6])
@pytest.mark.parametrize('ttl', [0, 1])
def test_transit_ttl_exhaustion(af, ttl):
    with pytest.raises(ValueError, match='^TTL_EXPIRED$'):
        encap(inner_packet(af, ttl), transit=True)
    # A locally originated packet does not consume a transit hop.
    inner = inner_packet(af, ttl)
    assert encap(inner).payload is inner


@pytest.mark.parametrize('kw', [{'flags': 1}, {'tag': 7}])
def test_single_segment_metadata_retains_a_nonempty_srh(kw):
    outer = encap(inner_packet(), (11,), **kw)
    assert outer.next_header == 43
    assert outer.srh.entries == (11,)
    assert (outer.srh.segments_left, outer.srh.last_entry) == (0, 0)
    assert outer.srh.flags == kw.get('flags', 0)
    assert outer.srh.tag == kw.get('tag', 0)
    assert pk.validate_chain(outer) is None


def test_tlvs_are_explicitly_unsupported_in_the_fixed_size_srh_model():
    with pytest.raises(ValueError, match='^SRH_TLV_UNSUPPORTED$'):
        encap(inner_packet(), (11,), tlvs=(b'\x00',))


@pytest.mark.parametrize(
    'srh',
    [
        pk.SRH((), 0, -1),
        pk.SRH((1,), -1, 0),
        pk.SRH((1,), 2, 0),
        pk.SRH((1,), 0, 1),
        pk.SRH((1,), 256, 0),
        pk.SRH((-1,), 0, 0),
        pk.SRH((1 << 128,), 0, 0),
        pk.SRH((1,) * 128, 0, 127),
        pk.SRH((1,), 0, 0, flags=256),
        pk.SRH((1,), 0, 0, tag=-1),
    ],
)
def test_malformed_srh(srh):
    outer = pk.IPv6Packet(1, 2, 43, srh=srh, payload=pk.L4Header(3, 4))
    assert pk.validate_chain(outer) == 'SRH_MALFORMED'


@pytest.mark.parametrize('sl', [0, 1, 2])
def test_standard_and_reduced_srh_bounds(sl):
    srh = pk.SRH((2, 1), sl, 1)
    packet = pk.IPv6Packet(1, 2, 43, srh=srh, payload=pk.L4Header(3, 4))
    assert pk.validate_chain(packet) is None  # SL == LE + 1 is reduced, not bad


@pytest.mark.parametrize(
    'packet',
    [
        pk.IPv6Packet(1, 2, 43),
        pk.IPv6Packet(1, 2, 17, srh=pk.SRH((2,), 0, 0)),
        pk.IPv6Packet(1, 2, 4, payload=inner_packet(6)),
        pk.IPv6Packet(1, 2, 41, payload=inner_packet(4)),
        pk.IPv6Packet(1, 2, 4, payload=pk.L4Header(3, 4)),
        pk.IPv6Packet(1, 2, 6, payload=inner_packet()),
        pk.IPv6Packet(1, 2, 43, srh=pk.SRH((2,), 0, 0), payload=inner_packet()),
        pk.IPv6Packet(1, 2, 43, srh=pk.SRH((2,), 0, 0, next_header=43)),
        pk.IPv6Packet(1, 2, 43, payload=pk.SRH((2,), 0, 0)),
        pk.IPv4Packet(1, 2, 41, payload=inner_packet()),
        pk.IPv4Packet(1, 2, 4),
        pk.IPv6Packet(1, 2, 256),
    ],
)
def test_typed_payload_must_agree_with_every_next_header(packet):
    assert pk.validate_chain(packet) == 'SRH_MALFORMED'


def test_validation_reaches_inner_header_and_allows_opaque_plain_payloads():
    bad = pk.IPv6Packet(1, 2, 41, payload=inner_packet(4))
    outer = pk.IPv6Packet(1, 2, 41, payload=bad)
    assert pk.validate_chain(outer) == 'SRH_MALFORMED'
    assert pk.validate_chain(pk.IPv4Packet(1, 2, 17)) is None
    assert pk.validate_chain(pk.IPv6Packet(1, 2, 58, payload=b'icmp')) is None


def test_encapsulation_and_decapsulation_errors():
    with pytest.raises(ValueError, match='^EMPTY_LIST$'):
        encap(inner_packet(), ())
    with pytest.raises(ValueError, match='^UNSUPPORTED_BEHAVIOR$'):
        encap(inner_packet(), behavior=99)
    for inner in (encap(inner_packet(), (11,)), encap(inner_packet())):
        with pytest.raises(ValueError, match='^NESTED_ENCAP_UNSUPPORTED$'):
            encap(inner)
    with pytest.raises(ValueError, match='^SRH_MALFORMED$'):
        encap(pk.IPv6Packet(1, 2, 43))
    with pytest.raises(ValueError, match='^SRH_MALFORMED$'):
        pk.decapsulate(pk.IPv6Packet(1, 2, 43))
    for packet in (inner_packet(4), inner_packet(6)):
        with pytest.raises(ValueError, match='^UPPER_LAYER_NOT_ALLOWED$'):
            pk.decapsulate(packet)


@pytest.mark.parametrize('protocol', [6, 17])
def test_flow_key_keeps_outer_fields_but_reaches_transport_through_srh(protocol):
    srh = pk.SRH((2,), 0, 0, next_header=protocol)
    packet = pk.IPv6Packet(
        11, 22, 43, flow_label=7, srh=srh, payload=pk.L4Header(123, 456)
    )
    assert pk.FlowKey.from_packet(packet) == pk.FlowKey(6, 11, 22, 43, 123, 456, 7)
    plain = replace(packet, srh=None, next_header=protocol)
    assert pk.FlowKey.from_packet(plain).proto == protocol


@pytest.mark.parametrize('af', [4, 6])
@pytest.mark.parametrize('entries', [(11,), (11, 22)])
def test_inner_flow_only_contributes_via_outer_flow_label(af, entries):
    first = inner_packet(af)
    second = replace(first, src=3, payload=pk.L4Header(5678, 443))
    a, b = encap(first, entries), encap(second, entries)
    key = pk.FlowKey.from_packet(a)
    assert key == pk.FlowKey.from_packet(b)
    assert (key.sport, key.dport) == (0, 0)
    assert key.proto == (43 if len(entries) > 1 else 4 if af == 4 else 41)
    a = replace(a, flow_label=flow_label_for(pk.FlowKey.from_packet(first), 42))
    b = replace(b, flow_label=flow_label_for(pk.FlowKey.from_packet(second), 42))
    assert LoadBalancer().hash(pk.FlowKey.from_packet(a)) != LoadBalancer().hash(
        pk.FlowKey.from_packet(b)
    )


def test_non_transport_protocol_does_not_expose_ports():
    packet = pk.IPv6Packet(1, 2, 41, payload=pk.L4Header(123, 456))
    assert pk.FlowKey.from_packet(packet).sport == 0


def test_templates_carry_srh_and_inner_packet_for_probes():
    srh = pk.SRH((22,), 1, 0, next_header=4)
    inner = pk.PacketTemplate(4, 1, 2, sport=65535)
    template = pk.PacketTemplate(6, 99, 11, srh=srh, inner=inner)
    packet = template.to_packet(1)
    assert packet.srh is srh
    assert packet.next_header == 43 and packet.payload.payload.sport == 0
    assert pk.validate_chain(packet) is None
    assert pk.frame_bytes(packet) == 1098
    assert template.flow_key(1) == pk.FlowKey.from_packet(packet)
    transport_template = pk.PacketTemplate(6, 99, 11, srh=srh)
    transport = transport_template.to_packet()
    assert transport.srh.next_header == 17 and transport.payload.sport == 49152
    assert pk.validate_chain(transport) is None
    validate_immutable(template)
    with pytest.raises(ValueError, match='SRH_REQUIRES_IPV6'):
        pk.PacketTemplate(4, 1, 2, srh=srh).to_packet()
