import collections
import dataclasses

import pytest

from netsim.model import hashing as h
from netsim.model import packets as pk
from netsim.model.addressing import IPV4, IPV6
from netsim.model.state import validate_immutable


class TestPackets:
    def test_records_are_immutable_and_sized(self):
        p = pk.IPv4Packet(
            0x0A000001,
            0x0A000004,
            pk.PROTO_UDP,
            payload=pk.L4Header(1, 2),
            payload_size=1000,
        )
        validate_immutable(p)
        with pytest.raises(dataclasses.FrozenInstanceError):
            p.ttl = 1  # type: ignore[misc]
        assert pk.ip_bytes(p) == 1020 and pk.frame_bytes(p) == 1034
        outer = pk.IPv6Packet(1, 2, pk.PROTO_IPV4, srh=pk.SRH((5, 6), 1, 1), payload=p)
        assert pk.ip_bytes(outer) == 40 + 8 + 32 + 1020
        assert (
            outer.dscp == 0
            and pk.IPv6Packet(1, 2, 17, traffic_class=0b101000_00).dscp == 40
        )

    def test_srh_validate(self):
        assert pk.SRH((1, 2), 1, 1).validate() is None
        assert pk.SRH((1,), 1, 0).validate() is None  # reduced: SL == LE + 1
        assert pk.SRH((1, 2), 3, 1).validate() == 'SRH_MALFORMED'
        assert pk.SRH((), 0, -1).validate() == 'SRH_MALFORMED'
        assert pk.SRH((1, 2), -1, 1).validate() == 'SRH_MALFORMED'
        assert pk.SRH((1, 2), 0, 0).validate() == 'SRH_MALFORMED'

    def test_flow_key_and_template(self):
        t = pk.PacketTemplate(IPV4, 0x0A000001, 0x0A000004)
        k0, k1 = t.flow_key(0), t.flow_key(1)
        assert k0 == pk.FlowKey(4, 0x0A000001, 0x0A000004, 17, 49152, 4789, 0)
        assert k1.sport == 49153 and len(k0.to_bytes()) == 42
        assert (
            t.flow_key(0).to_bytes()
            == pk.PacketTemplate(IPV4, 0x0A000001, 0x0A000004).flow_key(0).to_bytes()
        )
        t6 = pk.PacketTemplate(IPV6, 1, 2, flow_label=7, dscp=3)
        p6 = t6.to_packet()
        assert isinstance(p6, pk.IPv6Packet) and p6.flow_label == 7 and p6.dscp == 3
        assert pk.FlowKey.from_packet(p6).flow_label == 7
        assert pk.af_of_packet(p6) is IPV6


class TestHashing:
    def test_golden_values_are_stable(self):
        key = pk.FlowKey(4, 0x0A000001, 0x0A000004, 6, 12345, 80, 0)
        lb = h.LoadBalancer(seed=1)
        assert lb.hash(key) == h.flow_hash(lb.hashed_bytes(key), 1, b'ecmp')
        # Pin the value: any change here changes every HASH placement.
        assert lb.hash(key) == 0x573C3000FB06CA6F
        assert h.flow_label_for(key, 0) == 0x56EA3

    def test_personalization_seed_and_fields_matter(self):
        key = pk.FlowKey(4, 1, 2, 6, 10, 20, 0)
        ecmp = h.LoadBalancer(seed=5)
        lag = h.LoadBalancer(kind=h.BalancerKind.AGGREGATE_PORT, seed=5)
        assert ecmp.hash(key) != lag.hash(key)
        assert ecmp.hash(key) != h.LoadBalancer(seed=6).hash(key)
        l3 = h.LoadBalancer(ipv4_fields=h.L3)
        assert l3.hash(key) == l3.hash(key._replace(sport=99))
        assert ecmp.hash(key) != ecmp.hash(key._replace(sport=99))

    def test_region_selection_proportions(self):
        assert h.select_region(0, (1, 1, 1)) == 0
        assert h.select_region((1 << 64) - 1, (1, 1, 1)) == 2
        third = (1 << 64) // 3
        assert h.select_region(third - 1, (1, 1, 1)) == 0
        assert h.select_region(third, (1, 1, 1)) == 1
        assert h.select_region(2 * third, (1, 1, 1)) == 2
        assert h.select_region(3, (0, 1)) == 1
        with pytest.raises(ValueError):
            h.select_region(1, (0, 0))
        with pytest.raises(ValueError):
            h.select_region(1 << 64, (1,))

    def test_distribution_is_roughly_uniform(self):
        lb = h.LoadBalancer(seed=42)
        counts = collections.Counter()
        for i in range(6000):
            counts[
                lb.select(pk.FlowKey(4, 0x0A000001, 0x0A000004, 6, i, 80, 0), (1, 1, 3))
            ] += 1
        assert (
            abs(counts[0] - 1200) < 150
            and abs(counts[1] - 1200) < 150
            and abs(counts[2] - 3600) < 200
        )
