import ipaddress

import pytest

from netsim.model import addressing as a


class TestMacAddress:
    def test_parse_formats(self):
        for text in ('34:56:78:9a:bc:de', '34-56-78-9A-BC-DE', '3456.789a.bcde'):
            assert str(a.MacAddress.parse(text)) == '34:56:78:9a:bc:de'

    def test_rejects_bad_values(self):
        with pytest.raises(ValueError):
            a.MacAddress.parse('34:56:78:9a:bc')
        with pytest.raises(ValueError):
            a.MacAddress(1 << 48)
        with pytest.raises(TypeError):
            a.MacAddress('00:00:00:00:00:01')  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            a.MacAddress(True)  # type: ignore[arg-type]

    def test_bits(self):
        assert a.MacAddress.parse('01:00:5e:00:00:01').is_multicast
        assert not a.MacAddress.parse('00:00:5e:00:00:01').is_multicast
        assert a.MacAddress(a.LOCAL_ADMIN_BASE).is_locally_administered
        assert a.MacAddress(a.BROADCAST_MAC_INT).is_broadcast
        assert a.MacAddress(a.BROADCAST_MAC_INT).is_multicast

    def test_eui64_rfc4291_appendix_a(self):
        # RFC 4291 Appendix A: 34-56-78-9A-BC-DE -> 36-56-78-FF-FE-9A-BC-DE
        mac = a.MacAddress.parse('34-56-78-9A-BC-DE')
        assert mac.eui64() == 0x365678FFFE9ABCDE
        assert mac.link_local() == ipaddress.IPv6Address('fe80::3656:78ff:fe9a:bcde')
        assert a.is_link_local_v6(mac.link_local_int())

    def test_int_semantics_and_repr(self):
        mac = a.mac_from_index(5)
        assert mac == a.LOCAL_ADMIN_BASE + 5
        assert repr(mac) == "MacAddress('02:00:00:00:00:05')"
        assert not hasattr(mac, '__dict__')


class TestConversions:
    def test_to_int_and_back(self):
        v, af = a.to_int('10.1.2.3')
        assert (v, af) == (0x0A010203, a.IPV4)
        assert a.to_address(v, af) == ipaddress.IPv4Address('10.1.2.3')
        v6, af6 = a.to_int(ipaddress.IPv6Address('2001:db8::4'))
        assert af6 is a.IPV6 and a.to_address(v6, af6) == ipaddress.IPv6Address(
            '2001:db8::4'
        )

    def test_prefix_and_interface(self):
        assert a.prefix_to_int('10.1.13.0/31') == (0x0A010D00, 31, a.IPV4)
        assert a.prefix_to_int('10.1.13.1/31', strict=False) == (0x0A010D00, 31, a.IPV4)
        with pytest.raises(ValueError):
            a.prefix_to_int('10.1.13.1/31')
        with pytest.raises(ValueError):
            a.prefix_to_int('10.1.13.1')
        assert a.interface_to_int('10.1.13.1/31') == (0x0A010D01, 31, a.IPV4)
        assert a.to_network(0x0A010D00, 31, a.IPV4) == ipaddress.IPv4Network(
            '10.1.13.0/31'
        )

    def test_mask_for(self):
        assert a.mask_for(0, 32) == 0
        assert a.mask_for(32, 32) == 0xFFFFFFFF
        assert a.mask_for(31, 32) == 0xFFFFFFFE
        assert a.mask_for(64, 128) == 0xFFFFFFFFFFFFFFFF << 64
        with pytest.raises(ValueError):
            a.mask_for(33, 32)

    def test_family_bits(self):
        assert a.IPV4.bits == 32 and a.IPV6.bits == 128
        assert int(a.IPV6) == 6
