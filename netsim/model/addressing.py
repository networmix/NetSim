"""Address value types and conversions.

Inside the model, IPv4 and IPv6 addresses are plain ``int`` values and
prefixes are ``(network_int, prefix_len)`` pairs; ``ipaddress`` objects
appear only at the API boundary (``to_int`` / ``to_address``). ``hash()``
of an ``ipaddress`` object is never used for any decision (it is a string
hash, randomized per process).
"""

from __future__ import annotations

import ipaddress
from enum import IntEnum
from typing import Union

IPAddress = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]
IPNetwork = Union[ipaddress.IPv4Network, ipaddress.IPv6Network]
IPInterface = Union[ipaddress.IPv4Interface, ipaddress.IPv6Interface]


class AddressFamily(IntEnum):
    """Address family; the value is the IP version number."""

    IPV4 = 4
    IPV6 = 6

    @property
    def bits(self) -> int:
        return 32 if self == AddressFamily.IPV4 else 128


IPV4 = AddressFamily.IPV4
IPV6 = AddressFamily.IPV6

_MAX_MAC = (1 << 48) - 1
LOCAL_ADMIN_BASE = 0x02_00_00_00_00_00
"""First locally administered unicast MAC handed out by allocators."""

LINK_LOCAL_PREFIX = 0xFE80 << 112
"""``fe80::/64`` as an int; the low 64 bits carry the modified EUI-64."""


class MacAddress(int):
    """A 48-bit MAC address as an ``int`` subclass.

    Being an ``int`` makes comparisons in the forward step zero-cost and
    avoids any shared sentinel object for the broadcast address.
    """

    __slots__ = ()

    def __new__(cls, value: int) -> MacAddress:
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError(f'MacAddress needs an int, got {value!r}')
        if not 0 <= value <= _MAX_MAC:
            raise ValueError(f'MAC address out of range: {value:#x}')
        return super().__new__(cls, value)

    @classmethod
    def parse(cls, text: str) -> MacAddress:
        """Parse ``aa:bb:cc:dd:ee:ff``, ``aa-bb-cc-dd-ee-ff`` or ``aabb.ccdd.eeff``."""
        digits = text.replace(':', '').replace('-', '').replace('.', '')
        if len(digits) != 12:
            raise ValueError(f'invalid MAC address: {text!r}')
        try:
            return cls(int(digits, 16))
        except ValueError:
            raise ValueError(f'invalid MAC address: {text!r}') from None

    def __str__(self) -> str:
        v = int(self)
        return ':'.join(f'{(v >> shift) & 0xFF:02x}' for shift in range(40, -1, -8))

    def __repr__(self) -> str:
        return f'MacAddress({str(self)!r})'

    @property
    def is_multicast(self) -> bool:
        return bool((int(self) >> 40) & 1)

    @property
    def is_locally_administered(self) -> bool:
        return bool((int(self) >> 41) & 1)

    @property
    def is_broadcast(self) -> bool:
        return int(self) == _MAX_MAC

    def eui64(self) -> int:
        """Modified EUI-64 interface identifier (RFC 4291 Appendix A)."""
        v = int(self)
        high = (v >> 24) & 0xFFFFFF
        low = v & 0xFFFFFF
        return ((high ^ 0x020000) << 40) | (0xFFFE << 24) | low

    def link_local_int(self) -> int:
        """``fe80::/64`` address with the modified EUI-64, as an int."""
        return LINK_LOCAL_PREFIX | self.eui64()

    def link_local(self) -> ipaddress.IPv6Address:
        return ipaddress.IPv6Address(self.link_local_int())


BROADCAST_MAC_INT = _MAX_MAC


def mac_from_index(index: int, base: int = LOCAL_ADMIN_BASE) -> MacAddress:
    """Deterministic MAC for the *index*-th allocation from *base*."""
    return MacAddress(base + index)


# -- IP conversions -----------------------------------------------------------


def af_of(addr: object) -> AddressFamily:
    if isinstance(
        addr, (ipaddress.IPv4Address, ipaddress.IPv4Network, ipaddress.IPv4Interface)
    ):
        return IPV4
    if isinstance(
        addr, (ipaddress.IPv6Address, ipaddress.IPv6Network, ipaddress.IPv6Interface)
    ):
        return IPV6
    raise TypeError(f'not an ipaddress object: {addr!r}')


def to_int(addr: str | IPAddress) -> tuple[int, AddressFamily]:
    """Address string or object to ``(int, family)``."""
    obj = ipaddress.ip_address(addr) if isinstance(addr, str) else addr
    return int(obj), af_of(obj)


def to_address(value: int, af: AddressFamily) -> IPAddress:
    if af == IPV4:
        return ipaddress.IPv4Address(value)
    return ipaddress.IPv6Address(value)


def prefix_to_int(
    prefix: str | IPNetwork | IPInterface, *, strict: bool = True
) -> tuple[int, int, AddressFamily]:
    """Prefix string, network or interface to ``(network_int, prefix_len, family)``.

    With ``strict`` (default) host bits set in a network raise ``ValueError``,
    as ``ipaddress.ip_network`` does; interfaces are masked to their network.
    """
    if isinstance(prefix, str):
        if '/' not in prefix:
            raise ValueError(f'prefix needs a length: {prefix!r}')
        obj = (
            ipaddress.ip_interface(prefix).network
            if not strict
            else ipaddress.ip_network(prefix)
        )
    elif isinstance(prefix, (ipaddress.IPv4Interface, ipaddress.IPv6Interface)):
        obj = prefix.network
    else:
        obj = prefix
    return int(obj.network_address), obj.prefixlen, af_of(obj)


def interface_to_int(iface: str | IPInterface) -> tuple[int, int, AddressFamily]:
    """Interface address (host address with prefix length) to ``(host_int, prefix_len, family)``."""
    obj = ipaddress.ip_interface(iface) if isinstance(iface, str) else iface
    return int(obj.ip), obj.network.prefixlen, af_of(obj)


def to_network(net_int: int, plen: int, af: AddressFamily) -> IPNetwork:
    if af == IPV4:
        return ipaddress.IPv4Network((net_int, plen))
    return ipaddress.IPv6Network((net_int, plen))


def mask_for(plen: int, bits: int) -> int:
    """Network mask for *plen* leading bits out of *bits*."""
    if not 0 <= plen <= bits:
        raise ValueError(f'prefix length {plen} out of range for {bits} bits')
    return ((1 << bits) - 1) ^ ((1 << (bits - plen)) - 1)


def is_link_local_v6(value: int) -> bool:
    return (value >> 118) == (0xFE80 >> 6)
