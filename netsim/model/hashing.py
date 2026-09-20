"""Load balancers: keyed BLAKE2b over selected flow-key fields and RFC 2992
region selection over the 64-bit hash space.

FLUID placement uses ideal rational shares; HASH placement and probes use
``select``. The two differ only by the integer rounding of region
boundaries documented in ``select_region``.
"""

from __future__ import annotations

import hashlib
import struct
from enum import IntEnum

from netsim.model.packets import FlowKey
from netsim.model.state import record

FIELD_SRC = 1
FIELD_DST = 2
FIELD_PROTO = 4
FIELD_SPORT = 8
FIELD_DPORT = 16
FIELD_FLOW_LABEL = 32

L3 = FIELD_SRC | FIELD_DST | FIELD_PROTO
L3_L4 = L3 | FIELD_SPORT | FIELD_DPORT
L3_L4_LABEL = L3_L4 | FIELD_FLOW_LABEL


class BalancerKind(IntEnum):
    ECMP = 1
    AGGREGATE_PORT = 2


class HashMode(IntEnum):
    HASH_THRESHOLD = 1
    RESILIENT = 2  # deferred past Gate C


_PERSON = {BalancerKind.ECMP: b'ecmp', BalancerKind.AGGREGATE_PORT: b'lag'}
_SPACE = 1 << 64


@record
class LoadBalancer:
    kind: int = BalancerKind.ECMP
    ipv4_fields: int = L3_L4
    ipv6_fields: int = L3_L4_LABEL
    seed: int = 0
    mode: int = HashMode.HASH_THRESHOLD
    buckets: int = 256

    def hashed_bytes(self, key: FlowKey) -> bytes:
        fields = self.ipv4_fields if key.af == 4 else self.ipv6_fields
        return struct.pack(
            '>BQQBHHI',
            key.af,
            (key.src & ((1 << 64) - 1)) ^ (key.src >> 64) if fields & FIELD_SRC else 0,
            (key.dst & ((1 << 64) - 1)) ^ (key.dst >> 64) if fields & FIELD_DST else 0,
            key.proto if fields & FIELD_PROTO else 0,
            key.sport if fields & FIELD_SPORT else 0,
            key.dport if fields & FIELD_DPORT else 0,
            key.flow_label if fields & FIELD_FLOW_LABEL else 0,
        )

    def hash(self, key: FlowKey) -> int:
        return flow_hash(
            self.hashed_bytes(key), self.seed, _PERSON[BalancerKind(self.kind)]
        )

    def select(self, key: FlowKey, weights: tuple[int, ...]) -> int:
        """Index of the member selected for *key* among integer *weights*."""
        return select_region(self.hash(key), weights)


def flow_hash(data: bytes, seed: int, person: bytes) -> int:
    """64-bit keyed BLAKE2b; stable across processes and platforms."""
    h = hashlib.blake2b(data, digest_size=8, key=seed.to_bytes(8, 'big'), person=person)
    return int.from_bytes(h.digest(), 'big')


def select_region(hash_value: int, weights: tuple[int, ...]) -> int:
    """RFC 2992 hash-threshold selection.

    The 2**64 hash space is divided into consecutive regions whose sizes
    are ``floor(2**64 * w_i / total)``; any remainder from rounding goes to
    the last region. Members with zero weight get no region.
    """
    total = sum(weights)
    if total <= 0 or hash_value < 0 or hash_value >= _SPACE:
        raise ValueError('weights must be positive and the hash 64-bit')
    boundary = 0
    last = len(weights) - 1
    for i, w in enumerate(weights):
        if i == last:
            return i
        boundary += (_SPACE * w) // total
        if hash_value < boundary:
            return i
    return last


def flow_label_for(key: FlowKey, domain_seed: int) -> int:
    """20-bit flow label set at encapsulation (RFC 6437), domain-wide seed."""
    return flow_hash(key.to_bytes(), domain_seed, b'flowlbl') & 0xFFFFF
