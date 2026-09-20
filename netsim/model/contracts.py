"""Client identities and profiles: the one door for routes, policies and SIDs.

Every producer of routes is a client (FBOSS ``ClientID`` / ``FibClient``,
zebra route ``type`` + ``instance``): the user's statics, the oracle IGP,
protocol agents, controllers. Identity is ``(name, instance)``; the
profile carries the default distance and RFC 9256 §2.6 protocol origin.
"""

from __future__ import annotations

from netsim.model.state import record


@record
class ClientId:
    name: str
    instance: int = 0

    def __lt__(self, other: ClientId) -> bool:
        return (self.name, self.instance) < (other.name, other.instance)


@record
class ClientProfile:
    client: ClientId
    distance: int
    protocol_origin: int = 30
    """RFC 9256 §2.6: configuration/CLI 30, BGP SR-TE 20, PCEP 10."""
    originator: tuple[int, int] = (0, 0)


STATIC = ClientId('static', 0)
IGP = ClientId('igp', 0)

STATIC_PROFILE = ClientProfile(STATIC, distance=1)
IGP_PROFILE = ClientProfile(IGP, distance=110)

CONNECTED = ClientId('connected', 0)
LOCAL = ClientId('local', 0)
CONNECTED_PROFILE = ClientProfile(CONNECTED, distance=0)
LOCAL_PROFILE = ClientProfile(LOCAL, distance=0)
