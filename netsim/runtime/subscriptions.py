"""Device-local subscription tries; compare only subscribed, changed branches.

Map diffs prune unchanged shards. Record fields refine coarse sections (notably
interface config versus oper and the two RIB families) before notifying anyone.
The index contains names and paths only, never model roots or plugin objects.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from netsim.model.contracts import Path
from netsim.model.state import PMap, diff_pmap


@dataclass(slots=True)
class _Branch:
    children: dict[str, _Branch] = field(default_factory=dict)
    owners: set[str] = field(default_factory=set)


class SubscriptionIndex:
    def __init__(self) -> None:
        self.devices: dict[str, _Branch] = {}
        self.paths: dict[tuple[str, str], tuple[Path, ...]] = {}

    def add(self, device: str, agent: str, paths: tuple[Path, ...]) -> None:
        self.remove(device, agent)
        self.paths[device, agent] = paths
        root = self.devices.setdefault(device, _Branch())
        for path in paths:
            branch = root
            for part in path:
                branch = branch.children.setdefault(part, _Branch())
            branch.owners.add(agent)

    def remove(self, device: str, agent: str) -> None:
        root = self.devices.get(device)
        for path in self.paths.pop((device, agent), ()):
            if root is None:
                break
            branch = root
            chain = []
            for part in path:
                chain.append((branch, part))
                branch = branch.children[part]
            branch.owners.discard(agent)
            for parent, part in reversed(chain):
                child = parent.children[part]
                if child.owners or child.children:
                    break
                del parent.children[part]
        if root is not None and not root.owners and not root.children:
            del self.devices[device]

    def affected(self, device: str, old: Any, new: Any) -> dict[str, set[Path]]:
        root = self.devices.get(device)
        out: dict[str, set[Path]] = {}
        if root is None:
            return out

        def invalidate(branch: _Branch, path: Path) -> None:
            for owner in branch.owners:
                out.setdefault(owner, set()).add(path)
            for part, child in branch.children.items():
                invalidate(child, path + (part,))

        def visit(branch: _Branch, before: Any, after: Any, path: Path) -> None:
            if before is after:
                return
            # Opaque state is identity-compared, including descendant paths:
            # replacing it invalidates the subtree without walking its leaves.
            if (
                len(path) == 3
                and path[0] == 'agents'
                and path[2] in ('state', 'srdb_view')
            ):
                invalidate(branch, path)
                return
            if (
                before is None
                or after is None
                or type(before) is not type(after)
                or getattr(before, 'generation', None)
                != getattr(after, 'generation', None)
            ):
                invalidate(branch, path)
                return
            if before == after:
                return
            for owner in branch.owners:
                out.setdefault(owner, set()).add(path)
            if not branch.children:
                return
            if isinstance(before, PMap) or isinstance(after, PMap):
                a = before if isinstance(before, PMap) else PMap()
                b = after if isinstance(after, PMap) else PMap()
                for key in diff_pmap(a, b, by_identity=True).keys:
                    part = str(key)
                    child = branch.children.get(part)
                    if child is not None:
                        visit(child, a.get(key), b.get(key), path + (part,))
            else:
                for part, child in branch.children.items():
                    visit(
                        child,
                        getattr(before, part, None),
                        getattr(after, part, None),
                        path + (part,),
                    )

        visit(root, old, new, ())
        return out
