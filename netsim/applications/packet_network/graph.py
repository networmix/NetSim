"""Lightweight graph used by the packet-network simulation layer.

This replaces the former dependency on ``ngraph.lib.graph.StrictMultiDiGraph``
with a minimal, self-contained implementation that exposes only the API surface
that NetSim actually needs.
"""

from __future__ import annotations

from typing import Any, Dict, Tuple


class SimpleGraph:
    """A directed multi-graph that stores nodes and edges with attributes.

    Nodes are identified by arbitrary hashable *node_id* values.
    Edges are stored under integer keys that can be supplied explicitly or
    auto-assigned.
    """

    def __init__(self) -> None:
        self._nodes: dict = {}
        self._edges: dict = {}
        self._next_edge_id: int = 0

    # -- node operations --------------------------------------------------- #

    def add_node(self, node_id: Any, **attrs: Any) -> None:
        """Add (or update) a node with optional keyword attributes."""
        self._nodes[node_id] = attrs

    def get_nodes(self) -> Dict[Any, Dict[str, Any]]:
        """Return ``{node_id: {attr_name: attr_value, ...}, ...}``."""
        return self._nodes

    # -- edge operations --------------------------------------------------- #

    def add_edge(
        self, src: Any, dst: Any, key: Any = None, **attrs: Any
    ) -> Any:
        """Add an edge from *src* to *dst*.

        Parameters
        ----------
        src, dst:
            Source and destination node ids.
        key:
            Optional edge key.  When *None* the next available integer id is
            used automatically.
        **attrs:
            Arbitrary edge attributes.

        Returns
        -------
        The key under which the edge was stored.
        """
        if key is None:
            key = self._next_edge_id
            self._next_edge_id += 1
        else:
            # Keep the auto-id ahead of any explicitly supplied integer keys
            # so that future auto-assigned keys never collide.
            if isinstance(key, int) and key >= self._next_edge_id:
                self._next_edge_id = key + 1
        self._edges[key] = (src, dst, key, attrs)
        return key

    def get_edges(self) -> Dict[Any, Tuple[Any, Any, Any, Dict[str, Any]]]:
        """Return ``{edge_id: (src, dst, edge_id, {attrs}), ...}``."""
        return self._edges
