from itertools import cycle
from typing import Dict, Optional, Any
from abc import abstractmethod
from collections import defaultdict
from functools import reduce
from operator import and_
import re
import logging

from netsim.netgraph.graph import MultiDiGraph

from netsim.dict_path import Path, PathExpr, check_scope_level
from netsim.netbuilder.filter import bool_filter
from netsim.instructions import ExecutionContext, Instruction


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s:%(message)s"
)
logger = logging.getLogger(__name__)


class BuilderInstruction(Instruction):
    def __init__(
        self,
        path: Path,
        attr: Optional[Dict[str, Any]] = None,
        **kwargs: Dict[str, Any],
    ):
        super().__init__(**kwargs)
        self._path = path
        self._attr = {} if attr is None else attr

    def __repr__(self):
        return f"{type(self).__name__}({self._path}, attr={self._attr})"

    @abstractmethod
    def run(self, ctx: ExecutionContext) -> ExecutionContext:
        return ctx


class CreateGraph(BuilderInstruction):
    def run(self, ctx: ExecutionContext) -> ExecutionContext:
        ctx.add_graph(self._path, MultiDiGraph(**self._attr))
        return ctx


class CreateNodes(BuilderInstruction):
    def __init__(self, path: Path, attr=None):
        super().__init__(path, attr)

        self._name: Optional[str] = self._attr.get("name", None)
        self._name_template: str = self._attr.get(
            "name_template", "{path[1]}-{node_num}"
        )
        self._node_count: int = self._attr.get("node_count", 1)
        self._node_num_start: int = self._attr.get("node_num_start", 1)

    def _node_id_generator(self) -> str:
        for node_num in range(
            self._node_num_start, self._node_num_start + self._node_count
        ):
            yield self._name_template.format(path=self._path.path, node_num=node_num)

    def run(self, ctx: ExecutionContext) -> ExecutionContext:
        graphs = ctx.get_graphs_by_path(self._path).values()

        if self._name:
            for graph in graphs:
                graph.add_node(self._name, **self._attr.get("node_attr", {}))
        else:
            node_ids = set()
            for node_id in self._node_id_generator():
                for graph in graphs:
                    graph.add_node(node_id, **self._attr.get("node_attr", {}))
                node_ids.add(node_id)
            ctx.add_node_group(self._path, node_ids)
        return ctx


class SelectNodesRegexp(BuilderInstruction):
    def __init__(self, path: Path, attr=None):
        super().__init__(path, attr)

        self._regexp = self._attr.get("regexp")

    def run(self, ctx: ExecutionContext) -> ExecutionContext:
        graphs = ctx.get_graphs_by_path(self._path).values()

        graph_node_ids = defaultdict(set)
        for graph in graphs:
            for node_id in graph:
                if match := re.match(self._regexp, node_id):
                    graph_node_ids[graph].add(match.group())
                    logger.debug("Selector matched node id: %s", match.group())
        node_ids = reduce(and_, graph_node_ids.values())
        logger.debug("Selected node ids: %s", node_ids)
        ctx.add_node_group(self._path, node_ids)
        return ctx


class CreateAdjacencyMesh(BuilderInstruction):
    def __init__(self, path: Path, attr=None):
        super().__init__(path, attr)

        self._group_path_lhs = PathExpr(self._attr["group_lhs"])
        self._group_path_rhs = PathExpr(self._attr["group_rhs"])
        self._scope_level = self._attr.get("scope_level", 1)
        self._bidir = self._attr.get("bidirectional", False)
        self._edge_attr = self._attr.get("edge_attr", {})
        self._filter_dict = self._attr.get("filter", {})

    @staticmethod
    def _create_mesh(
        graph: MultiDiGraph, nodes_lhs, nodes_rhs, bidir, edge_attr, filter_dict
    ):
        for node_lhs in sorted(nodes_lhs):
            assert node_lhs in graph
            for node_rhs in sorted(nodes_rhs):
                assert node_rhs in graph
                if filter_dict and not bool_filter(node_lhs, node_rhs, filter_dict):
                    continue
                graph.add_edge(node_lhs, node_rhs, **edge_attr)
                if bidir:
                    graph.add_edge(node_rhs, node_lhs, **edge_attr)

    def run(self, ctx: ExecutionContext) -> ExecutionContext:
        graphs = ctx.get_graphs_by_path(self._path).values()
        groups_lhs = ctx.get_node_groups_by_expr(self._group_path_lhs)
        groups_rhs = ctx.get_node_groups_by_expr(self._group_path_rhs)

        for group_lhs in groups_lhs:
            for group_rhs in groups_rhs:
                if check_scope_level(group_lhs, group_rhs, self._scope_level):
                    for graph in graphs:
                        self._create_mesh(
                            graph,
                            groups_lhs[group_lhs],
                            groups_rhs[group_rhs],
                            self._bidir,
                            self._edge_attr,
                            self._filter_dict,
                        )
        return ctx


class CreateAdjacencyLadder(BuilderInstruction):
    def __init__(self, path: Path, attr=None):
        super().__init__(path, attr)

        self._group_path_lhs = PathExpr(self._attr["group_lhs"])
        self._group_path_rhs = PathExpr(self._attr["group_rhs"])
        self._scope_level = self._attr.get("scope_level", 1)
        self._bidir = self._attr.get("bidirectional", False)
        self._edge_attr = self._attr.get("edge_attr", {})

    @staticmethod
    def _create_ladder(graph: MultiDiGraph, nodes_lhs, nodes_rhs, bidir, edge_attr):
        if (
            max(len(nodes_lhs), len(nodes_rhs)) % min(len(nodes_lhs), len(nodes_rhs))
            != 0
        ):
            raise ValueError(
                f"Can't create a ladder connection between groups of size {len(nodes_lhs)} and {len(nodes_rhs)}"
            )

        if len(nodes_lhs) < len(nodes_rhs):
            zip_iter = zip(cycle(sorted(nodes_lhs), sorted(nodes_rhs)))
        else:
            zip_iter = zip(sorted(nodes_lhs), cycle(sorted(nodes_rhs)))

        for node_lhs, node_rhs in zip_iter:
            assert node_lhs in graph
            assert node_rhs in graph
            graph.add_edge(node_lhs, node_rhs, **edge_attr)
            if bidir:
                graph.add_edge(node_rhs, node_lhs, **edge_attr)

    def run(self, ctx: ExecutionContext) -> ExecutionContext:
        graphs = ctx.get_graphs_by_path(self._path).values()
        groups_lhs = ctx.get_node_groups_by_expr(self._group_path_lhs)
        groups_rhs = ctx.get_node_groups_by_expr(self._group_path_rhs)

        for group_lhs in groups_lhs:
            for group_rhs in groups_rhs:
                if check_scope_level(group_lhs, group_rhs, self._scope_level):
                    for graph in graphs:
                        self._create_ladder(
                            graph,
                            groups_lhs[group_lhs],
                            groups_rhs[group_rhs],
                            self._bidir,
                            self._edge_attr,
                        )
        return ctx


class CreateAdjacencyP2P(BuilderInstruction):
    def __init__(self, path: Path, attr=None):
        super().__init__(path, attr)

        self._node_lhs = self._attr["node_lhs"]
        self._node_rhs = self._attr["node_rhs"]
        self._bidir = self._attr.get("bidirectional", False)
        self._edge_attr = self._attr.get("edge_attr", {})

    @staticmethod
    def _create_p2p(graph: MultiDiGraph, node_lhs, node_rhs, bidir, edge_attr):
        graph.add_edge(node_lhs, node_rhs, **edge_attr)
        if bidir:
            graph.add_edge(node_rhs, node_lhs, **edge_attr)

    def run(self, ctx: ExecutionContext) -> ExecutionContext:
        graphs = ctx.get_graphs_by_path(self._path).values()
        for graph in graphs:
            self._create_p2p(
                graph, self._node_lhs, self._node_rhs, self._bidir, self._edge_attr
            )
        return ctx


BUILDER_INSTRUCTIONS = {
    "graphs": {"Create": CreateGraph},
    "groups": {"Create": CreateNodes, "Select": SelectNodesRegexp},
    "adjacency": {
        "Mesh": CreateAdjacencyMesh,
        "Ladder": CreateAdjacencyLadder,
        "P2P": CreateAdjacencyP2P,
    },
}
