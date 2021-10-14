from __future__ import annotations

from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass
from collections import deque
from typing import Dict, Set, Any, List

from netsim.netgraph.graph import MultiDiGraph

from netsim.dict_path import (
    Path,
    PathExpr,
    check_scope_level,
)


@dataclass
class DataContainer:
    pass


class ExecutionContext:
    def __init__(self):
        self._instr_queue = deque([])
        self._graphs: Dict[Path, MultiDiGraph] = {}
        self._node_groups: Dict[Path, Set[str]] = {}
        self._data: Dict[str, DataContainer] = {}

    def set_data(self, key: str, container: DataContainer) -> None:
        self._data[key] = container

    def get_data(self, key: str = "") -> DataContainer:
        if key:
            return self._data[key]
        return self._data

    def get_next_instr(self) -> Instruction:
        if self._instr_queue:
            return self._instr_queue.popleft()
        return None

    def set_instr(self, instr_list: List[Instruction]) -> None:
        self._instr_queue = deque(instr_list)

    def add_graph(self, graph_path: Path, graph: MultiDiGraph) -> None:
        self._graphs[graph_path] = graph

    def get_graphs_by_path(self, path: Path) -> Dict[Path, MultiDiGraph]:
        ret = {}
        for graph_path in self._graphs:
            if check_scope_level(graph_path, path, 1):
                ret[graph_path] = self._graphs[graph_path]
        return ret

    def get_graphs(self) -> Dict[Path, MultiDiGraph]:
        return self._graphs

    def add_node_group(self, group_path: Path, node_ids: Set[str]) -> None:
        self._node_groups[group_path] = node_ids

    def get_node_groups(self) -> Dict[Path, Set[str]]:
        return deepcopy(self._node_groups)

    def get_node_groups_by_expr(self, path_expr: PathExpr) -> Dict[Path, Set[str]]:
        ret = {}
        paths = path_expr.filter(self._node_groups.keys())
        for path in paths:
            ret[path] = self._node_groups[path]
        return ret


class Instruction(ABC):
    def __init__(self, **kwargs: Dict[str, Any]):
        self._attr = kwargs

    def __repr__(self) -> str:
        return f"{type(self).__name__}(attr={self._attr})"

    @abstractmethod
    def run(self, ctx: ExecutionContext) -> ExecutionContext:
        return ctx
