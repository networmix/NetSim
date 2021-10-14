from typing import Dict, Any
from dataclasses import dataclass, field

from netsim.netalgo.spf import spf

from netsim.netbuilder.dict_path import Path
from netsim.netbuilder.wf_instructions import WorkflowInstruction
from netsim.netbuilder.instructions import ExecutionContext, DataContainer


ALGORITHMS = {"spf": spf}


@dataclass
class ShortestPathData(DataContainer):
    graph_map: Dict[Path, Any] = field(init=False, default_factory=dict)


class ShortestPath(WorkflowInstruction):
    def __init__(self, attr: Dict[str, Any]):
        super().__init__(attr)
        self._alg = ALGORITHMS[attr["algorithm"]]
        self._graph_paths = [Path(["/", p]) for p in attr["graphs"]]

    def run(self, ctx: ExecutionContext) -> ExecutionContext:

        for path in self._graph_paths:
            graphs = ctx.get_graphs_by_path(path)

            for graph_path, graph in graphs.items():
                spd = ShortestPathData()
                spd.graph_map[graph_path] = {
                    node_id: self._alg(graph, node_id)[0] for node_id in graph
                }
                ctx.set_data(key=f"ShortestPath_{graph_path}", container=spd)
        return ctx
