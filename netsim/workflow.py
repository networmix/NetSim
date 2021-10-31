from __future__ import annotations

from typing import Dict, List, Any, Optional
import logging

from netsim.wf_instructions import (
    Instruction,
    ExecutionContext,
    BuildGraph,
    PrintSimData,
    RunNetSim,
    ValidateGraphJQ,
    LogGraphNodeLink,
    LogGraphLinks,
    LogContextData,
)
from netsim.analysers import ShortestPath


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s:%(message)s"
)
logger = logging.getLogger(__name__)


INSTRUCTIONS = {
    "BuildGraph": BuildGraph,
    "ValidateGraphJQ": ValidateGraphJQ,
    "LogGraphNodeLink": LogGraphNodeLink,
    "LogGraphLinks": LogGraphLinks,
    "ShortestPath": ShortestPath,
    "LogContextData": LogContextData,
    "RunNetSim": RunNetSim,
    "PrintSimData": PrintSimData,
}


class Workflow:
    def __init__(self, instr_list: List[Instruction]):
        self.instr_list = instr_list

    @classmethod
    def from_dict(cls, workflow_dict: Dict[str, Any]) -> Workflow:
        instr_list = []
        for instr_item in workflow_dict["instructions"]:
            ((instr_name, instr_attr),) = instr_item.items()
            instr_list.append(INSTRUCTIONS[instr_name](attr=instr_attr))
        return cls(instr_list)

    def run(self, ctx: Optional[ExecutionContext] = None) -> ExecutionContext:
        if ctx is None:
            ctx = ExecutionContext()
            ctx.set_instr(self.instr_list)
        while instr := ctx.get_next_instr():
            logger.info("Executing: %s", instr)
            ctx = instr.run(ctx)
        return ctx
