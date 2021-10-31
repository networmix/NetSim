# pylint: disable=c-extension-no-member
from __future__ import annotations

import cProfile
import pstats
from collections import deque
from copy import deepcopy
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
import logging
import json
import jq
import pprint

from netsim.netgraph.io import graph_to_node_link

from netsim.dict_path import (
    Path,
    dict_to_paths,
    process_dict,
)
from netsim.instructions import DataContainer, Instruction, ExecutionContext
from netsim.netbuilder.builder_instructions import BUILDER_INSTRUCTIONS
from netsim.netsim_common import NetSimObjectName
from netsim.netsim_simulator import NetSim
from netsim.sim_common import SimTime
from netsim.simstat import Stat


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s:%(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class NetSimData(DataContainer):
    until_time: SimTime
    stat_interval: SimTime
    stat_data: Dict[NetSimObjectName, Stat]
    sim_stat_data: Stat
    nsim_stat_data: Stat


class WorkflowInstruction(Instruction):
    def __init__(
        self,
        attr: Optional[Dict[str, Any]] = None,
        **kwargs: Dict[str, Any],
    ):
        super().__init__(**kwargs)
        self._attr = {} if attr is None else attr

    def __repr__(self):
        return f"{type(self).__name__}(attr={self._attr})"


class BuildGraph(WorkflowInstruction):
    BLUEPRINT_SECTIONS = ["graphs", "groups", "adjacency"]

    def __init__(self, attr: Dict[str, Any]):
        super().__init__(attr)

        self._blueprint_dict = {
            k: v for k, v in attr.items() if k in self.BLUEPRINT_SECTIONS
        }
        self._preprocess_bp_dict(self._blueprint_dict)

    def run(self, ctx: ExecutionContext) -> ExecutionContext:
        instr_list = self._create_instr(self._blueprint_dict, self.BLUEPRINT_SECTIONS)
        ctx = self._exec_instr(ctx, instr_list)
        return ctx

    @staticmethod
    def _exec_instr(
        ctx: ExecutionContext, instr_list: List[Instruction]
    ) -> ExecutionContext:
        inst_queue = deque(instr_list)

        while inst_queue:
            instr = inst_queue.popleft()
            ctx = instr.run(ctx)
        return ctx

    @staticmethod
    def _preprocess_bp_dict(bp_dict: Dict[str, Any]) -> None:
        process_dict(bp_dict)
        for section_name in ["groups", "adjacency"]:
            if section_name in bp_dict:
                if "$graphs" in bp_dict[section_name]:
                    tmp = bp_dict[section_name]["$graphs"]
                    graphs = bp_dict["graphs"].keys()
                    for graph in graphs:
                        bp_dict[section_name][graph] = deepcopy(tmp)
                    del bp_dict[section_name]["$graphs"]

    @classmethod
    def _create_instr(
        cls, bp_dict: Dict[str, Any], section_list: List[str]
    ) -> List[Instruction]:
        instr_list = []

        for section_name in section_list:
            instr_list.extend(cls._parse_section(bp_dict[section_name], section_name))
        return instr_list

    @classmethod
    def _parse_section(
        cls, section: Dict[str, Any], section_name: str
    ) -> List[Instruction]:
        instr_list = []
        parsed_section = dict_to_paths(section)
        for path_value in parsed_section:
            path, value = path_value
            instr_list.extend(cls._parse_instr_list(value, section_name, path))
        return instr_list

    @staticmethod
    def _parse_instr_list(instr_list_raw, section_name, path) -> List[Instruction]:
        instr_list = []
        for instr_item in instr_list_raw:
            ((instr_name, instr_attr),) = instr_item.items()
            instr_list.append(
                BUILDER_INSTRUCTIONS[section_name][instr_name](path, attr=instr_attr)
            )
        return instr_list


class ValidateGraphJQ(WorkflowInstruction):
    def __init__(self, attr: Dict[str, Any]):
        super().__init__(attr)
        self._graph_jq_expr_map = {Path(["/", k]): v for k, v in attr.items()}

    def run(self, ctx: ExecutionContext) -> ExecutionContext:

        for path in self._graph_jq_expr_map:
            graphs = ctx.get_graphs_by_path(path).values()

            for validator_block in self._graph_jq_expr_map[path]:
                jq_expr = jq.compile(validator_block["jq_expr"])
                exp_res = validator_block["expected"]

                for graph in graphs:
                    res = jq_expr.input(graph_to_node_link(graph)).first()
                    logger.debug("JQ Result: %s. Expected: %s", res, exp_res)
                    if isinstance(res, int):
                        exp_res = int(exp_res)
                    assert res == exp_res
        return ctx


class LogGraphNodeLink(WorkflowInstruction):
    def __init__(self, attr: Dict[str, Any]):
        super().__init__(attr)
        self._graph_paths = [Path(["/", p]) for p in attr]

    def run(self, ctx: ExecutionContext) -> ExecutionContext:

        for path in self._graph_paths:
            graphs = ctx.get_graphs_by_path(path).values()

            for graph in graphs:
                res = graph_to_node_link(graph)
                logger.debug(json.dumps(res))
        return ctx


class LogGraphLinks(WorkflowInstruction):
    def __init__(self, attr: Dict[str, Any]):
        super().__init__(attr)
        self._graph_paths = [Path(["/", p]) for p in attr]

    def run(self, ctx: ExecutionContext) -> ExecutionContext:

        for path in self._graph_paths:
            graphs = ctx.get_graphs_by_path(path).values()

            for graph in graphs:
                res = graph.get_edges()
                logger.debug(json.dumps(res))
        return ctx


class LogContextData(WorkflowInstruction):
    def __init__(self, attr: Dict[str, Any]):
        super().__init__(attr)

    def run(self, ctx: ExecutionContext) -> ExecutionContext:

        logger.debug(ctx.get_data())
        return ctx


class RunNetSim(WorkflowInstruction):
    def __init__(self, attr: Dict[str, Any]):
        super().__init__(attr)
        self._graph = attr["graph"]
        self._until_time = attr.get("until_time")
        self._stat_interval = attr.get("stat_interval")
        self._profile = attr.get("profile", False)
        self._profile_sort = attr.get("profile_sort", "tottime")

    def run(self, ctx: ExecutionContext) -> ExecutionContext:

        graph = list(ctx.get_graphs_by_path(Path(["/", self._graph])).values())[0]

        sim = NetSim(stat_interval=self._stat_interval)
        sim.load_graph(graph)

        if self._profile:
            with cProfile.Profile() as profiler:
                sim.run(until_time=self._until_time)
                stats = pstats.Stats(profiler).sort_stats(self._profile_sort)
                stats.print_stats()
        else:
            sim.run(until_time=self._until_time)
        stat_dict: Dict[NetSimObjectName, Stat] = {}
        for ns_obj in sim.get_ns_obj_iter():
            stat_dict[ns_obj.name] = ns_obj.stat

        key = "NetSim_" + self._graph
        ctx.set_data(
            key,
            NetSimData(
                self._until_time, self._stat_interval, stat_dict, sim.stat, sim.nstat
            ),
        )
        return ctx


class PrintSimData(WorkflowInstruction):
    def __init__(self, attr: Dict[str, Any]):
        super().__init__(attr)
        self._graph = attr["graph"]

    def run(self, ctx: ExecutionContext) -> ExecutionContext:
        key = "NetSim_" + self._graph
        simdata: NetSimData = ctx.get_data(key)

        for obj_name, obj_stat_samples in simdata.nsim_stat_data.stat_samples.items():
            for interval, stat_sample in obj_stat_samples.items():
                print(f"{obj_name} {interval}")
                sample_dict = stat_sample.todict()
                for key in sorted(sample_dict):
                    for k_pref in ["total", "avg"]:
                        if k_pref in key:
                            print(f"\t{key}: {sample_dict[key]}")

        return ctx
