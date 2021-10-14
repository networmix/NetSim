from netsim.netgraph.graph import MultiDiGraph

import tests.test_data as test_data
from netsim.netbuilder.wf_instructions import BuildGraph
from netsim.utils import load_resource, yaml_to_dict
from netsim.netbuilder.builder_instructions import (
    CreateGraph,
    CreateNodes,
    CreateAdjacencyMesh,
    ExecutionContext,
)
from netsim.netbuilder.dict_path import PathExpr, Path


def test_buildgraph_create_instr_1():
    blueprint_dict = yaml_to_dict(load_resource("2tierClos.yaml", test_data))
    BuildGraph._preprocess_bp_dict(blueprint_dict)
    section_list = ["graphs", "groups", "adjacency"]

    instr_list = BuildGraph._create_instr(blueprint_dict, section_list)
    print(instr_list)
    exp_instr_list = [
        CreateGraph(Path(("/", "2tierClos")), attr={"name": "2tierClos"}),
        CreateNodes(
            Path(("/", "2tierClos", "t1")),
            attr={
                "name_template": "{path[2]}-r{node_num}",
                "node_count": 4,
                "node_attr": {"role": "t1"},
            },
        ),
        CreateNodes(
            Path(("/", "2tierClos", "t2")),
            attr={"name_template": "{path[2]}-r{node_num}", "node_count": 4},
        ),
        CreateAdjacencyMesh(
            Path(("/", "2tierClos")),
            attr={
                "group_lhs": "t1",
                "group_rhs": "t2",
                "bidirectional": True,
                "edge_attr": {"capacity": 100000},
            },
        ),
    ]
    for inst, exp_inst in zip(instr_list, exp_instr_list):
        assert str(inst) == str(exp_inst)


def test_buildgraph_build_1():
    ctx = ExecutionContext()
    blueprint_dict = yaml_to_dict(load_resource("2tierClos.yaml", test_data))
    bg = BuildGraph(blueprint_dict)
    ctx_ret = bg.run(ctx)

    graphs = ctx_ret.get_graphs()
    assert len(graphs) == 1
    for graph_name in graphs:
        assert graphs[graph_name].get_edges() != {}


def test_buildgraph_build_2():
    ctx = ExecutionContext()
    blueprint_dict = yaml_to_dict(load_resource("Backbone.yaml", test_data))
    bg = BuildGraph(blueprint_dict)
    ctx_ret = bg.run(ctx)

    graphs = ctx_ret.get_graphs()
    assert len(graphs) == 1
    for graph_name in graphs:
        assert graphs[graph_name].get_edges() != {}


def test_buildgraph_build_3():
    ctx = ExecutionContext()
    blueprint_dict = yaml_to_dict(load_resource("3tierClos.yaml", test_data))
    bg = BuildGraph(blueprint_dict)
    ctx_ret = bg.run(ctx)

    graphs = ctx_ret.get_graphs()
    assert len(graphs) == 1
    for graph_name in graphs:
        assert graphs[graph_name].get_edges() != {}


def test_build_4():
    ctx = ExecutionContext()
    blueprint_dict = yaml_to_dict(load_resource("2tierClos_2graphs.yaml", test_data))
    bg = BuildGraph(blueprint_dict)
    ctx_ret = bg.run(ctx)

    graphs = ctx_ret.get_graphs()
    assert len(graphs) == 2
    for graph_name in graphs:
        assert graphs[graph_name].get_edges() != {}


def test_analyse_1():
    ctx = ExecutionContext()
    blueprint_dict = yaml_to_dict(load_resource("3tierClos_2graphs.yaml", test_data))
    bg = BuildGraph(blueprint_dict)
    ctx_ret = bg.run(ctx)

    graphs = ctx_ret.get_graphs()
    assert len(graphs) == 2
    for graph_name in graphs:
        assert graphs[graph_name].get_edges() != {}
