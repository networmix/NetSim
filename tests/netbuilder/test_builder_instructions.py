from netsim.netgraph.graph import MultiDiGraph
from netsim.netgraph.io import graph_to_node_link

from netsim.netbuilder.builder_instructions import (
    ExecutionContext,
    CreateGraph,
    CreateNodes,
    CreateAdjacencyMesh,
)
from netsim.netbuilder.dict_path import Path, ROOT


def test_CreateGraph1():
    instr = CreateGraph(Path(["/", "TestGraph"]), {"name": "2tierClos"})

    ctx = ExecutionContext()
    ret_ctx = instr.run(ctx)
    assert graph_to_node_link(
        ret_ctx.get_graphs()[Path(["/", "TestGraph"])]
    ) == graph_to_node_link(MultiDiGraph(name="2tierClos"))
    assert (
        repr(instr)
        == "CreateGraph(Path(('/', 'TestGraph')), attr={'name': '2tierClos'})"
    )


def test_CreateNodes1():
    name_template = "{path[2]}-{node_num}"

    group_path = Path(["/", "TestGraph", "t1"])
    instr = CreateNodes(
        group_path,
        {"name_template": name_template, "node_count": 16},
    )

    ctx = ExecutionContext()
    graph_path = Path(["/", "TestGraph"])
    ctx.add_graph(graph_path, MultiDiGraph(name="2tierClos"))

    ret_ctx = instr.run(ctx)
    graphs = ret_ctx.get_graphs_by_path(graph_path).values()

    for graph in graphs:
        assert len(graph) == 16
        for node_num in range(1, 17):
            assert (
                name_template.format(path=group_path.path, node_num=node_num) in graph
            )


def test_CreateAdjacencyMesh1():
    graph_path = Path(["/", "TestGraph"])
    instr = CreateAdjacencyMesh(
        path=graph_path,
        attr={
            "group_lhs": "t1",
            "group_rhs": "t2",
            "bidirectional": True,
            "edge_attr": {"capacity": 100000},
        },
    )

    graph = MultiDiGraph(name="2tierClos")
    graph.add_node("t1.01")
    graph.add_node("t1.02")
    graph.add_node("t2.01")
    graph.add_node("t2.02")

    ctx = ExecutionContext()
    ctx.add_graph(graph_path, graph)
    ctx.add_node_group(Path([ROOT, "TestGraph", "t1"]), set(["t1.01", "t1.02"]))
    ctx.add_node_group(Path([ROOT, "TestGraph", "t2"]), set(["t2.01", "t2.02"]))

    ret_ctx = instr.run(ctx)
    ret_graphs: MultiDiGraph = ret_ctx.get_graphs_by_path(graph_path).values()

    for ret_graph in ret_graphs:
        ret_edges = ret_graph.get_edges()
        print(ret_edges)
        for (k, v) in {
            1: ("t1.01", "t2.01", 1, {"capacity": 100000}),
            2: ("t2.01", "t1.01", 2, {"capacity": 100000}),
            3: ("t1.01", "t2.02", 3, {"capacity": 100000}),
            4: ("t2.02", "t1.01", 4, {"capacity": 100000}),
            5: ("t1.02", "t2.01", 5, {"capacity": 100000}),
            6: ("t2.01", "t1.02", 6, {"capacity": 100000}),
            7: ("t1.02", "t2.02", 7, {"capacity": 100000}),
            8: ("t2.02", "t1.02", 8, {"capacity": 100000}),
        }.items():
            assert ret_edges[k] == v
