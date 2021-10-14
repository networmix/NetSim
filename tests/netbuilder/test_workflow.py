import tests.test_data as test_data
from netsim.netbuilder.workflow import Workflow
from netsim.utils import load_resource, yaml_to_dict


def test_buildgraph_build_1():
    blueprint_dict = yaml_to_dict(load_resource("test_wf.yaml", test_data))
    wf = Workflow.from_dict(blueprint_dict)

    ctx_ret = wf.run()

    graphs = ctx_ret.get_graphs()
    assert len(graphs) == 1
    for graph_name in graphs:
        assert graphs[graph_name].get_edges() != {}


def test_buildgraph_build_2():
    blueprint_dict = yaml_to_dict(load_resource("test_wf_2.yaml", test_data))
    wf = Workflow.from_dict(blueprint_dict)

    ctx_ret = wf.run()

    graphs = ctx_ret.get_graphs()
    assert len(graphs) == 1
    for graph_name in graphs:
        assert graphs[graph_name].get_edges() != {}
