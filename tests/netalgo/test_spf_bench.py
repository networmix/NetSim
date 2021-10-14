from netsim.netgraph.graph import MultiDiGraph

from netsim.netalgo.spf import spf


def test_spf_bench_1(benchmark):
    g = MultiDiGraph()
    for node_num in range(100):
        node_id = str(node_num)
        g.add_node(node_id)
        for _node_id in g.get_nodes():
            if _node_id != node_id:
                for _metric in range(100, 0, -1):
                    g.add_edge(_node_id, node_id, metric=_metric)
                    g.add_edge(node_id, _node_id, metric=_metric)

    benchmark(spf, g, "0")
