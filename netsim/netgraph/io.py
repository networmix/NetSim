from typing import Dict, Iterable, List, Optional

from .graph import MultiDiGraph


def graph_to_node_link(graph: MultiDiGraph) -> Dict:
    """
    Return a node-link representation that is suitable for direct JSON serialization.
    This format is supported by NetworkX and D3.js libraries.
    {"graph": {**attr}
     "nodes": [{"id": node_id, "attr": {**attr}}, ...]
     "links": [{"source": node_n, "target": node_n, "key": edge_id, "attr": {**attr}}, ...
    """
    node_map = {node_id: num for num, node_id in enumerate(graph.get_nodes())}

    return {
        "graph": {**graph.get_graph()},
        "nodes": [
            {"id": node_id, "attr": {**graph.get_nodes()[node_id]}}
            for node_id in node_map
        ],
        "links": [
            {
                "source": node_map[edge_tuple[0]],
                "target": node_map[edge_tuple[1]],
                "key": edge_tuple[2],
                "attr": {**edge_tuple[3]},
            }
            for edge_tuple in graph.get_edges().values()
        ],
    }


def node_link_to_graph(data: Dict) -> MultiDiGraph:
    """
    Take a node-link representation of a graph and return a MultiDiGraph
    """
    node_map = {}
    graph = MultiDiGraph(**data["graph"])

    for node_n, node in enumerate(data["nodes"]):
        graph.add_node(node["id"], **node["attr"])
        node_map[node_n] = node["id"]

    for edge in data["links"]:
        graph.add_edge(
            node_map[edge["source"]],
            node_map[edge["target"]],
            edge["key"],
            **edge["attr"]
        )
    return graph


def edgelist_to_graph(
    lines: Iterable[str],
    columns: List[str],
    separator: str = " ",
    graph: Optional[MultiDiGraph] = None,
    source: str = "src",
    target: str = "dst",
    key: str = "key",
) -> MultiDiGraph:
    """
    Take strings and return a MultiDiGraph
    """
    graph = MultiDiGraph() if graph is None else graph

    for line in lines:
        tokens = line.split(sep=separator)
        if len(tokens) != len(columns):
            raise RuntimeError("")

        line_dict = dict(zip(columns, tokens))
        attr_dict = {
            k: v for k, v in line_dict.items() if k not in [source, target, key]
        }
        graph.add_edge(
            src_node=line_dict[source],
            dst_node=line_dict[target],
            edge_id=line_dict.get(key, 0),
            **attr_dict
        )

    return graph
