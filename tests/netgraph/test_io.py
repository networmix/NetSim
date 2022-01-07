from netsim.netgraph.io import graph_to_node_link, node_link_to_graph, edgelist_to_graph
from netsim.netgraph.graph import MultiDiGraph


def test_graph_to_node_link_1():
    g = MultiDiGraph(test_attr="TEST_graph")
    g.add_node("A", test_attr="TEST_node1")
    g.add_node("B", test_attr="TEST_node2")
    g.add_node("C", test_attr="TEST_node3")
    g.add_edge("A", "B", test_attr="TEST_edge1a")
    g.add_edge("B", "A", test_attr="TEST_edge1a")
    g.add_edge("A", "B", test_attr="TEST_edge1b")
    g.add_edge("B", "A", test_attr="TEST_edge1b")
    g.add_edge("B", "C", test_attr="TEST_edge2")
    g.add_edge("C", "B", test_attr="TEST_edge2")
    g.add_edge("C", "A", test_attr="TEST_edge3")
    g.add_edge("A", "C", test_attr="TEST_edge3")

    exp_ret = {
        "graph": {"test_attr": "TEST_graph"},
        "nodes": [
            {"id": "A", "attr": {"test_attr": "TEST_node1"}},
            {"id": "B", "attr": {"test_attr": "TEST_node2"}},
            {"id": "C", "attr": {"test_attr": "TEST_node3"}},
        ],
        "links": [
            {
                "source": 0,
                "target": 1,
                "key": 1,
                "attr": {"test_attr": "TEST_edge1a"},
            },
            {
                "source": 1,
                "target": 0,
                "key": 2,
                "attr": {"test_attr": "TEST_edge1a"},
            },
            {
                "source": 0,
                "target": 1,
                "key": 3,
                "attr": {"test_attr": "TEST_edge1b"},
            },
            {
                "source": 1,
                "target": 0,
                "key": 4,
                "attr": {"test_attr": "TEST_edge1b"},
            },
            {
                "source": 1,
                "target": 2,
                "key": 5,
                "attr": {"test_attr": "TEST_edge2"},
            },
            {
                "source": 2,
                "target": 1,
                "key": 6,
                "attr": {"test_attr": "TEST_edge2"},
            },
            {
                "source": 2,
                "target": 0,
                "key": 7,
                "attr": {"test_attr": "TEST_edge3"},
            },
            {
                "source": 0,
                "target": 2,
                "key": 8,
                "attr": {"test_attr": "TEST_edge3"},
            },
        ],
    }
    assert exp_ret == graph_to_node_link(g)


def test_node_link_to_graph_1():
    data = {
        "graph": {"test_attr": "TEST_graph"},
        "nodes": [
            {"id": "A", "attr": {"test_attr": "TEST_node1"}},
            {"id": "B", "attr": {"test_attr": "TEST_node2"}},
            {"id": "C", "attr": {"test_attr": "TEST_node3"}},
        ],
        "links": [
            {
                "source": 0,
                "target": 1,
                "key": 1,
                "attr": {"test_attr": "TEST_edge1a"},
            },
            {
                "source": 1,
                "target": 0,
                "key": 2,
                "attr": {"test_attr": "TEST_edge1a"},
            },
            {
                "source": 0,
                "target": 1,
                "key": 3,
                "attr": {"test_attr": "TEST_edge1b"},
            },
            {
                "source": 1,
                "target": 0,
                "key": 4,
                "attr": {"test_attr": "TEST_edge1b"},
            },
            {
                "source": 1,
                "target": 2,
                "key": 5,
                "attr": {"test_attr": "TEST_edge2"},
            },
            {
                "source": 2,
                "target": 1,
                "key": 6,
                "attr": {"test_attr": "TEST_edge2"},
            },
            {
                "source": 2,
                "target": 0,
                "key": 7,
                "attr": {"test_attr": "TEST_edge3"},
            },
            {
                "source": 0,
                "target": 2,
                "key": 8,
                "attr": {"test_attr": "TEST_edge3"},
            },
        ],
    }

    g = node_link_to_graph(data)
    assert "A" in g
    assert "B" in g
    assert "C" in g

    assert g._edges[1] == ("A", "B", 1, {"test_attr": "TEST_edge1a"})
    assert g._edges[2] == ("B", "A", 2, {"test_attr": "TEST_edge1a"})
    assert g._edges[3] == ("A", "B", 3, {"test_attr": "TEST_edge1b"})
    assert g._edges[4] == ("B", "A", 4, {"test_attr": "TEST_edge1b"})
    assert g._edges[5] == ("B", "C", 5, {"test_attr": "TEST_edge2"})
    assert g._edges[6] == ("C", "B", 6, {"test_attr": "TEST_edge2"})
    assert g._edges[7] == ("C", "A", 7, {"test_attr": "TEST_edge3"})
    assert g._edges[8] == ("A", "C", 8, {"test_attr": "TEST_edge3"})


def test_node_link_1():
    data = {
        "graph": {"test_attr": "TEST_graph"},
        "nodes": [
            {"id": "A", "attr": {"test_attr": "TEST_node1"}},
            {"id": "B", "attr": {"test_attr": "TEST_node2"}},
            {"id": "C", "attr": {"test_attr": "TEST_node3"}},
        ],
        "links": [
            {
                "source": 0,
                "target": 1,
                "key": 1,
                "attr": {"test_attr": "TEST_edge1a"},
            },
            {
                "source": 1,
                "target": 0,
                "key": 2,
                "attr": {"test_attr": "TEST_edge1a"},
            },
            {
                "source": 0,
                "target": 1,
                "key": 3,
                "attr": {"test_attr": "TEST_edge1b"},
            },
            {
                "source": 1,
                "target": 0,
                "key": 4,
                "attr": {"test_attr": "TEST_edge1b"},
            },
            {
                "source": 1,
                "target": 2,
                "key": 5,
                "attr": {"test_attr": "TEST_edge2"},
            },
            {
                "source": 2,
                "target": 1,
                "key": 6,
                "attr": {"test_attr": "TEST_edge2"},
            },
            {
                "source": 2,
                "target": 0,
                "key": 7,
                "attr": {"test_attr": "TEST_edge3"},
            },
            {
                "source": 0,
                "target": 2,
                "key": 8,
                "attr": {"test_attr": "TEST_edge3"},
            },
        ],
    }
    assert graph_to_node_link(node_link_to_graph(data)) == data


def test_edgelist_to_graph_1():
    columns = ["src", "dst", "test_attr"]
    lines = [
        "A B TEST_edge1a",
        "B A TEST_edge1a",
        "A B TEST_edge1b",
        "B A TEST_edge1b",
        "B C TEST_edge2",
        "C B TEST_edge2",
        "C A TEST_edge3",
        "A C TEST_edge3",
    ]

    g = edgelist_to_graph(lines, columns)

    assert "A" in g
    assert "B" in g
    assert "C" in g

    assert g._edges[1] == ("A", "B", 1, {"test_attr": "TEST_edge1a"})
    assert g._edges[2] == ("B", "A", 2, {"test_attr": "TEST_edge1a"})
    assert g._edges[3] == ("A", "B", 3, {"test_attr": "TEST_edge1b"})
    assert g._edges[4] == ("B", "A", 4, {"test_attr": "TEST_edge1b"})
    assert g._edges[5] == ("B", "C", 5, {"test_attr": "TEST_edge2"})
    assert g._edges[6] == ("C", "B", 6, {"test_attr": "TEST_edge2"})
    assert g._edges[7] == ("C", "A", 7, {"test_attr": "TEST_edge3"})
    assert g._edges[8] == ("A", "C", 8, {"test_attr": "TEST_edge3"})
