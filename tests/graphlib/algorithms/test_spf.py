# pylint: disable=protected-access,invalid-name
from netsim.graphlib.graph import MultiDiGraph
from netsim.graphlib.algorithms.spf import spf


def test_spf_1():
    g = MultiDiGraph()
    g.add_edge("A", "B", metric=10)
    g.add_edge("B", "C", metric=10)
    g.add_edge("C", "D", metric=10)

    costs, prev = spf(g, "A")
    assert costs == {"A": 0, "B": 10, "C": 20, "D": 30}
    assert prev == {"A": {}, "B": {"A": [1]}, "C": {"B": [2]}, "D": {"C": [3]}}


def test_spf_2():
    g = MultiDiGraph()
    g.add_edge("A", "B", metric=10)
    g.add_edge("A", "C", metric=10)
    g.add_edge("A", "D", metric=10)
    g.add_edge("B", "D", metric=10)
    g.add_edge("C", "D", metric=10)

    costs, prev = spf(g, "A")

    assert costs == {"A": 0, "B": 10, "C": 10, "D": 10}
    assert prev == {"A": {}, "B": {"A": [1]}, "C": {"A": [2]}, "D": {"A": [3]}}


def test_spf_3():
    g = MultiDiGraph()
    g.add_edge("A", "B", metric=11)
    g.add_edge("B", "A", metric=11)
    g.add_edge("A", "B", metric=10)
    g.add_edge("B", "A", metric=10)

    g.add_edge("A", "C", metric=10)
    g.add_edge("C", "A", metric=10)
    g.add_edge("A", "C", metric=11)
    g.add_edge("C", "A", metric=11)

    g.add_edge("A", "D", metric=10)
    g.add_edge("D", "A", metric=10)

    g.add_edge("B", "D", metric=10)
    g.add_edge("D", "B", metric=10)

    g.add_edge("C", "D", metric=10)
    g.add_edge("D", "C", metric=10)

    costs, prev = spf(g, "A")

    assert costs == {"A": 0, "B": 10, "C": 10, "D": 10}
    assert prev == {"A": {}, "B": {"A": [3]}, "C": {"A": [5]}, "D": {"A": [9]}}


def test_spf_4():
    g = MultiDiGraph()
    g.add_edge("A", "B", metric=11)
    g.add_edge("B", "A", metric=11)
    g.add_edge("A", "B", metric=10)
    g.add_edge("B", "A", metric=10)

    g.add_edge("A", "C", metric=10)
    g.add_edge("C", "A", metric=10)
    g.add_edge("A", "C", metric=10)
    g.add_edge("C", "A", metric=10)

    g.add_edge("A", "D", metric=20)
    g.add_edge("D", "A", metric=20)

    g.add_edge("B", "D", metric=10)
    g.add_edge("D", "B", metric=10)

    g.add_edge("C", "D", metric=10)
    g.add_edge("D", "C", metric=10)

    costs, prev = spf(g, "A")

    assert costs == {"A": 0, "B": 10, "C": 10, "D": 20}
    assert prev == {
        "A": {},
        "B": {"A": [3]},
        "C": {"A": [5, 7]},
        "D": {"A": [9], "B": [11], "C": [13]},
    }
