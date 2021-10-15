# pylint: disable=protected-access,invalid-name
from netsim import dict_path as path


def test_path_1():
    assert path.Path(["a", "b", "c"]).path == ("a", "b", "c")
    assert path.Path(["/", "a", "b", "c"]).path == ("/", "a", "b", "c")


def test_path_2():
    assert repr(path.Path(["a", "b", "c"])) == "Path(('a', 'b', 'c'))"
    assert repr(path.Path(["/", "a", "b", "c"])) == "Path(('/', 'a', 'b', 'c'))"


def test_path_3():
    path_set = set()

    p1 = path.Path(["/", "a", "b", "c"])
    p2 = path.Path(["a", "b", "c"])
    p3 = path.Path(["a", "b", "c"])

    path_set.add(p1)
    path_set.add(p2)
    path_set.add(p3)

    assert len(path_set) == 2


def test_path_4():
    p1 = path.Path(["b", "c"])
    p2 = path.Path(["a", "b", "c"])

    assert p1 > p2


def test_path_expr_1():
    p1 = path.Path(["/", "a", "b", "c"])
    p2 = path.Path(["a", "b", "c"])

    p_expr1 = path.PathExpr("")
    p_expr2 = path.PathExpr("/a")
    p_expr3 = path.PathExpr("/a/b")
    p_expr4 = path.PathExpr("/a/b/c")
    p_expr5 = path.PathExpr("/a/c")
    p_expr6 = path.PathExpr("/a/b/c/d")

    assert not p_expr1.match(p1)
    assert p_expr2.match(p1)
    assert p_expr3.match(p1)
    assert p_expr4.match(p1)
    assert not p_expr5.match(p1)
    assert not p_expr6.match(p1)

    assert not p_expr1.match(p2)
    assert not p_expr2.match(p2)


def test_path_expr_2():
    p1 = path.Path(["/", "a", "b", "c"])
    p2 = path.Path(["a", "b", "c"])

    p_expr1 = path.PathExpr("")
    p_expr2 = path.PathExpr("a")
    p_expr3 = path.PathExpr("a/b")
    p_expr4 = path.PathExpr("a/b/c")
    p_expr5 = path.PathExpr("a/c")

    assert not p_expr1.match(p1)
    assert p_expr2.match(p1)
    assert p_expr3.match(p1)
    assert p_expr4.match(p1)
    assert not p_expr5.match(p1)

    assert not p_expr1.match(p2)
    assert p_expr2.match(p2)
    assert p_expr3.match(p2)
    assert p_expr4.match(p2)
    assert not p_expr5.match(p2)


def test_path_expr_3():
    p1 = path.Path(["/", "a", "b", "c"])
    p_expr1 = path.PathExpr(".*")
    p_expr2 = path.PathExpr("a/.*/c")
    p_expr3 = path.PathExpr("a/.*/b")
    p_expr4 = path.PathExpr("/a/b/.*")
    p_expr5 = path.PathExpr("[a]{1}/./c")

    assert p_expr1.match(p1)
    assert p_expr2.match(p1)
    assert not p_expr3.match(p1)
    assert p_expr4.match(p1)
    assert p_expr5.match(p1)


def test_path_expr_4():
    p1 = path.Path(["/", "a", "b", "c"])
    p2 = path.Path(["/", "a", "c", "c"])
    p3 = path.Path(["/", "a", "b", "b"])

    p_expr1 = path.PathExpr("/a")
    p_expr2 = path.PathExpr("a/b")
    p_expr3 = path.PathExpr("/a/b")

    assert p_expr1.filter([p1, p2, p3]) == [p1, p2, p3]
    assert p_expr2.filter([p1, p2, p3]) == [p1, p3]
    assert p_expr3.filter([p1, p2, p3]) == [p1, p3]


def test_traverse_tree_1():
    td = {
        "k1": {"k2": "v1"},
        "k3": "v2",
    }

    ret = path._traverse_dict(td)

    assert ret == [
        path.PathValuePair(path=path.Path(["k1", "k2"]), value="v1"),
        path.PathValuePair(path=path.Path(["k3"]), value="v2"),
    ]


def test_traverse_tree_2():
    td = {
        "k1": {"k2": "v1"},
        "k3": "v2",
        "k4": {"k5": "v3"},
        "k6": "v4",
    }

    ret = path._traverse_dict(td)

    assert ret == [
        path.PathValuePair(path=path.Path(["k1", "k2"]), value="v1"),
        path.PathValuePair(path=path.Path(["k3"]), value="v2"),
        path.PathValuePair(path=path.Path(["k4", "k5"]), value="v3"),
        path.PathValuePair(path=path.Path(["k6"]), value="v4"),
    ]


def test_traverse_tree_3():
    td = {
        "k1": {"k2": "v1"},
        "k3": "v2",
        "k4": {"k5": "v3", "k6": "v4", "k7": {"k8": ["v5", "v6", "v7"]}},
    }

    ret = path._traverse_dict(td)

    assert ret == [
        path.PathValuePair(path=path.Path(["k1", "k2"]), value="v1"),
        path.PathValuePair(path=path.Path(["k3"]), value="v2"),
        path.PathValuePair(path=path.Path(["k4", "k5"]), value="v3"),
        path.PathValuePair(path=path.Path(["k4", "k6"]), value="v4"),
        path.PathValuePair(
            path=path.Path(["k4", "k7", "k8"]), value=["v5", "v6", "v7"]
        ),
    ]


def test_traverse_tree_4():
    td = {
        "k1": {"k2": "v1"},
        "k3": "v2",
        "k4": {"k5": "v3", "k6": "v4", "t1": {"k8": ["v5", "v6", "v7"]}},
    }

    ret = path._traverse_dict(td, terminators=["t1"])

    assert ret == [
        path.PathValuePair(path=path.Path(["k1", "k2"]), value="v1"),
        path.PathValuePair(path=path.Path(["k3"]), value="v2"),
        path.PathValuePair(
            path=path.Path(["k4"]),
            value={"k5": "v3", "k6": "v4", "t1": {"k8": ["v5", "v6", "v7"]}},
        ),
    ]


def test_dict_to_paths_1():
    td = {
        "k1": {"k2": "v1"},
        "k3": "v2",
        "k4": {"k5": "v3"},
        "k6": "v4",
    }

    ret = path.dict_to_paths(td)

    assert ret == [
        path.PathValuePair(path=path.Path(["/", "k1", "k2"]), value="v1"),
        path.PathValuePair(path=path.Path(["/", "k3"]), value="v2"),
        path.PathValuePair(path=path.Path(["/", "k4", "k5"]), value="v3"),
        path.PathValuePair(path=path.Path(["/", "k6"]), value="v4"),
    ]


def test_check_scope_level_1():
    p1 = path.Path(["/", "a", "b", "c"])
    p2 = path.Path(["/", "a", "d"])

    assert path.check_scope_level(p1, p2)
    assert path.check_scope_level(p1, p2, scope_level=1)
    assert not path.check_scope_level(p1, p2, scope_level=2)
