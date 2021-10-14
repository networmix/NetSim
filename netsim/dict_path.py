"""
This module implements a simple navigation and selection logic for dictionaries.
The logic here assumes that keys in nested dictionaries form a tree
(or a set of disjoint trees) with leaf nodes containing final values.
In the practical sense, functions of this module can:
    1. Convert a Python dictionary into a flat list of entries. Each
    entry is a pair of "Path" and "value" (PathValuePair). Path is a
    "path to value" - ordered collection of keys leading to the value.
    2. Search and select paths that satisfy a given query expression.
"""
import re
from typing import Dict, List, Any, Iterable, Tuple
from collections import namedtuple
from copy import deepcopy

SEP = "/"
ROOT = "/"
PathValuePair = namedtuple("PathValuePair", ["path", "value"])


class Path:
    def __init__(self, path: Iterable[str]):
        self._path: Tuple[str] = tuple(path)
        self._abs = False
        if path and path[0] == ROOT:
            self._abs = True

    def __eq__(self, other):
        # In Python3 it is enough to override __eq__
        return self._path == other.path

    def __hash__(self):
        return hash(self._path)

    def __lt__(self, other):
        return self._path < other.path

    def __repr__(self):
        return f"{type(self).__name__}({self._path})"

    @property
    def path(self) -> Tuple[str]:
        return self._path

    @property
    def abs(self) -> bool:
        return self._abs


ROOTPATH = Path([ROOT])


class PathExpr:
    def __init__(self, path_expr: str):
        self._abs = False
        self._expr_list = []
        self._parse_path_expr(path_expr)

    def _parse_path_expr(self, path_expr: str):
        if path_expr and path_expr[0] == ROOT:
            self._abs = True
            path_expr = path_expr[1:]
            self._expr_list.append(ROOT)
        for expr in path_expr.split(SEP):
            self._expr_list.append(expr)

    def filter(self, paths: Iterable[Path]) -> List[Path]:
        ret = []
        for path in paths:
            if self.match(path):
                ret.append(path)
        return ret

    @staticmethod
    def _match(expr_elt: str, path_elt: str) -> bool:
        if re.search(r"\W", expr_elt):
            match = re.search(expr_elt, path_elt)
            if match:
                return True
            return False
        return expr_elt == path_elt

    def match(self, path: Path) -> bool:
        if self._abs and not path.abs:
            return False

        path_tuple = path.path
        path_elt_idx = 0
        for expr_elt_idx, expr_elt in enumerate(self._expr_list):
            if expr_elt_idx >= len(path_tuple):
                return False

            if expr_elt_idx == 0 and not self._abs:
                while path_elt_idx < len(path_tuple) and not self._match(
                    self._expr_list[0], path_tuple[path_elt_idx]
                ):
                    path_elt_idx += 1

            if path_elt_idx >= len(path_tuple):
                return False

            res = self._match(expr_elt, path_tuple[path_elt_idx])
            if not res:
                return False
            path_elt_idx += 1
        return True


def dict_to_paths(
    dict_to_traverse: Dict, terminators: Iterable[str] = None, add_root=True
) -> List[PathValuePair]:
    """
    Convert a Python dictionary into a flat list of entries.
    Each entry is a pair of "Path" and "value" (PathValuePair).
    """
    start_path = [ROOT] if add_root else []
    return _traverse_dict(dict_to_traverse, terminators, start_path)


def _traverse_dict(
    elt_to_traverse: Any,
    terminators: Iterable[str] = None,
    cur_path: List = None,
    path_list: List = None,
) -> List[PathValuePair]:
    if cur_path is None:
        cur_path = []

    if path_list is None:
        path_list = []

    if not isinstance(elt_to_traverse, dict) or (
        (terminators is not None)
        and any((terminator in elt_to_traverse for terminator in terminators))
    ):
        path_list.append(PathValuePair(Path(cur_path), elt_to_traverse))
        return path_list

    for key, _elt_to_traverse in elt_to_traverse.items():
        _cur_path = list(cur_path)
        _cur_path.append(key)
        _traverse_dict(_elt_to_traverse, terminators, _cur_path, path_list)
    return path_list


def check_scope_level(path_lhs: Path, path_rhs: Path, scope_level: int = 0) -> bool:
    for scope_lvl in range(
        min(scope_level + 1, len(path_lhs.path), len(path_rhs.path))
    ):
        if path_lhs.path[scope_lvl] != path_rhs.path[scope_lvl]:
            return False
    return True


def process_dict(
    dict_to_process: Dict,
) -> Dict:
    def expand_keys(dict_to_process: Dict):
        if not isinstance(dict_to_process, dict):
            return

        old_keys = list(dict_to_process.keys())
        for old_key in old_keys:
            expand_keys(dict_to_process[old_key])
            if match := re.search(r"\[(?P<range_to_expand>.*)\]", old_key):
                if match := re.search(
                    r"(?P<range_lhs>\d+)-(?P<range_rhs>\d+)",
                    match.group("range_to_expand"),
                ):
                    for new_key in range(
                        int(match.group("range_lhs")), int(match.group("range_rhs")) + 1
                    ):
                        dict_to_process[new_key] = deepcopy(dict_to_process[old_key])
                    del dict_to_process[old_key]

    return expand_keys(dict_to_process)
