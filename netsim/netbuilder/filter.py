import re
from typing import Dict, Union


def _transform(
    val: Union[str, int, bool], transform_name: str
) -> Union[str, int, bool]:
    if transform_name == "int":
        return int(val)

    if transform_name == "mod2":
        return int(val % 2)


def bool_filter(node_lhs: str, node_rhs: str, filter_dict: Dict[str, str]) -> bool:
    lhs_regex = filter_dict["lhs_regex"]
    rhs_regex = filter_dict["rhs_regex"]
    lhs_transform = filter_dict.get("lhs_transform", None)
    rhs_transform = filter_dict.get("rhs_transform", None)
    cond = filter_dict.get("condition", None)

    lhs_match = re.match(lhs_regex, node_lhs)
    rhs_match = re.match(rhs_regex, node_rhs)

    if lhs_match and rhs_match:
        if cond is None:
            return True

        if cond:
            lhs_match_groups = lhs_match.groups()
            rhs_match_groups = rhs_match.groups()
            if lhs_match_groups and rhs_match_groups:
                for lhs_gr, rhs_gr in zip(lhs_match_groups, rhs_match_groups):
                    if lhs_transform:
                        lhs_gr = _transform(lhs_gr, lhs_transform)
                    if rhs_transform:
                        rhs_gr = _transform(rhs_gr, rhs_transform)
                    if cond == "eq" and lhs_gr != rhs_gr:
                        return False
                return True
    return False
