import math


def _close(a: float, b: float, *, rel: float = 1e-9, abs_: float = 1e-12) -> None:
    assert math.isclose(a, b, rel_tol=rel, abs_tol=abs_), f"{a=} {b=}"
