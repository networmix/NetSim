from typing import List, Union

import matplotlib.pyplot as plt


def display_hist(
    samples: List[Union[int, float]],
    title: str = "",
    x_label: str = "",
    y_label: str = "",
    bins: int = 100,
    density: bool = False,
) -> None:
    _, axis = plt.subplots()
    axis.hist(samples, bins=bins, density=density)
    axis.set_title(title)
    axis.set_xlabel(x_label)
    axis.set_ylabel(y_label)
