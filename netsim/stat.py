# pylint: disable=invalid-name
import random
import statistics
import math
import functools
from typing import (
    Callable,
    Generator,
    Iterable,
    List,
    Dict,
    Any,
    Tuple,
    Union,
    Optional,
)

import scipy.stats
import numpy as np


Sample = Iterable[Union[int, float]]


def make_generator(func: Callable) -> Generator[Any, None, None]:
    @functools.wraps(func)
    def wrapper_decorator(
        *args: List[Any], **kwargs: Dict[str, Any]
    ) -> Generator[Any, None, None]:
        while True:
            yield func(*args, **kwargs)

    return wrapper_decorator


@make_generator
def uniform(a: float, b: float) -> float:
    return a + (b - a) * random.random()


@make_generator
def normal(mu: float, sigma: float) -> float:
    return random.normalvariate(mu, sigma)


@make_generator
def exponential(lambd: float) -> float:
    return random.expovariate(lambd)


def sample_df(sample: Sample, r: int) -> int:
    """
    Degrees of freedom is the number of values that are free to vary in a data set.
    It is a mathematical restriction that needs to be put in place when estimating one statistic
    from an estimate of another. In other words, it is the number of ways or dimensions an independent
    value can move without violating constraints.

    To calculate degrees of freedom, subtract the number of "relations" from the number of observations.

    df - degrees of freedom is n - 1 - r where
      n is the number of observations and
      r is the number of "relations" or the number of parameters estimated for the distribution
    """
    return len(sample) - 1 - r


def sample_mean(sample: Sample) -> float:
    """
    Mean or arithmetic average:
    mu = sum(samples)/n
    can also be denote as x_bar
    """
    return statistics.fmean(sample)


def sample_stdev(sample: Sample) -> float:
    """
    Standard deviation of a sample:
    sigma = sqrt(sum(ai - mu)^2/(n - 1))
    Note: denominator is (n - 1) because we don't have a complete population.
    Dividing by n would underestimate the variability.
    Standard deviation for a complete population is called "sigma"
    """
    return statistics.stdev(sample)


def chi_square_critical(df: int, p: float = 0.05) -> float:
    """
    find Chi-Square critical value
    Degrees of freedom of an estimate is the number of independent pieces of information
    that went into calculating the estimate.

    df - degrees of freedom
    """
    return scipy.stats.chi2.ppf(1 - p, df)


def chi_square(observed: Union[int, float], expected: Union[int, float]):
    return (observed - expected) ** 2 / expected


def normal_cdf(x: float, mu: float, sigma: float) -> Tuple[List[float], List[float]]:
    """
    Cumulative Distribution Function (CDF) of a Normal distribution
    """
    return scipy.stats.norm.cdf(x, loc=mu, scale=sigma)


def histogram(
    sample: Sample, bins: int, normalize: bool = False
) -> Tuple[List[float], List[float]]:
    """
    Compute the histogram of a sample
    """
    hist, bin_edges = np.histogram(sample, bins=bins, density=normalize)
    return list(hist), list(bin_edges)


def sample_chi_square(
    sample: Sample,
    exp_distr: str,
    intervals_num: Optional[int] = None,
) -> float:

    sample_size = len(sample)
    # applicability test
    if sample_size < 20:
        raise AttributeError(
            f"Can't apply Chi-Square test: the sample size {sample_size} is less than 20"
        )

    # calculate the number of intervals if not given
    if intervals_num is None:
        if sample_size > 100:
            intervals_num = math.floor(sample_size ** 0.5)
        elif sample_size > 50:
            intervals_num = 10
        else:
            intervals_num = 5

    observed_nums, bin_edges = histogram(sample, bins=intervals_num)

    mu = sample_mean(sample)
    sigma = sample_stdev(sample)
    if exp_distr == "uniform":
        expected_nums = [len(sample) / intervals_num for _ in range(intervals_num)]
        print(expected_nums)
    elif exp_distr == "normal":
        pairs = zip(bin_edges, bin_edges[1:])
        expected_nums = [
            normal_cdf(right, mu, sigma) - normal_cdf(left, mu, sigma)
            for left, right in pairs
        ]
    else:
        raise AttributeError(f"Unknown distribution {exp_distr}")

    for o, e in zip(observed_nums, expected_nums):
        print(o, e, chi_square(o, e))
    return sum([chi_square(o, e) for o, e in zip(observed_nums, expected_nums)])


def sample_chi_square_test(
    sample: Sample,
    exp_distr: str,
    p: float,
    intervals_num: Optional[int] = None,
) -> bool:
    if exp_distr == "uniform":
        r = 0
    if exp_distr == "normal":
        r = 2
    df = sample_df(sample, r)
    return sample_chi_square(sample, exp_distr, intervals_num) < chi_square_critical(
        df, p
    )
