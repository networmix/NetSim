from enum import IntEnum
import random
import statistics
import math
import functools
import itertools
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
SEED = 0
random.seed(SEED)


class DistrFunc(IntEnum):
    """Enum for supported distribution functions."""

    Constant = 0
    Normal = 1
    Uniform = 2
    Exponential = 3


def make_generator(func: Callable) -> Generator[Any, None, None]:
    """
    Decorator that turns a distribution function into an infinite generator.

    Args:
      func: A callable that, when invoked, returns a random sample (float).

    Returns:
      A generator that yields infinite samples from the given distribution.
    """

    @functools.wraps(func)
    def wrapper_decorator(
        *args: List[Any], **kwargs: Dict[str, Any]
    ) -> Generator[Any, None, None]:
        while True:
            yield func(*args, **kwargs)

    return wrapper_decorator


@make_generator
def uniform(a: float, b: float) -> float:
    """
    Uniform distribution in [a, b).

    Args:
      a: Lower bound
      b: Upper bound

    Returns:
      Single draw from Uniform(a, b).
    """
    return a + (b - a) * random.random()


@make_generator
def normal(mu: float, sigma: float) -> float:
    """
    Normal distribution generator using built-in Python normalvariate.

    Args:
      mu: Mean of distribution
      sigma: Standard deviation

    Returns:
      Single draw from Normal(mu, sigma).
    """
    return random.normalvariate(mu, sigma)


@make_generator
def exponential(lambd: float) -> float:
    """
    Exponential distribution generator.

    Args:
      lambd: Rate parameter (1 / mean)

    Returns:
      Single draw from Exponential(1 / lambd).
    """
    return random.expovariate(lambd)


class DistrBuilder:
    """
    Factory-style builder that creates a (potentially finite) generator for
    the requested distribution, based on the parameters.
    """

    @classmethod
    def create(
        cls, distr_func: DistrFunc, params: Dict[str, Union[int, float]]
    ) -> Generator[Any, None, None]:
        """
        Creates and returns a generator for the specified distribution function.

        Args:
          distr_func: Distribution enum (Constant, Normal, Uniform, Exponential)
          params: Dictionary of parameters required by the distribution. Common keys:
            - 'count': (Optional) finite number of samples to yield
            - distribution-specific parameters

        Returns:
          A generator that yields samples from the specified distribution.
        """
        if distr_func == DistrFunc.Constant:
            constant = params["constant"]
            first = params.get("first", constant)
            count = params.get("count")

            def gen():
                # Yield 'first' once if specified, then always the same constant
                if first is not None:
                    yield first
                while True:
                    yield constant

            return gen() if not count else itertools.islice(gen(), count)

        if distr_func == DistrFunc.Normal:
            mu = params["mu"]
            sigma = params["sigma"]
            count = params.get("count")
            gen = normal(mu, sigma)
            return gen if not count else itertools.islice(gen, count)

        if distr_func == DistrFunc.Uniform:
            a = params["a"]
            b = params["b"]
            count = params.get("count")
            gen = uniform(a, b)
            return gen if not count else itertools.islice(gen, count)

        if distr_func == DistrFunc.Exponential:
            lambd = params["lambda"]
            count = params.get("count")
            gen = exponential(lambd)
            return gen if not count else itertools.islice(gen, count)

        raise RuntimeError(f"Unknown distribution function: {distr_func}")


def sample_df(sample: Sample, r: int) -> int:
    """
    Computes a general-purpose degrees of freedom measure:
    df = n - 1 - r

    This is often used for sampling-based calculations (e.g., sample variance).
    However, this is not the correct df for a Chi-Square goodness-of-fit test
    (that depends on bin count and parameters, not the raw sample size).

    Args:
      sample: The sample data.
      r: Number of estimated parameters.

    Returns:
      Degrees of freedom (integer).
    """
    return len(sample) - 1 - r


def sample_mean(sample: Sample) -> float:
    """
    Computes the arithmetic mean of the sample.

    Args:
      sample: The sample data.

    Returns:
      Mean (float).
    """
    return statistics.fmean(sample)


def sample_stdev(sample: Sample) -> float:
    """
    Computes the sample standard deviation.

    Args:
      sample: The sample data.

    Returns:
      Standard deviation (float).
    """
    return statistics.stdev(sample)


def sample_variance(sample: Sample) -> float:
    """
    Computes the sample variance.

    Args:
      sample: The sample data.

    Returns:
      Variance (float).
    """
    return statistics.variance(sample)


def chi_square_critical(df: int, p: float = 0.05) -> float:
    """
    Finds the Chi-Square critical value for a given df and significance level p.

    Args:
      df: Degrees of freedom
      p: Significance level (default 0.05)

    Returns:
      Critical value from the chi-squared distribution.
    """
    return scipy.stats.chi2.ppf(1 - p, df)


def chi_square(observed: float, expected: float) -> float:
    """
    Computes the basic Chi-Square term for a single bin/category.

    (observed - expected)^2 / expected

    Args:
      observed: Observed count
      expected: Expected count

    Returns:
      Single chi-square component.
    """
    # Avoid dividing by extremely small expected values:
    if expected <= 1e-12:
        return 0.0 if abs(observed) < 1e-12 else float("inf")
    return (observed - expected) ** 2 / expected


def normal_cdf(x: float, mu: float, sigma: float) -> float:
    """
    Computes the CDF of a Normal distribution at x.

    Args:
      x: Point at which to evaluate
      mu: Mean
      sigma: Standard deviation

    Returns:
      The probability that a Normal(mu, sigma) random variable <= x.
    """
    return float(scipy.stats.norm.cdf(x, loc=mu, scale=sigma))


def histogram(
    sample: Sample, bins: int, normalize: bool = False
) -> Tuple[List[float], List[float]]:
    """
    Computes the histogram of a sample using numpy.histogram.

    Args:
      sample: The sample data
      bins: Number of bins
      normalize: If True, returns the normalized histogram (pdf-like)

    Returns:
      (hist, bin_edges) as lists (not numpy arrays).
    """
    hist, bin_edges = np.histogram(sample, bins=bins, density=normalize)
    return list(hist), list(bin_edges)


def sample_chi_square(
    sample: Sample,
    exp_distr: str,
    intervals_num: Optional[int] = None,
) -> float:
    """
    Computes the Chi-Square statistic comparing a sample's histogram to an
    expected distribution (currently "uniform" or "normal" supported).

    For "uniform", we assume the distribution is uniform over the sample's range,
    so expected = n / intervals for each bin.

    For "normal", we use the sample's mean and stdev to parameterize a normal,
    then the expected count in each bin is:
       (CDF(bin_right) - CDF(bin_left)) * sample_size

    Args:
      sample: The sample data
      exp_distr: "uniform" or "normal"
      intervals_num: Number of histogram bins. If None, it’s chosen based on sample size.

    Returns:
      The Chi-Square statistic (float).
    """
    sample_size = len(sample)
    if sample_size < 20:
        raise ValueError(
            f"Can't apply Chi-Square test: sample size {sample_size} is less than 20"
        )

    if intervals_num is None:
        if sample_size > 100:
            intervals_num = math.floor(sample_size**0.5)
        elif sample_size > 50:
            intervals_num = 10
        else:
            intervals_num = 5

    observed_nums, bin_edges = histogram(sample, bins=intervals_num)

    mu = sample_mean(sample)
    sigma = sample_stdev(sample)

    if exp_distr == "uniform":
        # each bin has the same expected count
        expected_nums = [sample_size / intervals_num for _ in range(intervals_num)]
    elif exp_distr == "normal":
        # difference of normal CDFs times sample_size
        pairs = zip(bin_edges, bin_edges[1:])
        expected_nums = [
            sample_size * (normal_cdf(right, mu, sigma) - normal_cdf(left, mu, sigma))
            for left, right in pairs
        ]
    else:
        raise ValueError(f"Unknown distribution {exp_distr}")

    return sum(chi_square(o, e) for o, e in zip(observed_nums, expected_nums))


def sample_chi_square_test(
    sample: Sample,
    exp_distr: str,
    p: float,
    intervals_num: Optional[int] = None,
) -> bool:
    """
    Performs a Chi-Square goodness-of-fit test for either 'uniform' or 'normal'.
    Returns True if the Chi-Square statistic < critical value => we do NOT reject
    the hypothesis at significance p.

    NOTE: For a standard chi-square gof test:
        df = (number_of_bins - 1 - number_of_parameters_estimated)

    Args:
      sample: The sample data
      exp_distr: "uniform" or "normal"
      p: significance level
      intervals_num: Number of bins (optional)

    Returns:
      Boolean indicating whether the distribution is accepted at level p.
    """
    if intervals_num is None:
        sample_size = len(sample)
        if sample_size > 100:
            intervals_num = math.floor(sample_size**0.5)
        elif sample_size > 50:
            intervals_num = 10
        else:
            intervals_num = 5

    # Number of parameters estimated from the data:
    if exp_distr == "uniform":
        # No parameters estimated from sample for uniform test
        r = 0
    elif exp_distr == "normal":
        # We estimate mean and stdev from sample
        r = 2
    else:
        raise ValueError(f"Unknown distribution {exp_distr}")

    # For Chi-Square GOF: df = k - 1 - r
    # k = intervals_num
    df = intervals_num - 1 - r
    if df < 1:
        # Can't run a proper test if df < 1
        raise ValueError(
            f"Invalid df for Chi-Square test (bins={intervals_num}, r={r})."
        )

    chi_sq_value = sample_chi_square(sample, exp_distr, intervals_num)
    critical = chi_square_critical(df, p)
    return chi_sq_value < critical
