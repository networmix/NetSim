# pylint: disable=protected-access,invalid-name
from itertools import islice

from netsim import stat_base


SAMPLE_UNIFORM = [
    0.34,
    0.90,
    0.25,
    0.89,
    0.87,
    0.44,
    0.12,
    0.21,
    0.46,
    0.67,
    0.83,
    0.76,
    0.79,
    0.64,
    0.70,
    0.81,
    0.94,
    0.74,
    0.22,
    0.74,
    0.96,
    0.99,
    0.77,
    0.67,
    0.56,
    0.41,
    0.52,
    0.73,
    0.99,
    0.02,
    0.47,
    0.30,
    0.17,
    0.82,
    0.56,
    0.05,
    0.45,
    0.31,
    0.78,
    0.05,
    0.79,
    0.71,
    0.23,
    0.19,
    0.82,
    0.93,
    0.65,
    0.37,
    0.39,
    0.42,
    0.99,
    0.17,
    0.99,
    0.46,
    0.05,
    0.66,
    0.10,
    0.42,
    0.18,
    0.49,
    0.37,
    0.51,
    0.54,
    0.01,
    0.81,
    0.28,
    0.69,
    0.34,
    0.75,
    0.49,
    0.72,
    0.43,
    0.56,
    0.97,
    0.30,
    0.94,
    0.96,
    0.58,
    0.73,
    0.05,
    0.06,
    0.39,
    0.84,
    0.24,
    0.40,
    0.64,
    0.40,
    0.19,
    0.79,
    0.62,
    0.18,
    0.26,
    0.97,
    0.88,
    0.64,
    0.47,
    0.60,
    0.11,
    0.29,
    0.78,
]

SAMPLE_UNIFORM_BINS = [
    0.01,
    0.108,
    0.206,
    0.304,
    0.402,
    0.5,
    0.598,
    0.696,
    0.794,
    0.892,
    0.99,
]


def test_uniform_1():
    a = 10
    b = 20
    n = 100
    sample = list(islice(stat_base.uniform(a, b), 0, n))
    assert len(sample) == n


def test_df_1():
    assert stat_base.sample_df(SAMPLE_UNIFORM, r=0) == 99


def test_chi_sq_critical():
    assert stat_base.chi_square_critical(10, 0.05) == 18.307038053275146


def test_hist_1():
    hist, bin_edges = stat_base.histogram(SAMPLE_UNIFORM, 10)
    assert hist[0] == 8
    assert [round(v, 3) for v in bin_edges] == SAMPLE_UNIFORM_BINS


def test_chi_square_1():
    assert stat_base.chi_square(8, 10) == 0.4


def test_chi_square_sample_1():
    sample = SAMPLE_UNIFORM
    assert stat_base.sample_chi_square(sample, exp_distr="uniform") == 5.2


def test_chi_square_sample_2():
    sample = SAMPLE_UNIFORM
    assert stat_base.sample_chi_square(sample, exp_distr="normal") == 13040.443719539066


def test_chi_square_test_1():
    sample = SAMPLE_UNIFORM
    assert stat_base.sample_chi_square_test(sample, exp_distr="uniform", p=0.05)


def test_chi_square_test_2():
    sample = SAMPLE_UNIFORM
    assert not stat_base.sample_chi_square_test(sample, exp_distr="normal", p=0.05)
