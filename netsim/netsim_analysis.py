from __future__ import annotations
from io import TextIOWrapper
from json import loads
from typing import Any, Dict

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from netsim.netsim_common import NetSimObjectName
from netsim.stat_base import sample_stdev


class NetAnalyser:
    ...


class NetSimIntAnalyser(NetAnalyser):
    """
    Analyser of NetSim interval statistics
    """

    def __init__(self):
        self.data_frames: Dict[NetSimObjectName, pd.DataFrame] = {}

    @classmethod
    def init_with_nsim_stat(cls, fd: TextIOWrapper) -> NetSimIntAnalyser:
        analyser = cls()

        for interval_idx, line in enumerate(fd):
            data: Dict[str, Dict[str, Any]] = loads(line)
            entry = next(iter(data.values()))
            for obj_name, obj_stat in entry.items():
                df = pd.DataFrame(obj_stat, index=[interval_idx])
                analyser.data_frames.setdefault(
                    obj_name, pd.DataFrame(columns=list(obj_stat.keys()))
                )
                analyser.data_frames[obj_name] = analyser.data_frames[obj_name].append(
                    df
                )
        return analyser


class NetSimIntQueueAnalyser(NetSimIntAnalyser):
    def analyse_queue(self, obj_name: str) -> None:
        df = self.data_frames[obj_name]
        print(df.info())
        int_duration = df.duration[0]
        int_count = len(df)
        total_duration = int_duration * int_count
        sns.set_theme()
        sns.set_context("paper")
        fig = plt.figure()
        ax1 = fig.add_subplot(2, 2, 1)
        ax1.set_xlim([0, total_duration])
        ax1.set_title("Mean system delay over time")
        ax1.set_xlabel("Simulated Time (seconds)")
        ax1.set_ylabel("Mean system delay (seconds)")
        sns.scatterplot(x="timestamp", y="avg_latency_at_departure", data=df, ax=ax1)
        ax1.axhline(y=df.avg_latency_at_departure.mean(), color="red")

        ax2 = fig.add_subplot(2, 2, 2)
        ax2.set_title("Mean system delay (histogram)")
        ax2.set_xlabel("Mean system delay (seconds)")
        ax2.set_ylabel("")
        sns.histplot(x="avg_latency_at_departure", data=df, ax=ax2, stat="probability")

        ax3 = fig.add_subplot(2, 2, 3)
        ax3.set_xlim([0, total_duration])
        ax3.set_title("Mean queue length over time")
        ax3.set_xlabel("Simulated Time (seconds)")
        ax3.set_ylabel("Mean queue len")
        sns.scatterplot(x="timestamp", y="avg_queue_len", data=df, ax=ax3)
        ax3.axhline(y=df.avg_queue_len.mean(), color="red")

        ax4 = fig.add_subplot(2, 2, 4)
        ax4.set_title("Mean queue length (histogram)")
        ax4.set_xlabel("Mean queue len")
        ax4.set_ylabel("")
        sns.histplot(x="avg_queue_len", data=df, ax=ax4, stat="probability")

        fig.tight_layout()
        fig.savefig("NetSimIntQueueAnalyser.png", dpi=600)

        mean = df.avg_latency_at_departure.mean()
        sigma = sample_stdev(df.avg_latency_at_departure)
        ci = 1.96 * sigma / (len(df.avg_latency_at_departure)) ** 0.5

        print("Avg latency")
        print(f"\tMean: {mean:.12f}")
        print(f"\tStdev: {sigma:.12f}")
        print(f"\tCI: {ci:.12f}")
        print(f"\tError %: {ci/mean*100:.12f}")

        mean = df.avg_queue_len.mean()
        sigma = sample_stdev(df.avg_queue_len)
        ci = 1.96 * sigma / (len(df.avg_queue_len)) ** 0.5

        print("Avg queue len")
        print(f"\tMean: {mean:.12f}")
        print(f"\tStdev: {sigma:.12f}")
        print(f"\tCI: {ci:.12f}")
        print(f"\tError %: {ci/mean*100:.12f}")

        df["throughput"] = df.avg_send_rate_bps / 10000000
        mean = df.throughput.mean()
        sigma = sample_stdev(df.throughput)
        ci = 1.96 * sigma / (len(df.throughput)) ** 0.5

        print("Avg throughput")
        print(f"\tMean: {mean:.12f}")
        print(f"\tStdev: {sigma:.12f}")
        print(f"\tCI: {ci:.12f}")
        print(f"\tError %: {ci/mean*100:.12f}")


ANALYSER_TYPE_MAP: Dict[str, NetAnalyser] = {
    "NetSimIntQueueAnalyser": NetSimIntQueueAnalyser
}
