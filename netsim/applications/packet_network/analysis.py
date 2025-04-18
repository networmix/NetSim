from __future__ import annotations
from io import TextIOWrapper
from json import loads
from typing import Any, Dict

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from netsim.applications.packet_network.common import NetSimObjectName
from netsim.stat_base import sample_stdev


class NetAnalyser:
    """
    Base class for network data analysis. Subclasses should implement specialized analysis logic.
    """

    ...


class NetSimIntAnalyser(NetAnalyser):
    """
    Analyser of NetSim interval statistics. It reads lines of JSON data, converts them to pandas DataFrame objects,
    and stores them keyed by the NetSim object name.
    """

    def __init__(self):
        self.data_frames: Dict[NetSimObjectName, pd.DataFrame] = {}

    @classmethod
    def init_with_nsim_stat(cls, fd: TextIOWrapper) -> NetSimIntAnalyser:
        """
        Initialize the analyser from a text file descriptor where each line is JSON data from the simulator.

        Args:
            fd: A file descriptor with JSON lines.

        Returns:
            A NetSimIntAnalyser instance with data_frames populated.
        """
        analyser = cls()

        for interval_idx, line in enumerate(fd):
            data: Dict[str, Dict[str, Any]] = loads(line)
            # Each line is expected to have a top-level key (e.g. an interval).
            # We take the first value of that top-level dict to get the object stats.
            if not data:
                continue
            entry = next(iter(data.values()), {})
            for obj_name, obj_stat in entry.items():
                # Convert the JSON dictionary into a single-row DataFrame
                row_df = pd.DataFrame(obj_stat, index=[interval_idx])

                if obj_name not in analyser.data_frames:
                    analyser.data_frames[obj_name] = pd.DataFrame(
                        columns=list(obj_stat.keys())
                    )

                # Concatenate instead of DataFrame.append (which is deprecated)
                analyser.data_frames[obj_name] = pd.concat(
                    [analyser.data_frames[obj_name], row_df], ignore_index=False
                )
        return analyser


class NetSimIntQueueAnalyser(NetSimIntAnalyser):
    """
    Performs queue-specific analyses of the collected NetSim interval statistics for a given queue-like object.
    """

    def analyse_queue(self, obj_name: str) -> None:
        """
        Analyse queue statistics (latency, queue length, throughput, etc.) for the specified object name.

        Args:
            obj_name: The name of the queue-like object in self.data_frames to be analysed.
        """
        if obj_name not in self.data_frames:
            raise KeyError(f"No data found for object {obj_name}")

        df = self.data_frames[obj_name]
        if "duration" not in df.columns:
            raise RuntimeError("Duration column is missing in the data.")

        # Basic checks
        if len(df) < 2:
            raise RuntimeError(
                "NetSimIntQueueAnalyser requires data from at least two intervals."
            )

        # We assume each interval has the same duration, though the code could be extended to handle otherwise
        int_duration = df["duration"].iloc[0]
        int_count = len(df)
        total_duration = int_duration * int_count

        sns.set_theme()
        sns.set_context("paper")
        fig = plt.figure()

        # 1) Mean system delay over time
        ax1 = fig.add_subplot(2, 2, 1)
        ax1.set_xlim([0, total_duration])
        ax1.set_title("Mean system delay over time")
        ax1.set_xlabel("Simulated Time (seconds)")
        ax1.set_ylabel("Mean system delay (seconds)")
        if "avg_latency_at_departure" not in df.columns:
            raise RuntimeError("Column avg_latency_at_departure is missing.")
        sns.scatterplot(x="timestamp", y="avg_latency_at_departure", data=df, ax=ax1)
        ax1.axhline(y=df["avg_latency_at_departure"].mean(), color="red")

        # 2) Mean system delay (histogram)
        ax2 = fig.add_subplot(2, 2, 2)
        ax2.set_title("Mean system delay (histogram)")
        ax2.set_xlabel("Mean system delay (seconds)")
        ax2.set_ylabel("")
        sns.histplot(x="avg_latency_at_departure", data=df, ax=ax2, stat="probability")

        # 3) Mean queue length over time
        ax3 = fig.add_subplot(2, 2, 3)
        ax3.set_xlim([0, total_duration])
        ax3.set_title("Mean queue length over time")
        ax3.set_xlabel("Simulated Time (seconds)")
        ax3.set_ylabel("Mean queue len")
        if "avg_queue_len" not in df.columns:
            raise RuntimeError("Column avg_queue_len is missing.")
        sns.scatterplot(x="timestamp", y="avg_queue_len", data=df, ax=ax3)
        ax3.axhline(y=df["avg_queue_len"].mean(), color="red")

        # 4) Mean queue length (histogram)
        ax4 = fig.add_subplot(2, 2, 4)
        ax4.set_title("Mean queue length (histogram)")
        ax4.set_xlabel("Mean queue len")
        ax4.set_ylabel("")
        sns.histplot(x="avg_queue_len", data=df, ax=ax4, stat="probability")

        fig.tight_layout()
        fig.savefig("NetSimIntQueueAnalyser.png", dpi=600)

        # Compute confidence intervals for average latency
        mean = df["avg_latency_at_departure"].mean()
        sigma = sample_stdev(df["avg_latency_at_departure"])
        ci = 1.96 * sigma / (len(df["avg_latency_at_departure"])) ** 0.5

        print("Avg latency")
        print(f"\tMean: {mean:.12f}")
        print(f"\tStdev: {sigma:.12f}")
        print(f"\tCI: {ci:.12f}")
        print(f"\tError %: {ci / mean * 100:.12f}" if mean != 0 else "\tMean is 0")

        # Compute confidence intervals for average queue length
        mean = df["avg_queue_len"].mean()
        sigma = sample_stdev(df["avg_queue_len"])
        ci = 1.96 * sigma / (len(df["avg_queue_len"])) ** 0.5

        print("Avg queue len")
        print(f"\tMean: {mean:.12f}")
        print(f"\tStdev: {sigma:.12f}")
        print(f"\tCI: {ci:.12f}")
        print(f"\tError %: {ci / mean * 100:.12f}" if mean != 0 else "\tMean is 0")

        # Compute confidence intervals for throughput (convert Bps to a fraction of 10Mbps for example)
        if "avg_send_rate_bps" in df.columns:
            df["throughput"] = df["avg_send_rate_bps"] / 1e7
            mean = df["throughput"].mean()
            sigma = sample_stdev(df["throughput"])
            ci = 1.96 * sigma / (len(df["throughput"])) ** 0.5

            print("Avg throughput")
            print(f"\tMean: {mean:.12f}")
            print(f"\tStdev: {sigma:.12f}")
            print(f"\tCI: {ci:.12f}")
            print(f"\tError %: {ci / mean * 100:.12f}" if mean != 0 else "\tMean is 0")
        else:
            print(
                "avg_send_rate_bps column is missing. Throughput analysis is skipped."
            )


ANALYSER_TYPE_MAP: Dict[str, NetAnalyser] = {
    "NetSimIntQueueAnalyser": NetSimIntQueueAnalyser
}
