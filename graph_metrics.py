#!/usr/bin/env python3
"""
graph_metrics.py
────────────────
Reads merged_metrics.csv and plots CPU % over time, one line per node.

Usage:
    python graph_metrics.py
    python graph_metrics.py --input my_metrics.csv
    python graph_metrics.py --out chart.png
    python graph_metrics.py --since 1700000000 --until 1700003600
    python graph_metrics.py --smooth 10
"""

import argparse
import csv
import sys
from collections import defaultdict
from datetime import datetime

try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
except ImportError:
    sys.exit("pip install matplotlib")


def load(filepath, since=None, until=None):
    nodes = defaultdict(lambda: {"times": [], "cpu": []})
    with open(filepath, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts = float(row["timestamp"])
            if since and ts < since:
                continue
            if until and ts > until:
                continue
            node = row["node"]
            nodes[node]["times"].append(datetime.fromtimestamp(ts))
            nodes[node]["cpu"].append(float(row["cpu_pct"]))
    return nodes


def rolling_avg(values, window):
    result = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        result.append(sum(values[start:i+1]) / (i - start + 1))
    return result


def plot(nodes, smooth, out):
    node_names = sorted(nodes.keys())

    fig, ax = plt.subplots(figsize=(14, 5))
    fig.patch.set_facecolor("#0e1117")
    ax.set_facecolor("#161b22")
    ax.tick_params(colors="#8b949e", labelsize=9)
    ax.spines[:].set_color("#30363d")
    for spine in ax.spines.values():
        spine.set_linewidth(0.5)
    ax.yaxis.label.set_color("#8b949e")
    ax.grid(axis="y", color="#21262d", linewidth=0.6, linestyle="--")
    ax.grid(axis="x", color="#21262d", linewidth=0.4, linestyle=":")

    palette = [
        "#58a6ff", "#3fb950", "#f78166", "#d2a8ff",
        "#ffa657", "#79c0ff", "#56d364", "#ff7b72",
    ]

    for i, name in enumerate(node_names):
        color = palette[i % len(palette)]
        times = nodes[name]["times"]
        cpu   = nodes[name]["cpu"]
        if smooth > 1:
            cpu = rolling_avg(cpu, smooth)
        ax.plot(times, cpu, color=color, linewidth=1.2, label=name, alpha=0.9)

    ax.set_ylabel("CPU %", fontsize=10)
    ax.set_ylim(0, 100)
    ax.legend(
        loc="upper left", framealpha=0.15, fontsize=8,
        labelcolor="white", edgecolor="#30363d"
    )
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right")
    ax.tick_params(axis="x", colors="#8b949e", labelsize=8)

    smooth_label = f"  (rolling avg {smooth}s)" if smooth > 1 else ""
    fig.suptitle(
        f"Node CPU %{smooth_label}",
        color="#e6edf3", fontsize=13, fontweight="bold", y=1.01
    )

    plt.tight_layout()

    if out:
        plt.savefig(out, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
        print(f"Saved to {out}")
    else:
        plt.show()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",  default="merged_metrics.csv")
    ap.add_argument("--out",    default=None,  help="Save to file instead of showing")
    ap.add_argument("--since",  type=float,    default=None, help="Start of time range (unix timestamp)")
    ap.add_argument("--until",  type=float,    default=None, help="End of time range (unix timestamp)")
    ap.add_argument("--smooth", type=int,      default=1,    help="Rolling average window in samples")
    args = ap.parse_args()

    print(f"Reading {args.input} ...")
    nodes = load(args.input, args.since, args.until)

    if not nodes:
        sys.exit("No data found.")

    total = sum(len(v["times"]) for v in nodes.values())
    print(f"Loaded {total} samples across {len(nodes)} node(s): {sorted(nodes.keys())}")

    plot(nodes, args.smooth, args.out)


if __name__ == "__main__":
    main()