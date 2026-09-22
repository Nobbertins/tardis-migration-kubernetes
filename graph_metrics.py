#!/usr/bin/env python3
"""
graph_metrics.py
────────────────
Reads metrics.csv (from scrape_metrics.py) and plots CPU % over time, one
subplot per node. Each subplot is a stacked area chart showing which pods on
that node are responsible for how much of the CPU usage, with a dashed line
overlaid for the node's total cpu_pct as a sanity check against the stack.

Usage:
    python graph_metrics.py
    python graph_metrics.py --input my_metrics.csv
    python graph_metrics.py --out chart.png
    python graph_metrics.py --since 1700000000 --until 1700003600
    python graph_metrics.py --smooth 10
    python graph_metrics.py --top-pods 8
"""

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime

try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
except ImportError:
    sys.exit("pip install matplotlib")

OTHER_LABEL = "other"
OTHER_COLOR = "#484f58"


def load(filepath, since=None, until=None):
    """
    Returns {node: {"times": [...], "cpu": [...], "pods": [dict, dict, ...]}}
    "pods" is one dict per timestamp (parsed from the pods JSON column),
    aligned index-for-index with "times" and "cpu".
    """
    nodes = defaultdict(lambda: {"times": [], "cpu": [], "pods": []})
    with open(filepath, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts = float(row["timestamp"])
            if since and ts < since:
                continue
            if until and ts > until:
                continue
            node = row["node"]

            raw_pods = row.get("pods", "") or "{}"
            try:
                pods = json.loads(raw_pods)
            except (json.JSONDecodeError, TypeError):
                pods = {}

            nodes[node]["times"].append(datetime.fromtimestamp(ts))
            nodes[node]["cpu"].append(float(row["cpu_pct"]))
            nodes[node]["pods"].append(pods)
    return nodes


def rolling_avg(values, window):
    result = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        result.append(sum(values[start:i+1]) / (i - start + 1))
    return result


def build_pod_series(pods_over_time, top_n, smooth):
    """
    pods_over_time: list of {pod_name: pct} dicts, one per timestamp.
    Returns [(label, smoothed_values), ...] — the top_n pods by average
    contribution, each as its own series, plus a trailing "other" series
    summing everything not in the top_n. Ordered largest-average-first so
    the biggest consumers stack at the bottom of the chart.
    """
    all_pods = set()
    for snapshot in pods_over_time:
        all_pods.update(snapshot.keys())

    T = len(pods_over_time)
    raw = {pod: [snapshot.get(pod, 0.0) for snapshot in pods_over_time] for pod in all_pods}

    ranked = sorted(raw.items(), key=lambda kv: sum(kv[1]) / T if T else 0, reverse=True)
    top = ranked[:top_n]
    rest = ranked[top_n:]

    series = [(name, rolling_avg(vals, smooth) if smooth > 1 else vals) for name, vals in top]

    if rest:
        other_vals = [sum(vals[i] for _, vals in rest) for i in range(T)]
        series.append((OTHER_LABEL, rolling_avg(other_vals, smooth) if smooth > 1 else other_vals))

    return series


def plot(nodes, smooth, top_n, out):
    node_names = sorted(nodes.keys())

    fig, axes = plt.subplots(
        nrows=len(node_names), ncols=1,
        figsize=(14, 4.5 * len(node_names)), sharex=True,
    )
    if len(node_names) == 1:
        axes = [axes]
    fig.patch.set_facecolor("#0e1117")

    palette = [
        "#58a6ff", "#3fb950", "#f78166", "#d2a8ff",
        "#ffa657", "#79c0ff", "#56d364", "#ff7b72",
    ]

    for ax, name in zip(axes, node_names):
        ax.set_facecolor("#161b22")
        ax.tick_params(colors="#8b949e", labelsize=9)
        ax.spines[:].set_color("#30363d")
        for spine in ax.spines.values():
            spine.set_linewidth(0.5)
        ax.yaxis.label.set_color("#8b949e")
        ax.grid(axis="y", color="#21262d", linewidth=0.6, linestyle="--")
        ax.grid(axis="x", color="#21262d", linewidth=0.4, linestyle=":")

        times = nodes[name]["times"]
        cpu   = nodes[name]["cpu"]
        pods_over_time = nodes[name]["pods"]

        series = build_pod_series(pods_over_time, top_n, smooth)

        if not series:
            print(f"  warning: no per-pod data found for node {name!r} "
                  f"(pods column empty on every row) — plotting total line only",
                  file=sys.stderr)
        else:
            labels = [s[0] for s in series]
            values = [s[1] for s in series]
            colors = [
                OTHER_COLOR if label == OTHER_LABEL else palette[i % len(palette)]
                for i, label in enumerate(labels)
            ]
            ax.stackplot(times, *values, labels=labels, colors=colors, alpha=0.85)

        total = rolling_avg(cpu, smooth) if smooth > 1 else cpu
        ax.plot(times, total, color="white", linewidth=1.0, linestyle="--",
                 alpha=0.6, label="node total")

        ax.set_ylabel(f"{name}\nCPU %", fontsize=9)
        ax.set_ylim(0, max(100, max(cpu, default=0) * 1.05))
        ax.legend(
            loc="upper left", framealpha=0.15, fontsize=7,
            labelcolor="white", edgecolor="#30363d", ncol=2,
        )

    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    axes[-1].xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(axes[-1].xaxis.get_majorticklabels(), rotation=30, ha="right")
    axes[-1].tick_params(axis="x", colors="#8b949e", labelsize=8)

    smooth_label = f"  (rolling avg {smooth}s)" if smooth > 1 else ""
    fig.suptitle(
        f"Per-node CPU % by pod{smooth_label}",
        color="#e6edf3", fontsize=13, fontweight="bold", y=1.01,
    )

    plt.tight_layout()

    if out:
        plt.savefig(out, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
        print(f"Saved to {out}")
    else:
        plt.show()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",    default="metrics.csv")
    ap.add_argument("--out",      default=None,  help="Save to file instead of showing")
    ap.add_argument("--since",    type=float,    default=None, help="Start of time range (unix timestamp)")
    ap.add_argument("--until",    type=float,    default=None, help="End of time range (unix timestamp)")
    ap.add_argument("--smooth",   type=int,      default=1,    help="Rolling average window in samples")
    ap.add_argument("--top-pods", type=int,      default=6,    help="Pods shown individually per node; the rest are grouped into 'other'")
    args = ap.parse_args()

    print(f"Reading {args.input} ...")
    nodes = load(args.input, args.since, args.until)

    if not nodes:
        sys.exit("No data found.")

    total = sum(len(v["times"]) for v in nodes.values())
    print(f"Loaded {total} samples across {len(nodes)} node(s): {sorted(nodes.keys())}")

    plot(nodes, args.smooth, args.top_pods, args.out)


if __name__ == "__main__":
    main()