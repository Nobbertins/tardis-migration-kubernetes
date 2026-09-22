#!/usr/bin/env python3
"""Generate graphs and summaries from orchestrator experiment results.

Expected files (searched recursively):
  latencies_<run_id>.csv
  throughput_<run_id>.csv
  node_usage_<run_id>.csv

The directory layout produced by experiment.py is detected automatically:
  experiment_results/<experiment_id>/<scenario>/repeat_XX/<files>

Dependencies:
  pip install pandas matplotlib numpy
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


PLOT_DPI = 170
PERCENTILES = (0.50, 0.95, 0.99)
NUMERIC_LATENCY_COLUMNS = [
    "send_time",
    "receive_time",
    "duration_ms",
    "latency_ms",
    "overhead_ms",
    "total_latency_ms",
    "worker_latency_ms",
    "network_send_ms",
    "network_receive_ms",
    "status",
    "retries",
]
NUMERIC_THROUGHPUT_COLUMNS = [
    "timestamp",
    "elapsed_s",
    "interval_s",
    "dispatched_rps",
    "completed_rps",
    "failed_rps",
    "attempt_rps",
    "in_flight",
    "dispatched_total",
    "completed_total",
    "failed_total",
    "attempts_total",
]
NUMERIC_NODE_COLUMNS = [
    "timestamp",
    "elapsed_s",
    "cpu_cores",
    "memory_bytes",
]


def warn(message: str) -> None:
    print(f"warning: {message}", file=sys.stderr)


def slug(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", value).strip("_").lower()


def display_name(value: str) -> str:
    return value.replace("_", " ").replace("-", " ").title()


def infer_metadata(path: Path, root: Path, prefix: str) -> dict[str, str]:
    relative = path.relative_to(root)
    directories = list(relative.parts[:-1])
    repeat_index = next(
        (i for i, part in enumerate(directories) if part.startswith("repeat_")),
        None,
    )

    if repeat_index is not None:
        repeat = directories[repeat_index]
        scenario = (
            directories[repeat_index - 1] if repeat_index > 0 else "unknown"
        )
        experiment_parts = directories[: max(0, repeat_index - 1)]
        experiment_id = "/".join(experiment_parts) or root.name
    else:
        repeat = "repeat_unknown"
        scenario = directories[-1] if directories else "unknown"
        experiment_id = root.name

    filename_pattern = rf"^{re.escape(prefix)}_(.+)\.csv$"
    match = re.match(filename_pattern, path.name)
    run_id = match.group(1) if match else path.stem
    return {
        "experiment_id": experiment_id,
        "scenario": scenario,
        "repeat": repeat,
        "run_id": run_id,
        "source_file": str(relative),
    }


def load_result_files(
    root: Path,
    prefix: str,
    required_columns: set[str],
    numeric_columns: list[str],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    paths = sorted(root.rglob(f"{prefix}_*.csv"))
    for path in paths:
        try:
            frame = pd.read_csv(path)
        except (OSError, pd.errors.ParserError, pd.errors.EmptyDataError) as exc:
            warn(f"skipping unreadable {path}: {exc}")
            continue

        missing = required_columns.difference(frame.columns)
        if missing:
            warn(f"skipping {path}; missing columns: {sorted(missing)}")
            continue

        for column in numeric_columns:
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        for key, value in infer_metadata(path, root, prefix).items():
            frame[key] = value
        frames.append(frame)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def configure_plot_style() -> None:
    plt.style.use("seaborn-v0_8-whitegrid")
    plt.rcParams.update({
        "figure.figsize": (10, 5.5),
        "axes.titlesize": 14,
        "axes.labelsize": 11,
        "legend.fontsize": 9,
        "lines.linewidth": 1.8,
    })


def save_figure(fig: plt.Figure, output: Path, dpi: int) -> None:
    fig.tight_layout()
    fig.savefig(output, dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {output}")


def choose_latency_metric(latencies: pd.DataFrame, requested: str) -> str:
    if requested in latencies.columns and latencies[requested].notna().any():
        return requested
    if "latency_ms" in latencies.columns and latencies["latency_ms"].notna().any():
        warn(f"{requested} is unavailable; falling back to latency_ms")
        return "latency_ms"
    raise ValueError("latency files contain no usable latency metric")


def latency_label(metric: str) -> str:
    labels = {
        "total_latency_ms": "Total request latency (ms, including retries)",
        "latency_ms": "Successful-attempt latency (ms)",
        "overhead_ms": "Request overhead (ms)",
        "worker_latency_ms": "Worker execution latency (ms)",
    }
    return labels.get(metric, metric.replace("_", " ").title())


def add_latency_elapsed_time(latencies: pd.DataFrame) -> pd.DataFrame:
    latencies = latencies.copy()
    start_by_file = latencies.groupby("source_file")["send_time"].transform("min")
    latencies["elapsed_s"] = latencies["receive_time"] - start_by_file
    return latencies


def plot_latency_percentiles(
    latencies: pd.DataFrame,
    metric: str,
    output_dir: Path,
    dpi: int,
) -> None:
    scenarios = sorted(latencies["scenario"].dropna().unique())
    quantiles = (
        latencies.groupby("scenario")[metric]
        .quantile(PERCENTILES)
        .unstack()
        .reindex(scenarios)
    )

    fig, ax = plt.subplots()
    x = np.arange(len(scenarios))
    width = 0.24
    colors = ["#4C78A8", "#F58518", "#E45756"]
    for index, percentile in enumerate(PERCENTILES):
        ax.bar(
            x + (index - 1) * width,
            quantiles[percentile],
            width,
            label=f"p{percentile * 100:g}",
            color=colors[index],
        )
    ax.set_xticks(x, [display_name(s) for s in scenarios], rotation=15)
    ax.set_ylabel(latency_label(metric))
    ax.set_title("Latency Percentiles by Scenario")
    ax.legend()
    save_figure(fig, output_dir / "latency_percentiles.png", dpi)


def plot_latency_ecdf(
    latencies: pd.DataFrame,
    metric: str,
    output_dir: Path,
    dpi: int,
) -> None:
    fig, ax = plt.subplots()
    for scenario, group in latencies.groupby("scenario", sort=True):
        values = np.sort(group[metric].dropna().to_numpy())
        if values.size == 0:
            continue
        probability = np.arange(1, values.size + 1) / values.size
        ax.plot(values, probability, label=display_name(scenario))
    ax.set_xlabel(latency_label(metric))
    ax.set_ylabel("Cumulative fraction of requests")
    ax.set_title("Latency Distribution (ECDF)")
    ax.set_ylim(0, 1.01)
    ax.legend()
    save_figure(fig, output_dir / "latency_ecdf.png", dpi)


def plot_latency_over_time(
    latencies: pd.DataFrame,
    metric: str,
    output_dir: Path,
    time_bin_s: float,
    dpi: int,
) -> None:
    data = latencies.dropna(subset=["elapsed_s", metric]).copy()
    data["time_bin_s"] = np.floor(data["elapsed_s"] / time_bin_s) * time_bin_s

    for scenario, group in data.groupby("scenario", sort=True):
        binned = (
            group.groupby("time_bin_s")[metric]
            .quantile(PERCENTILES)
            .unstack()
        )
        if binned.empty:
            continue
        fig, ax = plt.subplots()
        for percentile, color in zip(
            PERCENTILES, ["#4C78A8", "#F58518", "#E45756"]
        ):
            ax.plot(
                binned.index,
                binned[percentile],
                label=f"p{percentile * 100:g}",
                color=color,
            )
        ax.set_xlabel("Elapsed experiment time (s)")
        ax.set_ylabel(latency_label(metric))
        ax.set_title(f"Latency Over Time — {display_name(scenario)}")
        ax.legend()
        save_figure(
            fig,
            output_dir / f"latency_over_time_{slug(scenario)}.png",
            dpi,
        )


def aggregate_timeseries(
    data: pd.DataFrame,
    value_column: str,
    time_bin_s: float,
) -> pd.DataFrame:
    working = data.dropna(subset=["elapsed_s", value_column]).copy()
    working["time_bin_s"] = (
        np.floor(working["elapsed_s"] / time_bin_s) * time_bin_s
    )
    per_run = (
        working.groupby(["scenario", "source_file", "time_bin_s"], as_index=False)[
            value_column
        ]
        .mean()
    )
    return (
        per_run.groupby(["scenario", "time_bin_s"])[value_column]
        .agg(mean="mean", q25=lambda x: x.quantile(0.25), q75=lambda x: x.quantile(0.75))
        .reset_index()
    )


def plot_throughput(
    throughput: pd.DataFrame,
    output_dir: Path,
    time_bin_s: float,
    dpi: int,
) -> None:
    completed = aggregate_timeseries(throughput, "completed_rps", time_bin_s)
    dispatched = aggregate_timeseries(throughput, "dispatched_rps", time_bin_s)
    scenarios = sorted(throughput["scenario"].dropna().unique())

    for scenario in scenarios:
        actual = completed[completed["scenario"] == scenario]
        offered = dispatched[dispatched["scenario"] == scenario]
        fig, ax = plt.subplots()
        ax.plot(
            actual["time_bin_s"],
            actual["mean"],
            color="#4C78A8",
            label="Completed RPS",
        )
        ax.fill_between(
            actual["time_bin_s"],
            actual["q25"],
            actual["q75"],
            color="#4C78A8",
            alpha=0.15,
            label="Completed interquartile range",
        )
        ax.plot(
            offered["time_bin_s"],
            offered["mean"],
            color="#F58518",
            linestyle="--",
            label="Dispatched RPS",
        )
        ax.fill_between(
            offered["time_bin_s"],
            offered["q25"],
            offered["q75"],
            color="#F58518",
            alpha=0.12,
            label="Dispatched interquartile range",
        )
        ax.set_xlabel("Elapsed experiment time (s)")
        ax.set_ylabel("Requests per second")
        ax.set_title(
            f"Dispatched and Completed Throughput — {display_name(scenario)}"
        )
        ax.legend()
        save_figure(
            fig,
            output_dir / f"throughput_{slug(scenario)}.png",
            dpi,
        )


def plot_concurrency(
    throughput: pd.DataFrame,
    output_dir: Path,
    time_bin_s: float,
    dpi: int,
) -> None:
    aggregated = aggregate_timeseries(throughput, "in_flight", time_bin_s)
    fig, ax = plt.subplots()
    for scenario, group in aggregated.groupby("scenario", sort=True):
        ax.plot(
            group["time_bin_s"],
            group["mean"],
            label=display_name(scenario),
        )
        if group["q25"].notna().any() and group["q75"].notna().any():
            ax.fill_between(
                group["time_bin_s"],
                group["q25"],
                group["q75"],
                alpha=0.12,
            )
    ax.set_xlabel("Elapsed experiment time (s)")
    ax.set_ylabel("In-flight logical requests")
    ax.set_title("Request Concurrency")
    ax.legend()
    save_figure(fig, output_dir / "in_flight_over_time.png", dpi)


def plot_node_usage(
    nodes: pd.DataFrame,
    output_dir: Path,
    time_bin_s: float,
    dpi: int,
) -> None:
    data = nodes.dropna(subset=["elapsed_s", "node"]).copy()
    data["time_bin_s"] = np.floor(data["elapsed_s"] / time_bin_s) * time_bin_s

    for scenario, scenario_data in data.groupby("scenario", sort=True):
        for column, ylabel, suffix in [
            ("cpu_cores", "CPU usage (cores)", "node_cpu"),
            ("memory_gib", "Memory usage (GiB)", "node_memory"),
        ]:
            usable = scenario_data.dropna(subset=[column])
            if usable.empty:
                continue
            binned = (
                usable.groupby(["node", "time_bin_s"], as_index=False)[column]
                .mean()
            )
            fig, ax = plt.subplots()
            for node, group in binned.groupby("node", sort=True):
                ax.plot(group["time_bin_s"], group[column], label=str(node))
            ax.set_xlabel("Elapsed experiment time (s)")
            ax.set_ylabel(ylabel)
            ax.set_title(f"{ylabel} — {display_name(scenario)}")
            ax.legend(title="Node", ncol=2)
            save_figure(
                fig,
                output_dir / f"{suffix}_{slug(scenario)}.png",
                dpi,
            )


def build_run_summary(
    latencies: pd.DataFrame,
    throughput: pd.DataFrame,
    metric: str,
) -> pd.DataFrame:
    keys = ["experiment_id", "scenario", "repeat", "run_id"]
    latency_rows = []
    if not latencies.empty:
        for group_keys, group in latencies.groupby(keys, dropna=False):
            values = group[metric].dropna()
            latency_rows.append(dict(zip(keys, group_keys)) | {
                "successful_requests": len(group),
                "retried_requests": int((group.get("retries", 0) > 0).sum()),
                "p50_ms": values.quantile(0.50),
                "p95_ms": values.quantile(0.95),
                "p99_ms": values.quantile(0.99),
                "max_ms": values.max(),
            })
    latency_summary = pd.DataFrame(latency_rows)

    throughput_rows = []
    if not throughput.empty:
        for group_keys, group in throughput.groupby(keys, dropna=False):
            group = group.sort_values("elapsed_s")
            final = group.iloc[-1]
            elapsed = pd.to_numeric(group["elapsed_s"], errors="coerce").max()
            completed = pd.to_numeric(group["completed_total"], errors="coerce").max()
            throughput_rows.append(dict(zip(keys, group_keys)) | {
                "failed_requests": final.get("failed_total", np.nan),
                "attempts": final.get("attempts_total", np.nan),
                "mean_throughput_rps": completed / elapsed if elapsed > 0 else np.nan,
                "peak_throughput_rps": group["completed_rps"].max(),
                "peak_in_flight": group["in_flight"].max(),
                "elapsed_s": elapsed,
            })
    throughput_summary = pd.DataFrame(throughput_rows)

    if latency_summary.empty:
        return throughput_summary
    if throughput_summary.empty:
        return latency_summary
    return latency_summary.merge(throughput_summary, on=keys, how="outer")


def build_scenario_summary(
    latencies: pd.DataFrame,
    run_summary: pd.DataFrame,
    metric: str,
) -> pd.DataFrame:
    rows = []
    scenarios = sorted(
        set(latencies.get("scenario", pd.Series(dtype=str)).dropna())
        | set(run_summary.get("scenario", pd.Series(dtype=str)).dropna())
    )
    for scenario in scenarios:
        latency_group = latencies[latencies["scenario"] == scenario]
        runs = run_summary[run_summary["scenario"] == scenario]
        values = latency_group[metric].dropna()
        successful = len(latency_group)
        retried = int((latency_group.get("retries", 0) > 0).sum())
        rows.append({
            "scenario": scenario,
            "runs": len(runs),
            "successful_requests": successful,
            "failed_requests": runs.get(
                "failed_requests", pd.Series(dtype=float)
            ).sum(min_count=1),
            "retry_rate": retried / successful if successful else np.nan,
            "p50_ms": values.quantile(0.50),
            "p95_ms": values.quantile(0.95),
            "p99_ms": values.quantile(0.99),
            "max_ms": values.max(),
            "mean_throughput_rps": runs.get(
                "mean_throughput_rps", pd.Series(dtype=float)
            ).mean(),
            "peak_throughput_rps": runs.get(
                "peak_throughput_rps", pd.Series(dtype=float)
            ).max(),
            "peak_in_flight": runs.get(
                "peak_in_flight", pd.Series(dtype=float)
            ).max(),
        })
    return pd.DataFrame(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "results_dir",
        nargs="?",
        type=Path,
        default=Path("experiment_results"),
        help="Experiment results root (default: experiment_results)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Graph output directory (default: RESULTS_DIR/graphs)",
    )
    parser.add_argument(
        "--latency-metric",
        choices=[
            "total_latency_ms",
            "latency_ms",
            "overhead_ms",
            "worker_latency_ms",
        ],
        default="total_latency_ms",
        help="Latency value used in comparisons (default: total_latency_ms)",
    )
    parser.add_argument(
        "--time-bin",
        type=float,
        default=10.0,
        help="Time-series aggregation width in seconds (default: 10)",
    )
    parser.add_argument("--dpi", type=int, default=PLOT_DPI)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.results_dir.resolve()
    if not root.is_dir():
        print(f"error: results directory does not exist: {root}", file=sys.stderr)
        return 2
    if args.time_bin <= 0:
        print("error: --time-bin must be greater than zero", file=sys.stderr)
        return 2

    output_dir = (args.output_dir or root / "graphs").resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    configure_plot_style()

    latencies = load_result_files(
        root,
        "latencies",
        {"send_time", "receive_time", "latency_ms"},
        NUMERIC_LATENCY_COLUMNS,
    )
    throughput = load_result_files(
        root,
        "throughput",
        {"elapsed_s", "completed_rps", "dispatched_rps", "in_flight"},
        NUMERIC_THROUGHPUT_COLUMNS,
    )
    nodes = load_result_files(
        root,
        "node_usage",
        {"elapsed_s", "node", "cpu_cores", "memory_bytes"},
        NUMERIC_NODE_COLUMNS,
    )

    if latencies.empty and throughput.empty and nodes.empty:
        print(
            f"error: no recognized orchestrator CSVs found under {root}",
            file=sys.stderr,
        )
        return 1

    metric = None
    if not latencies.empty:
        metric = choose_latency_metric(latencies, args.latency_metric)
        latencies = add_latency_elapsed_time(latencies)
        plot_latency_percentiles(latencies, metric, output_dir, args.dpi)
        plot_latency_ecdf(latencies, metric, output_dir, args.dpi)
        plot_latency_over_time(
            latencies, metric, output_dir, args.time_bin, args.dpi
        )
    else:
        warn("no latency CSV data found; skipping latency graphs")

    if not throughput.empty:
        plot_throughput(throughput, output_dir, args.time_bin, args.dpi)
        plot_concurrency(throughput, output_dir, args.time_bin, args.dpi)
    else:
        warn("no throughput CSV data found; skipping throughput graphs")

    if not nodes.empty:
        nodes["memory_gib"] = nodes["memory_bytes"] / 1024**3
        plot_node_usage(nodes, output_dir, args.time_bin, args.dpi)
    else:
        warn("no node-usage CSV data found; skipping node graphs")

    if metric is not None:
        run_summary = build_run_summary(latencies, throughput, metric)
        scenario_summary = build_scenario_summary(
            latencies, run_summary, metric
        )
        run_summary.to_csv(output_dir / "run_summary.csv", index=False)
        scenario_summary.to_csv(
            output_dir / "scenario_summary.csv", index=False
        )
        print(f"wrote {output_dir / 'run_summary.csv'}")
        print(f"wrote {output_dir / 'scenario_summary.csv'}")

    print(
        f"loaded {len(latencies)} latency rows, "
        f"{len(throughput)} throughput rows, and {len(nodes)} node rows"
    )
    print(f"graphs are in {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
