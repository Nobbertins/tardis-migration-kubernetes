#!/usr/bin/env python3
"""
Parse serverless simulation experiment logs into a datasheet (CSV/Excel) and bar charts.
Expects a log file with sections separated by lines like:
  -------------------------------------------Starting Load Peak-------------------------------------------
  -------------------------------------------Starting No Mig-------------------------------------------
  -------------------------------------------Starting Load Average-------------------------------------------
  -------------------------------------------Starting Tardis-------------------------------------------
Each section contains 10 runs, each run ending with a Tail Latency Report block.
"""

import re
import sys
import csv
import statistics
import os

# ── Try to import optional libs ──────────────────────────────────────────────
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
    HAS_MPL = True
except ImportError:
    HAS_MPL = False

try:
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    from openpyxl.chart import BarChart, Reference
    from openpyxl.chart.error_bar import ErrorBars
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False

# ── Constants ─────────────────────────────────────────────────────────────────
POLICY_HEADERS = {
    "load peak":    "Load Peak",
    "load avg": "Load Avg",
    "no mig":       "No Mig",
    "tardis":       "Tardis",
}

METRICS = ["p50", "p60", "p70", "p80", "p90", "p99", "max"]

# ── Parsing ───────────────────────────────────────────────────────────────────

def detect_policy(line: str):
    """Return canonical policy name if line is a section header, else None."""
    low = line.lower()
    for key, name in POLICY_HEADERS.items():
        if f"starting {key}" in low:
            return name
    return None


def parse_tail_latency_block(lines):
    """
    Given a list of lines from one Tail Latency Report block, return a dict
    {metric: float_ms}.
    """
    result = {}
    for line in lines:
        m = re.match(r"\s*(p\d+|max)\s*:\s*([\d.]+)ms", line, re.IGNORECASE)
        if m:
            key = m.group(1).lower()
            result[key] = float(m.group(2))
    return result


def parse_log_file(path: str):
    """
    Returns dict: { policy_name: [ {metric: float, ...}, ... ] }
    10 dicts per policy.
    """
    with open(path) as f:
        raw_lines = f.readlines()

    data = {name: [] for name in POLICY_HEADERS.values()}

    current_policy = None
    in_report = False
    report_lines = []

    for line in raw_lines:
        # Check for section header
        detected = detect_policy(line)
        if detected:
            current_policy = detected
            in_report = False
            report_lines = []
            continue

        if current_policy is None:
            continue

        # Detect start of tail latency block
        if "Tail Latency Report" in line:
            in_report = True
            report_lines = [line]
            continue

        if in_report:
            report_lines.append(line)
            # End of block: dashed separator line (or blank after metrics)
            if re.match(r"\s*-{10,}", line) and len(report_lines) > 3:
                block = parse_tail_latency_block(report_lines)
                if block:
                    data[current_policy].append(block)
                in_report = False
                report_lines = []

    # Flush any trailing block
    if in_report and report_lines and current_policy:
        block = parse_tail_latency_block(report_lines)
        if block:
            data[current_policy].append(block)

    return data

# ── Statistics ────────────────────────────────────────────────────────────────

def compute_stats(runs: list, metric: str):
    """Return (mean, stdev) for a given metric across runs."""
    vals = [r[metric] for r in runs if metric in r]
    if not vals:
        return None, None
    mean = statistics.mean(vals)
    stdev = statistics.stdev(vals) if len(vals) > 1 else 0.0
    return mean, stdev

# ── CSV output ────────────────────────────────────────────────────────────────

def write_csv(data: dict, out_path: str):
    policies = list(POLICY_HEADERS.values())
    rows = []

    # Header
    header = ["Policy", "Run"] + METRICS
    rows.append(header)

    for policy in policies:
        runs = data[policy]
        for i, run in enumerate(runs, 1):
            row = [policy, i] + [run.get(m, "") for m in METRICS]
            rows.append(row)
        # Blank row then stats
        rows.append([])
        mean_row = [policy, "MEAN"] + [
            f"{compute_stats(runs, m)[0]:.2f}" if compute_stats(runs, m)[0] is not None else ""
            for m in METRICS
        ]
        stdev_row = [policy, "STDEV"] + [
            f"{compute_stats(runs, m)[1]:.2f}" if compute_stats(runs, m)[1] is not None else ""
            for m in METRICS
        ]
        rows.append(mean_row)
        rows.append(stdev_row)
        rows.append([])

    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)

    print(f"CSV written → {out_path}")

# ── Excel output ──────────────────────────────────────────────────────────────

def write_excel(data: dict, out_path: str):
    if not HAS_OPENPYXL:
        print("openpyxl not available – skipping Excel output")
        return

    wb = openpyxl.Workbook()
    policies = list(POLICY_HEADERS.values())

    # ── Sheet 1: Raw data ────────────────────────────────────────────────────
    ws_raw = wb.active
    ws_raw.title = "Raw Data"

    header_fill   = PatternFill("solid", fgColor="1F4E79")
    policy_fill   = PatternFill("solid", fgColor="2E75B6")
    mean_fill     = PatternFill("solid", fgColor="D9E1F2")
    stdev_fill    = PatternFill("solid", fgColor="EEF3FF")
    header_font   = Font(bold=True, color="FFFFFF")
    policy_font   = Font(bold=True, color="FFFFFF")
    stat_font     = Font(bold=True)

    def hdr(ws, row, col, val, fill, font, align="center"):
        c = ws.cell(row=row, column=col, value=val)
        c.fill = fill
        c.font = font
        c.alignment = Alignment(horizontal=align, vertical="center")
        return c

    col_headers = ["Policy", "Run"] + [m.upper() for m in METRICS]
    for ci, ch in enumerate(col_headers, 1):
        hdr(ws_raw, 1, ci, ch, header_fill, header_font)

    current_row = 2
    for policy in policies:
        runs = data[policy]
        block_start = current_row

        for i, run in enumerate(runs, 1):
            ws_raw.cell(row=current_row, column=1, value=policy)
            ws_raw.cell(row=current_row, column=2, value=i)
            for ci, m in enumerate(METRICS, 3):
                ws_raw.cell(row=current_row, column=ci, value=run.get(m))
            current_row += 1

        # Means row
        mean_r = current_row
        c = ws_raw.cell(row=current_row, column=1, value=policy)
        c.fill = mean_fill; c.font = stat_font
        c = ws_raw.cell(row=current_row, column=2, value="MEAN")
        c.fill = mean_fill; c.font = stat_font
        for ci, m in enumerate(METRICS, 3):
            v, _ = compute_stats(runs, m)
            c = ws_raw.cell(row=current_row, column=ci, value=round(v, 3) if v else None)
            c.fill = mean_fill; c.font = stat_font
        current_row += 1

        # Stdev row
        c = ws_raw.cell(row=current_row, column=1, value=policy)
        c.fill = stdev_fill; c.font = stat_font
        c = ws_raw.cell(row=current_row, column=2, value="STDEV")
        c.fill = stdev_fill; c.font = stat_font
        for ci, m in enumerate(METRICS, 3):
            _, s = compute_stats(runs, m)
            c = ws_raw.cell(row=current_row, column=ci, value=round(s, 3) if s is not None else None)
            c.fill = stdev_fill; c.font = stat_font
        current_row += 2  # blank row gap

    # Column widths
    ws_raw.column_dimensions["A"].width = 14
    ws_raw.column_dimensions["B"].width = 7
    for ci in range(3, 3 + len(METRICS)):
        ws_raw.column_dimensions[get_column_letter(ci)].width = 10

    # ── Sheet 2: Summary table ───────────────────────────────────────────────
    ws_sum = wb.create_sheet("Summary")
    sum_headers = ["Policy"] + [f"{m.upper()} Mean" for m in METRICS] + [f"{m.upper()} StDev" for m in METRICS]
    for ci, h in enumerate(sum_headers, 1):
        hdr(ws_sum, 1, ci, h, header_fill, header_font)

    for ri, policy in enumerate(policies, 2):
        runs = data[policy]
        ws_sum.cell(row=ri, column=1, value=policy).font = Font(bold=True)
        for ci, m in enumerate(METRICS, 2):
            v, s = compute_stats(runs, m)
            ws_sum.cell(row=ri, column=ci, value=round(v, 3) if v else None)
            ws_sum.cell(row=ri, column=ci + len(METRICS), value=round(s, 3) if s is not None else None)

    ws_sum.column_dimensions["A"].width = 14
    for ci in range(2, len(sum_headers) + 2):
        ws_sum.column_dimensions[get_column_letter(ci)].width = 12

    wb.save(out_path)
    print(f"Excel written → {out_path}")

# ── Matplotlib charts ─────────────────────────────────────────────────────────

def make_bar_chart(data: dict, metric: str, out_path: str):
    if not HAS_MPL:
        print(f"matplotlib not available – skipping {metric} chart")
        return

    policies = list(POLICY_HEADERS.values())
    means, stdevs = [], []
    for p in policies:
        v, s = compute_stats(data[p], metric)
        means.append(v if v else 0)
        stdevs.append(s if s else 0)

    colors = ["#2E75B6", "#ED7D31", "#A9D18E", "#FF5252"]

    fig, ax = plt.subplots(figsize=(8, 5))
    x = range(len(policies))
    bars = ax.bar(
        x, means, yerr=stdevs, capsize=6, width=0.55,
        color=colors, edgecolor="white", linewidth=0.8,
        error_kw=dict(elinewidth=1.5, ecolor="#333333", capthick=1.5),
    )

    # Value labels on bars
    for bar, mean, stdev in zip(bars, means, stdevs):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            mean + stdev + max(means) * 0.01,
            f"{mean:.1f}ms",
            ha="center", va="bottom", fontsize=9, fontweight="bold",
        )

    ax.set_xticks(list(x))
    ax.set_xticklabels(policies, fontsize=11)
    ax.set_ylabel("Overhead Latency (ms)", fontsize=11)
    ax.set_title(f"{metric.upper()} Overhead Latency by Policy\n(mean ± 1 std dev, n=10 runs)", fontsize=12)
    ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())
    ax.grid(axis="y", linestyle="--", alpha=0.5, which="both")
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"{metric.upper()} chart → {out_path}")


def make_combined_chart(data: dict, out_path: str):
    """Side-by-side p50 + p90 on one figure."""
    if not HAS_MPL:
        return

    policies = list(POLICY_HEADERS.values())
    colors = ["#2E75B6", "#ED7D31", "#A9D18E", "#FF5252"]

    fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharey=False)

    for ax, metric in zip(axes, ["p50", "p90"]):
        means, stdevs = [], []
        for p in policies:
            v, s = compute_stats(data[p], metric)
            means.append(v if v else 0)
            stdevs.append(s if s else 0)

        x = range(len(policies))
        bars = ax.bar(
            x, means, yerr=stdevs, capsize=6, width=0.55,
            color=colors, edgecolor="white", linewidth=0.8,
            error_kw=dict(elinewidth=1.5, ecolor="#333333", capthick=1.5),
        )
        for bar, mean, stdev in zip(bars, means, stdevs):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                mean + stdev + max(means) * 0.015,
                f"{mean:.1f}",
                ha="center", va="bottom", fontsize=8.5, fontweight="bold",
            )
        ax.set_xticks(list(x))
        ax.set_xticklabels(policies, fontsize=10)
        ax.set_ylabel("Overhead Latency (ms)", fontsize=10)
        ax.set_title(f"{metric.upper()} Latency by Policy", fontsize=11, fontweight="bold")
        ax.grid(axis="y", linestyle="--", alpha=0.5)
        ax.set_axisbelow(True)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    fig.suptitle("Tail Latency Comparison (mean ± 1 std dev, n=10 runs each)", fontsize=12, y=1.02)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Combined chart → {out_path}")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log_file = sys.argv[1] if len(sys.argv) > 1 else "experiment.log"
    out_dir  = sys.argv[2] if len(sys.argv) > 2 else "."
    os.makedirs(out_dir, exist_ok=True)

    print(f"Parsing: {log_file}")
    data = parse_log_file(log_file)

    # Report what we found
    for policy, runs in data.items():
        print(f"  {policy}: {len(runs)} runs parsed")
        if runs:
            for m in METRICS:
                v, s = compute_stats(runs, m)
                if v is not None:
                    print(f"    {m}: mean={v:.2f}ms  stdev={s:.2f}ms")

    # Outputs
    write_csv(data, os.path.join(out_dir, "results.csv"))
    write_excel(data, os.path.join(out_dir, "results.xlsx"))
    make_bar_chart(data, "p50", os.path.join(out_dir, "chart_p50.png"))
    make_bar_chart(data, "p60", os.path.join(out_dir, "chart_p60.png"))
    make_bar_chart(data, "p70", os.path.join(out_dir, "chart_p70.png"))
    make_bar_chart(data, "p80", os.path.join(out_dir, "chart_p80.png"))
    make_bar_chart(data, "p90", os.path.join(out_dir, "chart_p90.png"))
    make_bar_chart(data, "p99", os.path.join(out_dir, "chart_p99.png"))
    make_combined_chart(data, os.path.join(out_dir, "chart_p50_p90_combined.png"))

    print("\nDone.")

if __name__ == "__main__":
    main()