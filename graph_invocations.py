"""
graph_invocations.py — Gantt chart for function invocation data.

Usage:
    python graph_invocations.py my_data.csv --start 0 --end 3600 --out out.png
    python graph_invocations.py my_data.csv --out full.png          # full dataset
    python graph_invocations.py my_data.csv --min-dur 0.5 --top 20 --start 0 --end 86400 --out day1.png
"""

import sys
import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.ticker import FuncFormatter, AutoLocator

# ── tuneable defaults ──────────────────────────────────────────────────────────
DEFAULT_CSV  = "invocations.csv"
DEFAULT_OUT  = "invocations_gantt.png"
BAR_ALPHA    = 0.85
MIN_BAR_PX   = 1e-9      # minimum bar width in data units (avoid invisible bars)
LABEL_CHARS  = 10
TOP_N        = 40
FIG_WIDTH    = 18
DPI          = 150
CONCUR_BINS  = 800        # x-resolution of the concurrency area chart
# ──────────────────────────────────────────────────────────────────────────────

COLORS = [
    "#4A7FD4", "#D4724A", "#4AB87F", "#9B4AD4", "#D4B84A",
    "#4AB4D4", "#D44A7F", "#7FD44A", "#D44A4A", "#4A4AD4",
    "#A04AB8", "#4AB8A0", "#B8A04A", "#7F4AB8", "#4A7FB8",
    "#D47F4A", "#4AD4B4", "#B84A4A", "#4AB84A", "#7F7FD4",
]

# ── time helpers ──────────────────────────────────────────────────────────────

def fmt_time(seconds: float) -> str:
    s = abs(seconds)
    if s < 120:       return f"{seconds:.2f}s"
    if s < 7200:      return f"{seconds/60:.2f}m"
    if s < 172800:    return f"{seconds/3600:.2f}h"
    return f"{seconds/86400:.3f}d"

def axis_formatter(span: float):
    if span < 120:    return FuncFormatter(lambda v, _: f"{v:.2f}s")
    if span < 7200:   return FuncFormatter(lambda v, _: f"{v/60:.1f}m")
    if span < 172800: return FuncFormatter(lambda v, _: f"{v/3600:.2f}h")
    return FuncFormatter(lambda v, _: f"{v/86400:.2f}d")

# ── data ──────────────────────────────────────────────────────────────────────

# Some trace files identify the deployed unit with a column literally named
# "function" (or "func"); others (like the simplified Azure Functions trace
# this tool was built around) call it "app". Prefer "func"/"function" when
# present so we always key on the actual function ID rather than silently
# falling back to "app". Traces can ALSO carry a separate, coarser "app"
# column (an owner/application grouping distinct from the individual
# function) — that's preserved under "owner" rather than discarded.
ID_COLUMN_CANDIDATES = ["func", "function", "function_id", "app"]

FUNCTION_ID_MAXLEN = 16  # must match genk8s.py's ID truncation so IDs line up end-to-end

def load(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip()

    id_col = next((c for c in ID_COLUMN_CANDIDATES if c in df.columns), None)
    if id_col is None:
        sys.exit(
            f"Error: no function identifier column found in {csv_path}. "
            f"Expected one of {ID_COLUMN_CANDIDATES}, found columns: {list(df.columns)}"
        )
    print(f"  Using '{id_col}' column as the function ID")
    if id_col != "app":
        # The trace may ALSO have its own "app" column representing the
        # coarser owner/application grouping (distinct from the individual
        # function). Keep that data but rename it to "owner" so it doesn't
        # collide with the "app" name we use internally for the function ID
        # — two same-named columns breaks df["app"] (returns a DataFrame
        # instead of a Series) and groupby("app").
        if "app" in df.columns:
            print(f"  Note: renaming pre-existing 'app' column -> 'owner' "
                  f"('{id_col}' is used as the function ID instead)")
            df = df.rename(columns={"app": "owner"})
        df = df.rename(columns={id_col: "app"})

    # Truncate to the same length genk8s.py uses (row[id_field].strip()[:16])
    # so the IDs in --functions-out match exactly what genk8s.py will look
    # up and deploy — otherwise a full-length hash here vs. a truncated one
    # there means nothing matches.
    df["app"] = df["app"].astype(str).str.strip().str[:FUNCTION_ID_MAXLEN]

    df["start"] = df["end_timestamp"] - df["duration"]
    return df

def pick_apps(df: pd.DataFrame, top_n: int, min_dur: float,
              t_start: float, t_end: float) -> list:
    in_window = df[
        (df["duration"] >= min_dur) &
        (df["start"] >= t_start) &
        (df["end_timestamp"] <= t_end)
    ]
    counts = in_window.groupby("app").size().sort_values(ascending=False)
    return counts.head(top_n).index.tolist()


# ── lane packing ──────────────────────────────────────────────────────────────

def assign_lanes(calls: pd.DataFrame) -> pd.Series:
    """
    Pack calls into the minimum number of sub-lanes so no two bars overlap.
    Returns a Series of integer lane indices aligned to calls.index.
    Uses a greedy interval-graph colouring (earliest-deadline-first).
    """
    calls = calls.sort_values("start")
    lanes = []          # list of current end-time per lane
    result = pd.Series(index=calls.index, dtype=int)

    for idx, row in calls.iterrows():
        placed = False
        for lane_idx, lane_end in enumerate(lanes):
            if row["start"] >= lane_end:
                lanes[lane_idx] = row["end_timestamp"]
                result[idx] = lane_idx
                placed = True
                break
        if not placed:
            result[idx] = len(lanes)
            lanes.append(row["end_timestamp"])

    return result

# ── concurrency ───────────────────────────────────────────────────────────────

def concurrency_curve(subset: pd.DataFrame, t_start: float, t_end: float, bins: int):
    """Return (times, counts) for an area chart of simultaneous active calls."""
    ts = np.linspace(t_start, t_end, bins)
    starts = subset["start"].values
    ends   = subset["end_timestamp"].values
    # vectorised: for each time point count how many calls are active
    counts = np.sum((starts[None, :] <= ts[:, None]) & (ends[None, :] > ts[:, None]), axis=1)
    return ts, counts

# ── plot ──────────────────────────────────────────────────────────────────────

def plot(df: pd.DataFrame, apps: list, t_start: float, t_end: float,
         min_dur: float, out: Path):

    mask = (
        df["app"].isin(apps) &
        (df["duration"] >= min_dur) &
        (df["start"] >= t_start) &
        (df["end_timestamp"] <= t_end)
    )
    subset = df[mask].copy()
    subset["vis_start"] = subset["start"].clip(lower=t_start)
    subset["vis_end"]   = subset["end_timestamp"].clip(upper=t_end)
    subset["vis_dur"]   = (subset["vis_end"] - subset["vis_start"]).clip(lower=MIN_BAR_PX)

    color_map = {app: COLORS[i % len(COLORS)] for i, app in enumerate(apps)}

    # ── lane assignment per app ───────────────────────────────────────────────
    subset["lane"] = 0
    for app, grp in subset.groupby("app"):
        subset.loc[grp.index, "lane"] = assign_lanes(grp).values

    max_lanes_per_app = subset.groupby("app")["lane"].max() + 1   # Series

    # build y mapping: each app gets as many sub-rows as it needs
    # y_base[app] = bottom y of the app's block; height = max_lanes
    app_order = list(reversed(apps))   # bottom to top
    y_base = {}
    y_cursor = 0
    for app in app_order:
        y_base[app] = y_cursor
        y_cursor += max_lanes_per_app.get(app, 1)

    total_rows = y_cursor
    BAR_H = 0.82   # bar height in row units

    # ── figure layout ─────────────────────────────────────────────────────────
    n_apps    = len(apps)
    gantt_h   = max(4, total_rows * max(0.18, min(0.45, 12 / total_rows)))
    concur_h  = 1.8
    fig_h     = gantt_h + concur_h + 1.2

    fig = plt.figure(figsize=(FIG_WIDTH, fig_h), facecolor="white")
    gs  = fig.add_gridspec(2, 1, height_ratios=[gantt_h, concur_h],
                            hspace=0.08, left=0.10, right=0.92,
                            top=0.96, bottom=0.07)
    ax_g = fig.add_subplot(gs[0])   # gantt
    ax_c = fig.add_subplot(gs[1])   # concurrency

    # ── gantt ─────────────────────────────────────────────────────────────────
    # zebra stripes per app block
    for app in app_order:
        yb = y_base[app]
        nh = max_lanes_per_app.get(app, 1)
        col = "#f5f5f5" if (app_order.index(app) % 2 == 0) else "white"
        ax_g.axhspan(yb - 0.5, yb + nh - 0.5, color=col, zorder=0)

    for _, row in subset.iterrows():
        app  = row["app"]
        lane = int(row["lane"])
        y    = y_base[app] + lane
        col  = color_map[app]
        bar  = mpatches.FancyArrow(
            row["vis_start"], y, row["vis_dur"], 0,
            width=BAR_H, head_width=0, head_length=0,
            length_includes_head=True,
            color=col, alpha=BAR_ALPHA, linewidth=0,
        )
        ax_g.add_patch(bar)

    # y-axis: one label per app, centred on its block
    yticks, ylabels = [], []
    for app in app_order:
        yb = y_base[app]
        nh = max_lanes_per_app.get(app, 1)
        yticks.append(yb + (nh - 1) / 2)
        ylabels.append(app[:LABEL_CHARS] + "...")

    ax_g.set_yticks(yticks)
    ax_g.set_yticklabels(ylabels, fontsize=7.5, fontfamily="monospace")
    for tick, app in zip(ax_g.get_yticklabels(), app_order):
        tick.set_color(color_map[app])

    # right-side call counts
    ax_r = ax_g.twinx()
    ax_r.set_ylim(ax_g.get_ylim())
    ax_r.set_yticks(yticks)
    counts_in_win = subset.groupby("app").size()
    ax_r.set_yticklabels(
        [f"{counts_in_win.get(a, 0)} calls" for a in app_order],
        fontsize=6.5, color="#999",
    )
    ax_r.tick_params(right=False)
    ax_r.spines[["top","right","left","bottom"]].set_visible(False)

    span = t_end - t_start or 1
    ax_g.set_xlim(t_start, t_end)
    ax_g.set_ylim(-0.6, total_rows - 0.4)
    ax_g.xaxis.set_major_formatter(axis_formatter(span))
    ax_g.xaxis.set_major_locator(AutoLocator())
    ax_g.tick_params(axis="x", labelsize=7, labelbottom=False)
    ax_g.set_axisbelow(True)
    ax_g.xaxis.grid(True, linestyle="--", linewidth=0.4, color="#ccc", alpha=0.7)
    ax_g.yaxis.grid(False)
    ax_g.spines[["top","right","left"]].set_visible(False)

    avg_dur = subset["duration"].mean() if len(subset) else 0
    ax_g.set_title(
        f"{len(subset):,} calls  |  {n_apps} functions  |  "
        f"window {fmt_time(t_start)} -> {fmt_time(t_end)}  |  "
        f"span {fmt_time(span)}  |  avg dur {avg_dur:.3f}s",
        fontsize=9, pad=6,
    )

    # ── concurrency panel ─────────────────────────────────────────────────────
    ts, counts = concurrency_curve(subset, t_start, t_end, CONCUR_BINS)

    ax_c.fill_between(ts, counts, step="mid", alpha=0.25, color="#4A7FD4")
    ax_c.step(ts, counts, where="mid", color="#4A7FD4", linewidth=1.0)

    ax_c.set_xlim(t_start, t_end)
    ax_c.set_ylim(0, max(counts.max() * 1.15, 1))
    ax_c.xaxis.set_major_formatter(axis_formatter(span))
    ax_c.xaxis.set_major_locator(AutoLocator())
    ax_c.tick_params(axis="x", labelsize=7.5)
    ax_c.tick_params(axis="y", labelsize=7)
    ax_c.set_ylabel("concurrent\ncalls", fontsize=7.5, labelpad=4)
    ax_c.set_axisbelow(True)
    ax_c.xaxis.grid(True, linestyle="--", linewidth=0.4, color="#ccc", alpha=0.7)
    ax_c.yaxis.grid(True, linestyle="--", linewidth=0.4, color="#ccc", alpha=0.5)
    ax_c.spines[["top","right"]].set_visible(False)
    ax_c.yaxis.set_major_locator(matplotlib.ticker.MaxNLocator(integer=True, nbins=4))

    # peak annotation
    peak = int(counts.max())
    peak_t = ts[counts.argmax()]
    ax_c.annotate(f"peak: {peak}",
                  xy=(peak_t, peak),
                  xytext=(8, 6), textcoords="offset points",
                  fontsize=7.5, color="#4A7FD4",
                  arrowprops=dict(arrowstyle="-", color="#4A7FD4", lw=0.8))

    plt.savefig(out, dpi=DPI, bbox_inches="tight", facecolor="white")
    print(f"Saved -> {out}  ({len(subset):,} calls, {n_apps} functions, peak concurrency {peak})")
    plt.close(fig)

# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Gantt chart for function invocations")
    parser.add_argument("csv",       nargs="?", default=DEFAULT_CSV)
    parser.add_argument("--out",     default=DEFAULT_OUT)
    parser.add_argument("--start",   type=float, default=None,  help="Window start (seconds)")
    parser.add_argument("--end",     type=float, default=None,  help="Window end (seconds)")
    parser.add_argument("--min-dur", type=float, default=0.0,   help="Min call duration to show (s)")
    parser.add_argument("--top",     type=int,   default=TOP_N, help="Max functions to show")
    parser.add_argument("--functions-out", default=None,
                        help="Write the list of plotted function IDs to this text file (one per line)")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        sys.exit(f"Error: '{csv_path}' not found.")

    print(f"Loading {csv_path} ...")
    df = load(csv_path)
    print(f"  {len(df):,} rows, {df['app'].nunique()} unique functions")

    t_min = df["start"].min()
    t_max = df["end_timestamp"].max()
    print(f"  time span: {fmt_time(t_min)} -> {fmt_time(t_max)}  ({fmt_time(t_max - t_min)} total)")

    t_start = args.start if args.start is not None else t_min
    t_end   = args.end   if args.end   is not None else t_max

    apps = pick_apps(df, args.top, args.min_dur, t_start, t_end)
    if not apps:
        sys.exit("No functions match the filters.")

    if args.functions_out:
        functions_out_path = Path(args.functions_out)
        with open(functions_out_path, "w") as f:
            f.write("\n".join(apps) + "\n")
        print(f"Wrote {len(apps)} function IDs -> {functions_out_path}")

    plot(df, apps, t_start, t_end, args.min_dur, Path(args.out))

if __name__ == "__main__":
    main()