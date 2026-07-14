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

# Must match orchestrator.py's MIN_DURATION_MS exactly, or the two scripts
# will disagree on which invocations count as "real" (orchestrator drops
# anything at/under this duration before dispatching).
MIN_DURATION_MS = 0.01

def load(csv_path: Path, group_by: str = "function", normalize: bool = True) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip()
    df["start"] = df["end_timestamp"] - df["duration"]

    if normalize:
        # Mirror orchestrator.py's normalize_times(): shift so the earliest
        # invocation in the WHOLE file starts at t=0. WINDOW_START/WINDOW_END
        # passed to the orchestrator are in this same "seconds since trace
        # start" coordinate — without this shift, --start/--end here would
        # select a different slice of the trace than the orchestrator does.
        shift = df["start"].min()
        df["start"] -= shift
        df["end_timestamp"] = df["start"] + df["duration"]

    # Match orchestrator's apply_window(), which drops invocations at/under
    # this duration regardless of window bounds. Without this, the graph can
    # show more invocations in a window than the orchestrator actually runs.
    df = df[df["duration"] * 1000 > MIN_DURATION_MS].reset_index(drop=True)

    # "entity" is whatever we're grouping/lane-packing/plotting by.
    # group_by="function" -> one row per (app, func) pair (deployment granularity)
    # group_by="app"      -> legacy behavior, one row per app
    if group_by == "function":
        df["entity"] = df["app"].astype(str).str.strip() + "::" + df["func"].astype(str).str.strip()
    else:
        df["entity"] = df["app"].astype(str).str.strip()
    return df

def entity_label(entity: str, chars: int = LABEL_CHARS) -> str:
    """Short display label for an entity id, e.g. 'app123.../func456...'."""
    if "::" in entity:
        app_part, func_part = entity.split("::", 1)
        half = max(chars // 2, 4)
        return f"{app_part[:half]}/{func_part[:half]}"
    return entity[:chars]

def pick_entities(df: pd.DataFrame, top_n: int, min_dur: float,
                   t_start: float, t_end: float) -> list:
    # "start and end within the window" — must match orchestrator's
    # apply_window() containment check exactly (start >= start, end <= end).
    in_window = df[
        (df["duration"] >= min_dur) &
        (df["start"] >= t_start) &
        (df["end_timestamp"] <= t_end)
    ]
    counts = in_window.groupby("entity").size().sort_values(ascending=False)
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

def plot(df: pd.DataFrame, entities: list, t_start: float, t_end: float,
         min_dur: float, out: Path):

    mask = (
        df["entity"].isin(entities) &
        (df["duration"] >= min_dur) &
        (df["start"] >= t_start) &
        (df["end_timestamp"] <= t_end)
    )
    subset = df[mask].copy()
    subset["vis_start"] = subset["start"].clip(lower=t_start)
    subset["vis_end"]   = subset["end_timestamp"].clip(upper=t_end)
    subset["vis_dur"]   = (subset["vis_end"] - subset["vis_start"]).clip(lower=MIN_BAR_PX)

    color_map = {ent: COLORS[i % len(COLORS)] for i, ent in enumerate(entities)}

    # ── lane assignment per entity ────────────────────────────────────────────
    subset["lane"] = 0
    for ent, grp in subset.groupby("entity"):
        subset.loc[grp.index, "lane"] = assign_lanes(grp).values

    max_lanes_per_entity = subset.groupby("entity")["lane"].max() + 1   # Series

    # build y mapping: each entity gets as many sub-rows as it needs
    # y_base[ent] = bottom y of the entity's block; height = max_lanes
    entity_order = list(reversed(entities))   # bottom to top
    y_base = {}
    y_cursor = 0
    for ent in entity_order:
        y_base[ent] = y_cursor
        y_cursor += max_lanes_per_entity.get(ent, 1)

    total_rows = y_cursor
    BAR_H = 0.82   # bar height in row units

    # ── figure layout ─────────────────────────────────────────────────────────
    n_entities = len(entities)
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
    # zebra stripes per entity block
    for ent in entity_order:
        yb = y_base[ent]
        nh = max_lanes_per_entity.get(ent, 1)
        col = "#f5f5f5" if (entity_order.index(ent) % 2 == 0) else "white"
        ax_g.axhspan(yb - 0.5, yb + nh - 0.5, color=col, zorder=0)

    for _, row in subset.iterrows():
        ent  = row["entity"]
        lane = int(row["lane"])
        y    = y_base[ent] + lane
        col  = color_map[ent]
        bar  = mpatches.FancyArrow(
            row["vis_start"], y, row["vis_dur"], 0,
            width=BAR_H, head_width=0, head_length=0,
            length_includes_head=True,
            color=col, alpha=BAR_ALPHA, linewidth=0,
        )
        ax_g.add_patch(bar)

    # y-axis: one label per entity, centred on its block
    yticks, ylabels = [], []
    for ent in entity_order:
        yb = y_base[ent]
        nh = max_lanes_per_entity.get(ent, 1)
        yticks.append(yb + (nh - 1) / 2)
        ylabels.append(entity_label(ent))

    ax_g.set_yticks(yticks)
    ax_g.set_yticklabels(ylabels, fontsize=7.5, fontfamily="monospace")
    for tick, ent in zip(ax_g.get_yticklabels(), entity_order):
        tick.set_color(color_map[ent])

    # right-side call counts
    ax_r = ax_g.twinx()
    ax_r.set_ylim(ax_g.get_ylim())
    ax_r.set_yticks(yticks)
    counts_in_win = subset.groupby("entity").size()
    ax_r.set_yticklabels(
        [f"{counts_in_win.get(a, 0)} calls" for a in entity_order],
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
    unit = "functions" if subset["entity"].str.contains("::").any() else "apps"
    ax_g.set_title(
        f"{len(subset):,} calls  |  {n_entities} {unit}  |  "
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
    print(f"Saved -> {out}  ({len(subset):,} calls, {n_entities} entities, peak concurrency {peak})")
    plt.close(fig)

# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Gantt chart for function invocations")
    parser.add_argument("csv",       nargs="?", default=DEFAULT_CSV)
    parser.add_argument("--out",     default=DEFAULT_OUT)
    parser.add_argument("--start",   type=float, default=None,  help="Window start (seconds, same coordinate as WINDOW_START in orchestrator.py)")
    parser.add_argument("--end",     type=float, default=None,  help="Window end (seconds, same coordinate as WINDOW_END in orchestrator.py)")
    parser.add_argument("--min-dur", type=float, default=MIN_DURATION_MS / 1000,
                        help=f"Min call duration to show (s). Defaults to orchestrator's "
                             f"drop threshold ({MIN_DURATION_MS}ms) so counts match.")
    parser.add_argument("--top",     type=int,   default=TOP_N, help="Max entities to show")
    parser.add_argument("--group-by", choices=["function", "app"], default="function",
                        help="Group/plot by (app,func) pair [default] or by app alone")
    parser.add_argument("--no-normalize", action="store_true",
                        help="Use raw CSV timestamps as-is instead of shifting the trace "
                             "to start at t=0. Only use this if orchestrator.py's own "
                             "normalize_times() was also skipped/disabled — otherwise "
                             "--start/--end will select a different window than the "
                             "orchestrator ran.")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        sys.exit(f"Error: '{csv_path}' not found.")

    print(f"Loading {csv_path} ...")
    raw_min = pd.read_csv(csv_path, usecols=["end_timestamp", "duration"]).pipe(
        lambda d: (d["end_timestamp"] - d["duration"]).min()
    )
    df = load(csv_path, group_by=args.group_by, normalize=not args.no_normalize)
    if not args.no_normalize:
        print(f"  normalized: shifted timestamps by -{raw_min:.3f}s so trace starts at t=0 "
              f"(matches orchestrator.py's normalize_times)")
    if args.group_by == "function":
        print(f"  {len(df):,} rows, {df['entity'].nunique()} unique functions "
              f"across {df['app'].nunique()} apps")
    else:
        print(f"  {len(df):,} rows, {df['entity'].nunique()} unique apps")

    t_min = df["start"].min()
    t_max = df["end_timestamp"].max()
    print(f"  time span: {fmt_time(t_min)} -> {fmt_time(t_max)}  ({fmt_time(t_max - t_min)} total)")

    t_start = args.start if args.start is not None else t_min
    t_end   = args.end   if args.end   is not None else t_max

    entities = pick_entities(df, args.top, args.min_dur, t_start, t_end)
    if not entities:
        sys.exit("No entities match the filters.")

    plot(df, entities, t_start, t_end, args.min_dur, Path(args.out))

if __name__ == "__main__":
    main()