"""
find_window.py — Find a time window suitable for testing Kubernetes migration policies.

Looks for windows where:
  - Many apps are active (have calls starting and ending within the window)
  - Each active app has a moderate call count (target ~10, configurable)
  - No single app dominates with hundreds/thousands of calls

Usage:
    python find_window.py data.csv
    python find_window.py data.csv --window 3600 --target 10 --max-calls 50 --top-results 5
    python find_window.py data.csv --window 600 --target 10 --max-calls 100 --out results.txt
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd


# ── defaults ──────────────────────────────────────────────────────────────────
DEFAULT_WINDOW    = 600    # window size in seconds to evaluate
TARGET_CALLS      = 5      # ideal calls-per-app within the window
MAX_CALLS         = 100     # exclude apps with more than this many calls (the "hundreds" filter)
MIN_APPS          = 5       # window must have at least this many qualifying apps
TOP_RESULTS       = 5       # how many candidate windows to print
STRIDE_DIVISOR    = 10      # stride = window / this  (controls scan resolution)
# ──────────────────────────────────────────────────────────────────────────────


def load(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    df["start"] = df["end_timestamp"] - df["duration"]
    return df


def fmt(seconds: float) -> str:
    s = abs(seconds)
    if s < 120:    return f"{seconds:.2f}s"
    if s < 7200:   return f"{seconds/60:.2f}m"
    if s < 86400:  return f"{seconds/3600:.3f}h"
    return f"{seconds/86400:.4f}d"


def score_window(counts: pd.Series, target: int, max_calls: int) -> dict:
    """
    Score a single window given per-app call counts (already filtered to fully-contained calls).

    Good windows have:
      - Many apps with counts close to `target`
      - No app exceeding `max_calls`
      - Low variance across app call counts (balanced load)

    Returns a dict of metrics, with a scalar `score` (higher = better).
    """
    # drop apps that exceed the max threshold — they're the "dominant" ones we want to avoid
    filtered = counts[counts <= max_calls]

    n_apps = len(filtered)
    if n_apps == 0:
        return {"score": -np.inf, "n_apps": 0, "mean_calls": 0,
                "max_calls": 0, "std_calls": 0, "dominant_apps": len(counts)}

    dominant_apps = len(counts) - n_apps
    mean_calls    = filtered.mean()
    std_calls     = filtered.std(ddof=0) if n_apps > 1 else 0.0
    max_c         = filtered.max()
    total_calls   = filtered.sum()

    # proximity to target: penalise being far from target call count
    target_penalty = abs(mean_calls - target) / target

    # reward more apps, penalise high variance and dominant outliers
    score = (
        n_apps                        # more apps = better
        - target_penalty * n_apps     # penalise distance from target
        - std_calls * 0.5             # penalise uneven load
        - dominant_apps * 3           # strongly penalise having dominant apps
    )

    return {
        "score":         score,
        "n_apps":        n_apps,
        "mean_calls":    mean_calls,
        "std_calls":     std_calls,
        "max_calls_app": max_c,
        "total_calls":   total_calls,
        "dominant_apps": dominant_apps,
    }


def scan(df: pd.DataFrame, window: float, target: int, max_calls: int,
         min_apps: int, stride: float, min_dur: float) -> pd.DataFrame:
    t_min = df["start"].min()
    t_max = df["end_timestamp"].max()

    if window >= (t_max - t_min):
        sys.exit(f"Window ({fmt(window)}) is >= total dataset span ({fmt(t_max - t_min)}). "
                 f"Choose a smaller --window.")

    candidates = []
    t = t_min
    total_steps = int((t_max - window - t_min) / stride) + 1
    step = 0

    while t + window <= t_max:
        step += 1
        if step % max(1, total_steps // 20) == 0:
            pct = 100 * (t - t_min) / (t_max - window - t_min + 1e-9)
            print(f"  scanning... {pct:.0f}%", end="\r", flush=True)

        w_end = t + window
        # fully-contained calls only, meeting min duration
        mask = (df["start"] >= t) & (df["end_timestamp"] <= w_end) & (df["duration"] >= min_dur)
        inside = df[mask]

        if len(inside) == 0:
            t += stride
            continue

        counts = inside.groupby("app").size()

        if len(counts[counts <= max_calls]) < min_apps:
            t += stride
            continue

        metrics = score_window(counts, target, max_calls)
        metrics["t_start"] = t
        metrics["t_end"]   = w_end
        candidates.append(metrics)

        t += stride

    print()  # newline after progress
    return pd.DataFrame(candidates).sort_values("score", ascending=False)


def print_results(results: pd.DataFrame, top_n: int, window: float,
                  target: int, max_calls: int, min_dur: float, out_path: Path | None):
    lines = []
    lines.append("=" * 64)
    lines.append(f"Top {min(top_n, len(results))} candidate windows")
    lines.append(f"  window size : {fmt(window)}")
    lines.append(f"  target calls: ~{target} per app")
    lines.append(f"  max calls   : {max_calls} per app (above = dominant)")
    lines.append(f"  min duration: {min_dur}s")
    lines.append("=" * 64)

    for rank, (_, row) in enumerate(results.head(top_n).iterrows(), 1):
        lines.append(f"\nRank #{rank}  (score: {row['score']:.2f})")
        lines.append(f"  window      : {fmt(row['t_start'])}  ->  {fmt(row['t_end'])}")
        lines.append(f"  start (s)   : {row['t_start']:.4f}")
        lines.append(f"  end   (s)   : {row['t_end']:.4f}")
        lines.append(f"  apps active : {int(row['n_apps'])}")
        lines.append(f"  mean calls  : {row['mean_calls']:.1f}  (target ~{target})")
        lines.append(f"  std  calls  : {row['std_calls']:.1f}")
        lines.append(f"  max  calls  : {int(row['max_calls_app'])}")
        lines.append(f"  dominant    : {int(row['dominant_apps'])} app(s) exceeded {max_calls} calls (excluded)")
        lines.append(f"  total calls : {int(row['total_calls'])}")
        lines.append(f"\n  graph it    : python graph_invocations.py data.csv "
                     f"--start {row['t_start']:.2f} --end {row['t_end']:.2f}"
                     + (f" --min-dur {min_dur}" if min_dur > 0 else ""))

    lines.append("\n" + "=" * 64)
    output = "\n".join(lines)
    print(output)

    if out_path:
        out_path.write_text(output)
        print(f"\nResults written -> {out_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Find a time window suitable for Kubernetes migration policy testing"
    )
    parser.add_argument("csv",            nargs="?",    default="invocations.csv")
    parser.add_argument("--window",       type=float,   default=DEFAULT_WINDOW,
                        help=f"Window size in seconds (default: {DEFAULT_WINDOW})")
    parser.add_argument("--target",       type=int,     default=TARGET_CALLS,
                        help=f"Target calls per app (default: {TARGET_CALLS})")
    parser.add_argument("--max-calls",    type=int,     default=MAX_CALLS,
                        help=f"Exclude apps with more calls than this (default: {MAX_CALLS})")
    parser.add_argument("--min-apps",     type=int,     default=MIN_APPS,
                        help=f"Min qualifying apps required (default: {MIN_APPS})")
    parser.add_argument("--top-results",  type=int,     default=TOP_RESULTS,
                        help=f"How many candidates to show (default: {TOP_RESULTS})")
    parser.add_argument("--min-dur",      type=float,   default=0.0,
                        help="Minimum call duration in seconds to include (default: 0)")
    parser.add_argument("--stride",       type=float,   default=None,
                        help="Scan step size in seconds (default: window / 10)")
    parser.add_argument("--out",          type=Path,    default=None,
                        help="Save results to a text file")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        sys.exit(f"Error: '{csv_path}' not found.")

    print(f"Loading {csv_path} ...")
    df = load(csv_path)
    t_min = df["start"].min()
    t_max = df["end_timestamp"].max()
    print(f"  {len(df):,} rows  |  {df['app'].nunique()} apps  |  "
          f"span {fmt(t_max - t_min)}")

    stride = args.stride if args.stride is not None else args.window / STRIDE_DIVISOR
    print(f"\nScanning with window={fmt(args.window)}, stride={fmt(stride)} ...")

    results = scan(df, args.window, args.target, args.max_calls, args.min_apps, stride, args.min_dur)

    if results.empty:
        print("No windows found matching the criteria. "
              "Try a larger --window, lower --min-apps, higher --max-calls, or lower --min-dur.")
        sys.exit(1)

    print(f"  {len(results):,} candidate windows evaluated\n")
    print_results(results, args.top_results, args.window,
                  args.target, args.max_calls, args.min_dur, args.out)


if __name__ == "__main__":
    main()