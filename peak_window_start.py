"""
peak_window_start.py
--------------------
Find the sliding time window of a given duration that contains the most
function calls whose START time falls within the window.

Usage:
    python peak_window_start.py <csv_file> <window_size>

Arguments:
    csv_file      Path to the CSV file with columns:
                      app, func, end_timestamp, duration
    window_size   Window length in seconds (e.g. 60, 300, 3600).
                  Accepts floats.
"""

import sys
import argparse
import numpy as np
import pandas as pd


def load_trace(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    required = {"app", "func", "end_timestamp", "duration"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV is missing columns: {missing}")

    df = df[df["duration"] > 0].copy()
    df["start_time"] = df["end_timestamp"] - df["duration"]
    return df


def find_peak_window(starts: np.ndarray, window: float):
    """
    Two-pointer sweep over sorted start times.
    A call counts if its start falls within [t, t + window].
    Returns (best_left_idx, best_right_idx_exclusive, count).
    """
    n = len(starts)
    best_count = 0
    best_left = 0
    right = 0

    for left in range(n):
        while right < n and starts[right] - starts[left] <= window:
            right += 1
        count = right - left
        if count > best_count:
            best_count = count
            best_left = left

    return best_left, best_left + best_count, best_count


def main():
    parser = argparse.ArgumentParser(
        description="Find the time window with the most Azure Function call starts."
    )
    parser.add_argument("csv_file", help="Path to the trace CSV")
    parser.add_argument(
        "window_size",
        type=float,
        help="Window duration in seconds (e.g. 60, 300, 3600)",
    )
    parser.add_argument(
        "--top", type=int, default=5,
        help="Number of top apps/functions to show in breakdown (default: 5)",
    )
    args = parser.parse_args()

    print(f"Loading trace from: {args.csv_file}")
    df = load_trace(args.csv_file)
    print(f"  Loaded {len(df):,} function calls across "
          f"{df['app'].nunique():,} apps and {df['func'].nunique():,} functions.")

    df_sorted = df.sort_values("start_time").reset_index(drop=True)
    starts = df_sorted["start_time"].to_numpy()

    total_span = starts[-1] - starts[0]
    print(f"  Trace spans {total_span:,.1f} s "
          f"({total_span/3600:.2f} h)  |  window = {args.window_size:,.1f} s\n")

    left_idx, right_idx, count = find_peak_window(starts, args.window_size)

    win_start = starts[left_idx]
    win_end   = win_start + args.window_size

    print("=" * 60)
    print(f"  Peak window found!")
    print(f"  Window start : {win_start:.4f} s")
    print(f"  Window end   : {win_end:.4f} s  (= start + {args.window_size} s)")
    print(f"  Calls inside : {count:,}")
    print("=" * 60)

    window_df = df_sorted.iloc[left_idx:right_idx]

    print(f"\nTop {args.top} apps by call count in this window:")
    for app, cnt in (
        window_df.groupby("app").size().sort_values(ascending=False).head(args.top).items()
    ):
        print(f"  {app[:20]}...  {cnt:,} calls")

    print(f"\nTop {args.top} functions by call count in this window:")
    for func, cnt in (
        window_df.groupby("func").size().sort_values(ascending=False).head(args.top).items()
    ):
        print(f"  {func[:20]}...  {cnt:,} calls")

    print(f"\nCall duration stats inside the window (seconds):")
    for stat, val in window_df["duration"].describe().items():
        print(f"  {stat:8s}: {val:.4f}")


if __name__ == "__main__":
    main()
