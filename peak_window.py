"""
peak_window.py
--------------
Find the sliding time window of a given duration that contains the most
function-call *starts* (or ends, configurable) in the Azure Functions trace.

Usage:
    python peak_window.py <csv_file> <window_size>

Arguments:
    csv_file      Path to the CSV file with columns:
                      app, func, end_timestamp, duration
    window_size   Window length in seconds (e.g. 60, 300, 3600).
                  Accepts floats.

Output:
    Prints the window boundaries, call count, and a breakdown of the top
    apps/functions active during that window.

Algorithm:
    Each function call has a *start* time = end_timestamp - duration.
    We use a sweep-line (sorted events + two-pointer) approach that runs
    in O(N log N) — fast enough for millions of rows.
"""

import sys
import argparse
import pandas as pd
import numpy as np


def load_trace(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    required = {"app", "func", "end_timestamp", "duration"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV is missing columns: {missing}")

    df = df[df["duration"] > 0].copy()
    df["start_time"] = df["end_timestamp"] - df["duration"]
    return df


def find_peak_window(starts: np.ndarray, ends: np.ndarray, window: float):
    """
    Find the window [t, t+window] that contains the most calls where BOTH
    the start and end of the call fall within the window.

    Strategy:
      - Calls are sorted by start time.
      - For each candidate window anchored at starts[left], every call from
        left onward that starts within the window (starts[i] <= starts[left] + window)
        is a *candidate*. Among those, we only count the ones whose end also
        fits (ends[i] <= starts[left] + window).
      - We binary-search for the rightmost candidate start, then count how
        many of those candidates have a conforming end using a sorted
        structure built incrementally.

    Runs in O(N log N).

    Returns (best_left_idx, best_right_idx_exclusive, count) where
    best_right_idx_exclusive is the first index whose start exceeds the window.
    The actual qualifying subset within [left, right) may be smaller if some
    calls end outside the window.
    """
    from bisect import bisect_left, bisect_right, insort

    n = len(starts)
    best_count = 0
    best_left = 0

    # Maintain a sorted list of end times for calls currently in the
    # candidate set (start falls within the window anchored at starts[left]).
    candidate_ends: list[float] = []
    right = 0  # next index to add to the candidate set

    for left in range(n):
        win_end = starts[left] + window

        # Expand: add all calls whose start is within this window
        while right < n and starts[right] <= win_end:
            insort(candidate_ends, ends[right])
            right += 1

        # Count how many of the current candidates also end within the window
        count = bisect_right(candidate_ends, win_end)

        if count > best_count:
            best_count = count
            best_left = left

        # Shrink: remove the call at `left` before advancing.
        # Must delete by position (not value) to avoid removing a duplicate
        # end timestamp that belongs to a different call still in the window.
        pos = bisect_left(candidate_ends, ends[left])
        del candidate_ends[pos]

    return best_left, right, best_count


def main():
    parser = argparse.ArgumentParser(
        description="Find the time window with the most Azure Function calls."
    )
    parser.add_argument("csv_file", help="Path to the trace CSV")
    parser.add_argument(
        "window_size",
        type=float,
        help="Window duration in seconds (e.g. 60, 300, 3600)",
    )
    parser.add_argument(
        "--top", type=int, default=5,
        help="Number of top apps/functions to show in breakdown (default: 5)"
    )
    args = parser.parse_args()

    print(f"Loading trace from: {args.csv_file}")
    df = load_trace(args.csv_file)
    print(f"  Loaded {len(df):,} function calls across "
          f"{df['app'].nunique():,} apps and {df['func'].nunique():,} functions.")

    # Sort by start time for the two-pointer sweep
    df_sorted = df.sort_values("start_time").reset_index(drop=True)
    starts = df_sorted["start_time"].to_numpy()
    ends   = df_sorted["end_timestamp"].to_numpy()

    total_span = starts[-1] - starts[0]
    print(f"  Trace spans {total_span:,.1f} s "
          f"({total_span/3600:.2f} h)  |  window = {args.window_size:,.1f} s\n")

    if args.window_size > total_span:
        print("Warning: window size exceeds the total trace span. "
              "The entire trace fits in one window.")

    left_idx, right_idx, count = find_peak_window(starts, ends, args.window_size)

    win_start = starts[left_idx]
    win_end   = win_start + args.window_size

    print("=" * 60)
    print(f"  Peak window found!")
    print(f"  Window start : {win_start:.4f} s")
    print(f"  Window end   : {win_end:.4f} s  (= start + {args.window_size} s)")
    print(f"  Calls inside : {count:,}")
    print("=" * 60)

    # Pull only rows where BOTH start and end fall within the window
    window_df = df_sorted.iloc[left_idx:right_idx]
    window_df = window_df[window_df["end_timestamp"] <= win_end]

    print(f"\nTop {args.top} apps by call count in this window:")
    top_apps = (
        window_df.groupby("app")
        .size()
        .sort_values(ascending=False)
        .head(args.top)
    )
    for app, cnt in top_apps.items():
        print(f"  {app[:20]}...  {cnt:,} calls")

    print(f"\nTop {args.top} functions by call count in this window:")
    top_funcs = (
        window_df.groupby("func")
        .size()
        .sort_values(ascending=False)
        .head(args.top)
    )
    for func, cnt in top_funcs.items():
        print(f"  {func[:20]}...  {cnt:,} calls")

    print(f"\nCall duration stats inside the window (seconds):")
    stats = window_df["duration"].describe()
    for stat, val in stats.items():
        print(f"  {stat:8s}: {val:.4f}")


if __name__ == "__main__":
    main()