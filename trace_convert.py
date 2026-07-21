"""
convert_2019_to_2021_trace.py
──────────────────────────────
Converts one day of the Azure Functions 2019 trace into a flat, per-invocation
trace file in the style of the 2021 trace used by orchestrator.py / genk8s.py
(a CSV with columns: app, duration, end_timestamp).

WHY THIS IS NEEDED
The 2019 trace does not log individual invocations. Instead, for each
function it gives:
  (a) invocations_per_function_md.anon.dNN.csv
      — a count of invocations per *minute* of the day (columns "1".."1440")
  (b) function_durations_percentiles.anon.dNN.csv
      — a *summary* of that function's execution-time distribution for the
        day (Minimum, Maximum, Average, and the 1st/25th/50th/75th/99th
        percentiles of execution time)

To get something orchestrator.py can replay, we need one row per invocation
with a concrete end_timestamp and duration. That means we have to *invent*
per-invocation detail that's consistent with the aggregates we do have. This
script does that using three different random-sampling techniques, chosen
deliberately for what each step needs:

  1. BINOMIAL THINNING to subsample invocation volume
     ---------------------------------------------------
     Each minute bucket holds a count `n` of invocations, not a list of
     invocations. To keep only `sample_rate` (e.g. 0.5%) of them while
     preserving the *shape* of the load (bursty minutes stay relatively
     bursty, quiet minutes stay quiet), the correct operation is to keep
     each of the n invocations independently with probability p, i.e. draw
     the kept count as Binomial(n, p).
     This is the discrete version of "Poisson thinning": if you thin a
     Poisson process by keeping each arrival independently with probability
     p, the result is itself a Poisson process with rate p times the
     original. So per-bucket Binomial sampling is the statistically
     faithful way to subsample a counts-per-bin trace — as opposed to just
     computing round(n * p), which throws away all the natural sampling
     variance and would make every proportionally-scaled minute look
     artificially smooth.

  2. UNIFORM JITTER to place invocations within their minute
     ---------------------------------------------------------
     We only know "k invocations finished sometime in minute m", not their
     exact second. With no further information, the maximum-entropy
     (least-assumption) choice is to treat arrivals within the minute as a
     homogeneous process and place each one uniformly at random in that
     60-second window. This is the standard way to "disaggregate" count-per-
     bin data into a continuous timeline.

  3. INVERSE-TRANSFORM SAMPLING (via linear interpolation) for durations
     ---------------------------------------------------------------------
     We don't have per-invocation durations, only 7 quantiles of the day's
     distribution for that function: 0th (Minimum), 1st, 25th, 50th, 75th,
     99th, 100th (Maximum). Treating these as knots of the inverse-CDF, we
     draw a uniform "target percentile" u ~ Uniform(0, 100) and linearly
     interpolate between the two bracketing knots to get a duration. This
     reproduces the median, skew, and tail-heaviness implied by the
     quantiles, which a simpler choice (e.g. always using the Average, or
     assuming a Normal distribution) would not.

COLUMNS: "app" = HashApp, "func" = HashFunction
     Each output row carries both ids: "app" is the 2019 dataset's
     HashApp (Azure Application-level id) and "func" is HashFunction
     (the specific function within that app). Downstream code that wants
     per-function granularity (matching the earlier "per function, not
     per app" request) should key off "func"; "app" is kept alongside it
     for grouping/reference.

USAGE
    python convert_2019_to_2021_trace.py \\
        --invocations invocations_per_function_md.anon.d01.csv \\
        --durations   function_durations_percentiles.anon.d01.csv \\
        --output      azurefunctions_2019_day1_0.5pct.txt \\
        --sample-rate 0.005 \\
        --seed 42
"""

import argparse
import csv
import sys

import numpy as np

MINUTES_PER_DAY = 1440
SECONDS_PER_MINUTE = 60.0

# The percentile columns available in function_durations_percentiles*.csv,
# in increasing order. We anchor the 0th/100th percentile to Minimum/Maximum
# rather than percentile_Average_0 because the dataset's own README flags
# percentile_Average_0 as occasionally mis-logged as 0 — Minimum/Maximum are
# the true extremes and safer tail anchors.
PERCENTILE_COLUMNS = [
    (0.0, "Minimum"),
    (1.0, "percentile_Average_1"),
    (25.0, "percentile_Average_25"),
    (50.0, "percentile_Average_50"),
    (75.0, "percentile_Average_75"),
    (99.0, "percentile_Average_99"),
    (100.0, "Maximum"),
]


def load_duration_knots(path):
    """
    Build, for each (HashOwner, HashApp, HashFunction), a pair of numpy
    arrays (percentiles, execution_time_ms) describing that function's
    execution-time distribution for the day. These are the knots used for
    inverse-transform sampling (np.interp) later.
    """
    knots = {}
    skipped_zero_count = 0
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                count = int(row["Count"])
            except (KeyError, ValueError):
                continue
            if count <= 0:
                skipped_zero_count += 1
                continue

            key = (row["HashOwner"], row["HashApp"], row["HashFunction"])
            try:
                values = [float(row[col]) for _, col in PERCENTILE_COLUMNS]
            except (KeyError, ValueError):
                continue

            # Enforce monotonic non-decreasing percentiles. Logged summary
            # stats can occasionally be slightly non-monotonic (e.g. due to
            # how the 30s-window averages get aggregated); np.interp
            # requires non-decreasing xp, so we clamp rather than drop the
            # function entirely.
            cleaned = []
            running_max = float("-inf")
            for v in values:
                v = max(v, running_max)
                cleaned.append(v)
                running_max = v

            percentiles = np.array([p for p, _ in PERCENTILE_COLUMNS], dtype=np.float64)
            durations_ms = np.array(cleaned, dtype=np.float64)
            knots[key] = (percentiles, durations_ms)

    print(f"  Loaded duration distributions for {len(knots)} functions "
          f"(skipped {skipped_zero_count} with Count<=0)")
    return knots


def process_invocations(invocations_path, duration_knots, sample_rate, rng):
    """
    Stream through the per-minute invocation-count file. For each function
    row (that also has a duration distribution), thin its per-minute counts,
    then generate concrete (end_timestamp, duration_ms) pairs for every
    invocation that survives thinning.

    Returns a list of (app_id, duration_seconds, end_timestamp_seconds).
    """
    minute_cols = [str(m) for m in range(1, MINUTES_PER_DAY + 1)]
    # Absolute second-of-day at the *start* of each minute bucket, used to
    # offset the within-minute uniform jitter.
    minute_starts = np.arange(MINUTES_PER_DAY, dtype=np.float64) * SECONDS_PER_MINUTE

    events = []
    total_functions = 0
    matched_functions = 0
    total_original_invocations = 0
    total_sampled_invocations = 0

    with open(invocations_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_functions += 1
            key = (row["HashOwner"], row["HashApp"], row["HashFunction"])
            knots = duration_knots.get(key)
            if knots is None:
                # No duration distribution for this function (e.g. Count<=0
                # in the durations file, or missing entirely) — can't
                # synthesize plausible durations, so skip it.
                continue
            matched_functions += 1
            percentiles, durations_ms = knots

            counts = np.array([int(row[c]) for c in minute_cols], dtype=np.int64)
            total_original_invocations += int(counts.sum())
            if counts.sum() == 0:
                continue

            # (1) Binomial thinning — see module docstring.
            kept_counts = rng.binomial(counts, sample_rate)
            total_kept = int(kept_counts.sum())
            if total_kept == 0:
                continue
            total_sampled_invocations += total_kept

            # Expand "k kept invocations in minute m" into one row per event.
            minute_idx_per_event = np.repeat(np.arange(MINUTES_PER_DAY), kept_counts)

            # (2) Uniform jitter within each minute.
            jitter = rng.uniform(0.0, SECONDS_PER_MINUTE, size=total_kept)
            end_timestamps = minute_starts[minute_idx_per_event] + jitter

            # (3) Inverse-transform sampling of durations via linear
            # interpolation through this function's percentile knots.
            u = rng.uniform(0.0, 100.0, size=total_kept)
            sampled_durations_ms = np.interp(u, percentiles, durations_ms)

            hash_app = row["HashApp"]
            hash_func = row["HashFunction"]
            for ts, dur_ms in zip(end_timestamps, sampled_durations_ms):
                events.append((hash_app, hash_func, dur_ms / 1000.0, ts))

    print(f"  Functions in invocation file : {total_functions}")
    print(f"  Functions matched to a duration distribution: {matched_functions}")
    print(f"  Total invocations (pre-sample) : {total_original_invocations:,}")
    print(f"  Total invocations (post-sample): {total_sampled_invocations:,}")
    if total_original_invocations:
        achieved_rate = total_sampled_invocations / total_original_invocations
        print(f"  Requested sample rate: {sample_rate:.4%}   "
              f"Achieved: {achieved_rate:.4%}")

    return events


def write_trace(events, output_path):
    """
    Write events sorted by end_timestamp, in the 2021-style flat CSV format,
    plus a separate 'func' column carrying HashFunction (per the updated
    naming: 'app' holds HashApp, 'func' holds HashFunction).
    """
    events.sort(key=lambda e: e[3])
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["app", "func", "duration", "end_timestamp"])
        for hash_app, hash_func, duration_s, end_ts in events:
            writer.writerow([hash_app, hash_func, f"{duration_s:.6f}", f"{end_ts:.6f}"])
    print(f"  Wrote {len(events):,} rows to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Convert 2019 Azure Functions trace (day) into a 2021-style flat invocation trace."
    )
    parser.add_argument("--invocations", default="invocations_per_function_md.anon.d01.csv",
                         help="Path to the per-minute invocation-count file for one day")
    parser.add_argument("--durations", default="function_durations_percentiles.anon.d01.csv",
                         help="Path to the per-function duration-percentiles file for the same day")
    parser.add_argument("--output", default="azurefunctions_2019_day1_sampled.txt",
                         help="Output path for the generated flat trace")
    parser.add_argument("--sample-rate", type=float, default=0.005,
                         help="Fraction of invocations to keep (default 0.005 = 0.5%%)")
    parser.add_argument("--seed", type=int, default=42,
                         help="Random seed, for reproducibility")
    args = parser.parse_args()

    if not (0 < args.sample_rate <= 1):
        sys.exit("--sample-rate must be in (0, 1]")

    rng = np.random.default_rng(args.seed)

    print(f"Loading duration distributions from {args.durations} ...")
    duration_knots = load_duration_knots(args.durations)

    print(f"\nProcessing invocation counts from {args.invocations} "
          f"(sample_rate={args.sample_rate:.4%}, seed={args.seed}) ...")
    events = process_invocations(args.invocations, duration_knots, args.sample_rate, rng)

    print(f"\nWriting output trace to {args.output} ...")
    write_trace(events, args.output)


if __name__ == "__main__":
    main()