#!/usr/bin/env python3
"""
estimate_tardis_params.py
─────────────────────────
Reads the Azure Functions invocation trace and estimates TARDIS hyperparameters
alpha and beta from a specified time window.

Usage:
    python estimate_tardis_params.py \
        --trace path/to/trace.csv \
        --start 0.0 \
        --end 180.0 \
        --scrape-interval 1.0 \
        --activity-threshold 0.0 \
        --window-w 10.0 \
        --p-min 0.3 \
        --freq-min 5

Alpha estimation: based on median number of active scrape ticks per app.
Beta estimation:  based on median gap (in ticks) between active windows of
                  related app pairs, where "related" means P(j active within
                  w seconds | i just became active) > p_min.
"""

import argparse
import csv
import math
from collections import defaultdict

import numpy as np


# ── load trace ────────────────────────────────────────────────────────────────

def load_trace(path: str, t_start: float, t_end: float):
    """
    Returns list of (app_id, inv_start, inv_end) for invocations that both
    start and end within [t_start, t_end].
    """
    invocations = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            end = float(row["end_timestamp"])
            dur = float(row["duration"])
            start = end - dur
            if start >= t_start and end <= t_end:
                invocations.append((row["app"], start, end))
    invocations.sort(key=lambda x: x[1])
    print(f"Loaded {len(invocations)} invocations from {len({r[0] for r in invocations})} apps "
          f"in window [{t_start:.1f}, {t_end:.1f}]s")
    return invocations


# ── build per-app active tick sets ───────────────────────────────────────────

def build_active_ticks(
    invocations: list,
    t_start: float,
    t_end: float,
    scrape_interval: float,
    activity_threshold: float,  # reserved for CPU-based filtering; 0.0 here
                                # since trace gives us invocation directly
) -> dict[str, set[int]]:
    """
    For each app, returns the set of scrape tick indices during which it has
    at least one active invocation.

    Tick index k covers the interval [t_start + k*dt, t_start + (k+1)*dt).
    An invocation is active during tick k if its [inv_start, inv_end) overlaps
    that interval.
    """
    dt = scrape_interval
    n_ticks = int(math.ceil((t_end - t_start) / dt))

    active: dict[str, set[int]] = defaultdict(set)

    for app, inv_start, inv_end in invocations:
        # first tick whose start <= inv_start
        k_start = int((inv_start - t_start) / dt)
        # last tick whose start < inv_end
        k_end   = int(math.ceil((inv_end - t_start) / dt))
        k_start = max(0, k_start)
        k_end   = min(n_ticks, k_end)
        for k in range(k_start, k_end):
            active[app].add(k)

    return dict(active)


# ── alpha estimation ──────────────────────────────────────────────────────────

def estimate_alpha(active_ticks: dict[str, set[int]]) -> tuple[float, dict]:
    """
    alpha = 1 / sqrt(f_p50)
    where f_p50 is the median number of active ticks per app.
    """
    counts = {app: len(ticks) for app, ticks in active_ticks.items()}
    values = sorted(counts.values())

    stats = {
        "per_app_active_ticks": counts,
        "min":    values[0],
        "p25":    values[len(values) // 4],
        "median": values[len(values) // 2],
        "p75":    values[3 * len(values) // 4],
        "max":    values[-1],
        "mean":   sum(values) / len(values),
    }

    f_p50 = stats["median"]
    alpha = 1.0 / math.sqrt(f_p50) if f_p50 > 0 else 1.0
    return alpha, stats


# ── related pair detection and beta estimation ────────────────────────────────

def find_related_pairs(
    active_ticks: dict[str, set[int]],
    window_w_ticks: int,
    p_min: float,
    freq_min: int,
) -> list[tuple[str, str, float, list[int]]]:
    """
    For each ordered pair (i, j), estimate P(j active within w ticks | i
    just became active). A pair is "related" if:
      - both apps have >= freq_min active ticks
      - P(j|i) > p_min AND P(i|j) > p_min  (symmetric filter)

    "i just became active" = tick k where k is in active[i] but k-1 is not
    (rising edge / bout start).

    Returns list of (app_i, app_j, conditional_prob, list_of_gap_ticks).
    """
    apps = [a for a, t in active_ticks.items() if len(t) >= freq_min]
    print(f"  Apps with >= {freq_min} active ticks: {len(apps)}")

    # precompute bout starts per app
    bout_starts: dict[str, list[int]] = {}
    for app in apps:
        ticks = active_ticks[app]
        starts = sorted(k for k in ticks if (k - 1) not in ticks)
        bout_starts[app] = starts

    related = []
    n = len(apps)
    pair_count = 0

    for ii, app_i in enumerate(apps):
        starts_i = bout_starts[app_i]
        ticks_j_sets = {app_j: active_ticks[app_j] for app_j in apps if app_j != app_i}

        for app_j, ticks_j in ticks_j_sets.items():
            if app_j <= app_i:  # avoid duplicates
                continue
            pair_count += 1

            starts_j = bout_starts[app_j]

            # P(j active within w | i starts)
            hits_ij = 0
            gaps_ij = []
            for k in starts_i:
                window = range(k, k + window_w_ticks + 1)
                hit = any(t in ticks_j for t in window)
                if hit:
                    hits_ij += 1
                    nearest = min(
                        (abs(k - sj) for sj in starts_j if abs(k - sj) <= window_w_ticks),
                        default=None
                    )
                    if nearest is not None:
                        gaps_ij.append(nearest)

            p_ij = hits_ij / len(starts_i) if starts_i else 0.0

            # P(i active within w | j starts) — symmetric check
            hits_ji = sum(
                1 for k in starts_j
                if any(t in active_ticks[app_i] for t in range(k, k + window_w_ticks + 1))
            )
            p_ji = hits_ji / len(starts_j) if starts_j else 0.0

            if p_ij > p_min and p_ji > p_min:
                related.append((app_i, app_j, min(p_ij, p_ji), gaps_ij))

    print(f"  Evaluated {pair_count} pairs, found {len(related)} related pairs")
    return related


def estimate_beta(
    related_pairs: list,
    fallback_w_ticks: int,
    scrape_interval: float,
) -> tuple[float, dict]:
    """
    beta = 1 / (2 * g_p50)
    where g_p50 is the median gap in ticks between related bout starts.
    Falls back to w/2 if no related pairs found.
    """
    all_gaps = []
    for _, _, _, gaps in related_pairs:
        all_gaps.extend(gaps)

    if not all_gaps:
        print("  WARNING: no related pairs found; using fallback g_p50 = window_w / 2")
        g_p50 = max(fallback_w_ticks / 2, 1)
        stats = {"n_gaps": 0, "g_p50": g_p50, "fallback": True}
    else:
        all_gaps_sorted = sorted(all_gaps)
        g_p50 = all_gaps_sorted[len(all_gaps_sorted) // 2]
        g_p50 = max(g_p50, 1)  # floor at 1 tick to avoid division by zero
        stats = {
            "n_gaps":  len(all_gaps),
            "min":     all_gaps_sorted[0],
            "p25":     all_gaps_sorted[len(all_gaps_sorted) // 4],
            "g_p50":   g_p50,
            "p75":     all_gaps_sorted[3 * len(all_gaps_sorted) // 4],
            "max":     all_gaps_sorted[-1],
            "mean":    sum(all_gaps) / len(all_gaps),
            "fallback": False,
        }

    beta = 1.0 / (2.0 * g_p50)
    # convert g_p50 to seconds for display
    stats["g_p50_seconds"] = g_p50 * scrape_interval
    return beta, stats


# ── tau prior ─────────────────────────────────────────────────────────────────

def estimate_tau_prior(
    active_ticks: dict[str, set[int]],
    related_pairs: list,
    n_unrelated_sample: int = 200,
) -> dict:
    """
    Simulate TARDIS offline on the active tick sequence and compute cosine
    similarity distributions for related vs unrelated pairs.

    Returns summary statistics to inform tau selection before implementation.
    Uses single embedding process (k=1) for speed; real implementation uses k=8.
    """
    import random

    # replay events in tick order
    all_apps = list(active_ticks.keys())
    if not all_apps:
        return {}

    max_tick = max(max(t) for t in active_ticks.values())
    d = max(8, int(math.ceil(math.log2(len(all_apps) + 1))))

    # precompute sorted tick lists per app for fast lookup
    sorted_ticks: dict[str, list[int]] = {
        app: sorted(ticks) for app, ticks in active_ticks.items()
    }

    # --- TARDIS simulation (single process) ---
    rng = np.random.default_rng(42)
    alpha_sim = 0.1  # rough mid-range for simulation
    beta_sim  = 0.05

    context = rng.standard_normal(d)
    context /= np.linalg.norm(context)

    embeddings: dict[str, np.ndarray] = {}

    # build tick -> active apps index
    tick_to_apps: dict[int, list[str]] = defaultdict(list)
    for app, ticks in active_ticks.items():
        for t in ticks:
            tick_to_apps[t].append(app)

    for tick in range(max_tick + 1):
        active_apps = tick_to_apps.get(tick, [])
        for app in active_apps:
            if app not in embeddings:
                e = rng.standard_normal(d)
                embeddings[app] = e / np.linalg.norm(e)
            e = embeddings[app]
            embeddings[app] = (
                math.sqrt(1 - alpha_sim) * e + math.sqrt(alpha_sim) * context
            )
            norm = np.linalg.norm(embeddings[app])
            if norm > 0:
                embeddings[app] /= norm

        # advance context
        noise = rng.standard_normal(d)
        context = (
            math.sqrt(1 - beta_sim) * context + math.sqrt(beta_sim) * noise
        )
        norm = np.linalg.norm(context)
        if norm > 0:
            context /= norm

    def cosine(a, b):
        na, nb = np.linalg.norm(a), np.linalg.norm(b)
        if na == 0 or nb == 0:
            return 0.0
        return float(np.dot(a, b) / (na * nb))

    embedded_apps = set(embeddings.keys())

    # related pair similarities
    related_sims = []
    for app_i, app_j, _, _ in related_pairs:
        if app_i in embedded_apps and app_j in embedded_apps:
            related_sims.append(cosine(embeddings[app_i], embeddings[app_j]))

    # unrelated pair similarities
    all_app_list = list(embedded_apps)
    related_set = {(a, b) for a, b, _, _ in related_pairs} | \
                  {(b, a) for a, b, _, _ in related_pairs}
    unrelated_sims = []
    attempts = 0
    while len(unrelated_sims) < n_unrelated_sample and attempts < 10000:
        a, b = random.sample(all_app_list, 2)
        if (a, b) not in related_set:
            unrelated_sims.append(cosine(embeddings[a], embeddings[b]))
        attempts += 1

    def summarize(sims, label):
        if not sims:
            return {label: "no data"}
        s = sorted(sims)
        return {
            "n":      len(s),
            "min":    s[0],
            "p25":    s[len(s) // 4],
            "median": s[len(s) // 2],
            "p75":    s[3 * len(s) // 4],
            "max":    s[-1],
            "mean":   sum(s) / len(s),
        }

    rel_stats   = summarize(related_sims,   "related")
    unrel_stats = summarize(unrelated_sims, "unrelated")

    # suggest tau as midpoint between median related and median unrelated sim
    tau_suggested = None
    if related_sims and unrelated_sims:
        tau_suggested = (rel_stats["median"] + unrel_stats["median"]) / 2.0

    return {
        "d_used":          d,
        "n_embedded_apps": len(embedded_apps),
        "related_sims":    rel_stats,
        "unrelated_sims":  unrel_stats,
        "tau_suggested":   tau_suggested,
    }


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trace",              type=str, default="AzureFunctionsInvocationTraceForTwoWeeksJan2021.txt")
    parser.add_argument("--start",              type=float, default=0.0)
    parser.add_argument("--end",                type=float, default=180.0)
    parser.add_argument("--scrape-interval",    type=float, default=1.0)
    parser.add_argument("--activity-threshold", type=float, default=0.0,
                        help="CPU%% threshold (unused for trace-based estimation)")
    parser.add_argument("--window-w",           type=float, default=10.0,
                        help="Co-occurrence window in seconds")
    parser.add_argument("--p-min",              type=float, default=0.3)
    parser.add_argument("--freq-min",           type=int,   default=3)
    args = parser.parse_args()

    dt             = args.scrape_interval
    window_w_ticks = int(math.ceil(args.window_w / dt))

    print(f"\n{'='*60}")
    print(f"TARDIS parameter estimation")
    print(f"  trace:    {args.trace}")
    print(f"  window:   [{args.start}, {args.end}]s")
    print(f"  dt:       {dt}s  →  {int((args.end - args.start)/dt)} ticks")
    print(f"  w:        {args.window_w}s = {window_w_ticks} ticks")
    print(f"  p_min:    {args.p_min},  freq_min: {args.freq_min}")
    print(f"{'='*60}\n")

    invocations = load_trace(args.trace, args.start, args.end)
    if not invocations:
        print("No invocations in window. Exiting.")
        return

    print("\n── Active tick computation ──")
    active_ticks = build_active_ticks(
        invocations, args.start, args.end, dt, args.activity_threshold
    )
    print(f"  Apps with any activity: {len(active_ticks)}")

    print("\n── Alpha estimation ──")
    alpha, alpha_stats = estimate_alpha(active_ticks)
    print(f"  Active ticks per app:")
    print(f"    min={alpha_stats['min']}  p25={alpha_stats['p25']}  "
          f"median={alpha_stats['median']}  p75={alpha_stats['p75']}  "
          f"max={alpha_stats['max']}  mean={alpha_stats['mean']:.1f}")
    print(f"  f_p50 = {alpha_stats['median']}  →  alpha = 1/sqrt({alpha_stats['median']}) = {alpha:.4f}")

    print("\n── Beta estimation ──")
    print("  Finding related pairs...")
    related_pairs = find_related_pairs(
        active_ticks, window_w_ticks, args.p_min, args.freq_min
    )
    beta, beta_stats = estimate_beta(related_pairs, window_w_ticks, dt)
    if not beta_stats.get("fallback"):
        print(f"  Gap distribution (ticks):")
        print(f"    min={beta_stats['min']}  p25={beta_stats['p25']}  "
              f"g_p50={beta_stats['g_p50']}  p75={beta_stats['p75']}  "
              f"max={beta_stats['max']}  mean={beta_stats['mean']:.1f}")
        print(f"  g_p50 = {beta_stats['g_p50']} ticks "
              f"= {beta_stats['g_p50_seconds']:.1f}s  →  "
              f"beta = 1/(2*{beta_stats['g_p50']}) = {beta:.4f}")
    else:
        print(f"  No related pairs found; beta estimated from fallback: {beta:.4f}")

    print("\n── Tau prior (offline TARDIS simulation) ──")
    tau_stats = estimate_tau_prior(active_ticks, related_pairs)
    if tau_stats:
        print(f"  Embedding dim d = {tau_stats['d_used']}  "
              f"({tau_stats['n_embedded_apps']} apps embedded)")
        print(f"  Related pair similarities:   {tau_stats['related_sims']}")
        print(f"  Unrelated pair similarities: {tau_stats['unrelated_sims']}")
        if tau_stats["tau_suggested"] is not None:
            print(f"  Suggested tau (midpoint of medians): {tau_stats['tau_suggested']:.3f}")

    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"  alpha = {alpha:.4f}")
    print(f"  beta  = {beta:.4f}")
    if tau_stats.get("tau_suggested") is not None:
        print(f"  tau   = {tau_stats['tau_suggested']:.3f}  (prior; tune after implementation)")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()