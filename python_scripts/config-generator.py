import argparse
import csv
import json
import os
import time
from collections import defaultdict

# ── config ────────────────────────────────────────────────────────────────────
TRACE_FILE   = "trace.txt"
OUTPUT_DIR   = "k8s/configs"
BUCKET_SIZE  = 60                # seconds per timestep
# ─────────────────────────────────────────────────────────────────────────────


def parse_trace(filepath):
    """
    Read the trace CSV and return a dict:
        app_id -> list of (start_time, end_time) tuples
    """
    apps = defaultdict(list)
    with open(filepath, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            app_id     = row["app"].strip()
            duration   = float(row["duration"])
            end_time   = float(row["end_timestamp"])
            start_time = end_time - duration
            apps[app_id].append((start_time, end_time))
    return apps


def build_load(invocations, bucket_size=BUCKET_SIZE, start_timestep=0, num_timesteps=None):
    """
    For each timestep, count how many invocations are active (i.e. started
    before the timestep ends and finished after it starts). Normalize by the
    peak count so the output is a list of floats in [0.0, 1.0].
    """
    if not invocations:
        return []

    min_start   = min(s for s, _ in invocations)
    max_end     = max(e for _, e in invocations)
    num_buckets = int((max_end - min_start) / bucket_size) + 1

    end_timestep = num_buckets if num_timesteps is None else start_timestep + num_timesteps
    end_timestep = min(end_timestep, num_buckets)

    # count concurrent invocations per timestep
    raw_load = [0] * (end_timestep - start_timestep)
    for start, end in invocations:
        inv_start_bucket = int((start - min_start) / bucket_size)
        inv_end_bucket   = int((end   - min_start) / bucket_size)
        for b in range(max(inv_start_bucket, start_timestep),
                       min(inv_end_bucket + 1, end_timestep)):
            raw_load[b - start_timestep] += 1

    # normalize to [0.0, 1.0]
    peak = max(raw_load) if max(raw_load) > 0 else 1
    return [round(v / peak, 4) for v in raw_load]


def write_app_json(app_id, load, output_dir, start_time):
    short_id = app_id[:16]
    app_dir  = os.path.join(output_dir, f"app-{short_id}")
    os.makedirs(app_dir, exist_ok=True)
    payload  = {
        "app_id":      app_id,
        "bucket_size": BUCKET_SIZE,
        "start_time":  start_time,
        "load":        load,
    }
    filepath = os.path.join(app_dir, "schedule.json")
    with open(filepath, "w") as f:
        json.dump(payload, f, indent=2)
    return filepath, short_id


def write_kustomization(app_entries, output_dir):
    k8s_dir = os.path.dirname(output_dir)
    os.makedirs(k8s_dir, exist_ok=True)

    lines = [
        "apiVersion: kustomize.config.k8s.io/v1beta1",
        "kind: Kustomization",
        "",
        "generatorOptions:",
        "  disableNameSuffixHash: true",
        "",
        "configMapGenerator:",
    ]
    for short_id, filepath in app_entries:
        rel_path = os.path.relpath(filepath, k8s_dir)
        lines += [
            f"  - name: app-{short_id}-config",
            f"    files:",
            f"      - {rel_path}",
        ]

    kustomization_path = os.path.join(k8s_dir, "kustomization.yaml")
    with open(kustomization_path, "w") as f:
        f.write("\n".join(lines) + "\n")

    return kustomization_path


def main():
    parser = argparse.ArgumentParser(description="Process Azure Functions trace into per-app CPU load schedules.")
    parser.add_argument("trace_file", nargs="?", default=TRACE_FILE)
    parser.add_argument("--limit", type=int, default=None,
                        help="Only process the first N apps")
    parser.add_argument("--start-timestep", type=int, default=0,
                        help="First timestep to include (default: 0)")
    parser.add_argument("--num-timesteps", type=int, default=None,
                        help="Number of timesteps to include (default: all)")
    parser.add_argument("--start-delay", type=int, default=60,
                        help="Seconds from now until simulation starts (default: 60)")
    args = parser.parse_args()

    print(f"Reading trace from: {args.trace_file}")
    apps = parse_trace(args.trace_file)
    print(f"Found {len(apps)} unique applications")

    if args.limit:
        apps = dict(list(apps.items())[:args.limit])
        print(f"Limiting to {len(apps)} applications")

    if args.num_timesteps:
        print(f"Timestep window: [{args.start_timestep}, {args.start_timestep + args.num_timesteps})")
    else:
        print(f"Timestep window: [{args.start_timestep}, end]")

    start_time = time.time() + args.start_delay
    print(f"Simulation start time: {start_time} (T+{args.start_delay}s from now)")

    app_entries = []
    for app_id, invocations in apps.items():
        load = build_load(
            invocations,
            start_timestep=args.start_timestep,
            num_timesteps=args.num_timesteps,
        )
        filepath, short_id = write_app_json(app_id, load, OUTPUT_DIR, start_time)
        app_entries.append((short_id, filepath))
        peak = max(load) if load else 0
        print(f"  wrote {filepath}  ({len(load)} timesteps, peak load {peak:.2f})")

    kustomization_path = write_kustomization(app_entries, OUTPUT_DIR)
    print(f"\nWrote kustomization: {kustomization_path}")
    print("Done.")


if __name__ == "__main__":
    main()