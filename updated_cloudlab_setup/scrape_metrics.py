#!/usr/bin/env python3
"""
scrape_metrics.py
─────────────────
After your experiment, run this from anywhere that has kubectl access.
It discovers every node-metrics-collector pod, pulls their /metrics,
/metrics/memory, and /metrics/io endpoints, merges the three per pod by
timestamp, and writes everything into a single CSV across all pods.

Usage:
    python scrape_metrics.py [--since <unix_ts>] [--out merged_metrics.csv]
    python scrape_metrics.py --flush          # POST /flush on each pod first
    python scrape_metrics.py --since <unix_ts> --duration 3600   # 1 hour window
"""

import argparse
import csv
import io
import subprocess
import sys
import urllib.request

NAMESPACE   = "default"
LABEL       = "app=cluster-metrics-collector"
LOCAL_PORT  = 19100   # local port for kubectl port-forward

# Columns in the final merged CSV, in order.
MERGED_FIELDNAMES = [
    "timestamp", "node",
    "cpu_pct", "cpu_pods",
    "mem_pct", "mem_pods",
    "io_read_bytes_per_sec", "io_write_bytes_per_sec", "io_pods",
]


def get_pod_names() -> list[str]:
    out = subprocess.check_output(
        ["kubectl", "get", "pods", "-n", NAMESPACE, "-l", LABEL,
         "-o", "jsonpath={.items[*].metadata.name}"],
        text=True,
    )
    return out.strip().split()


def port_forward(pod: str, local_port: int):
    """Return a Popen handle; caller must .terminate() it."""
    import time
    proc = subprocess.Popen(
        ["kubectl", "port-forward", "-n", NAMESPACE, pod,
         f"{local_port}:9100"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(0.8)   # give kube a moment to establish the tunnel
    return proc


def fetch(url: str) -> str:
    with urllib.request.urlopen(url, timeout=15) as r:
        return r.read().decode()


def fetch_csv_rows(base: str, path: str, since: float | None) -> list[dict]:
    url = f"{base}{path}?fmt=csv"
    if since:
        url += f"&since={since}"
    raw = fetch(url)
    return list(csv.DictReader(io.StringIO(raw)))


def merge_metric_rows(cpu_rows: list[dict], mem_rows: list[dict], io_rows: list[dict]) -> list[dict]:
    """
    Each of /metrics, /metrics/memory, /metrics/io comes from the same
    underlying per-tick sample, so a given timestamp appears identically
    (same float, same string form) across all three. Join on that.
    """
    mem_by_ts = {r["timestamp"]: r for r in mem_rows}
    io_by_ts  = {r["timestamp"]: r for r in io_rows}

    merged = []
    for cpu_row in cpu_rows:
        ts      = cpu_row["timestamp"]
        mem_row = mem_by_ts.get(ts, {})
        io_row  = io_by_ts.get(ts, {})

        merged.append({
            "timestamp":               ts,
            "node":                    cpu_row.get("node", ""),
            "cpu_pct":                 cpu_row.get("cpu_pct", ""),
            "cpu_pods":                cpu_row.get("pods", ""),
            "mem_pct":                 mem_row.get("mem_pct", ""),
            "mem_pods":                mem_row.get("pods", ""),
            "io_read_bytes_per_sec":   io_row.get("io_read_bytes_per_sec", ""),
            "io_write_bytes_per_sec":  io_row.get("io_write_bytes_per_sec", ""),
            "io_pods":                 io_row.get("pods", ""),
        })
    return merged


def scrape_pod(pod: str, since: float | None, until: float | None, do_flush: bool, local_port: int) -> list[dict]:
    pf = port_forward(pod, local_port)
    rows = []
    try:
        base = f"http://localhost:{local_port}"

        if do_flush:
            req = urllib.request.Request(f"{base}/flush", method="POST", data=b"")
            with urllib.request.urlopen(req, timeout=10) as r:
                print(f"  flush: {r.read().decode().strip()}")

        cpu_rows = fetch_csv_rows(base, "/metrics", since)
        mem_rows = fetch_csv_rows(base, "/metrics/memory", since)
        io_rows  = fetch_csv_rows(base, "/metrics/io", since)

        rows = merge_metric_rows(cpu_rows, mem_rows, io_rows)

        if until:
            rows = [r for r in rows if float(r["timestamp"]) <= until]

        print(f"  {pod}: {len(rows)} samples")
    finally:
        pf.terminate()
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", type=float, default=None,
                    help="Only include samples after this unix timestamp")
    ap.add_argument("--until", type=float, default=None,
                    help="Only include samples before this unix timestamp")
    ap.add_argument("--duration", type=float, default=None,
                    help="Window length in seconds, applied as until = since + duration. "
                         "Requires --since. Mutually exclusive with --until.")
    ap.add_argument("--out", default="merged_metrics.csv",
                    help="Output CSV path")
    ap.add_argument("--flush", action="store_true",
                    help="POST /flush on each pod before scraping (writes pod-local CSV too)")
    ap.add_argument("--port", type=int, default=LOCAL_PORT,
                    help="Base local port for port-forward (incremented per pod)")
    args = ap.parse_args()

    if args.duration is not None:
        if args.until is not None:
            ap.error("--duration cannot be combined with --until")
        if args.since is None:
            ap.error("--duration requires --since")
        if args.duration <= 0:
            ap.error("--duration must be positive")
        args.until = args.since + args.duration

    pods = get_pod_names()
    if not pods:
        print("No collector pods found.", file=sys.stderr)
        sys.exit(1)
    print(f"Found {len(pods)} collector pod(s): {pods}")
    if args.since is not None or args.until is not None:
        print(f"Window: since={args.since} until={args.until}")

    all_rows: list[dict] = []
    for i, pod in enumerate(pods):
        print(f"Scraping {pod} …")
        try:
            rows = scrape_pod(pod, args.since, args.until, args.flush, args.port + i)
            all_rows.extend(rows)
        except Exception as e:
            # A single pod failing (e.g. crashing under the weight of a huge
            # unbounded /metrics response) shouldn't lose the data already
            # scraped from the other pods.
            print(f"  {pod}: failed ({e}), skipping", file=sys.stderr)
            continue

    all_rows.sort(key=lambda r: float(r["timestamp"]))

    if not all_rows:
        print("No data collected.")
        return

    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MERGED_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nMerged {len(all_rows)} rows → {args.out}")


if __name__ == "__main__":
    main()