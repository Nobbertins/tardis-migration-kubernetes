#!/usr/bin/env python3
"""
scrape_metrics.py
─────────────────
After your experiment, run this from anywhere that has kubectl access.
It discovers every node-metrics-collector pod, pulls their /metrics endpoint,
and merges everything into a single CSV.

Usage:
    python scrape_metrics.py [--since <unix_ts>] [--out merged_metrics.csv]
    python scrape_metrics.py --flush          # POST /flush on each pod first
"""

import argparse
import csv
import io
import subprocess
import sys
import urllib.request

NAMESPACE   = "default"
LABEL       = "app=node-metrics-collector"
LOCAL_PORT  = 19100   # local port for kubectl port-forward


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


def scrape_pod(pod: str, since: float | None, until: float | None, do_flush: bool, local_port: int) -> list[dict]:
    pf = port_forward(pod, local_port)
    rows = []
    try:
        base = f"http://localhost:{local_port}"

        if do_flush:
            req = urllib.request.Request(f"{base}/flush", method="POST", data=b"")
            with urllib.request.urlopen(req, timeout=10) as r:
                print(f"  flush: {r.read().decode().strip()}")

        url = f"{base}/metrics?fmt=csv"
        if since:
            url += f"&since={since}"

        raw = fetch(url)
        reader = csv.DictReader(io.StringIO(raw))
        rows = list(reader)

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
    ap.add_argument("--out", default="merged_metrics.csv",
                    help="Output CSV path")
    ap.add_argument("--flush", action="store_true",
                    help="POST /flush on each pod before scraping (writes pod-local CSV too)")
    ap.add_argument("--port", type=int, default=LOCAL_PORT,
                    help="Base local port for port-forward (incremented per pod)")
    args = ap.parse_args()

    pods = get_pod_names()
    if not pods:
        print("No collector pods found.", file=sys.stderr)
        sys.exit(1)
    print(f"Found {len(pods)} collector pod(s): {pods}")

    all_rows: list[dict] = []
    for i, pod in enumerate(pods):
        print(f"Scraping {pod} …")
        rows = scrape_pod(pod, args.since, args.until, args.flush, args.port + i)
        all_rows.extend(rows)

    all_rows.sort(key=lambda r: float(r["timestamp"]))

    if not all_rows:
        print("No data collected.")
        return

    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_rows[0].keys())
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nMerged {len(all_rows)} rows → {args.out}")


if __name__ == "__main__":
    main()