#!/usr/bin/env python3
"""
avgload_controller.py
──────────────────────
Same as peakload_controller.py, but hotspot detection and victim/target
selection use the running *average* load over the window instead of the
true peak (agg="avg" on the shared PeakTracker from migration_common.py).

Migration mechanics live in migration_common.py, shared with
peakload_controller.py and tardis_controller.py.

Environment variables:
  POLL_INTERVAL      seconds between migration decisions (default 5)
  SCRAPE_INTERVAL    seconds between collector scrapes (default 1)
  LOAD_THRESHOLD     node cpu % that triggers migration (default 80)
  NAMESPACE          pod namespace to watch — see migration_common.py
  COLLECTOR_PORT     collector daemonset port — see migration_common.py
  DRY_RUN            if "true", print decisions without evicting — see migration_common.py
"""

import asyncio
import os
import time
from collections import defaultdict

import aiohttp

from migration_common import (
    PeakTracker,
    get_collector_pod_ips,
    get_worker_pods,
    scrape_all,
    run_migration,
)

# ── config ────────────────────────────────────────────────────────────────────
POLL_INTERVAL   = 15.0
SCRAPE_INTERVAL = 3.0
LOAD_THRESHOLD  = float(os.environ.get("LOAD_THRESHOLD", "80"))

NODE_LABEL_KEY  = "topology.kubernetes.io/node-name"
NODE_LABEL_MAP  = {
    "node-0": "alpha",
    "node-1": "beta",
    "node-2": "gamma",
}

# window = POLL_INTERVAL; agg="avg" is the only difference from peakload_controller.py
node_trackers: dict[str, PeakTracker] = defaultdict(lambda: PeakTracker(POLL_INTERVAL, agg="avg"))
pod_trackers:  dict[tuple, PeakTracker] = defaultdict(lambda: PeakTracker(POLL_INTERVAL, agg="avg"))


# ── migration logic ───────────────────────────────────────────────────────────

def update_trackers(ts: float, node_samples: dict[str, dict]):
    for node, sample in node_samples.items():
        node_trackers[node].record(ts, sample.get("cpu_pct", 0.0))
        for pod, pct in sample.get("pods", {}).items():
            pod_trackers[(node, pod)].record(ts, pct)


def decide_eviction(
    pod_to_node: dict[str, str],
    nodes: list[str],
) -> tuple[str, str] | None:
    """
    Returns (pod, src_node) for the highest-load pod on the most overloaded
    node (by running average), or None if no migration is needed.
    """
    loads = {n: node_trackers[n].peak() for n in nodes}
    print(f"  Node avg loads: { {n: f'{v:.1f}%' for n, v in loads.items()} }")

    overloaded = [(n, l) for n, l in loads.items() if l > LOAD_THRESHOLD]
    if not overloaded:
        return None

    src_node = max(overloaded, key=lambda x: x[1])[0]

    node_to_pods: dict[str, list[str]] = defaultdict(list)
    for pod, node in pod_to_node.items():
        node_to_pods[node].append(pod)

    available = node_to_pods[src_node]
    if not available:
        print(f"  No worker pods found on {src_node}")
        return None

    pod_loads = [(p, pod_trackers[(src_node, p)].peak()) for p in available]
    pod_loads.sort(key=lambda x: x[1], reverse=True)

    best_pod, best_load = pod_loads[0]
    if best_load == 0.0:
        print(f"  All pods on {src_node} have 0.0% avg load, skipping")
        return None

    print(f"  Decision: evict pod={best_pod} from {src_node} (pod_avg={best_load:.1f}%)")
    return (best_pod, src_node)


# ── main loops ────────────────────────────────────────────────────────────────

current_node_ips:    dict[str, str] = {}
current_pod_to_node: dict[str, str] = {}


async def scrape_loop(session: aiohttp.ClientSession):
    """Scrapes collectors every SCRAPE_INTERVAL seconds — no kubectl calls."""
    while True:
        if current_node_ips:
            node_samples = await scrape_all(session, current_node_ips)
            update_trackers(time.time(), node_samples)
        await asyncio.sleep(SCRAPE_INTERVAL)


async def discovery_loop():
    """Refreshes node IPs and pod placement every POLL_INTERVAL seconds."""
    global current_node_ips, current_pod_to_node
    while True:
        node_ips = get_collector_pod_ips()
        if node_ips:
            current_node_ips    = node_ips
            current_pod_to_node = get_worker_pods()
        await asyncio.sleep(POLL_INTERVAL)


async def decision_loop():
    """Runs migration decisions every POLL_INTERVAL seconds."""
    node_last_eviction: dict[str, float] = {}

    await asyncio.sleep(POLL_INTERVAL + 1)

    while True:
        print(f"\n[{time.strftime('%H:%M:%S')}] Controller tick")

        if not current_node_ips:
            print("  No collector pods found, skipping.")
            await asyncio.sleep(POLL_INTERVAL)
            continue

        now = time.time()
        cooling = {n for n, t in node_last_eviction.items() if now - t < POLL_INTERVAL}
        if cooling:
            print(f"  Nodes in cooldown (skipping): {cooling}")

        active_nodes = [n for n in current_node_ips.keys() if n not in cooling]
        decision = decide_eviction(current_pod_to_node, active_nodes)

        if not decision:
            print("  No migrations needed.")
            await asyncio.sleep(POLL_INTERVAL)
            continue

        pod, src_node = decision

        all_loads = {n: node_trackers[n].peak() for n in current_node_ips.keys()}
        dst_node  = min(all_loads, key=lambda n: all_loads[n])
        print(f"  Target node: {dst_node} (load={all_loads[dst_node]:.1f}%)")

        success = run_migration(pod, dst_node)
        if success:
            node_last_eviction[src_node] = time.time()
        await asyncio.sleep(POLL_INTERVAL)


async def control_loop():
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            scrape_loop(session),
            discovery_loop(),
            decision_loop(),
        )


def main():
    print(f"Controller starting: threshold={LOAD_THRESHOLD}% poll={POLL_INTERVAL}s scrape={SCRAPE_INTERVAL}s")
    asyncio.run(control_loop())


if __name__ == "__main__":
    main()