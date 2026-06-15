#!/usr/bin/env python3
"""
controller.py
─────────────
Polls the node-metrics-collector daemonset for per-node and per-pod CPU,
runs avg-load migration logic, then:
  1. Edits node placement to target
  2. Evicts the selected pod

Environment variables:
  POLL_INTERVAL      seconds between migration decisions (default 5)
  SCRAPE_INTERVAL    seconds between collector scrapes (default 1)
  LOAD_THRESHOLD     node cpu % that triggers migration (default 80)
  NAMESPACE          pod namespace to watch (default "default")
  COLLECTOR_PORT     port the collector daemonset listens on (default 9100)
  DRY_RUN            if "true", print decisions without evicting (default false)
"""

import asyncio
import json
import os
import subprocess
import time
from collections import defaultdict, deque

import aiohttp

# ── config ────────────────────────────────────────────────────────────────────
POLL_INTERVAL   = float(os.environ.get("POLL_INTERVAL",   "5"))
SCRAPE_INTERVAL = float(os.environ.get("SCRAPE_INTERVAL", "1"))
LOAD_THRESHOLD  = float(os.environ.get("LOAD_THRESHOLD",  "80"))
NAMESPACE       = os.environ.get("NAMESPACE",      "default")
COLLECTOR_PORT  = int(os.environ.get("COLLECTOR_PORT", "9100"))
DRY_RUN         = os.environ.get("DRY_RUN", "false").lower() == "true"

NODE_LABEL_KEY  = "topology.kubernetes.io/node-name"
NODE_LABEL_MAP  = {
    "node-0": "alpha",
    "node-1": "beta",
    "node-2": "gamma",
}

# ── avg load tracking ────────────────────────────────────────────────────────

class AvgTracker:
    """
    Sliding-window avg tracker over the last `window` seconds.
    One instance per node, one per (node, pod) pair.
    """
    def __init__(self, window: float):
        self.window = window
        self.buf: deque[tuple[float, float]] = deque()

    def record(self, ts: float, value: float):
        self.buf.append((ts, value))
        cutoff = ts - self.window
        while self.buf and self.buf[0][0] < cutoff:
            self.buf.popleft()

    def avg(self) -> float:
        if not self.buf:
            return 0.0
        return sum(v for _, v in self.buf) / len(self.buf)


# window = POLL_INTERVAL so avg reflects only the most recent decision period
node_trackers: dict[str, AvgTracker] = defaultdict(lambda: AvgTracker(POLL_INTERVAL))
pod_trackers:  dict[tuple, AvgTracker] = defaultdict(lambda: AvgTracker(POLL_INTERVAL))

# ── kubectl helpers ───────────────────────────────────────────────────────────

def kubectl(*args) -> str:
    cmd = ["kubectl", *args]
    print(f"  $ {' '.join(cmd)}")
    if DRY_RUN and any(a in ("taint", "patch", "delete") for a in args):
        print("  [dry-run skipped]")
        return ""
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  kubectl error: {result.stderr.strip()}")
    return result.stdout.strip()


def get_collector_pod_ips() -> dict[str, str]:
    """Returns {node_name: pod_ip} for all collector pods."""
    out = kubectl(
        "get", "pods", "-n", NAMESPACE,
        "-l", "app=node-metrics-collector",
        "-o", "json"
    )
    if not out:
        return {}
    data = json.loads(out)
    result = {}
    for pod in data.get("items", []):
        node = pod["spec"].get("nodeName", "")
        ip   = pod["status"].get("podIP", "")
        if node and ip:
            result[node] = ip
    return result


def get_worker_pods() -> dict[str, str]:
    """Returns {pod_name: node_name} for running worker pods."""
    out = kubectl(
        "get", "pods", "-n", NAMESPACE,
        "-l", "role=worker",
        "-o", "json"
    )
    if not out:
        return {}
    data = json.loads(out)
    result = {}
    for pod in data.get("items", []):
        name  = pod["metadata"]["name"]
        node  = pod["spec"].get("nodeName", "")
        phase = pod["status"].get("phase", "")
        if node and phase == "Running":
            result[name] = node
    return result


def get_pod_deployment(pod: str) -> tuple[str, dict] | None:
    """
    Returns (deployment_name, original_node_selector) for the Deployment
    that owns this pod, by following pod → ReplicaSet → Deployment.
    """
    out = kubectl("get", "pod", "-n", NAMESPACE, pod, "-o", "json")
    if not out:
        return None
    pod_data = json.loads(out)

    rs_name = None
    for ref in pod_data.get("metadata", {}).get("ownerReferences", []):
        if ref.get("kind") == "ReplicaSet":
            rs_name = ref["name"]
            break
    if not rs_name:
        print(f"  No ReplicaSet owner found for {pod}")
        return None

    out = kubectl("get", "replicaset", "-n", NAMESPACE, rs_name, "-o", "json")
    if not out:
        return None
    rs_data = json.loads(out)

    deploy_name = None
    for ref in rs_data.get("metadata", {}).get("ownerReferences", []):
        if ref.get("kind") == "Deployment":
            deploy_name = ref["name"]
            break
    if not deploy_name:
        print(f"  No Deployment owner found for ReplicaSet {rs_name}")
        return None

    out = kubectl("get", "deployment", "-n", NAMESPACE, deploy_name, "-o", "json")
    if not out:
        return None
    deploy_data = json.loads(out)
    original_selector = (
        deploy_data.get("spec", {})
                   .get("template", {})
                   .get("spec", {})
                   .get("nodeSelector", {})
    )
    return (deploy_name, original_selector)


def patch_deployment_node_selector(deploy_name: str, node_selector: dict):
    patch = json.dumps({"spec": {"template": {"spec": {"nodeSelector": node_selector}}}})
    print(f"  Patching {deploy_name} nodeSelector → {node_selector}")
    kubectl("patch", "deployment", "-n", NAMESPACE, deploy_name,
            "--type=merge", f"--patch={patch}")


def evict_pod(pod: str):
    print(f"  Evicting pod {pod}")
    kubectl("delete", "pod", "-n", NAMESPACE, pod, "--grace-period=0")


# ── collector scraping ────────────────────────────────────────────────────────

async def fetch_latest(session: aiohttp.ClientSession, ip: str) -> dict | None:
    url = f"http://{ip}:{COLLECTOR_PORT}/metrics/latest"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=3)) as resp:
            return await resp.json()
    except Exception as e:
        print(f"  Failed to fetch {url}: {e}")
        return None


async def scrape_all(session: aiohttp.ClientSession, node_ips: dict[str, str]) -> dict[str, dict]:
    tasks = {node: asyncio.create_task(fetch_latest(session, ip)) for node, ip in node_ips.items()}
    results = {}
    for node, task in tasks.items():
        sample = await task
        if sample:
            results[node] = sample
    return results


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
    node, or None if no migration is needed.
    """
    loads = {n: node_trackers[n].avg() for n in nodes}
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

    pod_loads = [(p, pod_trackers[(src_node, p)].avg()) for p in available]
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

    # stagger slightly so discovery runs first
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

        # find least loaded node to receive the evicted pod
        all_loads = {n: node_trackers[n].avg() for n in current_node_ips.keys()}
        dst_node  = min(all_loads, key=lambda n: all_loads[n])
        print(f"  Target node: {dst_node} (load={all_loads[dst_node]:.1f}%)")

        # find the owning Deployment and its original nodeSelector
        result = get_pod_deployment(pod)
        if not result:
            print(f"  Could not find Deployment for {pod}, skipping.")
            await asyncio.sleep(POLL_INTERVAL)
            continue

        deploy_name, original_selector = result

        # patch nodeSelector to pin replacement pod to dst_node
        dst_label = NODE_LABEL_MAP.get(dst_node, dst_node)
        patch_deployment_node_selector(
            deploy_name,
            {NODE_LABEL_KEY: dst_label}
        )

        evict_pod(pod)
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
    print(f"Controller starting: threshold={LOAD_THRESHOLD}% poll={POLL_INTERVAL}s scrape={SCRAPE_INTERVAL}s dry_run={DRY_RUN}")
    asyncio.run(control_loop())


if __name__ == "__main__":
    main()
