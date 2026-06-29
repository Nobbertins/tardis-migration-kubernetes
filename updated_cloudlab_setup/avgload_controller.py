#!/usr/bin/env python3
"""
controller.py
─────────────
Polls the node-metrics-collector daemonset for per-node and per-pod CPU,
runs peak-load migration logic, then:
  1. Taints all nodes except the least loaded (NoSchedule) to steer rescheduling
  2. Evicts the selected pod
  3. Removes the taints once the pod is gone

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
POLL_INTERVAL   = 15.0
SCRAPE_INTERVAL = 3.0
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

# ── peak load tracking ────────────────────────────────────────────────────────

class PeakTracker:
    """
    Sliding-window peak tracker over the last `window` seconds.
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

    def peak(self) -> float:
        if not self.buf:
            return 0.0
        return sum(v for _, v in self.buf) / len(self.buf)


# window = POLL_INTERVAL so peak reflects only the most recent decision period
node_trackers: dict[str, PeakTracker] = defaultdict(lambda: PeakTracker(POLL_INTERVAL))
pod_trackers:  dict[tuple, PeakTracker] = defaultdict(lambda: PeakTracker(POLL_INTERVAL))

# ── kubectl helpers ───────────────────────────────────────────────────────────

def kubectl(*args) -> str:
    cmd = ["kubectl", *args]
    print(f"  $ {' '.join(cmd)}")
    if DRY_RUN and any(a in ("taint", "patch", "delete", "scale", "pause", "resume") for a in args):
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


def get_pod_spec(pod: str) -> dict | None:
    """Get the full pod spec from an existing pod to use as a template."""
    out = kubectl("get", "pod", "-n", NAMESPACE, pod, "-o", "json")
    if not out:
        return None
    return json.loads(out)


def create_migration_pod(old_pod_data: dict, dst_node: str, new_pod_name: str) -> bool:
    """
    Create a standalone Pod on dst_node using the old pod's spec as a template.
    Strips Deployment/ReplicaSet owner references so it's unmanaged.
    Uses nodeName to pin directly to dst_node without touching the Deployment.
    """
    spec = old_pod_data["spec"]

    # override imagePullPolicy so migration pod uses cached image on dst_node
    # instead of always pulling — avoids registry failures killing the pod
    for container in spec.get("containers", []) + spec.get("initContainers", []):
        container["imagePullPolicy"] = "IfNotPresent"
        container.pop("terminationMessagePath", None)
        container.pop("terminationMessagePolicy", None)

    # pin directly to dst_node — no nodeSelector needed
    spec["nodeName"] = dst_node
    spec.pop("nodeSelector", None)

    pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": new_pod_name,
            "namespace": NAMESPACE,
            "labels": {
                k: v for k, v in old_pod_data["metadata"].get("labels", {}).items()
                if k != "pod-template-hash"  # ReplicaSet uses this to claim pods
            },
        },
        "spec": {
            **spec,
            "nodeName": dst_node,
            "restartPolicy": "Always",
        },
    }

    manifest_str = json.dumps(pod_manifest)
    print(f"  Creating migration pod {new_pod_name} on {dst_node}")
    #print(f"  Manifest: {manifest_str[:500]}")
    if DRY_RUN:
        print("  [dry-run skipped]")
        return True

    result = subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        input=manifest_str, capture_output=True, text=True
    )
    print(f"  kubectl apply stdout: {result.stdout.strip()}")
    if result.returncode != 0:
        print(f"  Failed to create pod: {result.stderr.strip()}")
        return False
    # immediately describe to catch any admission or scheduling errors
    # time.sleep(1)
    # desc = subprocess.run(
    #     ["kubectl", "describe", "pod", "-n", NAMESPACE, new_pod_name],
    #     capture_output=True, text=True
    # )
    # print(f"  describe: {desc.stdout[-800:] if desc.stdout else desc.stderr[-400:]}")
    return True


def wait_for_pod_healthy(pod_name: str, dst_node: str, timeout: float = 120) -> bool:
    """Wait until a named pod is Running and /health returns 200."""
    import urllib.request
    deadline = time.time() + timeout
    while time.time() < deadline:
        out = kubectl("get", "pod", "-n", NAMESPACE, pod_name, "-o", "json")
        if out:
            data = json.loads(out)
            if data.get("status", {}).get("phase") == "Running":
                pod_ip = data["status"].get("podIP", "")
                if pod_ip:
                    try:
                        with urllib.request.urlopen(
                            f"http://{pod_ip}:8080/health", timeout=2
                        ) as r:
                            if r.status == 200:
                                print(f"  Pod {pod_name} healthy on {dst_node}")
                                return True
                    except Exception:
                        pass
        time.sleep(2)
    print(f"  Warning: {pod_name} not healthy after {timeout}s")
    return False


def drain_worker(old_pod: str, task_timeout: float = 300):
    """
    POST /drain directly to the old pod's IP.
    Blocks until the old pod confirms all in-flight tasks are done.
    """
    import http.client

    out = kubectl("get", "pod", "-n", NAMESPACE, old_pod, "-o", "json")
    if not out or DRY_RUN:
        if DRY_RUN:
            print("  [dry-run skipped drain]")
        return
    pod_data = json.loads(out)
    pod_ip = pod_data.get("status", {}).get("podIP", "")
    if not pod_ip:
        print(f"  Could not get IP for {old_pod}, skipping drain")
        return

    print(f"  Draining {old_pod} at http://{pod_ip}:8080/drain")
    try:
        conn = http.client.HTTPConnection(pod_ip, 8080, timeout=5)
        conn.request("POST", "/drain", body=b"")
        conn.sock.settimeout(task_timeout)
        resp = conn.getresponse()
        print(f"  [{old_pod}] {resp.read().decode().strip()}")
        conn.close()
    except Exception as e:
        print(f"  [{old_pod}] drain failed: {e} — proceeding anyway")


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
    loads = {n: node_trackers[n].peak() for n in nodes}
    print(f"  Node peak loads: { {n: f'{v:.1f}%' for n, v in loads.items()} }")

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
        print(f"  All pods on {src_node} have 0.0% peak load, skipping")
        return None

    print(f"  Decision: evict pod={best_pod} from {src_node} (pod_peak={best_load:.1f}%)")
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
        all_loads = {n: node_trackers[n].peak() for n in current_node_ips.keys()}
        dst_node  = min(all_loads, key=lambda n: all_loads[n])
        print(f"  Target node: {dst_node} (load={all_loads[dst_node]:.1f}%)")

        # get old pod spec to use as template
        old_pod_data = get_pod_spec(pod)
        if not old_pod_data:
            print(f"  Could not get spec for {pod}, skipping.")
            await asyncio.sleep(POLL_INTERVAL)
            continue

        # new pod name = old pod name + "-migration"
        new_pod_name = f"{pod[:48]}-mig"

        # 1. create standalone pod on dst_node — Deployment untouched
        created = create_migration_pod(old_pod_data, dst_node, new_pod_name)
        if not created:
            print(f"  Failed to create migration pod, skipping.")
            await asyncio.sleep(POLL_INTERVAL)
            continue

        # 2. wait for new pod to be Running and healthy
        if not DRY_RUN:
            healthy = wait_for_pod_healthy(new_pod_name, dst_node)
            if not healthy:
                print(f"  Migration pod never became healthy — cleaning up")
                kubectl("delete", "pod", "-n", NAMESPACE, new_pod_name, "--grace-period=0")
                await asyncio.sleep(POLL_INTERVAL)
                continue

        # 3. drain old pod — finishes current task, rejects new ones
        #    orchestrator retries will go to new pod via service DNS
        drain_worker(pod)

        # 4. delete old pod — new pod is already serving
        print(f"  Deleting old pod {pod}")
        kubectl("delete", "pod", "-n", NAMESPACE, pod, "--grace-period=0")
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
