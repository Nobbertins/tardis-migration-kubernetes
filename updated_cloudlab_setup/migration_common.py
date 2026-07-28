#!/usr/bin/env python3
"""
migration_common.py
────────────────────
Shared kubectl/migration mechanics for the peak-load, avg-load, and
TARDIS pod-migration controllers (peakload_controller.py,
avgload_controller.py, tardis_controller.py).

Each controller keeps its own hotspot-detection / victim-selection /
target-selection *policy* — this module only implements the mechanical,
policy-independent parts that are identical across all three:

  - kubectl wrapper + shared config (NAMESPACE, DRY_RUN, COLLECTOR_PORT)
  - discovering collector pods and worker pods
  - deriving a pod's stable "deployment identity" from its `app` label
    instead of guessing at names via string-slicing (see note below)
  - the create -> wait -> drain -> delete migration mechanics
  - async collector-scraping helpers
  - a generic sliding-window tracker (peak or average, selectable)

Naming note
-----------
Worker Deployments/Services are always named exactly after the pod's own
`app` label (see genk8s.py's worker_deployment()/worker_service()), e.g.
"worker-<entity_id>", and that same label is copied verbatim onto every
migration pod. That makes the label — NOT the pod's own (Deployment- or
migration-suffixed) name — the one length-independent way to recover a
pod's original deployment/service name and to detect repeat migrations.
The previous version of these controllers derived names by slicing a
fixed number of characters off the pod name (e.g. pod[:23], pod[:48]),
which silently broke once entity ids got longer (switching worker
deployments from one-per-app to one-per-function). base_name_of() /
next_migration_pod_name() / is_deployment_managed() below fix that by
reading the label/ownerReferences instead of guessing a length.

Environment variables (shared across all three controllers):
  NAMESPACE        pod namespace to watch (default "default")
  COLLECTOR_PORT   port the collector daemonset listens on (default 9100)
  DRY_RUN          if "true", print decisions without evicting (default false)
"""

import asyncio
import http.client
import json
import os
import re
import subprocess
import time
import urllib.request
from collections import deque

import aiohttp

# ── shared config ─────────────────────────────────────────────────────────────
NAMESPACE      = os.environ.get("NAMESPACE", "default")
COLLECTOR_PORT = int(os.environ.get("COLLECTOR_PORT", "9100"))
DRY_RUN        = os.environ.get("DRY_RUN", "false").lower() == "true"
# ─────────────────────────────────────────────────────────────────────────────


# ── kubectl helpers ───────────────────────────────────────────────────────────

def kubectl(*args) -> str:
    cmd = ["kubectl", *args]
    print(f"  $ {' '.join(cmd)}")
    if DRY_RUN and any(a in ("taint", "patch", "delete", "scale", "pause", "resume", "label") for a in args):
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
    """Full pod spec — used both as a migration template and to read the
    pod's labels/ownerReferences for base_name_of()/is_deployment_managed()."""
    out = kubectl("get", "pod", "-n", NAMESPACE, pod, "-o", "json")
    if not out:
        return None
    return json.loads(out)

# ── stable identity helpers (see module docstring) ────────────────────────────

def base_name_of(pod_data: dict) -> str:
    """The stable 'worker-<entity_id>' identity, from the pod's app label —
    identical to the original Deployment/Service name regardless of pod
    name suffixes or entity id length."""
    return pod_data["metadata"]["labels"]["app"]


def is_deployment_managed(pod_data: dict) -> bool:
    """True if this pod is still owned by a Deployment's ReplicaSet (i.e.
    it hasn't been migrated to a standalone pod yet)."""
    owners = pod_data.get("metadata", {}).get("ownerReferences", [])
    return any(o.get("kind") == "ReplicaSet" for o in owners)


def next_migration_pod_name(pod_data: dict) -> str:
    """
    Next unique standalone-pod name for a migration: '<base_name>-mig1',
    '-mig2', etc. Built from the stable base_name (the app label) rather
    than the pod's own current name — the current name may carry a
    Deployment-generated suffix (first migration) or an earlier '-migN'
    suffix (repeat migration), and slicing either of those by a fixed
    character count breaks as soon as entity ids change length.
    """
    base = base_name_of(pod_data)
    cur  = pod_data["metadata"]["name"]
    m = re.match(rf"^{re.escape(base)}-mig(\d+)$", cur)
    n = int(m.group(1)) + 1 if m else 1
    return f"{base}-mig{n}"


def delete_old_pod(pod: str, pod_data: dict):
    """Delete the pre-migration workload: the Deployment if `pod` is still
    Deployment-managed, otherwise the standalone migration pod itself."""
    if is_deployment_managed(pod_data):
        deployment_name = base_name_of(pod_data)
        print(f"  Deleting old deployment {deployment_name} (pod {pod})")
        kubectl("delete", "deployment", "-n", NAMESPACE, deployment_name, "--grace-period=0")
    else:
        print(f"  Deleting old pod {pod}")
        kubectl("delete", "pod", "-n", NAMESPACE, pod, "--grace-period=0")


# ── migration pod lifecycle ───────────────────────────────────────────────────

def redirect_traffic_from_old_pod(old_pod: str):
    """
    Flip Service traffic onto the new migration pod by relabeling the OLD
    pod out of selection — not by patching the Service. Both the Service
    selector and the new pod key off `app`, and that label is copied
    verbatim onto migration pods (see module docstring), so the new pod
    already matches the Service as soon as it's Ready. Removing `app`
    from the old pod is what excludes it going forward.

    Also flips role -> "draining" so get_worker_pods() (selector
    role=worker) stops offering this pod up as a migration candidate
    while it finishes in-flight work.

    MUST be called only after the new pod is confirmed healthy, and
    before drain_worker() — draining still talks to the old pod directly
    by IP, so removing its labels doesn't affect that.
    """
    print(f"  Redirecting service traffic away from {old_pod} (dropping app label)")
    kubectl("label", "pod", "-n", NAMESPACE, old_pod,
            "app-", "role=draining", "--overwrite")

def create_migration_pod(old_pod_data: dict, dst_node: str, new_pod_name: str) -> bool:
    """
    Create a standalone Pod on dst_node using the old pod's spec as a template.
    Strips Deployment/ReplicaSet owner references so it's unmanaged.
    Uses nodeName to pin directly to dst_node without touching the Deployment.
    """
    spec = old_pod_data["spec"]

    for container in spec.get("containers", []) + spec.get("initContainers", []):
        container["imagePullPolicy"] = "IfNotPresent"
        container.pop("terminationMessagePath", None)
        container.pop("terminationMessagePolicy", None)

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
                if k != "pod-template-hash"
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
    return True


def wait_for_pod_healthy(pod_name: str, dst_node: str, timeout: float = 120) -> bool:
    """Wait until a named pod is Running and /health returns 200."""
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
    """POST /drain directly to the old pod's IP; blocks until it confirms drained."""
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


def run_migration(pod: str, dst_node: str, pre_drain_hook=None, post_delete_hook=None) -> bool:
    """
    Full create -> wait -> drain -> delete migration sequence, shared by
    all three controllers. Returns True if the migration completed (or was
    a DRY_RUN no-op), False if it aborted partway through — callers should
    only start a cooldown timer / mark the eviction on a True return.

    pre_drain_hook(pod, new_pod_name, old_pod_data) runs right after the new
    pod is confirmed healthy, before draining the old one — used by
    tardis_controller.py to transfer the TARDIS embedding.
    post_delete_hook(pod) runs right after the old pod/deployment is
    deleted — used by tardis_controller.py to forget the old embedding.
    """
    old_pod_data = get_pod_spec(pod)
    if not old_pod_data:
        print(f"  Could not get spec for {pod}, skipping.")
        return False

    new_pod_name = next_migration_pod_name(old_pod_data)

    created = create_migration_pod(old_pod_data, dst_node, new_pod_name)
    if not created:
        print("  Failed to create migration pod, skipping.")
        return False

    if not DRY_RUN:
        healthy = wait_for_pod_healthy(new_pod_name, dst_node)
        if not healthy:
            print("  Migration pod never became healthy — cleaning up")
            kubectl("delete", "pod", "-n", NAMESPACE, new_pod_name, "--grace-period=0")
            return False

    redirect_traffic_from_old_pod(pod) 

    if pre_drain_hook:
        pre_drain_hook(pod, new_pod_name, old_pod_data)

    drain_worker(pod)
    delete_old_pod(pod, old_pod_data)

    if post_delete_hook:
        post_delete_hook(pod)

    return True


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


# ── sliding-window tracker ────────────────────────────────────────────────────

class PeakTracker:
    """
    Sliding-window tracker over the last `window` seconds. One instance per
    node, one per (node, pod) pair.

    `agg` selects what .peak() reports:
      "max" — true peak load (peakload_controller.py, tardis_controller.py)
      "avg" — running average (avgload_controller.py)
    """
    def __init__(self, window: float, agg: str = "max"):
        self.window = window
        self.agg = agg
        self.buf: deque[tuple[float, float]] = deque()

    def record(self, ts: float, value: float):
        self.buf.append((ts, value))
        cutoff = ts - self.window
        while self.buf and self.buf[0][0] < cutoff:
            self.buf.popleft()

    def peak(self) -> float:
        if not self.buf:
            return 0.0
        if self.agg == "avg":
            return sum(v for _, v in self.buf) / len(self.buf)
        return max(v for _, v in self.buf)