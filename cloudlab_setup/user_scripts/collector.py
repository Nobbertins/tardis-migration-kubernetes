import asyncio
import json
import os
import time
import csv
import io
from aiohttp import web

from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.exceptions import ApiException

PORT             = int(os.environ.get("PORT", "9100"))
SCRAPE_INTERVAL  = float(os.environ.get("SCRAPE_INTERVAL", "1.0"))
RESULTS_FILE     = os.environ.get("RESULTS_FILE", "/results/metrics.csv")
MAX_SAMPLES      = int(os.environ.get("MAX_SAMPLES", "86400"))

# metrics-server itself only refreshes every 15-60s internally, and node/pod
# scheduling doesn't change every second, so these caches are refreshed on
# their own slower schedules rather than on every scrape.
NODE_REFRESH_INTERVAL = float(os.environ.get("NODE_REFRESH_INTERVAL", "60.0"))
POD_REFRESH_INTERVAL  = float(os.environ.get("POD_REFRESH_INTERVAL", "30.0"))

# Populated at startup
core_v1:    "client.CoreV1Api | None"        = None
custom_api: "client.CustomObjectsApi | None" = None
api_client: "client.ApiClient | None"        = None

# node name -> allocatable cores
node_cores: dict[str, float] = {}
node_cores_last_refresh: float = 0.0

# (namespace, pod) -> node name
pod_node_map: dict[tuple[str, str], str] = {}
pod_node_map_last_refresh: float = 0.0

samples: list[dict] = []
collector_task = None


# ── quantity parsing ──────────────────────────────────────────────────────────

def parse_cpu_quantity(qty: str | None) -> float:
    """Parse a Kubernetes CPU resource.Quantity string into whole cores.

    metrics-server reports usage.cpu as a Quantity, typically nanocores
    ('123456789n') or millicores ('150m'), occasionally a bare core count.
    Node allocatable/capacity is usually a bare integer core count.
    """
    if not qty:
        return 0.0
    qty = str(qty).strip()
    if qty.endswith("n"):
        return float(qty[:-1]) / 1_000_000_000
    if qty.endswith("u"):
        return float(qty[:-1]) / 1_000_000
    if qty.endswith("m"):
        return float(qty[:-1]) / 1_000
    if qty.endswith("k"):
        return float(qty[:-1]) * 1_000
    return float(qty)


# ── Kubernetes helpers ────────────────────────────────────────────────────────

async def wait_for_apis_ready(retries: int = 20, delay: float = 5.0) -> None:
    """Retry until the core API and the metrics-server aggregated API
    (metrics.k8s.io) are both reachable and serving data."""
    for attempt in range(1, retries + 1):
        try:
            await core_v1.list_node()
            await custom_api.list_cluster_custom_object(
                group="metrics.k8s.io", version="v1beta1", plural="pods"
            )
            print("[startup] core API and metrics.k8s.io are ready")
            return
        except ApiException as e:
            print(f"[startup] attempt {attempt}/{retries}: API not ready yet: "
                  f"HTTP {e.status} {e.reason}")
        except Exception as e:
            print(f"[startup] attempt {attempt}/{retries}: API not ready yet: {e}")
        await asyncio.sleep(delay)

    raise RuntimeError(
        f"core API / metrics.k8s.io did not become ready after {retries} attempts "
        f"(is the metrics-server deployment installed and healthy?)"
    )


async def refresh_node_cores(force: bool = False) -> None:
    """Refresh the node -> allocatable-cores map used as the denominator for
    every pod's cpu_pct on that node."""
    global node_cores, node_cores_last_refresh

    now = time.time()
    if not force and (now - node_cores_last_refresh) < NODE_REFRESH_INTERVAL:
        return

    try:
        node_list = await core_v1.list_node()
    except ApiException as e:
        print(f"[nodes] list_node failed: HTTP {e.status} {e.reason}")
        return
    except Exception as e:
        print(f"[nodes] list_node failed: {e}")
        return

    new_cores: dict[str, float] = {}
    for n in node_list.items:
        cpu_qty = (n.status.allocatable or {}).get("cpu") \
            or (n.status.capacity or {}).get("cpu")
        if cpu_qty:
            new_cores[n.metadata.name] = max(parse_cpu_quantity(cpu_qty), 0.001)

    if new_cores:
        node_cores = new_cores
        node_cores_last_refresh = now


async def refresh_pod_node_map(force: bool = False) -> None:
    """Refresh the (namespace, pod) -> node mapping for the whole cluster."""
    global pod_node_map, pod_node_map_last_refresh

    now = time.time()
    if not force and (now - pod_node_map_last_refresh) < POD_REFRESH_INTERVAL:
        return

    try:
        pod_list = await core_v1.list_pod_for_all_namespaces()
    except ApiException as e:
        print(f"[pods] list_pod_for_all_namespaces failed: HTTP {e.status} {e.reason}")
        return
    except Exception as e:
        print(f"[pods] list_pod_for_all_namespaces failed: {e}")
        return

    pod_node_map = {
        (p.metadata.namespace, p.metadata.name): p.spec.node_name
        for p in pod_list.items
        if p.spec.node_name  # skip unscheduled pods
    }
    pod_node_map_last_refresh = now


async def scrape_cluster_cpu() -> dict[str, dict[str, float]]:
    """
    Returns per-node, per-pod CPU as a percentage of that node's capacity:
        { node_name: { pod_name: pct, ... }, ... }
    """
    await refresh_node_cores()
    await refresh_pod_node_map()

    try:
        metrics = await custom_api.list_cluster_custom_object(
            group="metrics.k8s.io", version="v1beta1", plural="pods"
        )
    except ApiException as e:
        print(f"[metrics] list pod metrics failed: HTTP {e.status} {e.reason}")
        return {}
    except Exception as e:
        print(f"[metrics] list pod metrics failed: {e}")
        return {}

    out: dict[str, dict[str, float]] = {node: {} for node in node_cores}

    for item in metrics.get("items", []):
        meta = item.get("metadata", {})
        namespace = meta.get("namespace")
        pod = meta.get("name", "unknown")

        node = pod_node_map.get((namespace, pod))
        if node is None or node not in node_cores:
            continue  # pod not currently mapped to a known node

        cores = 0.0
        for container in item.get("containers", []):
            if container.get("name") == "POD":
                continue
            cores += parse_cpu_quantity(container.get("usage", {}).get("cpu"))

        out[node][pod] = round(cores / node_cores[node] * 100, 2)

    return out


# ── collection loop ───────────────────────────────────────────────────────────

async def collect_loop():
    await asyncio.sleep(SCRAPE_INTERVAL)

    while True:
        ts             = time.time()
        pods_by_node   = await scrape_cluster_cpu()

        # One sample record per node per tick, same shape as before, just all
        # produced from a single central poll instead of one pod per node.
        for node, pods_pct in pods_by_node.items():
            samples.append({
                "timestamp": ts,
                "node":      node,
                "cpu_pct":   round(sum(pods_pct.values()), 2),
                "pods":      pods_pct,
            })

        if len(samples) > MAX_SAMPLES:
            del samples[: len(samples) - MAX_SAMPLES]

        await asyncio.sleep(SCRAPE_INTERVAL)


# ── HTTP handlers ─────────────────────────────────────────────────────────────

async def handle_metrics(request):
    since = request.rel_url.query.get("since")
    node  = request.rel_url.query.get("node")
    fmt   = request.rel_url.query.get("fmt", "json")

    data = samples
    if since:
        try:
            since_f = float(since)
        except ValueError:
            raise web.HTTPBadRequest(text="'since' must be a unix timestamp")
        data = [s for s in data if s["timestamp"] >= since_f]
    if node:
        data = [s for s in data if s["node"] == node]

    if fmt == "csv":
        # Building the full CSV synchronously here blocks the event loop for
        # the duration of the join — with tens of thousands of samples that
        # can be long enough to miss the /health liveness probe (or spike
        # memory past the pod's limit) and get the container killed mid
        # response, which surfaces to scrapers as IncompleteRead. Offload
        # the CPU-bound formatting to a thread so /health stays responsive.
        loop = asyncio.get_event_loop()
        text = await loop.run_in_executor(None, _build_csv, data)
        return web.Response(text=text, content_type="text/csv")

    return web.json_response(data)


def _build_csv(data):
    # Hand-rolled string formatting here doesn't escape quotes inside the
    # JSON pods column, which corrupts every row once a pod name/value makes
    # the JSON contain a '"' — the csv module handles quoting/escaping
    # correctly per RFC 4180, so use it instead of building lines by hand.
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["timestamp", "node", "cpu_pct", "pods"])
    for s in data:
        writer.writerow([
            s["timestamp"],
            s["node"],
            s["cpu_pct"],
            json.dumps(s.get("pods", {})),
        ])
    return buf.getvalue()


async def handle_latest(request):
    node = request.rel_url.query.get("node")
    if not samples:
        raise web.HTTPServiceUnavailable(text="No samples collected yet")

    if node:
        matches = [s for s in reversed(samples) if s["node"] == node]
        if not matches:
            raise web.HTTPNotFound(text=f"No samples for node {node!r}")
        return web.json_response(matches[0])

    # No node specified: return the latest sample for every node currently
    # known, keyed by node name.
    latest_by_node: dict[str, dict] = {}
    for s in reversed(samples):
        latest_by_node.setdefault(s["node"], s)
    return web.json_response(latest_by_node)


async def handle_flush(request):
    if not samples:
        return web.Response(text="No samples to flush.")

    dirpath = os.path.dirname(RESULTS_FILE)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)

    write_header = not os.path.exists(RESULTS_FILE)
    with open(RESULTS_FILE, "a", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["timestamp", "node", "cpu_pct", "pods"],
            extrasaction="ignore",
        )
        if write_header:
            writer.writeheader()
        for s in samples:
            writer.writerow({**s, "pods": json.dumps(s.get("pods", {}))})

    n = len(samples)
    samples.clear()
    print(f"Flushed {n} samples to {RESULTS_FILE}")
    return web.Response(text=f"Flushed {n} samples to {RESULTS_FILE}")


async def handle_health(request):
    return web.Response(text="ok")


# ── startup / shutdown ────────────────────────────────────────────────────────

async def on_startup(app):
    global collector_task, api_client, core_v1, custom_api

    config.load_incluster_config()
    api_client = client.ApiClient()
    core_v1    = client.CoreV1Api(api_client)
    custom_api = client.CustomObjectsApi(api_client)

    await wait_for_apis_ready()
    await refresh_node_cores(force=True)
    await refresh_pod_node_map(force=True)

    print(f"cores_by_node={node_cores}  interval={SCRAPE_INTERVAL}s  "
          f"source=metrics.k8s.io  pods_mapped={len(pod_node_map)}")
    collector_task = asyncio.create_task(collect_loop())


async def on_shutdown(app):
    if collector_task:
        collector_task.cancel()
        try:
            await collector_task
        except asyncio.CancelledError:
            pass
    if api_client:
        await api_client.close()


def main():
    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    app.router.add_get( "/metrics",        handle_metrics)
    app.router.add_get( "/metrics/latest", handle_latest)
    app.router.add_post("/flush",          handle_flush)
    app.router.add_get( "/health",         handle_health)
    web.run_app(app, port=PORT)


if __name__ == "__main__":
    main()