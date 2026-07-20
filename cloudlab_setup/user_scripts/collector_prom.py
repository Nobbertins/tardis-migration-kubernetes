import asyncio
import json
import os
import ssl
import time
import csv
from aiohttp import web, ClientSession, ClientTimeout
import psutil

PORT            = int(os.environ.get("PORT", "9100"))
SCRAPE_INTERVAL = float(os.environ.get("SCRAPE_INTERVAL", "1.0"))
RESULTS_FILE    = os.environ.get("RESULTS_FILE", "/results/metrics.csv")
NODE_NAME       = os.environ.get("NODE_NAME", "unknown")
MAX_SAMPLES     = int(os.environ.get("MAX_SAMPLES", "86400"))

PROM_POD_NAME   = "prometheus-prometheus-kube-prometheus-prometheus-0"
PROM_NAMESPACE  = "monitoring"
PROM_PORT       = 9090
KUBE_API        = "https://kubernetes.default.svc"
TOKEN_FILE      = "/var/run/secrets/kubernetes.io/serviceaccount/token"
CA_FILE         = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

with open(TOKEN_FILE) as f:
    KUBE_TOKEN = f.read().strip()

KUBE_SSL = ssl.create_default_context(cafile=CA_FILE)

POD_CPU_QUERY = f"""
sum by (namespace, pod) (
  irate(container_cpu_usage_seconds_total{{container!="POD", node="{NODE_NAME}"}}[1m])
)
"""

# Populated at startup
NUM_CORES: int = 1
PROM_URL:  str = ""

samples: list[dict] = []
collector_task  = None
http_session: ClientSession | None = None


# ── Kubernetes helpers ────────────────────────────────────────────────────────

async def resolve_prom_url(retries: int = 20, delay: float = 5.0) -> str:
    kube_url = f"{KUBE_API}/api/v1/namespaces/{PROM_NAMESPACE}/pods/{PROM_POD_NAME}"

    for attempt in range(1, retries + 1):
        try:
            async with http_session.get(
                kube_url,
                headers={"Authorization": f"Bearer {KUBE_TOKEN}"},
                ssl=KUBE_SSL,
                timeout=ClientTimeout(total=5),
            ) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"Kube API returned HTTP {resp.status}")
                body   = await resp.json()
                pod_ip = body["status"].get("podIP")
                phase  = body["status"].get("phase", "unknown")
                if not pod_ip:
                    raise RuntimeError(f"Prometheus pod has no IP yet (phase={phase})")
        except Exception as e:
            print(f"[startup] attempt {attempt}/{retries}: pod lookup failed: {e}")
            await asyncio.sleep(delay)
            continue

        prom_url = f"http://{pod_ip}:{PROM_PORT}"

        try:
            async with http_session.get(
                f"{prom_url}/-/ready",
                ssl=False,
                timeout=ClientTimeout(total=5),
            ) as resp:
                if resp.status == 200:
                    print(f"[startup] Prometheus ready at {prom_url}")
                    return prom_url
                raise RuntimeError(f"/-/ready returned HTTP {resp.status}")
        except Exception as e:
            print(f"[startup] attempt {attempt}/{retries}: Prometheus not ready at {prom_url}: {e}")
            await asyncio.sleep(delay)

    raise RuntimeError(f"Prometheus did not become ready after {retries} attempts")


# ── Prometheus scraping ───────────────────────────────────────────────────────

async def prom_query(query: str) -> list[dict]:
    """Run an instant query against Prometheus and return the result list."""
    try:
        async with http_session.get(
            f"{PROM_URL}/api/v1/query",
            params={"query": query},
            ssl=False,
            timeout=ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                print(f"[prom] HTTP {resp.status}")
                return []
            body = await resp.json()
            return body.get("data", {}).get("result", [])
    except Exception as e:
        print(f"[prom] query failed: {e}")
        return []


async def detect_num_cores() -> int:
    results = await prom_query(f'machine_cpu_cores{{node="{NODE_NAME}"}}')
    if results:
        try:
            cores = int(float(results[0]["value"][1]))
            print(f"[startup] Prometheus reports {cores} core(s) for node {NODE_NAME!r}")
            return max(cores, 1)
        except (KeyError, ValueError, IndexError):
            pass

    results = await prom_query("machine_cpu_cores")
    if results:
        try:
            cores = int(float(results[0]["value"][1]))
            print(f"[startup] Prometheus (unlabelled) reports {cores} core(s)")
            return max(cores, 1)
        except (KeyError, ValueError, IndexError):
            pass

    cores = psutil.cpu_count(logical=True) or 1
    print(f"[startup] Falling back to psutil: {cores} core(s)")
    return cores


async def scrape_pod_cpu() -> dict[str, float]:
    """
    Returns per-pod CPU as a percentage of total node capacity:
        (irate cores) / NUM_CORES * 100
    Keys are pod names.
    """
    results = await prom_query(POD_CPU_QUERY)
    out: dict[str, float] = {}
    for item in results:
        m = item["metric"]
        pod = m.get("pod", "unknown")
        try:
            cores = float(item["value"][1])
        except (KeyError, ValueError, IndexError):
            continue
        pct = round(cores / NUM_CORES * 100, 2)
        out[pod] = pct
    return out


# ── collection loop ───────────────────────────────────────────────────────────

async def collect_loop():
    await asyncio.sleep(SCRAPE_INTERVAL)

    while True:
        ts          = time.time()
        pod_cpu_pct = await scrape_pod_cpu()
        cpu_total   = round(sum(pod_cpu_pct.values()), 2)

        samples.append({
            "timestamp": ts,
            "node":      NODE_NAME,
            "cpu_pct":   cpu_total,
            "pods":      pod_cpu_pct,
        })
        if len(samples) > MAX_SAMPLES:
            samples.pop(0)

        await asyncio.sleep(SCRAPE_INTERVAL)


# ── HTTP handlers ─────────────────────────────────────────────────────────────

async def handle_metrics(request):
    since = request.rel_url.query.get("since")
    fmt   = request.rel_url.query.get("fmt", "json")

    data = samples
    if since:
        try:
            since_f = float(since)
        except ValueError:
            raise web.HTTPBadRequest(text="'since' must be a unix timestamp")
        data = [s for s in samples if s["timestamp"] >= since_f]

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
    lines = ["timestamp,node,cpu_pct,pods"]
    for s in data:
        pods_json = json.dumps(s.get("pods", {}))
        lines.append(f"{s['timestamp']},{s['node']},{s['cpu_pct']},\"{pods_json}\"")
    return "\n".join(lines)


async def handle_latest(request):
    if not samples:
        raise web.HTTPServiceUnavailable(text="No samples collected yet")
    return web.json_response(samples[-1])


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
    global collector_task, http_session, NUM_CORES, PROM_URL
    http_session = ClientSession()
    PROM_URL     = await resolve_prom_url()
    NUM_CORES    = await detect_num_cores()
    print(f"Node={NODE_NAME}  cores={NUM_CORES}  interval={SCRAPE_INTERVAL}s  prom={PROM_URL}")
    collector_task = asyncio.create_task(collect_loop())


async def on_shutdown(app):
    if collector_task:
        collector_task.cancel()
        try:
            await collector_task
        except asyncio.CancelledError:
            pass
    if http_session:
        await http_session.close()


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