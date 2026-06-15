import asyncio
import json
import os
import re
import time
import csv
from aiohttp import web, ClientSession, ClientTimeout
import psutil

PORT            = int(os.environ.get("PORT", "9100"))
SCRAPE_INTERVAL = float(os.environ.get("SCRAPE_INTERVAL", "1.0"))
RESULTS_FILE    = os.environ.get("RESULTS_FILE", "/results/metrics.csv")
NODE_NAME       = os.environ.get("NODE_NAME", "unknown")
MAX_SAMPLES     = int(os.environ.get("MAX_SAMPLES", "86400"))
NODE_IP         = os.environ.get("NODE_IP", "127.0.0.1")
CADVISOR_URL    = f"https://{NODE_IP}:10250/metrics/cadvisor"

TOKEN_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/token"

with open(TOKEN_FILE) as f:
    KUBE_TOKEN = f.read().strip()

samples: list[dict] = []
collector_task  = None
http_session: ClientSession | None = None

# ── cAdvisor scraping ─────────────────────────────────────────────────────────

def parse_cadvisor(text: str) -> dict[str, float]:
    pod_cpu: dict[str, float] = {}

    for line in text.splitlines():
        if "container_cpu_usage_seconds_total" not in line:
            continue
        if line.startswith("#"):
            continue

        try:
            parts = line.split()
            # format: <metric>{<labels>} <value> [<timestamp_ms>]
            # always take index 1 (value), not -1 (which would be the timestamp)
            metric = parts[0]
            value = float(parts[1])
        except (ValueError, IndexError):
            continue

        if "{" not in metric:
            continue

        label_str = metric[metric.find("{") + 1 : metric.rfind("}")]
        labels = {}
        for part in label_str.split(","):
            if "=" not in part:
                continue
            k, _, v = part.partition("=")
            labels[k.strip()] = v.strip().strip('"')

        pod = labels.get("pod") or labels.get("pod_name") or ""
        namespace = labels.get("namespace") or ""

        # skip cgroup rollup lines (no pod) and lines without a namespace
        if not pod or not namespace:
            continue

        # only count the pod-level rollup line (container=""), not per-container lines
        # to avoid double-counting
        container = labels.get("container") or labels.get("container_name") or ""
        if container != "":
            continue

        pod_cpu[pod] = pod_cpu.get(pod, 0.0) + value

    print(f"[parse] found {len(pod_cpu)} pods: {list(pod_cpu.keys())[:5]}")
    return pod_cpu


async def scrape_cadvisor() -> dict[str, float]:
    try:
        async with http_session.get(
            CADVISOR_URL,
            headers={"Authorization": f"Bearer {KUBE_TOKEN}"},
            ssl=False,
            timeout=ClientTimeout(total=3)
        ) as resp:
            if resp.status != 200:
                print(f"[cadvisor] HTTP {resp.status}")
                return {}

            text = await resp.text(errors="ignore")
            return parse_cadvisor(text)

    except Exception as e:
        print(f"[cadvisor] scrape failed: {e}")
        return {}


# ── collection loop ───────────────────────────────────────────────────────────

async def collect_loop():
    psutil.cpu_percent()
    await asyncio.sleep(SCRAPE_INTERVAL)

    prev_pod_cpu: dict[str, float] = {}
    prev_ts: float = time.time()

    while True:
        ts        = time.time()
        cpu_total = psutil.cpu_percent()
        elapsed   = ts - prev_ts

        raw_pod_cpu = await scrape_cadvisor()
        pod_cpu_pct: dict[str, float] = {}

        if elapsed > 0 and prev_pod_cpu:
            for pod, cum in raw_pod_cpu.items():
                if pod in prev_pod_cpu:
                    delta = cum - prev_pod_cpu[pod]
                    if 0 < delta < 3600:
                        pod_cpu_pct[pod] = round(100.0 * delta / elapsed, 2)
                    else:
                        print(f"[delta] {pod} delta={delta:.6f} cum={cum:.6f} prev={prev_pod_cpu[pod]:.6f}")

        prev_pod_cpu = raw_pod_cpu
        prev_ts      = ts

        samples.append({
            "timestamp": ts,
            "node":      NODE_NAME,
            "cpu_pct":   round(cpu_total, 2),
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
            data = [s for s in samples if s["timestamp"] >= float(since)]
        except ValueError:
            raise web.HTTPBadRequest(text="'since' must be a unix timestamp")

    if fmt == "csv":
        lines = ["timestamp,node,cpu_pct,pods"]
        for s in data:
            pods_json = json.dumps(s.get("pods", {}))
            lines.append(f"{s['timestamp']},{s['node']},{s['cpu_pct']},\"{pods_json}\"")
        return web.Response(text="\n".join(lines), content_type="text/csv")

    return web.json_response(data)


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
        writer = csv.DictWriter(f, fieldnames=["timestamp", "node", "cpu_pct", "pods"], extrasaction="ignore")
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
    global collector_task, http_session
    http_session   = ClientSession()
    print(f"Node={NODE_NAME}  interval={SCRAPE_INTERVAL}s  cAdvisor={CADVISOR_URL}")
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