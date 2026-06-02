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
CADVISOR_URL    = f"http://{NODE_IP}:10255/metrics/cadvisor"

samples: list[dict] = []
collector_task  = None
http_session: ClientSession | None = None

# ── cAdvisor scraping ─────────────────────────────────────────────────────────

def parse_cadvisor(text: str) -> dict[str, float]:
    """
    Parse container_cpu_usage_seconds_total from cAdvisor prometheus text.
    Returns {pod_name: cumulative_cpu_seconds}, skipping pause containers.
    """
    pod_cpu: dict[str, float] = {}
    pattern = re.compile(
        r'^container_cpu_usage_seconds_total\{([^}]+)\}\s+([\d.e+]+)', re.MULTILINE
    )
    for m in pattern.finditer(text):
        labels: dict[str, str] = {}
        for part in m.group(1).split(","):
            k, _, v = part.partition("=")
            labels[k.strip()] = v.strip().strip('"')

        pod  = labels.get("pod", "")
        name = labels.get("name", "")
        if not pod or not name or "pause" in name:
            continue

        pod_cpu[pod] = pod_cpu.get(pod, 0.0) + float(m.group(2))
    return pod_cpu


async def scrape_cadvisor() -> dict[str, float]:
    try:
        async with http_session.get(CADVISOR_URL, timeout=ClientTimeout(total=3)) as resp:
            return parse_cadvisor(await resp.text())
    except Exception as e:
        print(f"cAdvisor scrape failed: {e}")
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

        raw_pod_cpu = await scrape_cadvisor()
        elapsed     = ts - prev_ts
        pod_cpu_pct: dict[str, float] = {}

        if elapsed > 0 and prev_pod_cpu:
            for pod, cum in raw_pod_cpu.items():
                if pod in prev_pod_cpu:
                    delta = cum - prev_pod_cpu[pod]
                    pod_cpu_pct[pod] = round(100.0 * delta / elapsed, 2)

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