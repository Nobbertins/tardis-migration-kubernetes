import asyncio
import os
import time
import subprocess
import multiprocessing
from aiohttp import web
import psutil

PORT      = 8080
POOL_SIZE = int(os.environ.get("POOL_SIZE", str(os.cpu_count() or 4)))

# ── drain state ───────────────────────────────────────────────────────────────
draining          = False
active_tasks      = 0
active_tasks_lock = asyncio.Lock()

pool = None


def burn_cpu(duration_s, intensity):
    start_time = time.time()
    subprocess.run([
        "timeout", f"{duration_s}", "stress-ng",
        "--cpu", "0", "--cpu-load", "60"
    ])
    return start_time


async def handle_invocation(request):
    global active_tasks

    if draining:
        return web.Response(status=503, text="draining")

    arrival_time = time.time()
    data         = await request.json()
    duration_s   = data["duration_ms"] / 1000
    intensity    = data.get("intensity", 1.0)

    async with active_tasks_lock:
        active_tasks += 1

    try:
        pool_start  = time.time()
        loop        = asyncio.get_event_loop()
        burn_start  = await loop.run_in_executor(None, lambda: pool.apply(burn_cpu, (duration_s, intensity)))
        burn_finish = time.time()

        output = (
            f"{arrival_time},{burn_finish},"
            f"parse={( arrival_time - arrival_time)*1000:.1f}ms "
            f"pool_wait={(burn_start - pool_start)*1000:.1f}ms "
            f"burn_latency={(burn_finish - burn_start - duration_s)*1000:.1f}ms "
            f"node_cpu_load={psutil.cpu_percent()}%"
        )
        return web.Response(text=output)
    finally:
        async with active_tasks_lock:
            active_tasks -= 1


async def handle_drain(request):
    """
    POST /drain — stop accepting new tasks and wait for in-flight ones to finish.
    Controller calls this before evicting the pod.
    """
    global draining
    draining = True
    print(f"[drain] Draining — waiting for {active_tasks} active task(s)")

    while True:
        async with active_tasks_lock:
            if active_tasks == 0:
                break
        await asyncio.sleep(0.5)

    print("[drain] Done, safe to evict")
    return web.Response(text="drained")


async def handle_health(request):
    if draining:
        return web.Response(status=503, text="draining")
    return web.Response(text="ok")


def main():
    psutil.cpu_percent()
    os.environ["OMP_NUM_THREADS"]      = "1"
    os.environ["OPENBLAS_NUM_THREADS"] = "1"
    os.environ["MKL_NUM_THREADS"]      = "1"

    global pool
    print(f"Starting worker with pool size {POOL_SIZE}")
    pool = multiprocessing.Pool(processes=POOL_SIZE)

    app = web.Application()
    app.router.add_post("/invoke", handle_invocation)
    app.router.add_post("/drain",  handle_drain)
    app.router.add_get( "/health", handle_health)
    web.run_app(app, port=PORT)

    pool.close()
    pool.join()


if __name__ == "__main__":
    main()