import asyncio
import math
import multiprocessing
import os
import time
import subprocess
from aiohttp import web
import psutil
import numpy as np

PORT         = 8080
POOL_SIZE    = int(os.environ.get("POOL_SIZE", str(os.cpu_count() or 4)))


def burn_cpu(duration_s, intensity):
    """
    Burn CPU at the given intensity (0.0 - 1.0) for duration_s seconds.
    Uses a duty cycle: within each 100ms slice, burn for intensity * 100ms
    then sleep for the remainder.
    """
    start_time = time.time()
    subprocess.run([
        "timeout", f"{duration_s}", "stress-ng",
        "--cpu", "0", "--cpu-load", "10"
    ])
    return start_time
# global pool — initialized once at startup
pool = None


async def handle_invocation(request):
    """
    Expects JSON: { "duration_ms": float, "intensity": float }
    Submits burn to the process pool and waits non-blocking for it to finish.
    """
    arrival_time = time.time()

    data       = await request.json()

    parse_time = time.time()

    duration_s = data["duration_ms"] / 1000
    intensity  = data.get("intensity", 1.0)

    pool_start = time.time()

    loop = asyncio.get_event_loop()
    # run_in_executor submits to the pool without blocking the event loop
    burn_start = await loop.run_in_executor(None, lambda: pool.apply(burn_cpu, (duration_s, intensity)))

    burn_finish = time.time()

    output = f"{arrival_time},{burn_finish},parse={( parse_time - arrival_time)*1000:.1f}ms pool_wait={(burn_start - pool_start)*1000:.1f}ms burn_latency={(burn_finish - burn_start - duration_s)*1000:.1f}ms node_cpu_load={psutil.cpu_percent()}%"
    return web.Response(text=output)


async def handle_health(request):
    return web.Response(text="ok")


def main():
    psutil.cpu_percent()
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["OPENBLAS_NUM_THREADS"] = "1"
    os.environ["MKL_NUM_THREADS"] = "1"
    global pool
    print(f"Starting worker with pool size {POOL_SIZE}")
    pool = multiprocessing.Pool(processes=POOL_SIZE)

    app = web.Application()
    app.router.add_post("/invoke", handle_invocation)
    app.router.add_get("/health", handle_health)
    web.run_app(app, port=PORT)

    pool.close()
    pool.join()


if __name__ == "__main__":
    main()

