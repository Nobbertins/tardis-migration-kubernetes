import asyncio
import math
import time
from multiprocessing import Process

from aiohttp import web

PORT = 8080


def burn_cpu(duration_s, intensity):
    """
    Burn CPU at the given intensity (0.0 - 1.0) for duration_s seconds.
    Uses a duty cycle: within each 100ms slice, burn for intensity * 100ms
    then sleep for the remainder.
    """
    if intensity <= 0:
        time.sleep(duration_s)
        return
    slice_s  = 0.1
    busy_s   = slice_s * intensity
    sleep_s  = slice_s * (1 - intensity)
    end      = time.time() + duration_s
    while time.time() < end:
        busy_end = time.time() + busy_s
        while time.time() < busy_end:
            math.sqrt(99999999 ** 2)
        if sleep_s > 0 and time.time() < end:
            time.sleep(sleep_s)


async def handle_invocation(request):
    """
    Expects JSON: { "duration_ms": float, "intensity": float }
    Spawns a burn process, waits for it to finish, then responds.
    Latency is measured from when the request arrives to when the
    burn completes — the orchestrator measures end-to-end.
    """
    data        = await request.json()
    duration_s  = data["duration_ms"] / 1000
    intensity   = data.get("intensity", 1.0)

    p = Process(target=burn_cpu, args=(duration_s, intensity))
    p.start()

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, p.join)

    return web.Response(text="done")


async def handle_health(request):
    return web.Response(text="ok")


def main():
    app = web.Application()
    app.router.add_post("/invoke", handle_invocation)
    app.router.add_get("/health", handle_health)
    web.run_app(app, port=PORT)


if __name__ == "__main__":
    main()