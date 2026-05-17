import asyncio
import csv
import os
import time

import aiohttp

# ── config ────────────────────────────────────────────────────────────────────
TRACE_FILE    = os.environ.get("TRACE_FILE", "trace.txt")
START_DELAY   = int(os.environ.get("START_DELAY", "60"))
TIME_SCALE    = float(os.environ.get("TIME_SCALE", "1.0"))
WORKER_SVC    = os.environ.get("WORKER_SVC", "http://worker-{app_id}.default.svc.cluster.local:8080")
RESULTS_FILE  = os.environ.get("RESULTS_FILE", "/results/latencies.csv")
WINDOW_START  = float(os.environ["WINDOW_START"]) if "WINDOW_START" in os.environ else None
WINDOW_END    = float(os.environ["WINDOW_END"])   if "WINDOW_END"   in os.environ else None
# ─────────────────────────────────────────────────────────────────────────────


def parse_trace(filepath):
    """
    Returns a list of invocations sorted by start_time:
        [{ app_id, start_time, duration_ms }, ...]
    """
    invocations = []
    with open(filepath, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            duration_ms = float(row["duration"]) * 1000
            end_time    = float(row["end_timestamp"])
            start_time  = end_time - (duration_ms / 1000)
            invocations.append({
                "app_id":      row["app"].strip()[:16],
                "start_time":  start_time,
                "duration_ms": duration_ms,
            })
    invocations.sort(key=lambda x: x["start_time"])
    return invocations


def normalize_times(invocations):
    """Re-zero all start times relative to the earliest invocation."""
    min_start = invocations[0]["start_time"]
    for inv in invocations:
        inv["start_time"] -= min_start
    return invocations


def apply_window(invocations, window_start, window_end):
    """
    Filter invocations to only those whose start_time falls within
    [window_start, window_end]. Times are already normalized to 0.
    """
    filtered = invocations
    if window_start is not None:
        filtered = [i for i in filtered if i["start_time"] >= window_start]
    if window_end is not None:
        filtered = [i for i in filtered if i["start_time"] <= window_end]
    # re-zero again relative to the window start
    if filtered:
        min_start = filtered[0]["start_time"]
        for inv in filtered:
            inv["start_time"] -= min_start
    return filtered


async def dispatch(session, app_id, duration_ms, send_time, results):
    """
    Send a single invocation request to the app's worker pod and record latency.
    """
    url = WORKER_SVC.format(app_id=app_id)
    try:
        async with session.post(
            f"{url}/invoke",
            json={"duration_ms": duration_ms, "intensity": 1.0},
            timeout=aiohttp.ClientTimeout(total=300),
        ) as resp:
            receive_time = time.time()
            latency_ms   = (receive_time - send_time) * 1000
            status       = resp.status
            print(f"[{app_id}] latency={latency_ms:.1f}ms status={status}")
            results.append({
                "app_id":      app_id,
                "duration_ms": duration_ms,
                "latency_ms":  latency_ms,
                "status":      status,
            })
    except Exception as e:
        print(f"[{app_id}] error: {e}")


def write_results(results, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["app_id", "duration_ms", "latency_ms", "status"])
        writer.writeheader()
        writer.writerows(results)
    print(f"Results written to {filepath}")


def print_tail_latency(results):
    if not results:
        print("No results to report.")
        return
    latencies = sorted(r["latency_ms"] for r in results)
    n = len(latencies)
    print(f"\n── Tail Latency Report ({'─' * 30})")
    print(f"  Total invocations : {n}")
    print(f"  p50  : {latencies[int(n * 0.50)]:.1f}ms")
    print(f"  p90  : {latencies[int(n * 0.90)]:.1f}ms")
    print(f"  p95  : {latencies[int(n * 0.95)]:.1f}ms")
    print(f"  p99  : {latencies[int(n * 0.99)]:.1f}ms")
    print(f"  p999 : {latencies[min(int(n * 0.999), n - 1)]:.1f}ms")
    print(f"  max  : {latencies[-1]:.1f}ms")


async def run(invocations):
    sim_start = time.time() + START_DELAY
    results   = []

    print(f"Simulation starts in {START_DELAY}s (at t={sim_start:.0f})")
    await asyncio.sleep(START_DELAY)
    print("Simulation started.")

    async with aiohttp.ClientSession() as session:
        tasks = []
        for inv in invocations:
            target = sim_start + inv["start_time"] * TIME_SCALE
            wait   = target - time.time()
            if wait > 0:
                await asyncio.sleep(wait)

            send_time = time.time()
            print(f"[{inv['app_id']}] dispatching duration={inv['duration_ms']:.0f}ms")
            task = asyncio.create_task(
                dispatch(session, inv["app_id"], inv["duration_ms"], send_time, results)
            )
            tasks.append(task)

        await asyncio.gather(*tasks)

    print_tail_latency(results)
    write_results(results, RESULTS_FILE)


def main():
    print(f"Reading trace from {TRACE_FILE}")
    invocations = parse_trace(TRACE_FILE)
    invocations = normalize_times(invocations)

    print(f"Loaded {len(invocations)} invocations before windowing")

    invocations = apply_window(invocations, WINDOW_START, WINDOW_END)
    print(f"Invocations after windowing: {len(invocations)}")

    if not invocations:
        print("No invocations in window, exiting.")
        return

    asyncio.run(run(invocations))


if __name__ == "__main__":
    main()