import asyncio
import csv
import os
import time

import aiohttp
import aiodns

# ── config ────────────────────────────────────────────────────────────────────
TRACE_FILE    = os.environ.get("TRACE_FILE", "trace.txt")
START_DELAY   = int(os.environ.get("START_DELAY", "120"))
TIME_SCALE    = float(os.environ.get("TIME_SCALE", "1.0"))
WORKER_SVC    = os.environ.get("WORKER_SVC", "http://worker-{app_id}.default.svc.cluster.local:8080")
RESULTS_FILE  = os.environ.get("RESULTS_FILE", "/results/latencies.csv")
WINDOW_START  = float(os.environ["WINDOW_START"]) if "WINDOW_START" in os.environ else None
WINDOW_END    = float(os.environ["WINDOW_END"])   if "WINDOW_END"   in os.environ else None
# ─────────────────────────────────────────────────────────────────────────────

program_start = 0

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
    Filter invocations to those whose start_time falls within
    [window_start, window_end]. Invocations that start within the window
    but have a duration extending past window_end are clamped so their
    duration ends at window_end — prevents runaway burns after the window closes.
    Times are already normalized to 0 before this is called.
    """
    filtered = invocations
    if window_start is not None:
        filtered = [i for i in filtered if i["start_time"] >= window_start]
    if window_end is not None:
        # discard any invocation that doesn't fully complete within the window
        filtered = [i for i in filtered
                    if i["start_time"] + i["duration_ms"] / 1000 <= window_end]

    # remove zero-duration invocations (worker hangs on these)
    filtered = [i for i in filtered if i["duration_ms"] > 0.01]
    
    # re-zero relative to the first invocation in the window
    if filtered:
        min_start = filtered[0]["start_time"]
        for inv in filtered:
            inv["start_time"] -= min_start

    return filtered


async def dispatch(session, app_id, duration_ms, send_time, results):
    """
    Send a single invocation request to the app's worker pod and record overhead.
    overhead_ms = latency_ms - duration_ms (extra time beyond the function's actual duration)
    """
    url = WORKER_SVC.format(app_id=app_id)
    try:
        async with session.post(
            f"{url}/invoke",
            json={"duration_ms": duration_ms, "intensity": 1.0},
            timeout=aiohttp.ClientTimeout(total=duration_ms / 1000 + 30),  # duration + 30s grace
        ) as resp:
            worker_response = (await resp.text()).split(',')
            worker_start = float(worker_response[0])
            worker_finish = float(worker_response[1])
            print(f"({time.time() - program_start}s) [{app_id}] worker log: {worker_response[2]}")
            receive_time = time.time()
            network_receive_ms = (receive_time - worker_finish) * 1000
            network_send_ms = (worker_start - send_time) * 1000
            print(f"worker_latency={((worker_finish-worker_start)*1000-duration_ms):.1f}ms network_send_ms={network_send_ms:.1f}ms network_receive_ms={network_receive_ms:.1f}ms")
            latency_ms   = (receive_time - send_time) * 1000
            overhead_ms  = latency_ms - duration_ms
            status       = resp.status
            print(f"overhead={overhead_ms:.1f}ms latency={latency_ms:.1f}ms duration={duration_ms:.0f}ms status={status}")
            results.append({
                "app_id":      app_id,
                "duration_ms": duration_ms,
                "latency_ms":  latency_ms,
                "overhead_ms": overhead_ms,
                "status":      status,
            })
    except asyncio.TimeoutError:
        print(f"[{app_id}] timed out after {duration_ms / 1000 + 30:.0f}s (duration={duration_ms:.0f}ms)")
    except aiohttp.ServerDisconnectedError:
        print(f"[{app_id}] server disconnected (duration={duration_ms:.0f}ms)")
    except aiohttp.ClientConnectorError as e:
        print(f"[{app_id}] connection error: {e}")
    except Exception as e:
        print(f"[{app_id}] unexpected error: {type(e).__name__}: {e}")


def write_results(results, filepath):
    if not results:
        print("No results to write.")
        return
    dirpath = os.path.dirname(filepath)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["app_id", "duration_ms", "latency_ms", "overhead_ms", "status"])
        writer.writeheader()
        writer.writerows(results)
    print(f"Results written to {filepath}")


def print_tail_latency(results):
    if not results:
        print("No results to report.")
        return
    overheads = sorted(r["overhead_ms"] for r in results)
    n = len(overheads)
    print(f"\n── Tail Latency Report (overhead = latency - duration) ──")
    print(f"  Total invocations : {n}")
    print(f"  p50  : {overheads[int(n * 0.50)]:.1f}ms")
    print(f"  p60  : {overheads[int(n * 0.60)]:.1f}ms")
    print(f"  p70  : {overheads[int(n * 0.70)]:.1f}ms")
    print(f"  p80  : {overheads[int(n * 0.80)]:.1f}ms")
    print(f"  p90  : {overheads[int(n * 0.90)]:.1f}ms")
    print(f"  p95  : {overheads[int(n * 0.95)]:.1f}ms")
    print(f"  p99  : {overheads[int(n * 0.99)]:.1f}ms")
    print(f"  p999 : {overheads[min(int(n * 0.999), n - 1)]:.1f}ms")
    print(f"  max  : {overheads[-1]:.1f}ms")


async def run(invocations):
    global program_start
    sim_start = time.time() + START_DELAY
    results   = []

    print(f"Simulation starts in {START_DELAY}s (at t={sim_start:.0f})")
    await asyncio.sleep(START_DELAY)
    print("Simulation started.")

    program_start = time.time()
    
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(
        resolver=aiohttp.AsyncResolver(),  # uses aiodns with caching
        ttl_dns_cache=300,                 # cache DNS results for 5 minutes
        use_dns_cache=True,
    )
    ) as session:
        tasks = []
        for inv in invocations:
            target = sim_start + inv["start_time"] * TIME_SCALE
            wait   = target - time.time()
            if wait > 0:
                await asyncio.sleep(wait)

            send_time = time.time()
            print(f"({send_time - program_start}s) [{inv['app_id']}] dispatching duration={inv['duration_ms']:.0f}ms")
            task = asyncio.create_task(
                dispatch(session, inv["app_id"], inv["duration_ms"], send_time, results)
            )
            tasks.append(task)

        await asyncio.gather(*tasks, return_exceptions=True)

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
