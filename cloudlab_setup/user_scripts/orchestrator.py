import asyncio
import csv
import os
import time

import aiohttp
import aiodns

# ── config ────────────────────────────────────────────────────────────────────
TRACE_FILE    = os.environ.get("TRACE_FILE", "trace.txt")
START_DELAY   = int(os.environ.get("START_DELAY", "180"))
TIME_SCALE    = float(os.environ.get("TIME_SCALE", "1.0"))
WORKER_SVC    = os.environ.get("WORKER_SVC", "http://worker-{app_id}.default.svc.cluster.local:8080")
RESULTS_FILE  = os.environ.get("RESULTS_FILE", "/results/latencies.txt")
WINDOW_START  = float(os.environ["WINDOW_START"]) if "WINDOW_START" in os.environ else None
WINDOW_END    = float(os.environ["WINDOW_END"])   if "WINDOW_END"   in os.environ else None

# retry config — used when a worker is evicted mid-task or restarting
MAX_RETRIES   = int(os.environ.get("MAX_RETRIES", "5"))
RETRY_DELAY   = float(os.environ.get("RETRY_DELAY", "2.0"))   # seconds between retries

# Invocations at/under this duration are dropped as degenerate trace rows.
# Must match MIN_DURATION_MS in graph_invocations.py — otherwise the graph
# will show invocations for a window that the orchestrator never dispatches.
MIN_DURATION_MS = float(os.environ.get("MIN_DURATION_MS", "0.01"))
# ─────────────────────────────────────────────────────────────────────────────

program_start = 0


def make_entity_id(app_id, func_id, app_chars=12, func_chars=12):
    """
    Combine an app hash and a func hash into one k8s-safe deployment id,
    e.g. worker-{entity_id}. Truncated (12+1+12=25 chars) to stay well
    under the 63-char DNS label limit for Service/Deployment names while
    keeping collision risk negligible (48 bits of entropy per half).
    """
    return f"{app_id.strip()[:app_chars]}-{func_id.strip()[:func_chars]}"


def parse_trace(filepath):
    invocations = []
    with open(filepath, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            duration_ms = float(row["duration"]) * 1000
            end_time    = float(row["end_timestamp"])
            start_time  = end_time - (duration_ms / 1000)
            invocations.append({
                # "app_id" now identifies one function deployment (app+func),
                # not a whole application — kept as "app_id" to match the
                # worker/service naming scheme (worker-{app_id}) unchanged.
                "app_id":      make_entity_id(row["app"], row["func"]),
                "start_time":  start_time,
                "duration_ms": duration_ms,
            })
    invocations.sort(key=lambda x: x["start_time"])
    return invocations


def normalize_times(invocations):
    min_start = invocations[0]["start_time"]
    for inv in invocations:
        inv["start_time"] -= min_start
    return invocations


def apply_window(invocations, window_start, window_end):
    """
    Keep only invocations that both start and end within [window_start,
    window_end] (both are in "seconds since trace start", i.e. after
    normalize_times() has already been applied to `invocations`).

    NOTE: graph_invocations.py's pick_entities()/plot() use the identical
    containment check (start >= t_start AND end <= t_end) plus the same
    MIN_DURATION_MS drop, so a matching --start/--end there will show
    exactly the invocations this function selects.
    """
    filtered = invocations
    if window_start is not None:
        filtered = [i for i in filtered if i["start_time"] >= window_start]
    if window_end is not None:
        filtered = [i for i in filtered
                    if i["start_time"] + i["duration_ms"] / 1000 <= window_end]

    filtered = [i for i in filtered if i["duration_ms"] > MIN_DURATION_MS]

    if filtered:
        min_start = filtered[0]["start_time"]
        for inv in filtered:
            inv["start_time"] -= min_start

    return filtered


async def dispatch(session, app_id, duration_ms, send_time, results):
    """
    Send an invocation to the worker, retrying on eviction-related errors:
      - 503 (worker draining — new pod not ready yet)
      - ServerDisconnectedError (pod killed mid-task)
      - ClientConnectorError (pod restarting, DNS not yet resolving)
      - TimeoutError (pod unresponsive during migration)

    Retries up to MAX_RETRIES times with RETRY_DELAY between attempts.
    On each retry send_time is reset so overhead is measured from the
    final successful attempt only.
    """
    url = WORKER_SVC.format(app_id=app_id)

    retryable = (
        aiohttp.ServerDisconnectedError,
        aiohttp.ClientConnectorError,
        aiohttp.ClientOSError,
        asyncio.TimeoutError,
    )

    for attempt in range(MAX_RETRIES + 1):
        if attempt > 0:
            print(f"[{app_id}] retry {attempt}/{MAX_RETRIES} in {RETRY_DELAY}s")
            await asyncio.sleep(RETRY_DELAY)
            # clear DNS cache so we resolve fresh — old pod IP may be gone
            session.connector.clear_dns_cache()
            send_time = time.time()

        try:
            async with session.post(
                f"{url}/invoke",
                json={"duration_ms": duration_ms, "intensity": 1.0},
                timeout=aiohttp.ClientTimeout(total=duration_ms / 1000 + 30),
            ) as resp:

                # 503 = worker draining, new pod not ready — retry
                if resp.status == 503:
                    body = await resp.text()
                    print(f"[{app_id}] 503 ({body}), will retry")
                    continue

                worker_response      = (await resp.text()).split(',')
                worker_start         = float(worker_response[0])
                worker_finish        = float(worker_response[1])
                receive_time         = time.time()
                network_receive_ms   = (receive_time - worker_finish) * 1000
                network_send_ms      = (worker_start - send_time) * 1000
                latency_ms           = (receive_time - send_time) * 1000
                overhead_ms          = latency_ms - duration_ms

                print(f"({time.time() - program_start:.1f}s) [{app_id}] worker log: {worker_response[2]}")
                print(f"worker_latency={((worker_finish-worker_start)*1000-duration_ms):.1f}ms "
                      f"network_send_ms={network_send_ms:.1f}ms "
                      f"network_receive_ms={network_receive_ms:.1f}ms")
                print(f"overhead={overhead_ms:.1f}ms latency={latency_ms:.1f}ms "
                      f"duration={duration_ms:.0f}ms status={resp.status}"
                      + (f" (after {attempt} retries)" if attempt > 0 else ""))

                results.append({
                    "app_id":      app_id,
                    "duration_ms": duration_ms,
                    "latency_ms":  latency_ms,
                    "overhead_ms": overhead_ms,
                    "status":      resp.status,
                    "retries":     attempt,
                })
                return  # success

        except retryable as e:
            if attempt == MAX_RETRIES:
                print(f"[{app_id}] failed after {MAX_RETRIES} retries: {type(e).__name__}: {e}")
                return
            print(f"[{app_id}] {type(e).__name__}: {e}")

        except Exception as e:
            print(f"[{app_id}] unexpected error: {type(e).__name__}: {e}")
            return


def write_results(results, filepath):
    if not results:
        print("No results to write.")
        return
    dirpath = os.path.dirname(filepath)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["app_id", "duration_ms", "latency_ms", "overhead_ms", "status", "retries"]
        )
        writer.writeheader()
        writer.writerows(results)
    print(f"Results written to {filepath}")

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
            resolver=aiohttp.AsyncResolver(),
            ttl_dns_cache=5,
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
            print(f"({send_time - program_start:.1f}s) [{inv['app_id']}] dispatching duration={inv['duration_ms']:.0f}ms")
            task = asyncio.create_task(
                dispatch(session, inv["app_id"], inv["duration_ms"], send_time, results)
            )
            tasks.append(task)

        await asyncio.gather(*tasks, return_exceptions=True)
    if not results:
        print("No results to report.")
        return
    overheads = sorted(r["overhead_ms"] for r in results)
    n = len(overheads)
    retried = sum(1 for r in results if r["retries"] > 0)
    # print(f"\n── Tail Latency Report (overhead = latency - duration) ──")
    # print(f"  Total invocations : {n}")
    # print(f"  Retried           : {retried}")
    # print(f"  p50  : {overheads[int(n * 0.50)]:.1f}ms")       # short TTL so migrated pods are found quickly
    # print(f"  p60  : {overheads[int(n * 0.60)]:.1f}ms")
    # print(f"  p70  : {overheads[int(n * 0.70)]:.1f}ms")
    # print(f"  p80  : {overheads[int(n * 0.80)]:.1f}ms")
    # print(f"  p90  : {overheads[int(n * 0.90)]:.1f}ms")
    # print(f"  p95  : {overheads[int(n * 0.95)]:.1f}ms")
    # print(f"  p99  : {overheads[int(n * 0.99)]:.1f}ms")
    # print(f"  p999 : {overheads[min(int(n * 0.999), n - 1)]:.1f}ms")
    # print(f"  max  : {overheads[-1]:.1f}ms")
    #RESULTS_FILE = f"/results/latencies{int(time.time())}.txt"
    output = f"\n── Tail Latency Report (overhead = latency - duration) ──\n  Total invocations : {n}\n  Retried           : {retried}\np50  : {overheads[int(n * 0.50)]:.1f}ms\n  p60  : {overheads[int(n * 0.60)]:.1f}ms\n  p70  : {overheads[int(n * 0.70)]:.1f}ms\n  p80  : {overheads[int(n * 0.80)]:.1f}ms\n  p90  : {overheads[int(n * 0.90)]:.1f}ms\n  p99  : {overheads[int(n * 0.99)]:.1f}ms\n  p995  : {overheads[int(n * 0.995)]:.1f}ms\n  p999  : {overheads[int(n * 0.999)]:.1f}ms\n  max  : {overheads[-1]:.1f}ms"
    print(output)
    dirpath = os.path.dirname(RESULTS_FILE)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    with open(RESULTS_FILE, "w", newline="") as f:
        f.write(output)
        f.close()

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