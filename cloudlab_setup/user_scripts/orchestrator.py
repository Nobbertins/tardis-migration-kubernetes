import asyncio
import csv
import json
import os
import ssl
import time

import aiohttp
import aiodns  # Required by aiohttp.AsyncResolver.


# ── config ────────────────────────────────────────────────────────────────────
TRACE_FILE = os.environ.get("TRACE_FILE", "trace.txt")
START_DELAY = int(os.environ.get("START_DELAY", "180"))
TIME_SCALE = float(os.environ.get("TIME_SCALE", "1.0"))
WORKER_SVC = os.environ.get(
    "WORKER_SVC",
    "http://worker-{app_id}.default.svc.cluster.local:8080",
)

RESULTS_DIR = os.environ.get("RESULTS_DIR", "/results")
METRICS_INTERVAL = float(os.environ.get("METRICS_INTERVAL", "1.0"))
ENABLE_NODE_METRICS = os.environ.get("ENABLE_NODE_METRICS", "1").lower() in {
    "1", "true", "yes", "on"
}
NODE_METRICS_TIMEOUT = float(os.environ.get("NODE_METRICS_TIMEOUT", "5.0"))

# Optional compatibility override. Otherwise the summary is timestamped too.
RESULTS_FILE = os.environ.get("RESULTS_FILE")
WINDOW_START = (
    float(os.environ["WINDOW_START"]) if "WINDOW_START" in os.environ else None
)
WINDOW_END = (
    float(os.environ["WINDOW_END"]) if "WINDOW_END" in os.environ else None
)

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))
RETRY_DELAY = float(os.environ.get("RETRY_DELAY", "2.0"))
MIN_DURATION_MS = float(os.environ.get("MIN_DURATION_MS", "0.01"))
# ─────────────────────────────────────────────────────────────────────────────

program_start = 0.0


def make_entity_id(app_id, func_id, app_chars=12, func_chars=12):
    return f"{app_id.strip()[:app_chars]}-{func_id.strip()[:func_chars]}"


def parse_trace(filepath):
    invocations = []
    with open(filepath, newline="") as f:
        for row in csv.DictReader(f):
            duration_ms = float(row["duration"]) * 1000
            end_time = float(row["end_timestamp"])
            invocations.append({
                "app_id": make_entity_id(row["app"], row["func"]),
                "start_time": end_time - duration_ms / 1000,
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
    filtered = invocations
    if window_start is not None:
        filtered = [i for i in filtered if i["start_time"] >= window_start]
    if window_end is not None:
        filtered = [
            i for i in filtered
            if i["start_time"] + i["duration_ms"] / 1000 <= window_end
        ]
    filtered = [i for i in filtered if i["duration_ms"] > MIN_DURATION_MS]

    if filtered:
        min_start = filtered[0]["start_time"]
        for inv in filtered:
            inv["start_time"] -= min_start
    return filtered


def parse_worker_response(body):
    """Accept the existing CSV response or JSON with optional pod/node fields."""
    body = body.strip()
    if body.startswith("{"):
        data = json.loads(body)
        return {
            "start": float(data["start"]),
            "finish": float(data["finish"]),
            "log": data.get("log", ""),
            "pod": data.get("pod", ""),
            "node": data.get("node", ""),
        }

    parts = body.split(",", 2)
    if len(parts) < 2:
        raise ValueError(f"invalid worker response: {body!r}")
    return {
        "start": float(parts[0]),
        "finish": float(parts[1]),
        "log": parts[2] if len(parts) == 3 else "",
        "pod": "",
        "node": "",
    }


async def dispatch(
    session,
    app_id,
    duration_ms,
    initial_send_time,
    initial_send_monotonic,
    results,
    metrics,
):
    """Send one logical invocation and update shared metrics."""
    url = WORKER_SVC.format(app_id=app_id)
    attempt_send_time = initial_send_time
    attempt_send_monotonic = initial_send_monotonic
    succeeded = False
    retryable = (
        aiohttp.ServerDisconnectedError,
        aiohttp.ClientConnectorError,
        aiohttp.ClientOSError,
        asyncio.TimeoutError,
    )

    try:
        for attempt in range(MAX_RETRIES + 1):
            if attempt:
                print(
                    f"[{app_id}] retry {attempt}/{MAX_RETRIES} "
                    f"in {RETRY_DELAY}s"
                )
                await asyncio.sleep(RETRY_DELAY)
                session.connector.clear_dns_cache()
                attempt_send_time = time.time()
                attempt_send_monotonic = time.perf_counter()

            metrics["attempts_total"] += 1
            try:
                async with session.post(
                    f"{url}/invoke",
                    json={"duration_ms": duration_ms, "intensity": 1.0},
                    timeout=aiohttp.ClientTimeout(
                        total=duration_ms / 1000 + 30
                    ),
                ) as resp:
                    body = await resp.text()
                    if resp.status == 503:
                        print(f"[{app_id}] 503 ({body}), will retry")
                        continue
                    if resp.status >= 400:
                        print(f"[{app_id}] HTTP {resp.status}: {body}")
                        return

                    worker = parse_worker_response(body)
                    receive_time = time.time()
                    receive_monotonic = time.perf_counter()
                    worker_latency_ms = (
                        worker["finish"] - worker["start"]
                    ) * 1000
                    network_send_ms = (
                        worker["start"] - attempt_send_time
                    ) * 1000
                    network_receive_ms = (
                        receive_time - worker["finish"]
                    ) * 1000
                    latency_ms = (
                        receive_monotonic - attempt_send_monotonic
                    ) * 1000
                    total_latency_ms = (
                        receive_monotonic - initial_send_monotonic
                    ) * 1000
                    overhead_ms = latency_ms - duration_ms

                    print(
                        f"({receive_time - program_start:.1f}s) [{app_id}] "
                        f"worker log: {worker['log']}"
                    )
                    print(
                        f"worker_latency_ms={worker_latency_ms:.1f} "
                        f"network_send_ms={network_send_ms:.1f} "
                        f"network_receive_ms={network_receive_ms:.1f}"
                    )
                    print(
                        f"overhead={overhead_ms:.1f}ms "
                        f"latency={latency_ms:.1f}ms "
                        f"duration={duration_ms:.0f}ms status={resp.status}"
                        + (
                            f" (after {attempt} retries)"
                            if attempt else ""
                        )
                    )

                    results.append({
                        "app_id": app_id,
                        "pod": worker["pod"],
                        "node": worker["node"],
                        "send_time": attempt_send_time,
                        "receive_time": receive_time,
                        "duration_ms": duration_ms,
                        "latency_ms": latency_ms,
                        "overhead_ms": overhead_ms,
                        "total_latency_ms": total_latency_ms,
                        "worker_latency_ms": worker_latency_ms,
                        "network_send_ms": network_send_ms,
                        "network_receive_ms": network_receive_ms,
                        "status": resp.status,
                        "retries": attempt,
                    })
                    metrics["completed_total"] += 1
                    succeeded = True
                    return

            except retryable as e:
                if attempt == MAX_RETRIES:
                    print(
                        f"[{app_id}] failed after {MAX_RETRIES} retries: "
                        f"{type(e).__name__}: {e}"
                    )
                    return
                print(f"[{app_id}] {type(e).__name__}: {e}")
            except Exception as e:
                print(
                    f"[{app_id}] unexpected error: "
                    f"{type(e).__name__}: {e}"
                )
                return
    finally:
        metrics["in_flight"] -= 1
        if not succeeded:
            metrics["failed_total"] += 1


def ensure_parent_dir(filepath):
    parent = os.path.dirname(filepath)
    if parent:
        os.makedirs(parent, exist_ok=True)


def write_results(results, filepath):
    fieldnames = [
        "app_id", "pod", "node", "send_time", "receive_time",
        "duration_ms", "latency_ms", "overhead_ms", "total_latency_ms",
        "worker_latency_ms", "network_send_ms", "network_receive_ms",
        "status", "retries",
    ]
    ensure_parent_dir(filepath)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print(f"Wrote {len(results)} latency rows to {filepath}")


def write_summary(results, filepath):
    if not results:
        print("No successful results to summarize.")
        return

    overheads = sorted(r["overhead_ms"] for r in results)
    n = len(overheads)
    retried = sum(1 for r in results if r["retries"] > 0)

    def percentile(p):
        return overheads[min(int(n * p), n - 1)]

    output = (
        "\n── Tail Latency Report (overhead = latency - duration) ──\n"
        f"  Total invocations : {n}\n"
        f"  Retried           : {retried}\n"
        f"  p50  : {percentile(0.50):.1f}ms\n"
        f"  p60  : {percentile(0.60):.1f}ms\n"
        f"  p70  : {percentile(0.70):.1f}ms\n"
        f"  p80  : {percentile(0.80):.1f}ms\n"
        f"  p90  : {percentile(0.90):.1f}ms\n"
        f"  p95  : {percentile(0.95):.1f}ms\n"
        f"  p99  : {percentile(0.99):.1f}ms\n"
        f"  p995 : {percentile(0.995):.1f}ms\n"
        f"  p999 : {percentile(0.999):.1f}ms\n"
        f"  max  : {overheads[-1]:.1f}ms"
    )
    print(output)
    ensure_parent_dir(filepath)
    with open(filepath, "w", newline="") as f:
        f.write(output)


async def record_throughput(metrics, filepath, stop_event, start_time):
    fieldnames = [
        "timestamp", "elapsed_s", "interval_s",
        "dispatched_rps", "completed_rps", "failed_rps", "attempt_rps",
        "in_flight", "dispatched_total", "completed_total",
        "failed_total", "attempts_total",
    ]
    ensure_parent_dir(filepath)
    last_time = time.time()
    previous = {
        "dispatched_total": 0,
        "completed_total": 0,
        "failed_total": 0,
        "attempts_total": 0,
    }

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        while True:
            stopped = False
            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=METRICS_INTERVAL
                )
                stopped = True
            except asyncio.TimeoutError:
                pass

            now = time.time()
            interval = now - last_time
            if interval > 0:
                writer.writerow({
                    "timestamp": now,
                    "elapsed_s": now - start_time,
                    "interval_s": interval,
                    "dispatched_rps": (
                        metrics["dispatched_total"]
                        - previous["dispatched_total"]
                    ) / interval,
                    "completed_rps": (
                        metrics["completed_total"]
                        - previous["completed_total"]
                    ) / interval,
                    "failed_rps": (
                        metrics["failed_total"] - previous["failed_total"]
                    ) / interval,
                    "attempt_rps": (
                        metrics["attempts_total"] - previous["attempts_total"]
                    ) / interval,
                    "in_flight": metrics["in_flight"],
                    "dispatched_total": metrics["dispatched_total"],
                    "completed_total": metrics["completed_total"],
                    "failed_total": metrics["failed_total"],
                    "attempts_total": metrics["attempts_total"],
                })
                f.flush()

            last_time = now
            for key in previous:
                previous[key] = metrics[key]
            if stopped:
                break

    print(f"Throughput metrics written to {filepath}")


def parse_cpu_cores(value):
    units = {"n": 1e-9, "u": 1e-6, "m": 1e-3}
    suffix = value[-1:] if value else ""
    if suffix in units:
        return float(value[:-1]) * units[suffix]
    return float(value)


def parse_memory_bytes(value):
    units = {
        "Ki": 1024, "Mi": 1024**2, "Gi": 1024**3,
        "Ti": 1024**4, "Pi": 1024**5, "Ei": 1024**6,
        "K": 1000, "M": 1000**2, "G": 1000**3,
        "T": 1000**4, "P": 1000**5, "E": 1000**6,
    }
    for suffix, multiplier in units.items():
        if value.endswith(suffix):
            return float(value[:-len(suffix)]) * multiplier
    return float(value)


async def record_node_usage(filepath, stop_event, start_time):
    fieldnames = [
        "timestamp", "elapsed_s", "node", "cpu_usage", "cpu_cores",
        "memory_usage", "memory_bytes",
    ]
    ensure_parent_dir(filepath)

    # Leave a valid header-only CSV if metrics are unavailable.
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        f.flush()

        host = os.environ.get("KUBERNETES_SERVICE_HOST")
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        if not host:
            print("Not running inside Kubernetes; node metrics CSV is empty.")
            return

        try:
            with open(
                "/var/run/secrets/kubernetes.io/serviceaccount/token"
            ) as token_file:
                token = token_file.read().strip()
            ssl_context = ssl.create_default_context(
                cafile="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
            )
        except OSError as e:
            print(f"Node metrics credentials unavailable: {e}")
            return

        url = (
            f"https://{host}:{port}"
            "/apis/metrics.k8s.io/v1beta1/nodes"
        )
        headers = {"Authorization": f"Bearer {token}"}
        timeout = aiohttp.ClientTimeout(total=NODE_METRICS_TIMEOUT)

        async with aiohttp.ClientSession() as session:
            while not stop_event.is_set():
                sample_start = time.time()
                try:
                    async with session.get(
                        url,
                        headers=headers,
                        ssl=ssl_context,
                        timeout=timeout,
                    ) as resp:
                        if resp.status != 200:
                            print(
                                "Node metrics request failed: "
                                f"{resp.status} {await resp.text()}"
                            )
                        else:
                            data = await resp.json()
                            for node in data.get("items", []):
                                cpu = node["usage"]["cpu"]
                                memory = node["usage"]["memory"]
                                writer.writerow({
                                    "timestamp": sample_start,
                                    "elapsed_s": sample_start - start_time,
                                    "node": node["metadata"]["name"],
                                    "cpu_usage": cpu,
                                    "cpu_cores": parse_cpu_cores(cpu),
                                    "memory_usage": memory,
                                    "memory_bytes": parse_memory_bytes(memory),
                                })
                            f.flush()
                except Exception as e:
                    print(
                        f"Node metrics error: {type(e).__name__}: {e}"
                    )

                remaining = METRICS_INTERVAL - (time.time() - sample_start)
                if remaining > 0:
                    try:
                        await asyncio.wait_for(
                            stop_event.wait(), timeout=remaining
                        )
                    except asyncio.TimeoutError:
                        pass

    print(f"Node usage metrics written to {filepath}")


async def run(invocations):
    global program_start

    sim_start = time.time() + START_DELAY
    results = []
    print(f"Simulation starts in {START_DELAY}s (at t={sim_start:.0f})")
    await asyncio.sleep(START_DELAY)
    print("Simulation started.")

    program_start = time.time()
    run_id = f"{program_start:.6f}"
    latency_file = os.path.join(RESULTS_DIR, f"latencies_{run_id}.csv")
    throughput_file = os.path.join(
        RESULTS_DIR, f"throughput_{run_id}.csv"
    )
    node_usage_file = os.path.join(
        RESULTS_DIR, f"node_usage_{run_id}.csv"
    )
    summary_file = RESULTS_FILE or os.path.join(
        RESULTS_DIR, f"summary_{run_id}.txt"
    )

    metrics = {
        "dispatched_total": 0,
        "completed_total": 0,
        "failed_total": 0,
        "attempts_total": 0,
        "in_flight": 0,
    }
    stop_metrics = asyncio.Event()
    recorder_tasks = [
        asyncio.create_task(
            record_throughput(
                metrics, throughput_file, stop_metrics, program_start
            )
        )
    ]
    if ENABLE_NODE_METRICS:
        recorder_tasks.append(
            asyncio.create_task(
                record_node_usage(
                    node_usage_file, stop_metrics, program_start
                )
            )
        )

    try:
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
                wait = target - time.time()
                if wait > 0:
                    await asyncio.sleep(wait)

                send_time = time.time()
                send_monotonic = time.perf_counter()
                print(
                    f"({send_time - program_start:.1f}s) "
                    f"[{inv['app_id']}] dispatching "
                    f"duration={inv['duration_ms']:.0f}ms"
                )
                metrics["dispatched_total"] += 1
                metrics["in_flight"] += 1
                tasks.append(asyncio.create_task(
                    dispatch(
                        session,
                        inv["app_id"],
                        inv["duration_ms"],
                        send_time,
                        send_monotonic,
                        results,
                        metrics,
                    )
                ))
            await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        stop_metrics.set()
        outcomes = await asyncio.gather(
            *recorder_tasks, return_exceptions=True
        )
        for outcome in outcomes:
            if isinstance(outcome, Exception):
                print(
                    f"Metrics recorder failed: "
                    f"{type(outcome).__name__}: {outcome}"
                )
        # Create the latency CSV even when every request failed.
        write_results(results, latency_file)

    write_summary(results, summary_file)


def main():
    if METRICS_INTERVAL <= 0:
        raise ValueError("METRICS_INTERVAL must be greater than zero")

    print(f"Reading trace from {TRACE_FILE}")
    invocations = parse_trace(TRACE_FILE)
    print(f"Loaded {len(invocations)} invocations before windowing")
    if not invocations:
        print("Trace contains no invocations, exiting.")
        return

    invocations = normalize_times(invocations)
    invocations = apply_window(invocations, WINDOW_START, WINDOW_END)
    print(f"Invocations after windowing: {len(invocations)}")
    if not invocations:
        print("No invocations in window, exiting.")
        return

    asyncio.run(run(invocations))


if __name__ == "__main__":
    main()
