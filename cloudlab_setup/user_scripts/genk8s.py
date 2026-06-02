import argparse
import csv
import os
from collections import defaultdict

# ── config ────────────────────────────────────────────────────────────────────
TRACE_FILE      = "AzureFunctionsInvocationTraceForTwoWeeksJan2021.txt"
OUTPUT_DIR      = "k8s"
WORKER_IMAGE    = "worker"
ORCH_IMAGE      = "orchestrator"
NAMESPACE       = "default"
# ─────────────────────────────────────────────────────────────────────────────


def parse_app_ids(filepath, limit=None):
    app_ids = []
    seen    = set()
    with open(filepath, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            app_id = row["app"].strip()[:16]
            if app_id not in seen:
                seen.add(app_id)
                app_ids.append(app_id)
            if limit and len(app_ids) >= limit:
                break
    return app_ids


def parse_time_range(filepath):
    """Return (min_start, max_start) across all invocations in the trace."""
    min_start = float("inf")
    max_start = float("-inf")
    with open(filepath, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            duration   = float(row["duration"])
            end_time   = float(row["end_timestamp"])
            start_time = end_time - duration
            min_start  = min(min_start, start_time)
            max_start  = max(max_start, start_time)
    return min_start, max_start


def worker_deployment(app_id, image):
    return f"""\
apiVersion: apps/v1
kind: Deployment
metadata:
  name: worker-{app_id}
  namespace: {NAMESPACE}
  labels:
    app: worker-{app_id}
    role: worker
spec:
  replicas: 1
  selector:
    matchLabels:
      app: worker-{app_id}
  template:
    metadata:
      labels:
        app: worker-{app_id}
        role: worker
    spec:
      containers:
        - name: worker
          image: {image}
          imagePullPolicy: Never
          ports:
            - containerPort: 8080
          readinessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 2
            periodSeconds: 5
"""


def worker_service(app_id):
    return f"""\
apiVersion: v1
kind: Service
metadata:
  name: worker-{app_id}
  namespace: {NAMESPACE}
spec:
  selector:
    app: worker-{app_id}
  ports:
    - protocol: TCP
      port: 8080
      targetPort: 8080
"""


def orchestrator_deployment(image, trace_file, start_delay, time_scale,
                             window_start, window_end):
    env = f"""\
            - name: TRACE_FILE
              value: "{trace_file}"
            - name: START_DELAY
              value: "{start_delay}"
            - name: TIME_SCALE
              value: "{time_scale}"
            - name: RESULTS_FILE
              value: "/results/latencies.csv\""""

    if window_start is not None:
        env += f"""
            - name: WINDOW_START
              value: "{window_start}\""""
    if window_end is not None:
        env += f"""
            - name: WINDOW_END
              value: "{window_end}\""""

    return f"""\
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orchestrator
  namespace: {NAMESPACE}
  labels:
    app: orchestrator
    role: orchestrator
spec:
  replicas: 1
  selector:
    matchLabels:
      app: orchestrator
  template:
    metadata:
      labels:
        app: orchestrator
        role: orchestrator
    spec:
      containers:
        - name: orchestrator
          image: {image}
          imagePullPolicy: Never
          env:
{env}
          volumeMounts:
            - name: results
              mountPath: /results
      volumes:
        - name: results
          emptyDir: {{}}
"""


def write_kustomization(app_ids, output_dir):
    lines = [
        "apiVersion: kustomize.config.k8s.io/v1beta1",
        "kind: Kustomization",
        "",
        "generatorOptions:",
        "  disableNameSuffixHash: true",
        "",
        "resources:",
        "  - orchestrator-deployment.yaml",
    ]
    for app_id in app_ids:
        lines.append(f"  - worker-{app_id}-deployment.yaml")
        lines.append(f"  - worker-{app_id}-service.yaml")

    path = os.path.join(output_dir, "kustomization.yaml")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
    return path


def main():
    parser = argparse.ArgumentParser(description="Generate Kubernetes deployment and service YAMLs.")
    parser.add_argument("trace_file",       nargs="?", default=TRACE_FILE)
    parser.add_argument("--limit",          type=int,   default=None,
                        help="Only generate for the first N apps")
    parser.add_argument("--worker-image",   default=WORKER_IMAGE,
                        help="Docker image name for worker pods")
    parser.add_argument("--orch-image",     default=ORCH_IMAGE,
                        help="Docker image name for orchestrator pod")
    parser.add_argument("--start-delay",    type=int,   default=120,
                        help="Seconds before simulation starts (default: 120)")
    parser.add_argument("--time-scale",     type=float, default=1.0,
                        help="Trace time compression factor (default: 1.0 = real time)")
    parser.add_argument("--output-dir",     default=OUTPUT_DIR,
                        help="Directory to write YAML files into (default: k8s/)")
    parser.add_argument("--window-start",   type=float, default=None,
                        help="Start of trace time window in seconds (default: beginning of trace)")
    parser.add_argument("--window-end",     type=float, default=None,
                        help="End of trace time window in seconds (default: end of trace)")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    # print available time range so the user knows what to pass to --window-start/end
    min_start, max_start = parse_time_range(args.trace_file)
    print(f"Trace time range: {min_start:.2f}s — {max_start:.2f}s")
    if args.window_start is not None or args.window_end is not None:
        ws = args.window_start if args.window_start is not None else min_start
        we = args.window_end   if args.window_end   is not None else max_start
        print(f"Time window:      {ws:.2f}s — {we:.2f}s")

    print(f"Reading app IDs from {args.trace_file}")
    app_ids = parse_app_ids(args.trace_file, limit=args.limit)
    print(f"Generating YAMLs for {len(app_ids)} apps")

    for app_id in app_ids:
        dep_path = os.path.join(args.output_dir, f"worker-{app_id}-deployment.yaml")
        svc_path = os.path.join(args.output_dir, f"worker-{app_id}-service.yaml")

        with open(dep_path, "w") as f:
            f.write(worker_deployment(app_id, args.worker_image))
        with open(svc_path, "w") as f:
            f.write(worker_service(app_id))

        print(f"  wrote {dep_path}")
        print(f"  wrote {svc_path}")

    orch_path = os.path.join(args.output_dir, "orchestrator-deployment.yaml")
    with open(orch_path, "w") as f:
        f.write(orchestrator_deployment(
            image        = args.orch_image,
            trace_file   = f"/app/{os.path.basename(args.trace_file)}",
            start_delay  = args.start_delay,
            time_scale   = args.time_scale,
            window_start = args.window_start,
            window_end   = args.window_end,
        ))
    print(f"  wrote {orch_path}")

    kustomization_path = write_kustomization(app_ids, args.output_dir)
    print(f"  wrote {kustomization_path}")

    print("\nDone. Apply with:")
    print(f"  kubectl apply -k {args.output_dir}/")


if __name__ == "__main__":
    main()
