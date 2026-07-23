import argparse
import csv
import os
import sys
from collections import defaultdict

# ── config ────────────────────────────────────────────────────────────────────
TRACE_FILE      = "azurefunctions_2019_day1.txt"
OUTPUT_DIR      = "k8s"
WORKER_IMAGE    = "nobbertins/worker:latest"
ORCH_IMAGE      = "nobbertins/orchestrator:latest"
NAMESPACE       = "default"

#usage
#python genk8s.py --nodes alpha beta gamma
# ─────────────────────────────────────────────────────────────────────────────
# Note: each deployment corresponds to one *function* (not one application).
# The trace CSV's "app" column is the function identifier used throughout.


# Some trace files identify the deployed unit with a column literally named
# "function"; others (like the simplified Azure Functions trace this tool was
# built around) call it "app". Prefer "function" when present.
ID_COLUMN_CANDIDATES = ["func", "function", "function_id", "app"]


def detect_id_field(fieldnames):
    for cand in ID_COLUMN_CANDIDATES:
        if cand in fieldnames:
            return cand
    raise KeyError(
        f"No function identifier column found. Expected one of "
        f"{ID_COLUMN_CANDIDATES}, found columns: {list(fieldnames)}"
    )


def parse_function_ids(filepath, limit=None):
    function_ids = []
    seen         = set()
    with open(filepath, newline="") as f:
        reader   = csv.DictReader(f)
        id_field = detect_id_field(reader.fieldnames)
        print(f"Using '{id_field}' column as the function ID")
        for row in reader:
            function_id = row[id_field].strip()[:16]
            if function_id not in seen:
                seen.add(function_id)
                function_ids.append(function_id)
            if limit and len(function_ids) >= limit:
                break
    # Sort alphabetically so node assignment is identical every run
    return sorted(function_ids)


def read_functions_file(filepath):
    """
    Read a newline-delimited list of function IDs, such as the file produced
    by graph_invocations.py's --functions-out option. Blank lines are ignored.
    """
    with open(filepath) as f:
        return [line.strip() for line in f if line.strip()]


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


def node_selector_snippet(node_name, indent=10):
    pad = " " * indent
    return f"{pad}nodeSelector:\n{pad}  topology.kubernetes.io/node-name: {node_name}\n"


def worker_deployment(function_id, image, node_name):
    node_selector = node_selector_snippet(node_name, indent=6)
    return f"""\
apiVersion: apps/v1
kind: Deployment
metadata:
  name: worker-{function_id}
  namespace: {NAMESPACE}
  labels:
    app: worker-{function_id}
    role: worker
spec:
  replicas: 1
  selector:
    matchLabels:
      app: worker-{function_id}
  template:
    metadata:
      labels:
        app: worker-{function_id}
        role: worker
    spec:
{node_selector}      tolerations:
        - key: node-role.kubernetes.io/control-plane
          effect: NoSchedule
      containers:
        - name: worker
          image: {image}
          imagePullPolicy: Always
          ports:
            - containerPort: 8080
          readinessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 2
            periodSeconds: 5
"""


def worker_service(function_id):
    return f"""\
apiVersion: v1
kind: Service
metadata:
  name: worker-{function_id}
  namespace: {NAMESPACE}
spec:
  selector:
    app: worker-{function_id}
  ports:
    - protocol: TCP
      port: 8080
      targetPort: 8080
"""


def orchestrator_deployment(image, trace_file, start_delay, time_scale,
                             window_start, window_end, node_name):
    node_selector = node_selector_snippet(node_name, indent=6)
    env = f"""\
            - name: TRACE_FILE
              value: "{trace_file}"
            - name: ALLOWED_APPS_FILE
              value: "/app/active_funcs.txt"
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
  namespace: orch
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
{node_selector}      tolerations:
        - key: node-role.kubernetes.io/control-plane
          effect: NoSchedule
      containers:
        - name: orchestrator
          image: {image}
          imagePullPolicy: Always
          env:
{env}
          volumeMounts:
            - name: results
              mountPath: /results
      volumes:
        - name: results
          emptyDir: {{}}
"""


def write_kustomization(function_ids, output_dir):
    lines = [
        "apiVersion: kustomize.config.k8s.io/v1beta1",
        "kind: Kustomization",
        "",
        "generatorOptions:",
        "  disableNameSuffixHash: true",
        "",
        "resources:",
    ]
    for function_id in function_ids:
        lines.append(f"  - worker-{function_id}-deployment.yaml")
        lines.append(f"  - worker-{function_id}-service.yaml")

    path = os.path.join(output_dir, "kustomization.yaml")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
    return path


def assign_nodes(function_ids, nodes):
    """Round-robin assign sorted function_ids across sorted nodes."""
    nodes = sorted(nodes)
    return {function_id: nodes[i % len(nodes)] for i, function_id in enumerate(function_ids)}


def main():
    parser = argparse.ArgumentParser(description="Generate Kubernetes deployment and service YAMLs.")
    parser.add_argument("trace_file",       nargs="?", default=TRACE_FILE)
    parser.add_argument("--nodes",          nargs="+", required=True,
                        help="List of node names to distribute pods across (e.g. --nodes node1 node2 node3)")
    parser.add_argument("--limit",          type=int,   default=None,
                        help="Only generate for the first N functions")
    parser.add_argument("--worker-image",   default=WORKER_IMAGE,
                        help="Docker image name for worker pods")
    parser.add_argument("--orch-image",     default=ORCH_IMAGE,
                        help="Docker image name for orchestrator pod")
    parser.add_argument("--start-delay",    type=int,   default=180,
                        help="Seconds before simulation starts (default: 180)")
    parser.add_argument("--time-scale",     type=float, default=1.0,
                        help="Trace time compression factor (default: 1.0 = real time)")
    parser.add_argument("--output-dir",     default=OUTPUT_DIR,
                        help="Directory to write YAML files into (default: k8s/)")
    parser.add_argument("--window-start",   type=float, default=None,
                        help="Start of trace time window in seconds (default: beginning of trace)")
    parser.add_argument("--window-end",     type=float, default=None,
                        help="End of trace time window in seconds (default: end of trace)")
    parser.add_argument("--functions-file", default=None,
                        help="Optional text file (one function ID per line, e.g. from "
                             "graph_invocations.py --functions-out) restricting generation "
                             "to only these functions")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    min_start, max_start = parse_time_range(args.trace_file)
    print(f"Trace time range: {min_start:.2f}s — {max_start:.2f}s")
    if args.window_start is not None or args.window_end is not None:
        ws = args.window_start if args.window_start is not None else min_start
        we = args.window_end   if args.window_end   is not None else max_start
        print(f"Time window:      {ws:.2f}s — {we:.2f}s")

    print(f"Reading function IDs from {args.trace_file}")
    function_ids = parse_function_ids(args.trace_file, limit=args.limit)
    print(f"Found {len(function_ids)} functions in trace")

    if args.functions_file:
        allowed      = set(read_functions_file(args.functions_file))
        before       = len(function_ids)
        function_ids = [f for f in function_ids if f in allowed]
        print(f"Restricting to functions listed in {args.functions_file}: {before} -> {len(function_ids)} functions")
        missing = allowed - set(function_ids)
        if missing:
            print(f"  Note: {len(missing)} function(s) from {args.functions_file} not found in trace "
                  f"(or excluded by --limit): {sorted(missing)[:10]}"
                  + (" ..." if len(missing) > 10 else ""))

    print(f"Generating YAMLs for {len(function_ids)} functions")

    if not function_ids:
        sys.exit("No functions left to generate after filtering, exiting.")

    nodes      = sorted(args.nodes)
    node_map   = assign_nodes(function_ids, nodes)
    orch_node  = nodes[0]

    # Print distribution summary
    print(f"\nNode assignment (round-robin over {len(nodes)} nodes, alphabetical order):")
    for node in nodes:
        assigned = [f for f, n in node_map.items() if n == node]
        print(f"  {node}: {len(assigned)} workers")
    print(f"  {orch_node}: orchestrator\n")

    for function_id in function_ids:
        dep_path = os.path.join(args.output_dir, f"worker-{function_id}-deployment.yaml")
        svc_path = os.path.join(args.output_dir, f"worker-{function_id}-service.yaml")

        with open(dep_path, "w") as f:
            f.write(worker_deployment(function_id, args.worker_image, node_map[function_id]))
        with open(svc_path, "w") as f:
            f.write(worker_service(function_id))

        print(f"  wrote {dep_path}  →  {node_map[function_id]}")
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
            node_name    = orch_node,
        ))
    print(f"  wrote {orch_path}  →  {orch_node}")

    kustomization_path = write_kustomization(function_ids, args.output_dir)
    print(f"  wrote {kustomization_path}")

    print("\nDone. Apply with:")
    print(f"  kubectl apply -k {args.output_dir}/")
    print("\nNote: nodes must have the label 'topology.kubernetes.io/node-name=<name>'.")
    print("Label them with:")
    for node in nodes:
        print(f"  kubectl label node {node} topology.kubernetes.io/node-name={node} --overwrite")


if __name__ == "__main__":
    main()