# TARDIS VM Migration Simulation in Kubernetes

Implements the TARDIS VM-migration policy (and baseline migration policies) as a
live Kubernetes migration controller, and evaluates it against a realistic
serverless workload replayed from the **Azure Functions Invocations 2021** trace
on a 3-node CloudLab cluster.

- **GitHub:** https://github.com/Nobbertins/tardis-migration-kubernetes
- **Docker Hub images:**
  - Orchestrator: [`nobbertins/orchestrator`](https://hub.docker.com/r/nobbertins/orchestrator)
  - Worker: [`nobbertins/worker`](https://hub.docker.com/r/nobbertins/worker)
  - Metrics Collector: [`nobbertins/collector`](https://hub.docker.com/r/nobbertins/collector)

## How the experiment works

A 3-node Kubernetes cluster (`node-0`/control-plane + `node-1` + `node-2`) runs:

- **Worker** pods — one per function in the trace — that simulate the real CPU
  cost of a serverless invocation using `stress-ng` at a tuned load target.
- An **orchestrator** that replays a chosen window of the Azure Functions
  Invocations 2021 trace, dispatching each invocation to the matching worker over
  `aiohttp` and recording end-to-end latency.
- A **collector** DaemonSet (backed by `kube-prometheus-stack`) exposing per-node
  and per-pod CPU utilization.
- A **migration controller** — `none` (no migration), `load-peak` (migrates based on peak load), `load-average` (migrates based on
  average load), or `TARDIS` — that watches the collector's metrics and migrates worker pods off
  overloaded nodes by creating a copy on a less-loaded node, draining, then deleting the original.

Node placement is deliberately unbalanced at the start of each run (a handful of
active workers are pinned to node-0) so that migration has something to do.
Nodes are labeled `alpha`/`beta`/`gamma` (node-0/1/2) via the
`topology.kubernetes.io/node-name` label, which is what the worker manifests'
`nodeSelector` targets. The orchestrator runs in its own `orch` namespace;
workers run in the default namespace.

## How to run the experiment

```bash
git clone https://github.com/Nobbertins/tardis-migration-kubernetes.git
cd tardis-migration-kubernetes

# 1. (optional) find a good trace window and regenerate manifests for it
python migration_window.py AzureFunctionsInvocationTraceForTwoWeeksJan2021.txt --window 3600
python genk8s.py AzureFunctionsInvocationTraceForTwoWeeksJan2021.txt \
  --window-start <start> --window-end <end> --output-dir updated_cloudlab_setup/k8s

# 2. estimate TARDIS hyperparameters for that same window
python tardis_heuristics.py --trace AzureFunctionsInvocationTraceForTwoWeeksJan2021.txt \
  --start <start> --end <end>

# 3. push everything to your CloudLab node-0 and kick off the experiment
chmod +x remote_setup.sh
./remote_setup.sh <user>@<node-0-hostname>

# 4. once experiment.py has finished, pull results back
chmod +x remote_finish.sh
./remote_finish.sh <user>@<node-0-hostname>

# 5. turn the accumulated log into a datasheet + tail-latency charts
python parse_experiment.py exp_results.txt results/
```

What each step does:

1. `migration_window.py` scans the trace for a good replay window; `genk8s.py`
   then generates the worker/orchestrator manifests + `kustomization.yaml` for
   that window.
2. `tardis_heuristics.py` estimates `alpha`/`beta`/`tau` for the TARDIS
   controller from the same window — plug these into the controller's config
   before running.
3. `remote_setup.sh` copies `updated_cloudlab_setup/` to node-0, runs its
   `experiment_setup.sh` (labels nodes, installs `kube-prometheus-stack` and the
   collector, sets up the `orch` namespace and orchestrator), then launches
   `experiment.py` in the background (`nohup ... &`), which drives all four
   scenarios — Load Peak, No Mig, Load Average, and TARDIS — deploying workers,
   running each scenario's migration controller (or none), and collecting
   results per run into `experiment_results/<run_id>/<scenario>/repeat_NN/` on
   node-0. How many repeats each scenario gets is controlled by a constant in
   `experiment.py` — raise it, or invoke `experiment.py` again, for more runs
   per scenario.
4. `remote_finish.sh` copies `script.log` and `exp_results.txt` back locally
   once `experiment.py` has finished.
5. `parse_experiment.py` parses `exp_results.txt`'s per-scenario Tail Latency
   Report blocks into a CSV, an Excel workbook, and per-percentile bar charts
   (mean ± std dev across whatever number of repeats were actually run).

## Scripts reference

Quick usage for each script at the repo root, plus the two CloudLab-side scripts
under `updated_cloudlab_setup/`.

### `genk8s.py`

```bash
python genk8s.py [trace_file] \
  [--limit N] [--worker-image IMAGE] [--orch-image IMAGE] \
  [--start-delay SECONDS] [--time-scale FACTOR] [--output-dir DIR] \
  [--window-start SECONDS] [--window-end SECONDS]
```

Generates worker `Deployment`+`Service` YAMLs, `orchestrator-deployment.yaml`,
and a `kustomization.yaml` for a given trace window. Apply with
`kubectl apply -k <output-dir>/`.

### `migration_window.py`

```bash
python migration_window.py [data.csv] \
  [--window SECONDS] [--target CALLS_PER_APP] [--max-calls N] \
  [--min-apps N] [--top-results N] [--min-dur SECONDS] \
  [--stride SECONDS] [--out results.txt]
```

Scans the trace for a window with many apps active at a moderate, balanced
call count each, and prints candidate `--start`/`--end` windows to feed into
`genk8s.py` and `tardis_heuristics.py`.

### `graph_invocations.py`

```bash
python graph_invocations.py [data.csv] \
  --start SECONDS --end SECONDS \
  [--out out.png] [--min-dur SECONDS] [--top N] \
  [--group-by function|app] [--no-normalize]
```

Renders a Gantt chart + concurrency curve for a trace window. Uses the same
timestamp normalization and duration filtering as the orchestrator, so
`--start`/`--end` here match what the orchestrator will actually replay.

### `graph_metrics.py`

```bash
python graph_metrics.py [--input merged_metrics.csv] [--out chart.png] \
  [--since UNIX_TS] [--until UNIX_TS] [--smooth N]
```

Plots per-node CPU% over time from a metrics CSV (timestamp, node, cpu_pct).

### `tardis_heuristics.py`

```bash
python tardis_heuristics.py \
  --trace path/to/trace.csv --start SECONDS --end SECONDS \
  [--scrape-interval SECONDS] [--window-w SECONDS] \
  [--p-min PROB] [--freq-min N]
```

Estimates `alpha`, `beta`, and a suggested `tau` for the TARDIS controller from
a trace window. Run over the same window you're about to replay.

### `parse_experiment.py`

```bash
python parse_experiment.py [exp_results.txt] [out_dir]
```

Parses the four scenario sections (Load Peak / No Mig / Load Average / Tardis)
and their Tail Latency Report blocks into `results.csv`, `results.xlsx`
(requires `openpyxl`), and per-percentile bar charts `chart_p50.png` …
`chart_p999.png` plus `chart_p50_p90_combined.png` (requires `matplotlib`).

### `remote_setup.sh`

```bash
chmod +x remote_setup.sh
./remote_setup.sh user@node-0-hostname
```

Copies `updated_cloudlab_setup/` to node-0, runs its `experiment_setup.sh`, then
launches `experiment.py` detached in the background.

### `remote_finish.sh`

```bash
chmod +x remote_finish.sh
./remote_finish.sh user@node-0-hostname
```

Copies `updated_cloudlab_setup/script.log` and `updated_cloudlab_setup/exp_results.txt`
back to your current local directory.

### `updated_cloudlab_setup/experiment_setup.sh`

Run once per CloudLab allocation (via `remote_setup.sh`, or manually on node-0):
labels the three nodes `alpha`/`beta`/`gamma`, installs `aiohttp`/`numpy` so the
migration controllers can run directly on node-0, installs
`kube-prometheus-stack` into a `monitoring` namespace, applies `rbac.yaml` and
`daemonset.yaml` (the collector), and deploys the orchestrator into a new
`orch` namespace.

### `updated_cloudlab_setup/experiment.py`

The end-to-end driver launched by `remote_setup.sh`. For each of the four
scenarios (Load Peak, No Mig, Load Average, TARDIS), for a configurable number
of repeats, it: resets the orchestrator, deploys workers (`kubectl apply -k k8s/`),
runs that scenario's migration controller (or just waits, for No Mig) for a
configurable run duration, copies that run's result files out of the
orchestrator pod into `experiment_results/<run_id>/<scenario>/repeat_NN/`, and
appends the orchestrator's logs to `exp_results.txt`. Run duration and number of
repeats per scenario are both set by constants at the top of the script —
adjust them for your trace window and desired sample size.

## Building the Docker images yourself

The pre-built images (`nobbertins/orchestrator`, `nobbertins/worker`,
`nobbertins/collector`) are what `genk8s.py` and the CloudLab setup scripts
reference by default. If you want to build your own — e.g. after modifying
worker load generation, orchestrator dispatch logic, or the collector — the
Dockerfiles live in `cloudlab_setup/user_scripts/`:

```bash
cd cloudlab_setup/user_scripts

docker build -f orchestrator.Dockerfile -t <your-registry>/orchestrator:<tag> .
docker build -f worker.Dockerfile      -t <your-registry>/worker:<tag>      .
docker build -f collector.Dockerfile   -t <your-registry>/collector:<tag>   .

docker push <your-registry>/orchestrator:<tag>
docker push <your-registry>/worker:<tag>
docker push <your-registry>/collector:<tag>
```

Then point the manifests at your images instead of the defaults:

- **Worker / orchestrator:** pass `--worker-image` and `--orch-image` to
  `genk8s.py` when regenerating manifests for your trace window, e.g.:

  ```bash
  python genk8s.py AzureFunctionsInvocationTraceForTwoWeeksJan2021.txt \
    --window-start <start> --window-end <end> \
    --worker-image <your-registry>/worker:<tag> \
    --orch-image <your-registry>/orchestrator:<tag> \
    --output-dir updated_cloudlab_setup/k8s
  ```

- **Collector:** update the image reference in `daemonset.yaml` (applied by
  `experiment_setup.sh`) to point at `<your-registry>/collector:<tag>`.

Make sure the built images are pushed somewhere your CloudLab nodes can pull
from (a public registry, or a private one with imagePullSecrets configured),
since `remote_setup.sh` doesn't build images locally on the cluster.

## Configuration notes

- Worker CPU load is generated with `stress-ng --cpu 0 -l 40` (40% target),
  tuned so that a run produces meaningful contention without pinning every node
  to 100% (which would make migration pointless because there's nowhere better
  to move a pod to).
- `genk8s.py --window-start`/`--window-end` and `tardis_heuristics.py --start`/`--end`
  both operate in the same "seconds since the start of the trace file" coordinate
  space that `graph_invocations.py` also normalizes to — keep these consistent
  across all three when working with the same window.