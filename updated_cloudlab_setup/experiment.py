import os
import re
import subprocess
import sys
import time
from pathlib import Path

REPEATS = 1
MINUTES = 63
SINGLE_RUN_TIME = 60 * MINUTES + 300 #30 sec extra so it doesn't end early for whatever reason

# Each invocation of this script gets its own directory. Within it, every
# scenario/repeat gets a separate copy of the orchestrator's /results files.
EXPERIMENT_RUN_ID = f"{time.time():.6f}"
EXPERIMENT_RESULTS_DIR = (
    Path(os.environ.get("EXPERIMENT_RESULTS_DIR", "experiment_results"))
    / EXPERIMENT_RUN_ID
)
EXPERIMENT_RESULTS_DIR.mkdir(parents=True, exist_ok=True)

def kubectl(*args) -> str:
    cmd = ["kubectl", *args]
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  kubectl error: {result.stderr.strip()}")
    return result.stdout.strip()

def run_controller(controller, duration):
    try:
        result = subprocess.run([sys.executable, "-u", controller], timeout=duration, capture_output=True, text=True)
    except subprocess.TimeoutExpired as e:
        print(e.stdout.decode().strip() if e.stdout else "")
        print(f"{controller} finished after {e.timeout} seconds")

def first_completed_run_id(filenames):
    """Return the earliest run ID that has a completed latency CSV."""
    completed_runs = []
    for filename in filenames:
        match = re.fullmatch(r"latencies_([0-9]+(?:\.[0-9]+)?)\.csv", filename)
        if match:
            run_id = match.group(1)
            completed_runs.append((float(run_id), run_id))

    if not completed_runs:
        raise RuntimeError(
            "the orchestrator has not written a completed "
            "latencies_<run_id>.csv file"
        )
    return min(completed_runs)[1]

def save_orchestrator_results(scenario, repeat):
    """Copy only the files belonging to the first completed orchestrator run."""
    destination = EXPERIMENT_RESULTS_DIR / scenario / f"repeat_{repeat + 1:02d}"
    destination.mkdir(parents=True, exist_ok=True)

    list_cmd = [
        "kubectl",
        "exec",
        "-n",
        "orch",
        orch_pod,
        "--",
        "ls",
        "-1",
        "/results",
    ]
    print(f"  $ {' '.join(list_cmd)}")
    listed = subprocess.run(list_cmd, capture_output=True, text=True)
    if listed.returncode != 0:
        raise RuntimeError(
            f"failed to list orchestrator results for {scenario} "
            f"repeat {repeat + 1}: {listed.stderr.strip()}"
        )

    filenames = {
        Path(line.strip()).name
        for line in listed.stdout.splitlines()
        if line.strip()
    }
    run_id = first_completed_run_id(filenames)
    required = {
        f"latencies_{run_id}.csv",
        f"throughput_{run_id}.csv",
    }
    optional = {
        f"node_usage_{run_id}.csv",
        f"summary_{run_id}.txt",
    }
    missing_required = required - filenames
    if missing_required:
        raise RuntimeError(
            f"completed orchestrator run {run_id} is missing required files: "
            f"{sorted(missing_required)}"
        )

    selected = sorted(required | (optional & filenames))
    for filename in selected:
        copy_cmd = [
            "kubectl",
            "cp",
            "-n",
            "orch",
            f"{orch_pod}:/results/{filename}",
            str(destination / filename),
        ]
        print(f"  $ {' '.join(copy_cmd)}")
        copied = subprocess.run(copy_cmd, capture_output=True, text=True)
        if copied.returncode != 0:
            raise RuntimeError(
                f"failed to save {filename} for {scenario} "
                f"repeat {repeat + 1}: {copied.stderr.strip()}"
            )

    if f"node_usage_{run_id}.csv" not in filenames:
        print(f"  node usage file is unavailable for orchestrator run {run_id}")

    saved_files = sum(1 for path in destination.iterdir() if path.is_file())
    print(
        f"  saved {saved_files} files for completed orchestrator "
        f"run {run_id} to {destination}"
    )

    # Do not copy the whole /results directory: a Deployment restarts its
    # completed container, and the next process immediately creates another
    # throughput/node pair before it has completed a logical experiment run.

orch_pod = kubectl("get", "pods", "-n", "orch", "-o", "jsonpath={.items[0].metadata.name}")

def reset_orch():
    #get orchestrator pod name
    global orch_pod
    kubectl("delete", "pod", "-n", "orch", orch_pod)
    time.sleep(1)
    kubectl("apply", "-f", "k8s/orchestrator-deployment.yaml")
    time.sleep(2)
    orch_pod = kubectl("get", "pods", "-n", "orch", "-o", "jsonpath={.items[0].metadata.name}")
    idx = orch_pod.find("orchestrator")
    orch_pod = orch_pod[idx:idx+29]
    print("orchestrator pod: ", orch_pod)

results = ""

print("-------------------------------------------Starting Load Peak-------------------------------------------")
results += "-------------------------------------------Starting Load Peak-------------------------------------------"
for i in range(REPEATS):
    end_time = time.time() + SINGLE_RUN_TIME
    reset_orch()
    kubectl("apply", "-k", "k8s/")
    print(f"running controller for {end_time - time.time()}s (should be greater than 600)")
    run_controller("peakload_controller.py", end_time - time.time())
    save_orchestrator_results("load_peak", i)
    results += kubectl("logs", orch_pod, "--previous", "-n", "orch")
    results += "\n-----------------------------------------\n"
    kubectl("delete", "deployment", "--all")
    kubectl("delete", "pods", "-l", "role=worker") #delete migrated pods
    
#no mig
print("-------------------------------------------Starting No Mig-------------------------------------------")
results += "-------------------------------------------Starting No Mig-------------------------------------------"
for i in range(REPEATS):
    end_time = time.time() + SINGLE_RUN_TIME
    reset_orch()
    kubectl("apply", "-k", "k8s/")
    print(f"running for {end_time - time.time()}s (should be greater than 600)")
    while time.time() < end_time:
        time.sleep(1)
    save_orchestrator_results("no_migration", i)
    results += kubectl("logs", orch_pod, "--previous", "-n", "orch")
    results += "\n-----------------------------------------\n"
    kubectl("delete", "deployment", "--all")

print("-------------------------------------------Starting Load Avg-------------------------------------------")
results += "-------------------------------------------Starting Load Avg-------------------------------------------"
for i in range(REPEATS):
    end_time = time.time() + SINGLE_RUN_TIME
    reset_orch()
    kubectl("apply", "-k", "k8s/")
    print(f"running controller for {end_time - time.time()}s (should be greater than 600)")
    run_controller("avgload_controller.py", end_time - time.time())
    save_orchestrator_results("load_average", i)
    results += kubectl("logs", orch_pod, "--previous", "-n", "orch")
    results += "\n-----------------------------------------\n"
    kubectl("delete", "deployment", "--all")
    kubectl("delete", "pods", "-l", "role=worker") #delete migrated pods

print("-------------------------------------------Starting TARDIS-------------------------------------------")
results += "-------------------------------------------Starting TARDIS-------------------------------------------"
for i in range(REPEATS):
    end_time = time.time() + SINGLE_RUN_TIME
    reset_orch()
    kubectl("apply", "-k", "k8s/")
    print(f"running controller for {end_time - time.time()}s (should be greater than 600)")
    run_controller("tardis_controller.py", end_time - time.time())
    save_orchestrator_results("tardis", i)
    results += kubectl("logs", orch_pod, "--previous", "-n", "orch")
    results += "\n-----------------------------------------\n"
    kubectl("delete", "deployment", "--all")
    kubectl("delete", "pods", "-l", "role=worker") #delete migrated pods

with open("exp_results.txt", "w") as f:
    results += "EXPERIMENT DONE!!!\n"
    f.write(results)
    f.close()

print(f"Experiment CSVs saved under {EXPERIMENT_RESULTS_DIR}")
