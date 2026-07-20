import subprocess
import sys
import time

REPEATS = 10
MINUTES = 63
SINGLE_RUN_TIME = 60 * MINUTES + 30 #30 sec extra so it doesn't end early for whatever reason

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
        pass
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
    results += kubectl("logs", orch_pod, "--previous", "-n", "orch")
    results += "\n-----------------------------------------\n"
    kubectl("delete", "deployment", "--all")
    kubectl("delete", "pods", "-l", "role=worker") #delete migrated pods

with open("exp_results.txt", "w") as f:
    results += "EXPERIMENT DONE!!!\n"
    f.write(results)
    f.close()