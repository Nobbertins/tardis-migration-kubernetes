import json
import math
import time
import subprocess

SCHEDULE_PATH = "/etc/config/schedule.json"
SLICE_S       = 0.01   # duty cycle slice size in seconds (10ms)

def burn_cpu(duration_s, intensity):
    subprocess.run([
        "stress-ng", "--cpu", "1",
        "--cpu-load", str(int(intensity * 100)),
        "--timeout", f"{duration_s}s"
    ])


def run(schedule):
    bucket_size = schedule["bucket_size"]
    start_time  = schedule["start_time"]
    load        = schedule["load"]

    print(f"Starting simulation: {len(load)} timesteps, {bucket_size}s each")
    print(f"Waiting until start time {start_time}...")

    wait = start_time - time.time()
    if wait > 0:
        time.sleep(wait)
    else:
        print("Warning: start time already passed, starting immediately")

    print("Simulation started.")

    for i, intensity in enumerate(load):
        timestep_start = start_time + i * bucket_size
        now            = time.time()

        # how much of this timestep is left after accounting for any drift
        remaining_s = (timestep_start + bucket_size) - now
        if remaining_s <= 0:
            # timestep already passed entirely due to drift, skip
            print(f"Timestep {i}: skipped (drifted past)")
            continue

        print(f"Timestep {i}: intensity {intensity:.4f}, burning for {remaining_s:.2f}s")
        burn_cpu(remaining_s, intensity)

    print("Simulation complete.")


if __name__ == "__main__":
    with open(SCHEDULE_PATH) as f:
        schedule = json.load(f)
    run(schedule)