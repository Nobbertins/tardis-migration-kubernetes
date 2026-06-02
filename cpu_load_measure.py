import psutil
import time

# --- Setup ---
psutil.cpu_percent()  # warm-up, discard

# --- Simulator loop ---
while True:
    time.sleep(1)  # your sample interval — you control this
    
    load = psutil.cpu_percent()
    #avg = sum(load) / len(load)
    
    print(f"Per-core: {load}")
    #print(f"Average:  {avg:.1f}%")