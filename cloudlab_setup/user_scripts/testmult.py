import time
import numpy as np
start2 = time.time()
a = np.ones(100_000)
start = time.time()
a += 1.0
print(start - start2)
print(time.time() - start)
