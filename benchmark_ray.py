from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import time

import ray

if __name__ == "__main__":
  # Start Ray.
  ray.init(start_ray_local=True, num_workers=10)

  # Define a remote functions.
  @ray.remote
  def f(x):
    return x

  # Wait for everything to start.
  time.sleep(1)

  # Define a counter that continually increments to avoid speedups from
  # previously cached results.
  i = 0

  # Benchmark submitting tasks.
  start = time.time()
  results = []
  for _ in range(1000):
    results.append(f.remote(i))
    i += 1
  end = time.time()
  print("Submitting one task took on average {}ms.".format(end - start))
  # Wait for the benchmark to finish.
  ray.get(results)

  # Benchmark submitting a task and getting the result.
  start = time.time()
  for _ in range(1000):
    ray.get(f.remote(i))
    i += 1
  end = time.time()
  print("Submitting one task and getting the result took on average "
        "{}ms.".format(end - start))

  # Benchmark submitting one thousand tasks and getting the results.
  start = time.time()
  for _ in range(10):
    results = []
    for _ in range(1000):
      results.append(f.remote(i))
      i += 1
    ray.get(results)
  end = time.time()
  print("Submitting one thousand tasks and getting the results took on average "
        "{}ms.".format((end - start) * 100))

  # Benchmark composing ten tasks and getting the result.
  start = time.time()
  for _ in range(1000):
    x = i
    i += 1
    for _ in range(10):
      x = f.remote(x)
    ray.get(x)
  end = time.time()
  print("Composing ten tasks and getting the result took on average "
        "{}ms".format(end - start))

  # Benchmark putting a small array in the object store.
  values = [np.random.normal(size=1) for _ in range(1000)]
  start = time.time()
  value_ids = [ray.put(value) for value in values]
  end = time.time()
  print("Putting an array of size 1 took on average {}ms".format(end - start))

  # Benchmark getting a small array.
  start = time.time()
  [ray.get(value_id) for value_id in value_ids]
  end = time.time()
  print("Getting an array of size 1 took on average {}ms".format(end - start))

  # Benchmark putting a larger array in the object store.
  values = [np.random.normal(size=10000) for _ in range(10)]
  start = time.time()
  value_ids = [ray.put(value) for value in values]
  end = time.time()
  print("Putting an array of size 10000 took on average "
        "{}ms".format((end - start) * 100))

  # Benchmark getting a larger array.
  start = time.time()
  [ray.get(value_id) for value_id in value_ids]
  end = time.time()
  print("Getting an array of size 10000 took on average "
        "{}ms".format((end - start) * 100))
