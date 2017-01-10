from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import time

import distributed

if __name__ == "__main__":
  # Start Dask.
  c = distributed.Client()

  # Define a function.
  def f(x):
    return x

  # Define a counter that continually increments to avoid speedups from
  # previously cached results.
  i = 0

  # Benchmark submitting tasks.
  start = time.time()
  results = []
  for _ in range(1000):
    results.append(c.submit(f, i))
    i += 1
  end = time.time()
  print("Submitting one task took on average {}ms.".format(end - start))
  # Wait for the benchmark to finish.
  c.gather(results)

  # Benchmark submitting a task and getting the result.
  start = time.time()
  for _ in range(1000):
    c.submit(f, i).result()
    i += 1
  end = time.time()
  print("Submitting one task and getting the result took on average "
        "{}ms.".format(end - start))

  # Benchmark submitting one thousand tasks and getting the results.
  start = time.time()
  for _ in range(10):
    results = []
    for _ in range(1000):
      results.append(c.submit(f, i))
      i += 1
    c.gather(results)
  end = time.time()
  print("Submitting one thousand tasks and getting the results took on average "
        "{}ms.".format((end - start) * 100))

  # Benchmark composing ten tasks and getting the result.
  start = time.time()
  for _ in range(1000):
    x = i
    i += 1
    for _ in range(10):
      x = c.submit(f, x)
    x.result()
  end = time.time()
  print("Composing ten tasks and getting the result took on average "
        "{}ms".format(end - start))

  # Define another helper function.
  def g(i, n):
    return np.random.normal(size=n)

  # Benchmark getting a small array.
  value_ids = []
  for _ in range(1000):
    value_ids.append(c.submit(g, i, 1))
    i += 1
  start = time.time()
  [value_id.result() for value_id in value_ids]
  end = time.time()
  print("Getting an array of size 1 took on average {}ms".format(end - start))

  # Benchmark getting a larger array.
  value_ids = []
  for _ in range(10):
    value_ids.append(c.submit(g, i, 10000))
    i += 1
  start = time.time()
  [value_id.result() for value_id in value_ids]
  end = time.time()
  print("Getting an array of size 10000 took on average "
        "{}ms".format((end - start) * 100))
