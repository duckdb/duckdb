// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Non-Arrow system benchmarks, provided for convenience.

#include <algorithm>
#include <cstdint>
#include <limits>
#include <random>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"

namespace arrow {

// Generate a vector of indices such as following the indices describes
// a path over the whole vector.  The path is randomized to avoid triggering
// automatic prefetching in the CPU.
std::vector<int32_t> RandomPath(int32_t size) {
  std::default_random_engine gen(42);
  std::vector<int32_t> indices(size);

  for (int32_t i = 0; i < size; ++i) {
    indices[i] = i;
  }
  std::shuffle(indices.begin(), indices.end(), gen);
  std::vector<int32_t> path(size, -999999);
  int32_t prev;
  prev = indices[size - 1];
  for (int32_t i = 0; i < size; ++i) {
    int32_t next = indices[i];
    path[prev] = next;
    prev = next;
  }
  return path;
}

// Cache / main memory latency, depending on the working set size
static void BM_memory_latency(benchmark::State& state) {
  const auto niters = static_cast<int32_t>(state.range(0));
  const std::vector<int32_t> path = RandomPath(niters / 4);

  int32_t total = 0;
  int32_t index = 0;
  for (auto _ : state) {
    total += index;
    index = path[index];
  }
  benchmark::DoNotOptimize(total);
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_memory_latency)->RangeMultiplier(2)->Range(2 << 10, 2 << 24);

}  // namespace arrow
