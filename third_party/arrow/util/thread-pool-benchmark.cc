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

#include "benchmark/benchmark.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <vector>

#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/task-group.h"
#include "arrow/util/thread-pool.h"

namespace arrow {
namespace internal {

struct Workload {
  explicit Workload(int32_t size) : size_(size), data_(kDataSize) {
    std::default_random_engine gen(42);
    std::uniform_int_distribution<uint64_t> dist(0, std::numeric_limits<uint64_t>::max());
    std::generate(data_.begin(), data_.end(), [&]() { return dist(gen); });
  }

  void operator()();

 private:
  static constexpr int32_t kDataSize = 32;

  int32_t size_;
  std::vector<uint64_t> data_;
};

void Workload::operator()() {
  uint64_t result = 0;
  for (int32_t i = 0; i < size_ / kDataSize; ++i) {
    for (const auto v : data_) {
      result = (result << (v % 64)) - v;
    }
  }
  benchmark::DoNotOptimize(result);
}

struct Task {
  explicit Task(int32_t size) : workload_(size) {}

  Status operator()() {
    workload_();
    return Status::OK();
  }

 private:
  Workload workload_;
};

// This benchmark simply provides a baseline indicating the raw cost of our workload
// depending on the workload size.  Number of items / second in this (serial)
// benchmark can be compared to the numbers obtained in BM_ThreadPoolSpawn.
static void BM_WorkloadCost(benchmark::State& state) {
  const auto workload_size = static_cast<int32_t>(state.range(0));

  Workload workload(workload_size);
  for (auto _ : state) {
    workload();
  }

  state.SetItemsProcessed(state.iterations());
}

// Benchmark ThreadPool::Spawn
static void BM_ThreadPoolSpawn(benchmark::State& state) {
  const auto nthreads = static_cast<int>(state.range(0));
  const auto workload_size = static_cast<int32_t>(state.range(1));

  Workload workload(workload_size);

  // Spawn enough tasks to make the pool start up overhead negligible
  const int32_t nspawns = 200000000 / workload_size + 1;

  for (auto _ : state) {
    state.PauseTiming();
    std::shared_ptr<ThreadPool> pool;
    ABORT_NOT_OK(ThreadPool::Make(nthreads, &pool));
    state.ResumeTiming();

    for (int32_t i = 0; i < nspawns; ++i) {
      // Pass the task by reference to avoid copying it around
      ABORT_NOT_OK(pool->Spawn(std::ref(workload)));
    }

    // Wait for all tasks to finish
    ABORT_NOT_OK(pool->Shutdown(true /* wait */));
    state.PauseTiming();
    pool.reset();
    state.ResumeTiming();
  }
  state.SetItemsProcessed(state.iterations() * nspawns);
}

// Benchmark serial TaskGroup
static void BM_SerialTaskGroup(benchmark::State& state) {
  const auto workload_size = static_cast<int32_t>(state.range(0));

  Task task(workload_size);

  const int32_t nspawns = 10000000 / workload_size + 1;

  for (auto _ : state) {
    auto task_group = TaskGroup::MakeSerial();
    for (int32_t i = 0; i < nspawns; ++i) {
      // Pass the task by reference to avoid copying it around
      task_group->Append(std::ref(task));
    }
    ABORT_NOT_OK(task_group->Finish());
  }
  state.SetItemsProcessed(state.iterations() * nspawns);
}

// Benchmark threaded TaskGroup
static void BM_ThreadedTaskGroup(benchmark::State& state) {
  const auto nthreads = static_cast<int>(state.range(0));
  const auto workload_size = static_cast<int32_t>(state.range(1));

  std::shared_ptr<ThreadPool> pool;
  ABORT_NOT_OK(ThreadPool::Make(nthreads, &pool));

  Task task(workload_size);

  const int32_t nspawns = 10000000 / workload_size + 1;

  for (auto _ : state) {
    auto task_group = TaskGroup::MakeThreaded(pool.get());
    for (int32_t i = 0; i < nspawns; ++i) {
      // Pass the task by reference to avoid copying it around
      task_group->Append(std::ref(task));
    }
    ABORT_NOT_OK(task_group->Finish());
  }
  ABORT_NOT_OK(pool->Shutdown(true /* wait */));

  state.SetItemsProcessed(state.iterations() * nspawns);
}

static const int32_t kWorkloadSizes[] = {1000, 10000, 100000};

static void WorkloadCost_Customize(benchmark::internal::Benchmark* b) {
  for (const auto w : kWorkloadSizes) {
    b->Args({w});
  }
  b->ArgNames({"task_cost"});
}

static void ThreadPoolSpawn_Customize(benchmark::internal::Benchmark* b) {
  for (const int32_t w : kWorkloadSizes) {
    for (const int nthreads : {1, 2, 4, 8}) {
      b->Args({nthreads, w});
    }
  }
  b->ArgNames({"threads", "task_cost"});
}

static const int kRepetitions = 1;

BENCHMARK(BM_WorkloadCost)->Repetitions(kRepetitions)->Apply(WorkloadCost_Customize);

BENCHMARK(BM_ThreadPoolSpawn)
    ->UseRealTime()
    ->Repetitions(kRepetitions)
    ->Apply(ThreadPoolSpawn_Customize);

BENCHMARK(BM_SerialTaskGroup)
    ->UseRealTime()
    ->Repetitions(kRepetitions)
    ->Apply(WorkloadCost_Customize);

BENCHMARK(BM_ThreadedTaskGroup)
    ->UseRealTime()
    ->Repetitions(kRepetitions)
    ->Apply(ThreadPoolSpawn_Customize);

}  // namespace internal
}  // namespace arrow
