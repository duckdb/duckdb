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

#ifndef _WIN32
#include <sys/wait.h>
#include <unistd.h>
#endif

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io-util.h"
#include "arrow/util/macros.h"
#include "arrow/util/thread-pool.h"

namespace arrow {
namespace internal {

static void sleep_for(double seconds) {
  std::this_thread::sleep_for(
      std::chrono::nanoseconds(static_cast<int64_t>(seconds * 1e9)));
}

static void busy_wait(double seconds, std::function<bool()> predicate) {
  const double period = 0.001;
  for (int i = 0; !predicate() && i * period < seconds; ++i) {
    sleep_for(period);
  }
}

template <typename T>
static void task_add(T x, T y, T* out) {
  *out = x + y;
}

template <typename T>
static void task_slow_add(double seconds, T x, T y, T* out) {
  sleep_for(seconds);
  *out = x + y;
}

typedef std::function<void(int, int, int*)> AddTaskFunc;

template <typename T>
static T add(T x, T y) {
  return x + y;
}

template <typename T>
static T slow_add(double seconds, T x, T y) {
  sleep_for(seconds);
  return x + y;
}

template <typename T>
static T inplace_add(T& x, T y) {
  return x += y;
}

// A class to spawn "add" tasks to a pool and check the results when done

class AddTester {
 public:
  explicit AddTester(int nadds) : nadds(nadds), xs(nadds), ys(nadds), outs(nadds, -1) {
    int x = 0, y = 0;
    std::generate(xs.begin(), xs.end(), [&] {
      ++x;
      return x;
    });
    std::generate(ys.begin(), ys.end(), [&] {
      y += 10;
      return y;
    });
  }

  AddTester(AddTester&&) = default;

  void SpawnTasks(ThreadPool* pool, AddTaskFunc add_func) {
    for (int i = 0; i < nadds; ++i) {
      ASSERT_OK(pool->Spawn([=] { add_func(xs[i], ys[i], &outs[i]); }));
    }
  }

  void CheckResults() {
    for (int i = 0; i < nadds; ++i) {
      ASSERT_EQ(outs[i], (i + 1) * 11);
    }
  }

  void CheckNotAllComputed() {
    for (int i = 0; i < nadds; ++i) {
      if (outs[i] == -1) {
        return;
      }
    }
    ASSERT_TRUE(0) << "all values were computed";
  }

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(AddTester);

  int nadds;
  std::vector<int> xs;
  std::vector<int> ys;
  std::vector<int> outs;
};

class TestThreadPool : public ::testing::Test {
 public:
  void TearDown() {
    fflush(stdout);
    fflush(stderr);
  }

  std::shared_ptr<ThreadPool> MakeThreadPool() { return MakeThreadPool(4); }

  std::shared_ptr<ThreadPool> MakeThreadPool(int threads) {
    std::shared_ptr<ThreadPool> pool;
    Status st = ThreadPool::Make(threads, &pool);
    return pool;
  }

  void SpawnAdds(ThreadPool* pool, int nadds, AddTaskFunc add_func) {
    AddTester add_tester(nadds);
    add_tester.SpawnTasks(pool, add_func);
    ASSERT_OK(pool->Shutdown());
    add_tester.CheckResults();
  }

  void SpawnAddsThreaded(ThreadPool* pool, int nthreads, int nadds,
                         AddTaskFunc add_func) {
    // Same as SpawnAdds, but do the task spawning from multiple threads
    std::vector<AddTester> add_testers;
    std::vector<std::thread> threads;
    for (int i = 0; i < nthreads; ++i) {
      add_testers.emplace_back(nadds);
    }
    for (auto& add_tester : add_testers) {
      threads.emplace_back([&] { add_tester.SpawnTasks(pool, add_func); });
    }
    for (auto& thread : threads) {
      thread.join();
    }
    ASSERT_OK(pool->Shutdown());
    for (auto& add_tester : add_testers) {
      add_tester.CheckResults();
    }
  }
};

TEST_F(TestThreadPool, ConstructDestruct) {
  // Stress shutdown-at-destruction logic
  for (int threads : {1, 2, 3, 8, 32, 70}) {
    auto pool = this->MakeThreadPool(threads);
  }
}

// Correctness and stress tests using Spawn() and Shutdown()

TEST_F(TestThreadPool, Spawn) {
  auto pool = this->MakeThreadPool(3);
  SpawnAdds(pool.get(), 7, task_add<int>);
}

TEST_F(TestThreadPool, StressSpawn) {
  auto pool = this->MakeThreadPool(30);
  SpawnAdds(pool.get(), 1000, task_add<int>);
}

TEST_F(TestThreadPool, StressSpawnThreaded) {
  auto pool = this->MakeThreadPool(30);
  SpawnAddsThreaded(pool.get(), 20, 100, task_add<int>);
}

TEST_F(TestThreadPool, SpawnSlow) {
  // This checks that Shutdown() waits for all tasks to finish
  auto pool = this->MakeThreadPool(2);
  SpawnAdds(pool.get(), 7, [](int x, int y, int* out) {
    return task_slow_add(0.02 /* seconds */, x, y, out);
  });
}

TEST_F(TestThreadPool, StressSpawnSlow) {
  auto pool = this->MakeThreadPool(30);
  SpawnAdds(pool.get(), 1000, [](int x, int y, int* out) {
    return task_slow_add(0.002 /* seconds */, x, y, out);
  });
}

TEST_F(TestThreadPool, StressSpawnSlowThreaded) {
  auto pool = this->MakeThreadPool(30);
  SpawnAddsThreaded(pool.get(), 20, 100, [](int x, int y, int* out) {
    return task_slow_add(0.002 /* seconds */, x, y, out);
  });
}

TEST_F(TestThreadPool, QuickShutdown) {
  AddTester add_tester(100);
  {
    auto pool = this->MakeThreadPool(3);
    add_tester.SpawnTasks(pool.get(), [](int x, int y, int* out) {
      return task_slow_add(0.02 /* seconds */, x, y, out);
    });
    ASSERT_OK(pool->Shutdown(false /* wait */));
    add_tester.CheckNotAllComputed();
  }
  add_tester.CheckNotAllComputed();
}

TEST_F(TestThreadPool, SetCapacity) {
  auto pool = this->MakeThreadPool(3);
  ASSERT_EQ(pool->GetCapacity(), 3);
  ASSERT_EQ(pool->GetActualCapacity(), 3);

  ASSERT_OK(pool->SetCapacity(5));
  ASSERT_EQ(pool->GetCapacity(), 5);
  ASSERT_EQ(pool->GetActualCapacity(), 5);

  ASSERT_OK(pool->SetCapacity(2));
  ASSERT_EQ(pool->GetCapacity(), 2);
  // Wait for workers to wake up and secede
  busy_wait(0.5, [&] { return pool->GetActualCapacity() == 2; });
  ASSERT_EQ(pool->GetActualCapacity(), 2);

  ASSERT_OK(pool->SetCapacity(5));
  ASSERT_EQ(pool->GetCapacity(), 5);
  ASSERT_EQ(pool->GetActualCapacity(), 5);

  // Downsize while tasks are pending
  for (int i = 0; i < 10; ++i) {
    ASSERT_OK(pool->Spawn(std::bind(sleep_for, 0.01 /* seconds */)));
  }
  ASSERT_OK(pool->SetCapacity(2));
  ASSERT_EQ(pool->GetCapacity(), 2);
  busy_wait(0.5, [&] { return pool->GetActualCapacity() == 2; });
  ASSERT_EQ(pool->GetActualCapacity(), 2);

  // Ensure nothing got stuck
  ASSERT_OK(pool->Shutdown());
}

// Test Submit() functionality

TEST_F(TestThreadPool, Submit) {
  auto pool = this->MakeThreadPool(3);
  {
    auto fut = pool->Submit(add<int>, 4, 5);
    ASSERT_EQ(fut.get(), 9);
  }
  {
    auto fut = pool->Submit(add<std::string>, "foo", "bar");
    ASSERT_EQ(fut.get(), "foobar");
  }
  {
    auto fut = pool->Submit(slow_add<int>, 0.01 /* seconds */, 4, 5);
    ASSERT_EQ(fut.get(), 9);
  }
  {
    // Reference passing
    std::string s = "foo";
    auto fut = pool->Submit(inplace_add<std::string>, std::ref(s), "bar");
    ASSERT_EQ(fut.get(), "foobar");
    ASSERT_EQ(s, "foobar");
  }
  {
    // `void` return type
    auto fut = pool->Submit(sleep_for, 0.001);
    fut.get();
  }
}

// Test fork safety on Unix

#if !(defined(_WIN32) || defined(ARROW_VALGRIND) || defined(ADDRESS_SANITIZER) || \
      defined(THREAD_SANITIZER))
TEST_F(TestThreadPool, ForkSafety) {
  pid_t child_pid;
  int child_status;

  {
    // Fork after task submission
    auto pool = this->MakeThreadPool(3);
    auto fut = pool->Submit(add<int>, 4, 5);
    ASSERT_EQ(fut.get(), 9);

    child_pid = fork();
    if (child_pid == 0) {
      // Child: thread pool should be usable
      fut = pool->Submit(add<int>, 3, 4);
      if (fut.get() != 7) {
        std::exit(1);
      }
      // Shutting down shouldn't hang or fail
      Status st = pool->Shutdown();
      std::exit(st.ok() ? 0 : 2);
    } else {
      // Parent
      ASSERT_GT(child_pid, 0);
      ASSERT_GT(waitpid(child_pid, &child_status, 0), 0);
      ASSERT_TRUE(WIFEXITED(child_status));
      ASSERT_EQ(WEXITSTATUS(child_status), 0);
      ASSERT_OK(pool->Shutdown());
    }
  }
  {
    // Fork after shutdown
    auto pool = this->MakeThreadPool(3);
    ASSERT_OK(pool->Shutdown());

    child_pid = fork();
    if (child_pid == 0) {
      // Child
      // Spawning a task should return with error (pool was shutdown)
      Status st = pool->Spawn([] {});
      if (!st.IsInvalid()) {
        std::exit(1);
      }
      // Trigger destructor
      pool.reset();
      std::exit(0);
    } else {
      // Parent
      ASSERT_GT(child_pid, 0);
      ASSERT_GT(waitpid(child_pid, &child_status, 0), 0);
      ASSERT_TRUE(WIFEXITED(child_status));
      ASSERT_EQ(WEXITSTATUS(child_status), 0);
    }
  }
}
#endif

TEST(TestGlobalThreadPool, Capacity) {
  // Sanity check
  auto pool = GetCpuThreadPool();
  int capacity = pool->GetCapacity();
  ASSERT_GT(capacity, 0);
  ASSERT_EQ(pool->GetActualCapacity(), capacity);
  ASSERT_EQ(GetCpuThreadPoolCapacity(), capacity);

  // Exercise default capacity heuristic
  ASSERT_OK(DelEnvVar("OMP_NUM_THREADS"));
  ASSERT_OK(DelEnvVar("OMP_THREAD_LIMIT"));
  int hw_capacity = std::thread::hardware_concurrency();
  ASSERT_EQ(ThreadPool::DefaultCapacity(), hw_capacity);
  ASSERT_OK(SetEnvVar("OMP_NUM_THREADS", "13"));
  ASSERT_EQ(ThreadPool::DefaultCapacity(), 13);
  ASSERT_OK(SetEnvVar("OMP_NUM_THREADS", "7,5,13"));
  ASSERT_EQ(ThreadPool::DefaultCapacity(), 7);
  ASSERT_OK(DelEnvVar("OMP_NUM_THREADS"));

  ASSERT_OK(SetEnvVar("OMP_THREAD_LIMIT", "1"));
  ASSERT_EQ(ThreadPool::DefaultCapacity(), 1);
  ASSERT_OK(SetEnvVar("OMP_THREAD_LIMIT", "999"));
  if (hw_capacity <= 999) {
    ASSERT_EQ(ThreadPool::DefaultCapacity(), hw_capacity);
  }
  ASSERT_OK(SetEnvVar("OMP_NUM_THREADS", "6,5,13"));
  ASSERT_EQ(ThreadPool::DefaultCapacity(), 6);
  ASSERT_OK(SetEnvVar("OMP_THREAD_LIMIT", "2"));
  ASSERT_EQ(ThreadPool::DefaultCapacity(), 2);

  // Invalid env values
  ASSERT_OK(SetEnvVar("OMP_NUM_THREADS", "0"));
  ASSERT_OK(SetEnvVar("OMP_THREAD_LIMIT", "0"));
  ASSERT_EQ(ThreadPool::DefaultCapacity(), hw_capacity);
  ASSERT_OK(SetEnvVar("OMP_NUM_THREADS", "zzz"));
  ASSERT_OK(SetEnvVar("OMP_THREAD_LIMIT", "x"));
  ASSERT_EQ(ThreadPool::DefaultCapacity(), hw_capacity);
  ASSERT_OK(SetEnvVar("OMP_THREAD_LIMIT", "-1"));
  ASSERT_OK(SetEnvVar("OMP_NUM_THREADS", "99999999999999999999999999"));
  ASSERT_EQ(ThreadPool::DefaultCapacity(), hw_capacity);

  ASSERT_OK(DelEnvVar("OMP_NUM_THREADS"));
  ASSERT_OK(DelEnvVar("OMP_THREAD_LIMIT"));
}

}  // namespace internal
}  // namespace arrow
