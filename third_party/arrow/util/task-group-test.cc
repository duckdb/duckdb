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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <random>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/task-group.h"
#include "arrow/util/thread-pool.h"

namespace arrow {
namespace internal {

static void sleep_for(double seconds) {
  std::this_thread::sleep_for(
      std::chrono::nanoseconds(static_cast<int64_t>(seconds * 1e9)));
}

// Generate random sleep durations
static std::vector<double> RandomSleepDurations(int nsleeps, double min_seconds,
                                                double max_seconds) {
  std::vector<double> sleeps;
  std::default_random_engine engine;
  std::uniform_real_distribution<> sleep_dist(min_seconds, max_seconds);
  for (int i = 0; i < nsleeps; ++i) {
    sleeps.push_back(sleep_dist(engine));
  }
  return sleeps;
}

// Check TaskGroup behaviour with a bunch of all-successful tasks
void TestTaskGroupSuccess(std::shared_ptr<TaskGroup> task_group) {
  const int NTASKS = 10;
  auto sleeps = RandomSleepDurations(NTASKS, 1e-3, 4e-3);

  // Add NTASKS sleeps
  std::atomic<int> count(0);
  for (int i = 0; i < NTASKS; ++i) {
    task_group->Append([&, i]() {
      sleep_for(sleeps[i]);
      count += i;
      return Status::OK();
    });
  }
  ASSERT_TRUE(task_group->ok());

  ASSERT_OK(task_group->Finish());
  ASSERT_TRUE(task_group->ok());
  ASSERT_EQ(count.load(), NTASKS * (NTASKS - 1) / 2);
  // Finish() is idempotent
  ASSERT_OK(task_group->Finish());
}

// Check TaskGroup behaviour with some successful and some failing tasks
void TestTaskGroupErrors(std::shared_ptr<TaskGroup> task_group) {
  const int NSUCCESSES = 2;
  const int NERRORS = 20;

  std::atomic<int> count(0);

  for (int i = 0; i < NSUCCESSES; ++i) {
    task_group->Append([&]() {
      count++;
      return Status::OK();
    });
  }
  ASSERT_TRUE(task_group->ok());
  for (int i = 0; i < NERRORS; ++i) {
    task_group->Append([&]() {
      sleep_for(1e-2);
      count++;
      return Status::Invalid("some message");
    });
  }

  // Task error is propagated
  ASSERT_RAISES(Invalid, task_group->Finish());
  ASSERT_FALSE(task_group->ok());
  if (task_group->parallelism() == 1) {
    // Serial: exactly two successes and an error
    ASSERT_EQ(count.load(), 3);
  } else {
    // Parallel: at least two successes and an error
    ASSERT_GE(count.load(), 3);
    ASSERT_LE(count.load(), 2 * task_group->parallelism());
  }
  // Finish() is idempotent
  ASSERT_RAISES(Invalid, task_group->Finish());
}

// Check TaskGroup behaviour with a bunch of all-successful tasks and task groups
void TestTaskSubGroupsSuccess(std::shared_ptr<TaskGroup> task_group) {
  const int NTASKS = 50;
  const int NGROUPS = 7;

  auto sleeps = RandomSleepDurations(NTASKS, 1e-4, 1e-3);
  std::vector<std::shared_ptr<TaskGroup>> groups = {task_group};

  // Create some subgroups
  for (int i = 0; i < NGROUPS - 1; ++i) {
    groups.push_back(task_group->MakeSubGroup());
  }

  // Add NTASKS sleeps amonst all groups
  std::atomic<int> count(0);
  for (int i = 0; i < NTASKS; ++i) {
    groups[i % NGROUPS]->Append([&, i]() {
      sleep_for(sleeps[i]);
      count += i;
      return Status::OK();
    });
  }
  ASSERT_TRUE(task_group->ok());

  // Finish all subgroups first, then main group
  for (int i = NGROUPS - 1; i >= 0; --i) {
    ASSERT_OK(groups[i]->Finish());
  }
  ASSERT_TRUE(task_group->ok());
  ASSERT_EQ(count.load(), NTASKS * (NTASKS - 1) / 2);
  // Finish() is idempotent
  ASSERT_OK(task_group->Finish());
}

// Check TaskGroup behaviour with both successful and failing tasks and task groups
void TestTaskSubGroupsErrors(std::shared_ptr<TaskGroup> task_group) {
  const int NTASKS = 50;
  const int NGROUPS = 7;
  const int FAIL_EVERY = 17;
  std::vector<std::shared_ptr<TaskGroup>> groups = {task_group};

  // Create some subgroups
  for (int i = 0; i < NGROUPS - 1; ++i) {
    groups.push_back(task_group->MakeSubGroup());
  }

  // Add NTASKS sleeps amonst all groups
  for (int i = 0; i < NTASKS; ++i) {
    groups[i % NGROUPS]->Append([&, i]() {
      sleep_for(1e-3);
      // As NGROUPS > NTASKS / FAIL_EVERY, some subgroups are successful
      if (i % FAIL_EVERY == 0) {
        return Status::Invalid("some message");
      } else {
        return Status::OK();
      }
    });
  }

  // Finish all subgroups first, then main group
  int nsuccessful = 0;
  for (int i = NGROUPS - 1; i > 0; --i) {
    Status st = groups[i]->Finish();
    if (st.ok()) {
      ++nsuccessful;
    } else {
      ASSERT_RAISES(Invalid, st);
    }
  }
  ASSERT_RAISES(Invalid, task_group->Finish());
  ASSERT_FALSE(task_group->ok());
  // Finish() is idempotent
  ASSERT_RAISES(Invalid, task_group->Finish());
}

// Check TaskGroup behaviour with tasks spawning other tasks
void TestTasksSpawnTasks(std::shared_ptr<TaskGroup> task_group) {
  const int N = 6;

  std::atomic<int> count(0);
  // Make a task that recursively spawns itself
  std::function<std::function<Status()>(int)> make_task = [&](int i) {
    return [&, i]() {
      count++;
      if (i > 0) {
        // Exercise parallelism by spawning two tasks at once and then sleeping
        task_group->Append(make_task(i - 1));
        task_group->Append(make_task(i - 1));
        sleep_for(1e-3);
      }
      return Status::OK();
    };
  };

  task_group->Append(make_task(N));

  ASSERT_OK(task_group->Finish());
  ASSERT_TRUE(task_group->ok());
  ASSERT_EQ(count.load(), (1 << (N + 1)) - 1);
}

TEST(SerialTaskGroup, Success) { TestTaskGroupSuccess(TaskGroup::MakeSerial()); }

TEST(SerialTaskGroup, Errors) { TestTaskGroupErrors(TaskGroup::MakeSerial()); }

TEST(SerialTaskGroup, TasksSpawnTasks) { TestTasksSpawnTasks(TaskGroup::MakeSerial()); }

TEST(SerialTaskGroup, SubGroupsSuccess) {
  TestTaskSubGroupsSuccess(TaskGroup::MakeSerial());
}

TEST(SerialTaskGroup, SubGroupsErrors) {
  TestTaskSubGroupsErrors(TaskGroup::MakeSerial());
}

TEST(ThreadedTaskGroup, Success) {
  auto task_group = TaskGroup::MakeThreaded(GetCpuThreadPool());
  TestTaskGroupSuccess(task_group);
}

TEST(ThreadedTaskGroup, Errors) {
  // Limit parallelism to ensure some tasks don't get started
  // after the first failing ones
  std::shared_ptr<ThreadPool> thread_pool;
  ASSERT_OK(ThreadPool::Make(4, &thread_pool));

  TestTaskGroupErrors(TaskGroup::MakeThreaded(thread_pool.get()));
}

TEST(ThreadedTaskGroup, TasksSpawnTasks) {
  auto task_group = TaskGroup::MakeThreaded(GetCpuThreadPool());
  TestTasksSpawnTasks(task_group);
}

TEST(ThreadedTaskGroup, SubGroupsSuccess) {
  std::shared_ptr<ThreadPool> thread_pool;
  ASSERT_OK(ThreadPool::Make(4, &thread_pool));

  TestTaskSubGroupsSuccess(TaskGroup::MakeThreaded(thread_pool.get()));
}

TEST(ThreadedTaskGroup, SubGroupsErrors) {
  std::shared_ptr<ThreadPool> thread_pool;
  ASSERT_OK(ThreadPool::Make(4, &thread_pool));

  TestTaskSubGroupsErrors(TaskGroup::MakeThreaded(thread_pool.get()));
}

}  // namespace internal
}  // namespace arrow
