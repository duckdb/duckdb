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

#include "arrow/util/task-group.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <utility>

#include "arrow/util/logging.h"
#include "arrow/util/thread-pool.h"

namespace arrow {
namespace internal {

////////////////////////////////////////////////////////////////////////
// Serial TaskGroup implementation

class SerialTaskGroup : public TaskGroup {
 public:
  void AppendReal(std::function<Status()> task) override {
    DCHECK(!finished_);
    if (status_.ok()) {
      status_ &= task();
    }
  }

  Status current_status() override { return status_; }

  bool ok() override { return status_.ok(); }

  Status Finish() override {
    if (!finished_) {
      finished_ = true;
      if (parent_) {
        parent_->status_ &= status_;
      }
    }
    return status_;
  }

  int parallelism() override { return 1; }

  std::shared_ptr<TaskGroup> MakeSubGroup() override {
    auto child = new SerialTaskGroup();
    child->parent_ = this;
    return std::shared_ptr<TaskGroup>(child);
  }

 protected:
  Status status_;
  bool finished_ = false;
  SerialTaskGroup* parent_ = nullptr;
};

////////////////////////////////////////////////////////////////////////
// Threaded TaskGroup implementation

class ThreadedTaskGroup : public TaskGroup {
 public:
  explicit ThreadedTaskGroup(ThreadPool* thread_pool)
      : thread_pool_(thread_pool), nremaining_(0), ok_(true) {}

  ~ThreadedTaskGroup() override {
    // Make sure all pending tasks are finished, so that dangling references
    // to this don't persist.
    ARROW_UNUSED(Finish());
  }

  void AppendReal(std::function<Status()> task) override {
    // The hot path is unlocked thanks to atomics
    // Only if an error occurs is the lock taken
    if (ok_.load(std::memory_order_acquire)) {
      nremaining_.fetch_add(1, std::memory_order_acquire);
      Status st = thread_pool_->Spawn([this, task]() {
        if (ok_.load(std::memory_order_acquire)) {
          // XXX what about exceptions?
          Status st = task();
          UpdateStatus(std::move(st));
        }
        OneTaskDone();
      });
      UpdateStatus(std::move(st));
    }
  }

  Status current_status() override {
    std::lock_guard<std::mutex> lock(mutex_);
    return status_;
  }

  bool ok() override { return ok_.load(); }

  Status Finish() override {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!finished_) {
      cv_.wait(lock, [&]() { return nremaining_.load() == 0; });
      // Current tasks may start other tasks, so only set this when done
      finished_ = true;
      if (parent_) {
        parent_->OneTaskDone();
      }
    }
    return status_;
  }

  int parallelism() override { return thread_pool_->GetCapacity(); }

  std::shared_ptr<TaskGroup> MakeSubGroup() override {
    std::lock_guard<std::mutex> lock(mutex_);
    auto child = new ThreadedTaskGroup(thread_pool_);
    child->parent_ = this;
    nremaining_.fetch_add(1, std::memory_order_acquire);
    return std::shared_ptr<TaskGroup>(child);
  }

 protected:
  void UpdateStatus(Status&& st) {
    // Must be called unlocked, only locks on error
    if (ARROW_PREDICT_FALSE(!st.ok())) {
      std::lock_guard<std::mutex> lock(mutex_);
      ok_.store(false, std::memory_order_release);
      status_ &= std::move(st);
    }
  }

  void OneTaskDone() {
    // Can be called unlocked thanks to atomics
    auto nremaining = nremaining_.fetch_sub(1, std::memory_order_release) - 1;
    DCHECK_GE(nremaining, 0);
    if (nremaining == 0) {
      // Take the lock so that ~ThreadedTaskGroup cannot destroy cv
      // before cv.notify_one() has returned
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.notify_one();
    }
  }

  // These members are usable unlocked
  ThreadPool* thread_pool_;
  std::atomic<int32_t> nremaining_;
  std::atomic<bool> ok_;

  // These members use locking
  std::mutex mutex_;
  std::condition_variable cv_;
  Status status_;
  bool finished_ = false;
  ThreadedTaskGroup* parent_ = nullptr;
};

std::shared_ptr<TaskGroup> TaskGroup::MakeSerial() {
  return std::shared_ptr<TaskGroup>(new SerialTaskGroup);
}

std::shared_ptr<TaskGroup> TaskGroup::MakeThreaded(ThreadPool* thread_pool) {
  return std::shared_ptr<TaskGroup>(new ThreadedTaskGroup(thread_pool));
}

}  // namespace internal
}  // namespace arrow
