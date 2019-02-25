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

#include "arrow/util/thread-pool.h"

#include <algorithm>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "arrow/util/io-util.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace internal {

struct ThreadPool::State {
  State() : desired_capacity_(0), please_shutdown_(false), quick_shutdown_(false) {}

  // NOTE: in case locking becomes too expensive, we can investigate lock-free FIFOs
  // such as https://github.com/cameron314/concurrentqueue

  std::mutex mutex_;
  std::condition_variable cv_;
  std::condition_variable cv_shutdown_;

  std::list<std::thread> workers_;
  // Trashcan for finished threads
  std::vector<std::thread> finished_workers_;
  std::deque<std::function<void()>> pending_tasks_;

  // Desired number of threads
  int desired_capacity_;
  // Are we shutting down?
  bool please_shutdown_;
  bool quick_shutdown_;
};

ThreadPool::ThreadPool()
    : sp_state_(std::make_shared<ThreadPool::State>()),
      state_(sp_state_.get()),
      shutdown_on_destroy_(true) {
#ifndef _WIN32
  pid_ = getpid();
#endif
}

ThreadPool::~ThreadPool() {
  if (shutdown_on_destroy_) {
    ARROW_UNUSED(Shutdown(false /* wait */));
  }
}

void ThreadPool::ProtectAgainstFork() {
#ifndef _WIN32
  pid_t current_pid = getpid();
  if (pid_ != current_pid) {
    // Reinitialize internal state in child process after fork()
    // Ideally we would use pthread_at_fork(), but that doesn't allow
    // storing an argument, hence we'd need to maintain a list of all
    // existing ThreadPools.
    int capacity = state_->desired_capacity_;

    auto new_state = std::make_shared<ThreadPool::State>();
    new_state->please_shutdown_ = state_->please_shutdown_;
    new_state->quick_shutdown_ = state_->quick_shutdown_;

    pid_ = current_pid;
    sp_state_ = new_state;
    state_ = sp_state_.get();

    // Launch worker threads anew
    if (!state_->please_shutdown_) {
      ARROW_UNUSED(SetCapacity(capacity));
    }
  }
#endif
}

Status ThreadPool::SetCapacity(int threads) {
  ProtectAgainstFork();
  std::unique_lock<std::mutex> lock(state_->mutex_);
  if (state_->please_shutdown_) {
    return Status::Invalid("operation forbidden during or after shutdown");
  }
  if (threads <= 0) {
    return Status::Invalid("ThreadPool capacity must be > 0");
  }
  CollectFinishedWorkersUnlocked();

  state_->desired_capacity_ = threads;
  int diff = static_cast<int>(threads - state_->workers_.size());
  if (diff > 0) {
    LaunchWorkersUnlocked(diff);
  } else if (diff < 0) {
    // Wake threads to ask them to stop
    state_->cv_.notify_all();
  }
  return Status::OK();
}

int ThreadPool::GetCapacity() {
  ProtectAgainstFork();
  std::unique_lock<std::mutex> lock(state_->mutex_);
  return state_->desired_capacity_;
}

int ThreadPool::GetActualCapacity() {
  ProtectAgainstFork();
  std::unique_lock<std::mutex> lock(state_->mutex_);
  return static_cast<int>(state_->workers_.size());
}

Status ThreadPool::Shutdown(bool wait) {
  ProtectAgainstFork();
  std::unique_lock<std::mutex> lock(state_->mutex_);

  if (state_->please_shutdown_) {
    return Status::Invalid("Shutdown() already called");
  }
  state_->please_shutdown_ = true;
  state_->quick_shutdown_ = !wait;
  state_->cv_.notify_all();
  state_->cv_shutdown_.wait(lock, [this] { return state_->workers_.empty(); });
  if (!state_->quick_shutdown_) {
    DCHECK_EQ(state_->pending_tasks_.size(), 0);
  } else {
    state_->pending_tasks_.clear();
  }
  CollectFinishedWorkersUnlocked();
  return Status::OK();
}

void ThreadPool::CollectFinishedWorkersUnlocked() {
  for (auto& thread : state_->finished_workers_) {
    // Make sure OS thread has exited
    thread.join();
  }
  state_->finished_workers_.clear();
}

void ThreadPool::LaunchWorkersUnlocked(int threads) {
  std::shared_ptr<State> state = sp_state_;

  for (int i = 0; i < threads; i++) {
    state_->workers_.emplace_back();
    auto it = --(state_->workers_.end());
    *it = std::thread([state, it] { WorkerLoop(state, it); });
  }
}

void ThreadPool::WorkerLoop(std::shared_ptr<State> state,
                            std::list<std::thread>::iterator it) {
  std::unique_lock<std::mutex> lock(state->mutex_);

  // Since we hold the lock, `it` now points to the correct thread object
  // (LaunchWorkersUnlocked has exited)
  DCHECK_EQ(std::this_thread::get_id(), it->get_id());

  // If too many threads, we should secede from the pool
  const auto should_secede = [&]() -> bool {
    return state->workers_.size() > static_cast<size_t>(state->desired_capacity_);
  };

  while (true) {
    // By the time this thread is started, some tasks may have been pushed
    // or shutdown could even have been requested.  So we only wait on the
    // condition variable at the end of the loop.

    // Execute pending tasks if any
    while (!state->pending_tasks_.empty() && !state->quick_shutdown_) {
      // We check this opportunistically at each loop iteration since
      // it releases the lock below.
      if (should_secede()) {
        break;
      }
      {
        std::function<void()> task = std::move(state->pending_tasks_.front());
        state->pending_tasks_.pop_front();
        lock.unlock();
        task();
      }
      lock.lock();
    }
    // Now either the queue is empty *or* a quick shutdown was requested
    if (state->please_shutdown_ || should_secede()) {
      break;
    }
    // Wait for next wakeup
    state->cv_.wait(lock);
  }

  // We're done.  Move our thread object to the trashcan of finished
  // workers.  This has two motivations:
  // 1) the thread object doesn't get destroyed before this function finishes
  //    (but we could call thread::detach() instead)
  // 2) we can explicitly join() the trashcan threads to make sure all OS threads
  //    are exited before the ThreadPool is destroyed.  Otherwise subtle
  //    timing conditions can lead to false positives with Valgrind.
  DCHECK_EQ(std::this_thread::get_id(), it->get_id());
  state->finished_workers_.push_back(std::move(*it));
  state->workers_.erase(it);
  if (state->please_shutdown_) {
    // Notify the function waiting in Shutdown().
    state->cv_shutdown_.notify_one();
  }
}

Status ThreadPool::SpawnReal(std::function<void()> task) {
  {
    ProtectAgainstFork();
    std::lock_guard<std::mutex> lock(state_->mutex_);
    if (state_->please_shutdown_) {
      return Status::Invalid("operation forbidden during or after shutdown");
    }
    CollectFinishedWorkersUnlocked();
    state_->pending_tasks_.push_back(std::move(task));
  }
  state_->cv_.notify_one();
  return Status::OK();
}

Status ThreadPool::Make(int threads, std::shared_ptr<ThreadPool>* out) {
  auto pool = std::shared_ptr<ThreadPool>(new ThreadPool());
  RETURN_NOT_OK(pool->SetCapacity(threads));
  *out = std::move(pool);
  return Status::OK();
}

// ----------------------------------------------------------------------
// Global thread pool

static int ParseOMPEnvVar(const char* name) {
  // OMP_NUM_THREADS is a comma-separated list of positive integers.
  // We are only interested in the first (top-level) number.
  std::string str;
  if (!GetEnvVar(name, &str).ok()) {
    return 0;
  }
  auto first_comma = str.find_first_of(',');
  if (first_comma != std::string::npos) {
    str = str.substr(0, first_comma);
  }
  try {
    return std::max(0, std::stoi(str));
  } catch (...) {
    return 0;
  }
}

int ThreadPool::DefaultCapacity() {
  int capacity, limit;
  capacity = ParseOMPEnvVar("OMP_NUM_THREADS");
  if (capacity == 0) {
    capacity = std::thread::hardware_concurrency();
  }
  limit = ParseOMPEnvVar("OMP_THREAD_LIMIT");
  if (limit > 0) {
    capacity = std::min(limit, capacity);
  }
  if (capacity == 0) {
    ARROW_LOG(WARNING) << "Failed to determine the number of available threads, "
                          "using a hardcoded arbitrary value";
    capacity = 4;
  }
  return capacity;
}

// Helper for the singleton pattern
std::shared_ptr<ThreadPool> ThreadPool::MakeCpuThreadPool() {
  std::shared_ptr<ThreadPool> pool;
  DCHECK_OK(ThreadPool::Make(ThreadPool::DefaultCapacity(), &pool));
  // On Windows, the global ThreadPool destructor may be called after
  // non-main threads have been killed by the OS, and hang in a condition
  // variable.
  // On Unix, we want to avoid leak reports by Valgrind.
#ifdef _WIN32
  pool->shutdown_on_destroy_ = false;
#endif
  return pool;
}

ThreadPool* GetCpuThreadPool() {
  static std::shared_ptr<ThreadPool> singleton = ThreadPool::MakeCpuThreadPool();
  return singleton.get();
}

}  // namespace internal

int GetCpuThreadPoolCapacity() { return internal::GetCpuThreadPool()->GetCapacity(); }

Status SetCpuThreadPoolCapacity(int threads) {
  return internal::GetCpuThreadPool()->SetCapacity(threads);
}

}  // namespace arrow
