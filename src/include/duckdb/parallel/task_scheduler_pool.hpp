//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/task_scheduler_pool.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/task_scheduler_pool_type.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {

class TaskScheduler;
class DatabaseInstance;
struct QueueProducerToken;
struct TaskSchedulerThread;

class TaskSchedulerPool {
public:
	explicit TaskSchedulerPool(DatabaseInstance &db, TaskSchedulerPoolType pool_type);
	~TaskSchedulerPool();

public:
	void SetThreads(idx_t n);
	int32_t NumberOfThreads();
	void RelaunchThreads(TaskScheduler &scheduler, bool destroy);

private:
	DatabaseInstance &db;
	//! The type of this pool
	const TaskSchedulerPoolType pool_type;
	//! The active background threads of the task scheduler
	vector<unique_ptr<TaskSchedulerThread>> threads;
	//! Markers used by the various threads, if the markers are set to "false" the thread execution is stopped
	vector<unique_ptr<atomic<bool>>> markers;
	//! Requested thread count (set by the 'threads' setting)
	atomic<int32_t> requested_thread_count;
	//! The amount of threads currently running
	atomic<int32_t> current_thread_count;
};

}; // namespace duckdb
