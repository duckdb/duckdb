//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/task_scheduler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/parallel/task.hpp"

#include <queue>

namespace duckdb {

class Pipeline;
class Executor;

struct Semaphore;

struct ExecutorTask {
	ExecutorTask(Executor &executor_) : executor(executor_), finished(false) {
	}

	Executor &executor;
	bool finished;
};

//! The TaskScheduler is responsible for managing tasks and threads
class TaskScheduler {
	// timeout for semaphore wait, default 50ms
	constexpr static int64_t TASK_TIMEOUT_USECS = 50000;

public:
	TaskScheduler();
	~TaskScheduler();

	static TaskScheduler &GetScheduler(ClientContext &context);

	//! Schedule a query to be worked on by the task scheduler
	void Schedule(Executor *executor);
	//! Finish the execution of a specified query
	void Finish(Executor *executor);
	//! Run queries forever until "marker" is set to false, "marker" must remain valid until the thread is joined
	void ExecuteForever(bool *marker);

	//! Sets the amount of active threads executing tasks for the system; n-1 background threads will be launched.
	//! The main thread will also be used for execution
	void SetThreads(int32_t n);

private:
	//! The lock used in the scheduler
	mutex scheduler_lock;
	//! To-be-executed queries
	vector<shared_ptr<ExecutorTask>> tasks;
	//! Semaphore
	unique_ptr<Semaphore> semaphore;
	//! The active background threads of the task scheduler
	vector<unique_ptr<thread>> threads;
	//! Markers used by the various threads, if the markers are set to "false" the thread execution is stopped
	vector<unique_ptr<bool>> markers;
};

} // namespace duckdb
