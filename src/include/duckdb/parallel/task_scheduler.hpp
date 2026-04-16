//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/task_scheduler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parallel/task.hpp"

namespace duckdb {

struct ConcurrentQueue;
class ClientContext;
class DatabaseInstance;
class TaskScheduler;

struct ProducerToken {
	virtual ~ProducerToken() = 0;
};

//! The TaskScheduler is responsible for managing tasks and threads
class TaskScheduler {
public:
	explicit TaskScheduler(DatabaseInstance &db);
	virtual ~TaskScheduler();

	DUCKDB_API static TaskScheduler &GetScheduler(ClientContext &context);
	DUCKDB_API static TaskScheduler &GetScheduler(DatabaseInstance &db);

	virtual unique_ptr<ProducerToken> CreateProducer() = 0;
	//! Schedule a task to be executed by the task scheduler
	virtual void ScheduleTask(ProducerToken &producer, shared_ptr<Task> task) = 0;
	virtual void ScheduleTasks(ProducerToken &producer, vector<shared_ptr<Task>> &tasks) = 0;
	//! Fetches a task from a specific producer, returns true if successful or false if no tasks were available
	virtual bool GetTaskFromProducer(ProducerToken &token, shared_ptr<Task> &task) = 0;
	//! Run tasks forever until "marker" is set to false, "marker" must remain valid until the thread is joined
	virtual void ExecuteForever(atomic<bool> *marker) = 0;
	//! Run tasks until `marker` is set to false, `max_tasks` have been completed, or until there are no more tasks
	//! available. Returns the number of tasks that were completed.
	virtual idx_t ExecuteTasks(atomic<bool> *marker, idx_t max_tasks) = 0;
	//! Run tasks until `max_tasks` have been completed, or until there are no more tasks available
	virtual void ExecuteTasks(idx_t max_tasks) = 0;

	//! Sets the amount of background threads to be used for execution, based on the number of total threads
	//! and the number of external threads. External threads, e.g. the main thread, will also be used for execution.
	//! Launches `total_threads - external_threads` background worker threads.
	virtual void SetThreads(idx_t total_threads, idx_t external_threads) = 0;

	virtual void RelaunchThreads() = 0;

	//! Returns the number of threads
	DUCKDB_API virtual int32_t NumberOfThreads() = 0;

	[[nodiscard]] virtual idx_t GetNumberOfTasks() const = 0;
	[[nodiscard]] virtual idx_t GetProducerCount() const = 0;
	virtual idx_t GetTaskCountForProducer(ProducerToken &token) const = 0;

	//! Send signals to n threads, signalling for them to wake up and attempt to execute a task
	virtual void Signal(idx_t n) = 0;

	//! Yield to other threads
	static void YieldThread();

	//! Set the allocator flush threshold
	virtual void SetAllocatorFlushTreshold(idx_t threshold) = 0;
	//! Sets the allocator background thread
	virtual void SetAllocatorBackgroundThreads(bool enable) = 0;

	//! Get the number of the CPU on which the calling thread is currently executing.
	//! Fallback to calling thread id if CPU number is not available.
	//! Result do not need to be exact 'return 0' is a valid fallback strategy
	static idx_t GetEstimatedCPUId();

protected:
	[[nodiscard]] DatabaseInstance &GetDatabase() const;
	[[nodiscard]] TaskExecutionMode GetWorkerExecutionMode() const;

private:
	DatabaseInstance &db;
};

//! Creates the default task scheduler implementation. Custom schedulers can delegate to this helper.
unique_ptr<TaskScheduler> CreateBuiltinTaskScheduler(DatabaseInstance &db);

} // namespace duckdb
