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
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parallel/task.hpp"
#include "duckdb/common/array.hpp"
#include "duckdb/common/enums/task_scheduler_pool_type.hpp"

namespace duckdb {

struct LightWeightSemaphoreWrapper;
struct QueueProducerToken;
class ClientContext;
class DatabaseInstance;
class TaskScheduler;
class TaskSchedulerPool;
class TaskSchedulerQueue;

//! The TaskScheduler is responsible for managing tasks and threads
class TaskScheduler {
	//! Timeout for semaphore wait, default 5ms
	constexpr static int64_t TASK_TIMEOUT_USECS = 5000;

public:
	explicit TaskScheduler(DatabaseInstance &db);
	~TaskScheduler();

public:
	DUCKDB_API static TaskScheduler &GetScheduler(ClientContext &context);
	DUCKDB_API static TaskScheduler &GetScheduler(DatabaseInstance &db);

	unique_ptr<ProducerToken> CreateProducer();
	//! Returns the number of threads
	DUCKDB_API int32_t NumberOfThreads();

	idx_t GetNumberOfTasks() const;
	idx_t GetProducerCount() const;
	idx_t GetTaskCountForProducer(ProducerToken &token) const;

	//! Schedule a task to be executed by the task scheduler
	void ScheduleTask(ProducerToken &producer, shared_ptr<Task> task);
	void ScheduleTasks(ProducerToken &producer, vector<shared_ptr<Task>> &tasks);
	//! Fetches a task from a specific producer, returns true if successful or false if no tasks were available
	bool GetTaskFromProducer(ProducerToken &token, shared_ptr<Task> &task);
	//! Run tasks forever until "marker" is set to false, "marker" must remain valid until the thread is joined
	void ExecuteForever(atomic<bool> *marker);
	void ExecuteForever(atomic<bool> *marker, TaskSchedulerPoolType pool_type);
	//! Run tasks until `marker` is set to false, `max_tasks` have been completed, or until there are no more tasks
	//! available. Returns the number of tasks that were completed.
	idx_t ExecuteTasks(atomic<bool> *marker, idx_t max_tasks);
	//! Run tasks until `max_tasks` have been completed, or until there are no more tasks available
	void ExecuteTasks(idx_t max_tasks);
	//! Send signals to n threads, signalling for them to wake up and attempt to execute a task
	void Signal(idx_t n);

	//! Sets the amount of background threads to be used for execution, based on the number of total threads
	//! and the number of external threads. External threads, e.g. the main thread, will also be used for execution.
	//! Launches `total_threads - external_threads` background worker threads.
	void SetThreads(idx_t total_threads, idx_t external_threads);
	void RelaunchThreads();

	//! Yield to other threads
	static void YieldThread();
	//! Get the number of the CPU on which the calling thread is currently executing.
	//! Fallback to calling thread id if CPU number is not available.
	//! Result do not need to be exact 'return 0' is a valid fallback strategy
	static idx_t GetEstimatedCPUId();

private:
	TaskSchedulerPool &GetPool(TaskSchedulerPoolType pool_type);
	TaskSchedulerQueue &GetQueue(TaskSchedulerPoolType pool_type) const;

	//! Schedule a task to be executed by the task scheduler
	void ScheduleTaskInternal(ProducerToken &producer, shared_ptr<Task> task, TaskSchedulerPoolType pool_type);
	void ScheduleTasksInternal(ProducerToken &producer, vector<shared_ptr<Task>> &tasks,
	                           TaskSchedulerPoolType pool_type);

	//! Fetches a task, returns true if successful or false if no tasks were available
	bool GetTaskInternal(shared_ptr<Task> &task);
	bool GetTaskInternal(shared_ptr<Task> &task, TaskSchedulerPoolType pool_type);

	void SetThreadsInternal(TaskSchedulerPoolType pool_type, idx_t n);

private:
	DatabaseInstance &db;
	//! Lock for modifying the thread count
	mutex thread_lock;
	//! The thread pools
	array<unique_ptr<TaskSchedulerPool>, TASK_SCHEDULER_POOL_TYPE_COUNT> pools;
	//! The task queues
	array<unique_ptr<TaskSchedulerQueue>, TASK_SCHEDULER_POOL_TYPE_COUNT> queues;
#ifndef DUCKDB_NO_THREADS
	//! Semaphore to signal threads to wake up and execute a task
	unique_ptr<LightWeightSemaphoreWrapper> semaphore;
#endif
};

} // namespace duckdb
