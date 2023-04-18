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
#include "duckdb/common/vector.hpp"
#include "duckdb/parallel/task.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/types/hash.hpp"

#include <list>
#include <unordered_map>
#include <chrono>

namespace duckdb {

struct ConcurrentQueue;
struct QueueProducerToken;
class ClientContext;
class DatabaseInstance;
class TaskScheduler;

struct SchedulerThread;

struct ProducerToken {
	ProducerToken(TaskScheduler &scheduler, unique_ptr<QueueProducerToken> token);
	~ProducerToken();

	TaskScheduler &scheduler;
	unique_ptr<QueueProducerToken> token;
	mutex producer_lock;
};

struct HugeIntHash {
	std::size_t operator()(const hugeint_t &k) const {
		return Hash(k);
	}
};

//! The TaskScheduler is responsible for managing tasks and threads
class TaskScheduler {
	// timeout for semaphore wait, default 5ms
	constexpr static int64_t TASK_TIMEOUT_USECS = 5000;

public:
	TaskScheduler(DatabaseInstance &db);
	~TaskScheduler();

	DUCKDB_API static TaskScheduler &GetScheduler(ClientContext &context);
	DUCKDB_API static TaskScheduler &GetScheduler(DatabaseInstance &db);

	unique_ptr<ProducerToken> CreateProducer();
	//! Schedule a task to be executed by the task scheduler
	void ScheduleTask(shared_ptr<ProducerToken> producer, unique_ptr<Task> task);
	//! Fetches a task from a specific producer, returns true if successful or false if no tasks were available
	bool GetTaskFromProducer(ProducerToken &token, unique_ptr<Task> &task);
	//! Run tasks forever until "marker" is set to false, "marker" must remain valid until the thread is joined
	void ExecuteForever(atomic<bool> *marker);
	//! Run tasks until `marker` is set to false, `max_tasks` have been completed, or until there are no more tasks
	//! available. Returns the number of tasks that were completed.
	idx_t ExecuteTasks(atomic<bool> *marker, idx_t max_tasks);
	//! Run tasks until `max_tasks` have been completed, or until there are no more tasks available
	void ExecuteTasks(idx_t max_tasks);

	//! Sets the amount of active threads executing tasks for the system; n-1 background threads will be launched.
	//! The main thread will also be used for execution
	void SetThreads(int32_t n);
	//! Returns the number of threads
	DUCKDB_API int32_t NumberOfThreads();

	//! Send signals to n threads, signalling for them to wake up and attempt to execute a task
	void Signal(idx_t n);

	//! This is the callback function that can be be called from anywhere.
	static void RescheduleCallback(shared_ptr<DatabaseInstance> db, hugeint_t callback_uuid);

	// Deschedule task based on its interrupt state
	void DescheduleTask(unique_ptr<Task> task);
private:
	void SetThreadsInternal(int32_t n);

	//! Deschedules a task which will be re-queued when the callback with callback_uuid has been made, or immediately
	//! if it has already occured
	void DescheduleTaskCallback(unique_ptr<Task> task, hugeint_t callback_uuid);
	//! Deschedules a task which will be re-queued after end_time has been reached
	void DescheduleTaskSleeping(unique_ptr<Task> task, uint64_t end_time);

	//! Should be called regularly to reschedule Task that have finished sleeping
	// TODO: this doesn't actually work atm: I have not found a good way to poll this yet.
	void RescheduleSleepingTasks();

private:
	DatabaseInstance &db;
	//! The task queue
	unique_ptr<ConcurrentQueue> queue;
	//! Lock for modifying the thread count
	mutex thread_lock;
	//! The active background threads of the task scheduler
	vector<unique_ptr<SchedulerThread>> threads;
	//! Markers used by the various threads, if the markers are set to "false" the thread execution is stopped
	vector<unique_ptr<atomic<bool>>> markers;

	//! Lock for the blocked tasks
	mutex blocked_task_lock;
	//! Maps callback_uuid -> task, these tasks are awaiting some externally registered callback for rescheduling
	std::unordered_map<hugeint_t, unique_ptr<Task>, HugeIntHash> blocked_tasks;
	//! Set of callbacks that did not match any of the blocked tasks: this means they have not yet have been descheduled,
	//! registering them here will allow immediately requeueing them from the thread that is about to deschedule it.
	std::unordered_set<hugeint_t, HugeIntHash> buffered_callbacks;

	//! Lock for the sleeping tasks
	mutex sleeping_task_lock;
	//! Tasks that are sleeping and need to be rescheduled by the scheduler after their sleep time is achieved
	std::map<uint64_t, unique_ptr<Task>> sleeping_tasks;
	//! Prevents unnecessary locking when checking for sleeping tasks in the scheduler
	atomic<bool> have_sleeping_tasks { false };
};

} // namespace duckdb
