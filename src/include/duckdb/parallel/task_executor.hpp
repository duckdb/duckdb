//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/task_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/parallel/task.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/execution/task_error_manager.hpp"
#include "duckdb/common/enums/task_scheduler_type.hpp"

namespace duckdb {

class TaskExecutor;
class TaskExecutorTask;

//! A unit of work scheduled on a TaskExecutor
//! Deliberately not a Task: it cannot be handed to the TaskScheduler directly, it cannot be descheduled or
//! rescheduled, and it cannot report partial progress. The executor wraps it and owns cancellation, error handling
//! and task accounting, so implementations only have to describe their work.
class BaseExecutorTask {
public:
	explicit BaseExecutorTask(TaskExecutor &executor);
	virtual ~BaseExecutorTask() = default;

public:
	//! Perform the task's work - throwing is allowed, the executor captures the error and cancels the other tasks
	virtual void ExecuteTask() = 0;
	//! Called instead of ExecuteTask when the task is retired without running its work, because another task errored
	//! or because the executor was cancelled. Exactly one of ExecuteTask or Cancel runs for every scheduled task.
	//! Anything a waiter watches must be settled here as well as in ExecuteTask, or that waiter never wakes up.
	virtual void Cancel() {
	}
	virtual string TaskType() const {
		return "UnnamedTask";
	}

protected:
	TaskExecutor &executor;
};

//! The TaskExecutor is a helper class that enables parallel scheduling and execution of tasks
class TaskExecutor {
public:
	explicit TaskExecutor(ClientContext &context, TaskSchedulerType type = TaskSchedulerType::REGULAR);
	explicit TaskExecutor(TaskScheduler &scheduler, TaskSchedulerType type = TaskSchedulerType::REGULAR);
	~TaskExecutor();

	//! Push an error into the TaskExecutor
	void PushError(ErrorData error);
	//! Whether or not any task has encountered an error
	bool HasError();
	//! Throw an error that was encountered during execution (if HasError() is true)
	void ThrowError();
	//! Get the first error that was encountered during execution (if HasError() is true)
	ErrorData GetError();
	//! Whether the executor has been cancelled
	bool IsCancelled() const;

	//! Schedule a new task
	void ScheduleTask(unique_ptr<BaseExecutorTask> task);

	//! Work on tasks until all tasks are finished. Throws an exception if any error occurred while executing the tasks.
	void WorkOnTasks();
	//! Cancel tasks that have not started yet and work on tasks until all tasks are finished. Does not throw.
	void CancelAndDrain();

	//! Get a task - returns true if a task was found
	bool GetTask(shared_ptr<Task> &task);

public:
	//! Joins the executor when the scope is left, so that its tasks never outlive the state they reference.
	//! Only needed for an executor that outlives the scope scheduling onto it, where its own destructor is not the
	//! join. Errors are swallowed: the exception that is unwinding wins, and a recorded task error is still there.
	class JoinGuard {
	public:
		explicit JoinGuard(TaskExecutor &executor);
		~JoinGuard();

	private:
		TaskExecutor &executor;
	};

private:
	//! Work on tasks until all tasks are finished
	void DrainTasks();
	//! Label a task as finished - called by the wrapper the executor puts around every scheduled task
	void FinishTask();

private:
	friend class TaskExecutorTask;

	TaskScheduler &scheduler;
	const TaskSchedulerType type;
	TaskErrorManager error_manager;
	unique_ptr<ProducerToken> token;
	idx_t completed_tasks DUCKDB_GUARDED_BY(token->producer_lock) = 0;
	idx_t total_tasks DUCKDB_GUARDED_BY(token->producer_lock) = 0;
	atomic<bool> cancelled {false};
	optional_ptr<ClientContext> context;
};

} // namespace duckdb
