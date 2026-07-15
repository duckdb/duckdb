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

	//! Schedule a new task
	void ScheduleTask(unique_ptr<Task> task);
	//! Label a task as finished
	void FinishTask();

	//! Work on tasks until all tasks are finished. Throws an exception if any error occurred while executing the tasks.
	void WorkOnTasks();
	//! Cancel tasks that have not started yet and work on tasks until all tasks are finished. Does not throw.
	void CancelAndDrain();

	//! Get a task - returns true if a task was found
	bool GetTask(shared_ptr<Task> &task);

private:
	//! Work on tasks until all tasks are finished
	void DrainTasks();

private:
	friend class BaseExecutorTask;

	TaskScheduler &scheduler;
	const TaskSchedulerType type;
	TaskErrorManager error_manager;
	unique_ptr<ProducerToken> token;
	idx_t completed_tasks DUCKDB_GUARDED_BY(token->producer_lock) = 0;
	idx_t total_tasks DUCKDB_GUARDED_BY(token->producer_lock) = 0;
	atomic<bool> cancelled {false};
	optional_ptr<ClientContext> context;
};

class BaseExecutorTask : public Task {
public:
	explicit BaseExecutorTask(TaskExecutor &executor);

	virtual void ExecuteTask() = 0;
	TaskExecutionResult Execute(TaskExecutionMode mode) override;

protected:
	TaskExecutor &executor;
};

} // namespace duckdb
