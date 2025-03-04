//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/task_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/parallel/task.hpp"
#include "duckdb/execution/task_error_manager.hpp"

namespace duckdb {
class TaskScheduler;

//! The TaskExecutor is a helper class that enables parallel scheduling and execution of tasks
class TaskExecutor {
public:
	explicit TaskExecutor(ClientContext &context);
	explicit TaskExecutor(TaskScheduler &scheduler);
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

	//! Get a task - returns true if a task was found
	bool GetTask(shared_ptr<Task> &task);

private:
	TaskScheduler &scheduler;
	TaskErrorManager error_manager;
	unique_ptr<ProducerToken> token;
	atomic<idx_t> completed_tasks;
	atomic<idx_t> total_tasks;
	friend class BaseExecutorTask;
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
