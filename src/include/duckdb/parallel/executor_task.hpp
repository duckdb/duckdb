//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/executor_task.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/task.hpp"
#include "duckdb/parallel/event.hpp"

namespace duckdb {

//! Execute a task within an executor, including exception handling
//! This should be used within queries
class ExecutorTask : public Task {
public:
	ExecutorTask(Executor &executor, shared_ptr<Event> event);
	ExecutorTask(ClientContext &context, shared_ptr<Event> event);
	~ExecutorTask() override;

public:
	void Deschedule() override;
	void Reschedule() override;

public:
	Executor &executor;
	shared_ptr<Event> event;

public:
	virtual TaskExecutionResult ExecuteTask(TaskExecutionMode mode) = 0;
	TaskExecutionResult Execute(TaskExecutionMode mode) override;
};

} // namespace duckdb
