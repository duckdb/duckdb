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
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {
class PhysicalOperator;
class ThreadContext;

//! Execute a task within an executor, including exception handling
//! This should be used within queries
class ExecutorTask : public Task {
public:
	ExecutorTask(Executor &executor, shared_ptr<Event> event);
	ExecutorTask(ClientContext &context, shared_ptr<Event> event, const PhysicalOperator &op);
	~ExecutorTask() override;

public:
	void Deschedule() override;
	void Reschedule() override;

public:
	Executor &executor;
	shared_ptr<Event> event;
	unique_ptr<ThreadContext> thread_context;
	optional_ptr<const PhysicalOperator> op;

public:
	virtual TaskExecutionResult ExecuteTask(TaskExecutionMode mode) = 0;
	TaskExecutionResult Execute(TaskExecutionMode mode) override;
};

} // namespace duckdb
