//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/task.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {
class ClientContext;
class Executor;

//! Generic parallel task
class Task {
public:
	virtual ~Task() {
	}

	//! Execute the task
	virtual void Execute() = 0;
};

//! Execute a task within an executor, including exception handling
//! This should be used within queries
class ExecutorTask : public Task {
public:
	ExecutorTask(Executor &executor);
	ExecutorTask(ClientContext &context);
	virtual ~ExecutorTask();

	Executor &executor;

public:
	virtual void ExecuteTask() = 0;
	void Execute() override;
};

} // namespace duckdb
