#include "duckdb/common/enum_util.hpp"

#pragma once

namespace duckdb {

class InterruptState;
class TaskExecutor;
class Executor;

class AsyncTask {
public:
	virtual ~AsyncTask() {};
	virtual void Execute() = 0;
};

class AsyncResultType {
public:
	AsyncResultType(SourceResultType t);
	AsyncResultType(unique_ptr<AsyncTask> &&task);
	void ScheduleTasks(InterruptState &interrupt_state, Executor &executor);
	SourceResultType GetResultType() const {
		return result_type;
	}

private:
	SourceResultType result_type;
	unique_ptr<AsyncTask> async_task;
};
} // namespace duckdb
