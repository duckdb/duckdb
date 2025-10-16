#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

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
	explicit AsyncResultType(SourceResultType t);
	explicit AsyncResultType(vector<unique_ptr<AsyncTask>> &&task);
	void ScheduleTasks(InterruptState &interrupt_state, Executor &executor);
	SourceResultType GetResultType() const {
		return result_type;
	}
	vector<unique_ptr<AsyncTask>> &&GetAsyncTasks() {
		return std::move(async_tasks);
	}

private:
	SourceResultType result_type;
	vector<unique_ptr<AsyncTask>> async_tasks;
};
} // namespace duckdb
