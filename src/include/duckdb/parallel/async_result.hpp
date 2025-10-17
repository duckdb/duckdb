#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"

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
	AsyncResultType() = default;
	AsyncResultType(AsyncResultType &&) = default;
	explicit AsyncResultType(SourceResultType t);
	explicit AsyncResultType(vector<unique_ptr<AsyncTask>> &&task);
	AsyncResultType &operator=(SourceResultType t);
	AsyncResultType &operator=(AsyncResultType &&) noexcept;
	void ScheduleTasks(InterruptState &interrupt_state, Executor &executor);
	bool HasTasks() const {
		D_ASSERT((result_type == TableFunctionResultType::BLOCKED) != async_tasks.empty());
		return !async_tasks.empty();
	}
	TableFunctionResultType GetResultType() const {
		D_ASSERT((result_type == TableFunctionResultType::BLOCKED) != async_tasks.empty());
		return result_type;
	}
	vector<unique_ptr<AsyncTask>> &&GetAsyncTasks() {
		result_type = TableFunctionResultType::INVALID;
		return std::move(async_tasks);
	}

private:
	TableFunctionResultType result_type {TableFunctionResultType::DEFAULT};
	vector<unique_ptr<AsyncTask>> async_tasks {};
};
} // namespace duckdb
