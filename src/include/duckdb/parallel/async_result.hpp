//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/async_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"

namespace duckdb {

class InterruptState;
class TaskExecutor;
class Executor;

class AsyncTask {
public:
	virtual ~AsyncTask() {};
	virtual void Execute() = 0;
};

class AsyncResult {
public:
	AsyncResult() = default;
	AsyncResult(AsyncResult &&) = default;
	explicit AsyncResult(SourceResultType t);
	explicit AsyncResult(AsyncResultType t);
	explicit AsyncResult(vector<unique_ptr<AsyncTask>> &&task);
	AsyncResult &operator=(SourceResultType t);
	AsyncResult &operator=(AsyncResultType t);
	AsyncResult &operator=(AsyncResult &&) noexcept;
	void ScheduleTasks(InterruptState &interrupt_state, Executor &executor);
	static AsyncResultType GetAsyncResultType(SourceResultType s);

	bool HasTasks() const {
		D_ASSERT((result_type == AsyncResultType::BLOCKED) != async_tasks.empty());
		return !async_tasks.empty();
	}
	AsyncResultType GetResultType() const {
		D_ASSERT((result_type == AsyncResultType::BLOCKED) != async_tasks.empty());
		return result_type;
	}
	vector<unique_ptr<AsyncTask>> &&GetAsyncTasks() {
		result_type = AsyncResultType::INVALID;
		return std::move(async_tasks);
	}

private:
	AsyncResultType result_type {AsyncResultType::INVALID};
	vector<unique_ptr<AsyncTask>> async_tasks {};
};
} // namespace duckdb
