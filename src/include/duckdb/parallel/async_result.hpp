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
	explicit AsyncResult(AsyncResultType t);

public:
	AsyncResult() = default;
	AsyncResult(AsyncResult &&) = default;
	AsyncResult(SourceResultType t); // NOLINT
	explicit AsyncResult(vector<unique_ptr<AsyncTask>> &&task);
	AsyncResult &operator=(SourceResultType t);
	AsyncResult &operator=(AsyncResultType t);
	AsyncResult &operator=(AsyncResult &&) noexcept;
	void ScheduleTasks(InterruptState &interrupt_state, Executor &executor);
	static AsyncResultType GetAsyncResultType(SourceResultType s);

	bool HasTasks() const {
		D_ASSERT(result_type != AsyncResultType::INVALID);
		if (async_tasks.empty()) {
			D_ASSERT(result_type != AsyncResultType::BLOCKED);
			return false;
		} else {
			D_ASSERT(result_type == AsyncResultType::BLOCKED);
			return true;
		}
	}
	AsyncResultType GetResultType() const {
		D_ASSERT(result_type != AsyncResultType::INVALID);
		if (async_tasks.empty()) {
			D_ASSERT(result_type != AsyncResultType::BLOCKED);
		} else {
			D_ASSERT(result_type == AsyncResultType::BLOCKED);
		}
		return result_type;
	}
	vector<unique_ptr<AsyncTask>> &&ExtractAsyncTasks() {
		D_ASSERT(result_type != AsyncResultType::INVALID);
		result_type = AsyncResultType::INVALID;
		return std::move(async_tasks);
	}

private:
	AsyncResultType result_type {AsyncResultType::INVALID};
	vector<unique_ptr<AsyncTask>> async_tasks {};
};
} // namespace duckdb
