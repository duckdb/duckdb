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

enum class AsyncResultsExecutionMode : uint8_t {
	SYNCHRONOUS,  // BLOCKED should not bubble up, and they should be executed synchronously
	TASK_EXECUTOR // BLOCKED is allowed
};

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
	// Schedule held async_tasks into the Executor, eventually unblocking InterruptState
	// needs to be called with non-emopty async_tasks and from BLOCKED state, will empty the async_tasks and transform
	// into INVALID
	void ScheduleTasks(InterruptState &interrupt_state, Executor &executor);
	// Execute tasks synchronously at callsite
	// needs to be called with non-emopty async_tasks and from BLOCKED state, will empty the async_tasks and transform
	// into HAVE_MORE_OUTPUT
	void ExecuteTasksSynchronously();

	static AsyncResultType GetAsyncResultType(SourceResultType s);

	// Check whether there are tasks associated
	bool HasTasks() const;
	AsyncResultType GetResultType() const;
	// Extract associated tasks, moving them away, will empty async_tasks and trasnform to INVALID
	vector<unique_ptr<AsyncTask>> &&ExtractAsyncTasks();

#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
	static vector<unique_ptr<AsyncTask>> GenerateTestTasks();
#endif

	static AsyncResultsExecutionMode
	ConvertToAsyncResultExecutionMode(const PhysicalTableScanExecutionStrategy &execution_mode);

private:
	AsyncResultType result_type {AsyncResultType::INVALID};
	vector<unique_ptr<AsyncTask>> async_tasks {};
};
} // namespace duckdb
