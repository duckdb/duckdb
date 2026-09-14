//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/pending_query_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/query_result.hpp"
#include "duckdb/common/enums/pending_execution_result.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context_lock.hpp"

namespace duckdb {
class ClientContext;
class PreparedStatementData;

class PendingQueryResult : public BaseQueryResult {
	friend class ClientContext;

public:
	static constexpr const QueryResultType TYPE = QueryResultType::PENDING_RESULT;

public:
	DUCKDB_API PendingQueryResult(shared_ptr<ClientContext> context, PreparedStatementData &statement,
	                              vector<LogicalType> types, bool allow_stream_result);
	DUCKDB_API explicit PendingQueryResult(ErrorData error_message);
	DUCKDB_API ~PendingQueryResult() override;
	DUCKDB_API bool AllowStreamResult() const;
	PendingQueryResult(const PendingQueryResult &) = delete;
	PendingQueryResult &operator=(const PendingQueryResult &) = delete;

public:
	//! Executes a single task within the query, returning whether or not the query is ready.
	//! If this returns RESULT_READY, the Execute function can be called to obtain a pointer to the result.
	//! If this returns RESULT_NOT_READY, the ExecuteTask function should be called again.
	//! If this returns EXECUTION_ERROR, an error occurred during execution.
	//! If this returns NO_TASKS_AVAILABLE, this means currently no meaningful work can be done by the current executor,
	//!	    but tasks may become available in the future.
	//! The error message can be obtained by calling GetError() on the PendingQueryResult.
	DUCKDB_API PendingExecutionResult ExecuteTask() DUCKDB_EXCLUDES(GetClientContext().context_lock);
	DUCKDB_API PendingExecutionResult CheckPulse() DUCKDB_EXCLUDES(GetClientContext().context_lock);
	//! Halt execution of the thread until a Task is ready to be executed (use with caution)
	void WaitForTask() DUCKDB_EXCLUDES(GetClientContext().context_lock);

	//! Returns the result of the query as an actual query result.
	//! This returns (mostly) instantly if ExecuteTask has been called until RESULT_READY was returned.
	DUCKDB_API unique_ptr<QueryResult> Execute() DUCKDB_EXCLUDES(GetClientContext().context_lock);

	DUCKDB_API void Close() DUCKDB_EXCLUDES(GetClientContext().context_lock);

	//! Function to determine whether execution is considered finished
	DUCKDB_API static bool IsResultReady(PendingExecutionResult result);
	DUCKDB_API static bool IsExecutionFinished(PendingExecutionResult result);

private:
	shared_ptr<ClientContext> context;
	bool allow_stream_result;

private:
	void CheckExecutableInternal(ClientContextLock &lock) DUCKDB_REQUIRES(lock);

	PendingExecutionResult ExecuteTaskInternal(ClientContextLock &lock) DUCKDB_REQUIRES(lock);
	unique_ptr<QueryResult> ExecuteInternal(ClientContextLock &lock) DUCKDB_REQUIRES(lock);
	ClientContext &GetClientContext() const;
};

} // namespace duckdb
