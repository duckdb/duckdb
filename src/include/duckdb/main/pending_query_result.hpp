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

namespace duckdb {
class ClientContext;
class ClientContextLock;
class PreparedStatementData;

class PendingQueryResult : public BaseQueryResult {
	friend class ClientContext;

public:
	DUCKDB_API PendingQueryResult(shared_ptr<ClientContext> context, PreparedStatementData &statement,
	                              vector<LogicalType> types, bool allow_stream_result);
	DUCKDB_API explicit PendingQueryResult(string error_message);
	DUCKDB_API ~PendingQueryResult();

public:
	//! Executes a single task within the query, returning whether or not the query is ready.
	//! If this returns RESULT_READY, the Execute function can be called to obtain a pointer to the result.
	//! If this returns RESULT_NOT_READY, the ExecuteTask function should be called again.
	//! If this returns EXECUTION_ERROR, an error occurred during execution.
	//! The error message can be obtained by calling GetError() on the PendingQueryResult.
	DUCKDB_API PendingExecutionResult ExecuteTask();

	//! Returns the result of the query as an actual query result.
	//! This returns (mostly) instantly if ExecuteTask has been called until RESULT_READY was returned.
	DUCKDB_API unique_ptr<QueryResult> Execute();

	DUCKDB_API void Close();

private:
	shared_ptr<ClientContext> context;
	bool allow_stream_result;

private:
	void CheckExecutableInternal(ClientContextLock &lock);

	PendingExecutionResult ExecuteTaskInternal(ClientContextLock &lock);
	unique_ptr<QueryResult> ExecuteInternal(ClientContextLock &lock);
	unique_ptr<ClientContextLock> LockContext();
};

} // namespace duckdb
