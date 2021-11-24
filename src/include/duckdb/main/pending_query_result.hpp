//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/pending_query_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once


#include "duckdb/main/query_result.hpp"

namespace duckdb {

enum class PendingExecutionResult : uint8_t {
	RESULT_READY,
	RESULT_NOT_READY,
	EXECUTION_ERROR
};

class PendingQueryResult : public BaseQueryResult {
public:
	DUCKDB_API PendingQueryResult(StatementType statement_type);
	DUCKDB_API PendingQueryResult(StatementType statement_type, vector<LogicalType> types, vector<string> names);
	DUCKDB_API explicit PendingQueryResult(string error_message);

public:
	//! Executes a single task within the query, returning whether or not the query is ready.
	//! If this returns RESULT_READY, the Execute function can be called to obtain a pointer to the result.
	//! If this returns RESULT_NOT_READY, the ExecuteTask function should be called again.
	//! If this returns EXECUTION_ERROR, an error occurred during execution.
	//! The error message can be obtained by calling GetError() on the PendingQueryResult.
	DUCKDB_API PendingExecutionResult ExecuteTask();

	//! Returns the result of the query as an actual query result.
	//! This returns (mostly) instantly if ExecuteTask has been called until RESULT_READY was returned.
	DUCKDB_API unique_ptr<QueryResult> Execute(bool allow_streaming_result = false);

	inline void Close() {
		is_open = false;
	}
private:
	//! Whether or not the PendingQueryResult is still open
	bool is_open;
};

} // namespace duckdb