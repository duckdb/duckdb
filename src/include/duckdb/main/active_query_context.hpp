//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/active_query_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/progress_bar/progress_bar.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

namespace duckdb {
class BaseQueryResult;

struct ActiveQueryContext {
public:
	//! The query that is currently being executed
	string query;
	//! Prepared statement data
	shared_ptr<PreparedStatementData> prepared;
	//! The query executor
	unique_ptr<Executor> executor;
	//! The progress bar
	unique_ptr<ProgressBar> progress_bar;
	//! Whether the foreign keys of the rows appended by this statement have been verified already
	bool foreign_keys_verified = false;

public:
	void SetOpenResult(BaseQueryResult &result) {
		open_result = &result;
	}
	bool IsOpenResult(BaseQueryResult &result) {
		return open_result == &result;
	}
	bool HasOpenResult() const {
		return open_result != nullptr;
	}

private:
	//! The currently open result
	BaseQueryResult *open_result = nullptr;
};

} // namespace duckdb
