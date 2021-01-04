//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/stream_query_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/query_result.hpp"

namespace duckdb {

class ClientContext;
class MaterializedQueryResult;
class PreparedStatementData;

class StreamQueryResult : public QueryResult {
public:
	//! Create a successful StreamQueryResult. StreamQueryResults should always be successful initially (it makes no
	//! sense to stream an error).
	StreamQueryResult(StatementType statement_type, shared_ptr<ClientContext> context, vector<LogicalType> types,
	                  vector<string> names, shared_ptr<PreparedStatementData> prepared = nullptr);
	~StreamQueryResult() override;

	//! Whether or not the StreamQueryResult is still open
	bool is_open;

public:
	//! Fetches a DataChunk from the query result.
	unique_ptr<DataChunk> FetchRaw() override;
	//! Converts the QueryResult to a string
	string ToString() override;
	//! Materializes the query result and turns it into a materialized query result
	unique_ptr<MaterializedQueryResult> Materialize();

	//! Closes the StreamQueryResult
	void Close();

private:
	//! The client context this StreamQueryResult belongs to
	shared_ptr<ClientContext> context;
	//! The prepared statement data this StreamQueryResult was created with (if any)
	shared_ptr<PreparedStatementData> prepared;
};

} // namespace duckdb
