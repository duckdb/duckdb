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

class StreamQueryResult : public QueryResult {
public:
	//! Create a successful StreamQueryResult. StreamQueryResults should always be successful initially (it makes no
	//! sense to stream an error).
	StreamQueryResult(StatementType statement_type, ClientContext &context, vector<SQLType> sql_types,
	                  vector<TypeId> types, vector<string> names);
	~StreamQueryResult() override;

	//! Fetches a DataChunk from the query result. Returns an empty chunk if the result is empty, or nullptr on error.
	unique_ptr<DataChunk> Fetch() override;
	//! Converts the QueryResult to a string
	string ToString() override;
	//! Materializes the query result and turns it into a materialized query result
	unique_ptr<MaterializedQueryResult> Materialize();

	//! Closes the StreamQueryResult
	void Close();

	//! Whether or not the StreamQueryResult is still open
	bool is_open;

private:
	//! The client context this StreamQueryResult belongs to
	ClientContext &context;
};

} // namespace duckdb
