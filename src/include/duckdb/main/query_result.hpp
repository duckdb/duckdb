//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/query_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/enums/statement_type.hpp"

namespace duckdb {

enum class QueryResultType : uint8_t { MATERIALIZED_RESULT, STREAM_RESULT };

//! The QueryResult object holds the result of a query. It can either be a MaterializedQueryResult, in which case the
//! result contains the entire result set, or a StreamQueryResult in which case the Fetch method can be called to
//! incrementally fetch data from the database.
class QueryResult {
public:
	//! Creates an successful empty query result
	QueryResult(QueryResultType type, StatementType statement_type);
	//! Creates a successful query result with the specified names and types
	QueryResult(QueryResultType type, StatementType statement_type, vector<SQLType> sql_types, vector<TypeId> types,
	            vector<string> names);
	//! Creates an unsuccessful query result with error condition
	QueryResult(QueryResultType type, string error);
	virtual ~QueryResult() {
	}

	//! The type of the result (MATERIALIZED or STREAMING)
	QueryResultType type;
	//! The type of the statement that created this result
	StatementType statement_type;
	//! The SQL types of the result
	vector<SQLType> sql_types;
	//! The types of the result
	vector<TypeId> types;
	//! The names of the result
	vector<string> names;
	//! Whether or not execution was successful
	bool success;
	//! The error string (in case execution was not successful)
	string error;
	//! The next result (if any)
	unique_ptr<QueryResult> next;

public:
	//! Fetches a DataChunk from the query result. Returns an empty chunk if the result is empty, or nullptr on failure.
	virtual unique_ptr<DataChunk> Fetch() = 0;
	// Converts the QueryResult to a string
	virtual string ToString() = 0;
	//! Prints the QueryResult to the console
	void Print();
	//! Returns true if the two results are identical; false otherwise. Note that this method is destructive; it calls
	//! Fetch() until both results are exhausted. The data in the results will be lost.
	bool Equals(QueryResult &other);

protected:
	string HeaderToString();

private:
	QueryResult(const QueryResult &) = delete;
};

} // namespace duckdb
