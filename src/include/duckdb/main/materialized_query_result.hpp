//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/materialized_query_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/main/query_result.hpp"

namespace duckdb {

class ClientContext;

class MaterializedQueryResult : public QueryResult {
public:
	friend class ClientContext;
	//! Creates a successful query result with the specified names and types
	DUCKDB_API MaterializedQueryResult(StatementType statement_type, StatementProperties properties,
	                                   vector<LogicalType> types, vector<string> names,
	                                   const shared_ptr<ClientContext> &context);
	//! Creates an unsuccessful query result with error condition
	DUCKDB_API explicit MaterializedQueryResult(string error);

	ChunkCollection collection;

	//! The client context this MaterializedQueryResult belongs to
	std::weak_ptr<ClientContext> context;

public:
	//! Fetches a DataChunk from the query result.
	//! This will consume the result (i.e. the chunks are taken directly from the ChunkCollection).
	DUCKDB_API unique_ptr<DataChunk> Fetch() override;
	DUCKDB_API unique_ptr<DataChunk> FetchRaw() override;
	//! Converts the QueryResult to a string
	DUCKDB_API string ToString() override;

	//! Gets the (index) value of the (column index) column
	DUCKDB_API Value GetValue(idx_t column, idx_t index);

	template <class T>
	T GetValue(idx_t column, idx_t index) {
		auto value = GetValue(column, index);
		return (T)value.GetValue<int64_t>();
	}
};

} // namespace duckdb
