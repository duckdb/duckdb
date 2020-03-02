//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/materialized_query_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/main/query_result.hpp"

namespace duckdb {

class MaterializedQueryResult : public QueryResult {
public:
	//! Creates an empty successful query result
	MaterializedQueryResult(StatementType statement_type);
	//! Creates a successful query result with the specified names and types
	MaterializedQueryResult(StatementType statement_type, vector<SQLType> sql_types, vector<TypeId> types,
	                        vector<string> names);
	//! Creates an unsuccessful query result with error condition
	MaterializedQueryResult(string error);

	//! Fetches a DataChunk from the query result. Returns an empty chunk if the result is empty, or nullptr on failure.
	//! This will consume the result (i.e. the chunks are taken directly from the ChunkCollection).
	unique_ptr<DataChunk> Fetch() override;
	//! Converts the QueryResult to a string
	string ToString() override;

	//! Gets the (index) value of the (column index) column
	Value GetValue(idx_t column, idx_t index);

	template <class T> T GetValue(idx_t column, idx_t index) {
		auto value = GetValue(column, index);
		return (T)value.GetValue<int64_t>();
	}

	ChunkCollection collection;
};

} // namespace duckdb
