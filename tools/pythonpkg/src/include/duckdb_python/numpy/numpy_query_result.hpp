//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/numpy/numpy_query_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/winapi.hpp"
#include "duckdb_python/numpy/numpy_result_conversion.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/common/preserved_error.hpp"

namespace duckdb {

class ClientContext;

class NumpyQueryResult : public QueryResult {
public:
	static constexpr const QueryResultType TYPE = QueryResultType::NUMPY_RESULT;

public:
	friend class ClientContext;
	//! Creates a successful query result with the specified names and types
	DUCKDB_API NumpyQueryResult(StatementType statement_type, StatementProperties properties, vector<string> names,
	                            unique_ptr<NumpyResultConversion> collection, ClientProperties client_properties);
	//! Creates an unsuccessful query result with error condition
	DUCKDB_API explicit NumpyQueryResult(PreservedError error);

public:
	//! Fetches a DataChunk from the query result.
	//! This will consume the result (i.e. the result can only be scanned once with this function)
	DUCKDB_API unique_ptr<DataChunk> Fetch() override;
	DUCKDB_API unique_ptr<DataChunk> FetchRaw() override;
	//! Converts the QueryResult to a string
	DUCKDB_API string ToString() override;
	DUCKDB_API string ToBox(ClientContext &context, const BoxRendererConfig &config) override;

	DUCKDB_API idx_t RowCount() const;

	//! Returns a reference to the underlying numpy result
	NumpyResultConversion &Collection();

private:
	unique_ptr<NumpyResultConversion> collection;
};

} // namespace duckdb
