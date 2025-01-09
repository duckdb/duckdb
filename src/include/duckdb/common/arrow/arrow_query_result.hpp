//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/arrow/arrow_query_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/winapi.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class ClientContext;

class ArrowQueryResult : public QueryResult {
public:
	static constexpr QueryResultType TYPE = QueryResultType::ARROW_RESULT;

public:
	friend class ClientContext;
	//! Creates a successful query result with the specified names and types
	DUCKDB_API ArrowQueryResult(StatementType statement_type, StatementProperties properties, vector<string> names_p,
	                            vector<LogicalType> types_p, ClientProperties client_properties, idx_t batch_size);
	//! Creates an unsuccessful query result with error condition
	DUCKDB_API explicit ArrowQueryResult(ErrorData error);

public:
	//! Fetches a DataChunk from the query result.
	//! This will consume the result (i.e. the result can only be scanned once with this function)
	DUCKDB_API unique_ptr<DataChunk> Fetch() override;
	DUCKDB_API unique_ptr<DataChunk> FetchRaw() override;
	//! Converts the QueryResult to a string
	DUCKDB_API string ToString() override;

public:
	vector<unique_ptr<ArrowArrayWrapper>> ConsumeArrays();
	vector<unique_ptr<ArrowArrayWrapper>> &Arrays();
	void SetArrowData(vector<unique_ptr<ArrowArrayWrapper>> arrays);
	idx_t BatchSize() const;

private:
	vector<unique_ptr<ArrowArrayWrapper>> arrays;
	idx_t batch_size;
};

} // namespace duckdb
