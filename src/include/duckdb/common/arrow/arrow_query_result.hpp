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
	//! Converts the QueryResult to a string
	DUCKDB_API string ToString() override;

public:
	vector<unique_ptr<ArrowArrayWrapper>> ConsumeArrays();
	vector<unique_ptr<ArrowArrayWrapper>> &Arrays();
	void SetArrowData(vector<unique_ptr<ArrowArrayWrapper>> arrays);
	idx_t BatchSize() const;

	//! True once the Arrow schema has been built and cached (see BuildCachedSchema).
	bool HasCachedSchema() const {
		return cached_schema.arrow_schema.release != nullptr;
	}
	//! Build this result's Arrow schema (from its own types/names/client
	//! properties) and cache it. The producing arrow collector calls this during
	//! Finalize, while the transaction is still active. Building the schema later,
	//! at fetch time, breaks arrow type extensions whose schema callback does a
	//! catalog lookup -- e.g. GeoArrow CRS resolution in ArrowGeometry::WriteCRS,
	//! which asserts an active transaction that no longer exists post-commit.
	//! See duckdb-python#475 / duckdb-spatial#788.
	DUCKDB_API void BuildCachedSchema();
	//! Deep-copy the cached schema into `out` (the caller owns and must release
	//! it). This is how consumers obtain a materialized result's Arrow schema --
	//! they must never rebuild it via ArrowConverter::ToArrowSchema post-commit.
	//! Throws if no schema was cached.
	DUCKDB_API void GetSchema(ArrowSchema &out) const;

protected:
	DUCKDB_API unique_ptr<DataChunk> FetchInternal() override;

private:
	vector<unique_ptr<ArrowArrayWrapper>> arrays;
	idx_t batch_size;
	//! Owns its ArrowSchema; released by ~ArrowSchemaWrapper when the result dies.
	//! `mutable` so GetSchema() can hand nanoarrow a (non-const) pointer to copy.
	mutable ArrowSchemaWrapper cached_schema;
};

} // namespace duckdb
