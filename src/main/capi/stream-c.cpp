#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/allocator.hpp"

duckdb_data_chunk duckdb_stream_fetch_chunk(duckdb_result result) {
	return duckdb_fetch_chunk(result);
}

duckdb_data_chunk duckdb_fetch_chunk(duckdb_result result) {
	if (!result.internal_data) {
		return nullptr;
	}
	auto &result_data = *((duckdb::DuckDBResultData *)result.internal_data);
	if (result_data.result_set_type == duckdb::CAPIResultSetType::CAPI_RESULT_TYPE_DEPRECATED) {
		return nullptr;
	}
	if (result_data.result->type != duckdb::QueryResultType::STREAM_RESULT &&
	    result_data.result->type != duckdb::QueryResultType::MATERIALIZED_RESULT) {
		// We can only fetch from a StreamQueryResult or MaterializedQueryResult
		return nullptr;
	}
	result_data.result_set_type = duckdb::CAPIResultSetType::CAPI_RESULT_TYPE_STREAMING;

	auto &result_object = (duckdb::QueryResult &)*result_data.result;
	if (result_object.HasError()) {
		return nullptr;
	}
	auto chunk = result_object.Fetch();
	return reinterpret_cast<duckdb_data_chunk>(chunk.release());
}
