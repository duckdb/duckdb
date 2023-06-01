#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/allocator.hpp"

duckdb_data_chunk duckdb_stream_fetch_chunk(duckdb_result result) {
	if (!result.internal_data) {
		return nullptr;
	}
	auto &result_data = *((duckdb::DuckDBResultData *)result.internal_data);
	if (result_data.result_set_type == duckdb::CAPIResultSetType::CAPI_RESULT_TYPE_DEPRECATED) {
		return nullptr;
	}
	if (result_data.result->type != duckdb::QueryResultType::STREAM_RESULT) {
		// We can only fetch from a StreamQueryResult
		return nullptr;
	}
	result_data.result_set_type = duckdb::CAPIResultSetType::CAPI_RESULT_TYPE_STREAMING;
	auto &streaming = (duckdb::StreamQueryResult &)*result_data.result;
	if (!streaming.IsOpen()) {
		return nullptr;
	}
	// FetchRaw ? Do we care about flattening them?
	auto chunk = streaming.Fetch();
	return reinterpret_cast<duckdb_data_chunk>(chunk.release());
}
