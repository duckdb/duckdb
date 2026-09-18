#include "duckdb/main/query_result.hpp"
#include "duckdb/main/chunk_scan_state/query_result.hpp"

namespace duckdb {

QueryResultChunkScanState::QueryResultChunkScanState(QueryResult &result) : ChunkScanState(), result(result) {
}

QueryResultChunkScanState::~QueryResultChunkScanState() {
}

bool QueryResultChunkScanState::InternalLoad(ErrorData &error) {
	D_ASSERT(!finished);
	return result.TryFetchOrError(current_chunk, error);
}

bool QueryResultChunkScanState::HasError() const {
	return result.HasError();
}

ErrorData &QueryResultChunkScanState::GetError() {
	D_ASSERT(result.HasError());
	return result.GetErrorObject();
}

const vector<LogicalType> &QueryResultChunkScanState::Types() const {
	return result.GetTypes();
}

const vector<Identifier> &QueryResultChunkScanState::Names() const {
	return result.GetNames();
}

bool QueryResultChunkScanState::LoadNextChunk(ErrorData &error) {
	if (finished) {
		return !finished;
	}
	auto load_result = InternalLoad(error);
	if (!load_result) {
		finished = true;
	}
	offset = 0;
	return !finished;
}

} // namespace duckdb
