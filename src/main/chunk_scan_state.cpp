#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/chunk_scan_state.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/stream_query_result.hpp"

namespace duckdb {

idx_t ChunkScanState::CurrentOffset() const {
	return offset;
}

void ChunkScanState::IncreaseOffset(idx_t increment) {
	D_ASSERT(increment <= RemainingInChunk());
	offset += increment;
}

bool ChunkScanState::Finished() const {
	return finished;
}

bool ChunkScanState::ScanStarted() const {
	return current_chunk != nullptr;
}

DataChunk &ChunkScanState::CurrentChunk() {
	// Scan must already be started
	D_ASSERT(current_chunk);
	if (!current_chunk) {
		// Scan is not started yet
		PreservedError ignored;
		D_ASSERT(LoadNextChunk(ignored));
	}
	return *current_chunk;
}

idx_t ChunkScanState::RemainingInChunk() const {
	D_ASSERT(current_chunk);
	D_ASSERT(offset <= current_chunk->size());
	return current_chunk->size() - offset;
}

// QueryResult ChunkScanState

QueryResultChunkScanState::QueryResultChunkScanState(QueryResult &result) : ChunkScanState(0), result(result) {
}

QueryResultChunkScanState::~QueryResultChunkScanState() {
}

bool QueryResultChunkScanState::InternalLoad(PreservedError &error) {
	D_ASSERT(!finished);
	if (result.type == QueryResultType::STREAM_RESULT) {
		auto &stream_result = result.Cast<StreamQueryResult>();
		if (!stream_result.IsOpen()) {
			return true;
		}
	}
	return result.TryFetch(current_chunk, error);
}

bool QueryResultChunkScanState::HasError() const {
	return result.HasError();
}

PreservedError &QueryResultChunkScanState::GetError() {
	return result.GetErrorObject();
}

vector<LogicalType> &QueryResultChunkScanState::Types() {
	return result.types;
}

bool QueryResultChunkScanState::LoadNextChunk(PreservedError &error) {
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
