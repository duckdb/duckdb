#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/chunk_scan_state.hpp"

namespace duckdb {

ChunkScanState::ChunkScanState() {
}

ChunkScanState::~ChunkScanState() {
}

idx_t ChunkScanState::CurrentOffset() const {
	return offset;
}

void ChunkScanState::IncreaseOffset(idx_t increment, bool unsafe) {
	D_ASSERT(unsafe || increment <= RemainingInChunk());
	offset += increment;
}

bool ChunkScanState::ChunkIsEmpty() const {
	return !current_chunk || current_chunk->size() == 0;
}

bool ChunkScanState::Finished() const {
	return finished;
}

bool ChunkScanState::ScanStarted() const {
	return !ChunkIsEmpty();
}

DataChunk &ChunkScanState::CurrentChunk() {
	// Scan must already be started
	D_ASSERT(current_chunk);
	return *current_chunk;
}

idx_t ChunkScanState::RemainingInChunk() const {
	if (ChunkIsEmpty()) {
		return 0;
	}
	D_ASSERT(current_chunk);
	D_ASSERT(offset <= current_chunk->size());
	return current_chunk->size() - offset;
}

} // namespace duckdb
