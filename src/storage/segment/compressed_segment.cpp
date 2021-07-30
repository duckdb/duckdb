#include "duckdb/storage/segment/compressed_segment.hpp"

namespace duckdb {

CompressedSegment::CompressedSegment(DatabaseInstance &db, PhysicalType type, idx_t row_start, CompressionFunction *function_p, block_id_t block_id)
    : BaseSegment(db, type, row_start), function(function_p) {
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	if (block_id == INVALID_BLOCK) {
		// no block id specified: allocate a buffer for the uncompressed segment
		this->block = buffer_manager.RegisterMemory(Storage::BLOCK_SIZE, false);
	} else {
		this->block = buffer_manager.RegisterBlock(block_id);
	}
	if (function->init_segment) {
		function->init_segment(*this, block_id);
	}
}

void CompressedSegment::InitializeScan(ColumnScanState &state) {
	state.scan_state = function->init_scan(*this);
}

void CompressedSegment::Scan(ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result) {
	function->scan_vector(*this, state, start, scan_count, result);
}

void CompressedSegment::ScanPartial(ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result, idx_t result_offset) {
	function->scan_partial(*this, state, start, scan_count, result, result_offset);
}

void CompressedSegment::FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	function->fetch_row(*this, state, row_id, result, result_idx);
}

idx_t CompressedSegment::Append(SegmentStatistics &stats, VectorData &data, idx_t offset, idx_t count) {
	if (!function->append) {
		throw InternalException("Attempting to append to a compressed segment without append method");
	}
	return function->append(*this, stats, data, offset, count);
}

void CompressedSegment::RevertAppend(idx_t start_row) {
	if (function->revert_append) {
		function->revert_append(*this, start_row);
	}
	BaseSegment::RevertAppend(start_row);
}

}
