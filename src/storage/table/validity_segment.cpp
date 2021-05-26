#include "duckdb/storage/table/validity_segment.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/storage/statistics/validity_statistics.hpp"

namespace duckdb {

ValiditySegment::ValiditySegment(DatabaseInstance &db, idx_t row_start, block_id_t block_id)
    : UncompressedSegment(db, PhysicalType::BIT, row_start) {
	// figure out how many vectors we want to store in this block

	auto vector_size = ValidityMask::STANDARD_MASK_SIZE;
	this->max_tuples = Storage::BLOCK_SIZE / vector_size * STANDARD_VECTOR_SIZE;
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	if (block_id == INVALID_BLOCK) {
		// no block id specified: allocate a buffer for the uncompressed segment
		this->block = buffer_manager.RegisterMemory(Storage::BLOCK_ALLOC_SIZE, false);
		// pin the block and initialize
		auto handle = buffer_manager.Pin(block);
		memset(handle->node->buffer, 0xFF, Storage::BLOCK_SIZE);
	} else {
		this->block = buffer_manager.RegisterBlock(block_id);
	}
}

ValiditySegment::~ValiditySegment() {
}

void ValiditySegment::InitializeScan(ColumnScanState &state) {
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	state.primary_handle = buffer_manager.Pin(block);
}

void ValiditySegment::FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	D_ASSERT(row_id >= 0 && row_id < row_t(this->tuple_count));
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto handle = buffer_manager.Pin(block);
	ValidityMask mask((validity_t *)handle->node->buffer);
	if (!mask.RowIsValidUnsafe(row_id)) {
		FlatVector::SetNull(result, result_idx, true);
	}
}

idx_t ValiditySegment::Append(SegmentStatistics &stats, VectorData &data, idx_t offset, idx_t vcount) {
	idx_t append_count = MinValue<idx_t>(vcount, max_tuples - tuple_count);
	if (data.validity.AllValid()) {
		// no null values: skip append
		tuple_count += append_count;
		return append_count;
	}
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto handle = buffer_manager.Pin(block);

	auto &validity_stats = (ValidityStatistics &)*stats.statistics;
	ValidityMask mask((validity_t *)handle->node->buffer);
	for (idx_t i = 0; i < append_count; i++) {
		auto idx = data.sel->get_index(offset + i);
		if (!data.validity.RowIsValidUnsafe(idx)) {
			mask.SetInvalidUnsafe(tuple_count + i);
			validity_stats.has_null = true;
		}
	}
	tuple_count += append_count;
	return append_count;
}

void ValiditySegment::Scan(ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result, idx_t result_offset) {
	// FIXME: this should be optimized and use shifts/masks to copy multiple values at once
// #if STANDARD_VECTOR_SIZE >= 64
	idx_t base_tuple = start;
	ValidityMask source_mask((validity_t *) state.primary_handle->node->buffer);
	auto &target = FlatVector::Validity(result);
	for (idx_t i = 0; i < scan_count; i++) {
		target.Set(result_offset + i, source_mask.RowIsValid(base_tuple + i));
	}
}

void ValiditySegment::RevertAppend(idx_t start_row) {
	idx_t start_bit = start_row - this->row_start;
	UncompressedSegment::RevertAppend(start_row);

	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto handle = buffer_manager.Pin(block);
	idx_t revert_start;
	if (start_bit % 8 != 0) {
		// handle sub-bit stuff (yay)
		idx_t byte_pos = start_bit / 8;
		idx_t bit_start = byte_pos * 8;
		idx_t bit_end = (byte_pos + 1) * 8;
		ValidityMask mask((validity_t *)handle->node->buffer + byte_pos);
		for (idx_t i = start_bit; i < bit_end; i++) {
			mask.SetValid(i - bit_start);
		}
		revert_start = bit_end / 8;
	} else {
		revert_start = start_bit / 8;
	}
	// for the rest, we just memset
	memset(handle->node->buffer + revert_start, 0xFF, Storage::BLOCK_SIZE - revert_start);
}

} // namespace duckdb
