#include "duckdb/storage/table/validity_segment.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

ValiditySegment::ValiditySegment(DatabaseInstance &db, idx_t row_start, block_id_t block_id) :
	UncompressedSegment(db, PhysicalType::BIT, row_start) {
	// figure out how many vectors we want to store in this block

	this->vector_size = ValidityMask::STANDARD_MASK_SIZE;
	this->max_vector_count = Storage::BLOCK_SIZE / vector_size;
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
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto handle = buffer_manager.Pin(block);
	ValidityMask mask((validity_t *) handle->node->buffer);
	if (!mask.RowIsValidUnsafe(row_id - this->row_start)) {
		FlatVector::SetNull(result, result_idx, true);
	}
}

idx_t ValiditySegment::Append(SegmentStatistics &stats, VectorData &data, idx_t offset, idx_t vcount) {
	idx_t append_count = MinValue<idx_t>(vcount, max_vector_count * STANDARD_VECTOR_SIZE - tuple_count);
	if (data.validity.AllValid()) {
		// no null values: skip append
		tuple_count += append_count;
		return append_count;
	}
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto handle = buffer_manager.Pin(block);

	ValidityMask mask((validity_t *) handle->node->buffer);
	for(idx_t i = 0; i < append_count; i++) {
		auto idx = data.sel->get_index(i);
		if (!data.validity.RowIsValidUnsafe(idx)) {
			mask.SetInvalidUnsafe(tuple_count + i);
		}
	}
	tuple_count += append_count;
	return append_count;
}

void ValiditySegment::FetchBaseData(ColumnScanState &state, idx_t vector_index, Vector &result) {
	auto vector_ptr = state.primary_handle->node->buffer + vector_index * ValidityMask::STANDARD_MASK_SIZE;
	ValidityMask vector_mask(vector_ptr);
	if (!vector_mask.CheckAllValid(STANDARD_VECTOR_SIZE)) {
		FlatVector::Validity(result).Copy(vector_mask, STANDARD_VECTOR_SIZE);
	}
}

}
