#include "duckdb/storage/table/validity_segment.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

ValiditySegment::ValiditySegment(DatabaseInstance &db, idx_t start, idx_t count, block_id_t block_id) :
	SegmentBase(start, count), db(db) {
	this->max_vector_count = Storage::BLOCK_SIZE / ValidityMask::STANDARD_MASK_SIZE;
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


void ValiditySegment::Fetch(idx_t vector_index, ValidityMask &result) {
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto handle = buffer_manager.Pin(block);

	auto vector_ptr = handle->node->buffer + vector_index * ValidityMask::STANDARD_MASK_SIZE;
	ValidityMask vector_mask(vector_ptr);
	if (!vector_mask.CheckAllValid(STANDARD_VECTOR_SIZE)) {
		result.Copy(vector_mask, STANDARD_VECTOR_SIZE);
	}
}


idx_t ValiditySegment::Append(VectorData &data, idx_t offset, idx_t vcount) {
	idx_t append_count = MinValue<idx_t>(vcount, max_vector_count * STANDARD_VECTOR_SIZE - this->count);
	if (data.validity.AllValid()) {
		// no null values: skip append
		this->count += append_count;
		return append_count;
	}
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto handle = buffer_manager.Pin(block);

	ValidityMask mask((validity_t *) handle->node->buffer);
	for(idx_t i = 0; i < append_count; i++) {
		auto idx = data.sel->get_index(i);
		if (!data.validity.RowIsValidUnsafe(idx)) {
			mask.SetInvalidUnsafe(count + i);
		}
	}
	this->count += append_count;
	return append_count;
}

bool ValiditySegment::IsValid(idx_t row_index) {
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto handle = buffer_manager.Pin(block);
	ValidityMask mask((validity_t *) handle->node->buffer);
	return mask.RowIsValidUnsafe(row_index - this->start);
}


}
