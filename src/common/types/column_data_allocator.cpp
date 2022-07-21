#include "duckdb/common/types/column_data_allocator.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

ColumnDataAllocator::ColumnDataAllocator(BufferManager &buffer_manager) : buffer_manager(buffer_manager) {
}

BufferHandle ColumnDataAllocator::Pin(uint32_t block_id) {
	return buffer_manager.Pin(blocks[block_id].handle);
}

void ColumnDataAllocator::AllocateBlock() {
	BlockMetaData data;
	data.size = 0;
	data.capacity = Storage::BLOCK_ALLOC_SIZE;
	data.handle = buffer_manager.RegisterMemory(Storage::BLOCK_ALLOC_SIZE, false);
	blocks.push_back(move(data));
}

void ColumnDataAllocator::AllocateData(idx_t size, uint32_t &block_id, uint32_t &offset,
                                       ChunkManagementState *chunk_state) {
	if (blocks.empty() || blocks.back().Capacity() < size) {
		AllocateBlock();
		if (chunk_state && !blocks.empty()) {
			auto &last_block = blocks.back();
			auto new_block_id = blocks.size() - 1;
			auto pinned_block = buffer_manager.Pin(last_block.handle);
			chunk_state->handles[new_block_id] = move(pinned_block);
		}
	}
	auto &block = blocks.back();
	D_ASSERT(size <= block.capacity - block.size);
	block_id = blocks.size() - 1;
	offset = block.size;
	block.size += size;
}

void ColumnDataAllocator::Initialize(ColumnDataAllocator &other) {
	D_ASSERT(other.HasBlocks());
	blocks.push_back(other.blocks.back());
}

uint32_t BlockMetaData::Capacity() {
	D_ASSERT(size <= capacity);
	return capacity - size;
}

}
