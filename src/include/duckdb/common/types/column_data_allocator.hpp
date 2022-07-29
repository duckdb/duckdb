//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/column_data_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column_data_collection.hpp"

namespace duckdb {
struct ChunkMetaData;

struct BlockMetaData {
	//! The underlying block handle
	shared_ptr<BlockHandle> handle;
	//! How much space is currently used within the block
	uint32_t size;
	//! How much space is available in the block
	uint32_t capacity;

	uint32_t Capacity();
};

class ColumnDataAllocator {
public:
	ColumnDataAllocator(Allocator &allocator);
	ColumnDataAllocator(BufferManager &buffer_manager);
	ColumnDataAllocator(ClientContext &context, ColumnDataAllocatorType allocator_type);

	//! Returns an allocator object to allocate with. This returns the allocator in IN_MEMORY_ALLOCATOR, and a buffer
	//! allocator in case of BUFFER_MANAGER_ALLOCATOR.
	Allocator &GetAllocator();
	//! Returns the allocator type
	ColumnDataAllocatorType GetType() {
		return type;
	}

public:
	void AllocateData(idx_t size, uint32_t &block_id, uint32_t &offset, ChunkManagementState *chunk_state);

	void Initialize(ColumnDataAllocator &other);
	void InitializeChunkState(ChunkManagementState &state, ChunkMetaData &meta_data);
	data_ptr_t GetDataPointer(ChunkManagementState &state, uint32_t block_id, uint32_t offset);

private:
	void AllocateBlock();
	BufferHandle Pin(uint32_t block_id);

	bool HasBlocks() {
		return !blocks.empty();
	}

private:
	ColumnDataAllocatorType type;
	union {
		//! The allocator object (if this is a IN_MEMORY_ALLOCATOR)
		Allocator *allocator;
		//! The buffer manager (if this is a BUFFER_MANAGER_ALLOCATOR)
		BufferManager *buffer_manager;
	} alloc;
	//! The set of blocks used by the column data collection
	vector<BlockMetaData> blocks;
	//! The set of allocated data
	vector<AllocatedData> allocated_data;
};

} // namespace duckdb
