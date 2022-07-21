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

	void AllocateBlock();
	void AllocateData(idx_t size, uint32_t &block_id, uint32_t &offset, ChunkManagementState *chunk_state);

	BufferHandle Pin(uint32_t block_id);

	bool HasBlocks() {
		return !blocks.empty();
	}
	void Initialize(ColumnDataAllocator &other);

private:
	//! The buffer manager
	BufferManager &buffer_manager;
	//! The set of blocks used by the column data collection
	vector<BlockMetaData> blocks;
};

} // namespace duckdb
