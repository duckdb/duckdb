//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row_chunk.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

struct RowDataBlock {
	RowDataBlock(BufferManager &buffer_manager, idx_t byte_capacity)
	    : count(0), byte_offset(0), byte_capacity(byte_capacity) {
		block = buffer_manager.RegisterMemory(byte_capacity, false);
	}
	idx_t count;
	idx_t byte_offset;
	idx_t byte_capacity;
	shared_ptr<BlockHandle> block;
};

struct BlockAppendEntry {
	BlockAppendEntry(data_ptr_t baseptr_, idx_t count_) : baseptr(baseptr_), count(count_) {
	}
	data_ptr_t baseptr;
	idx_t count;
};

class RowChunk {
public:
	RowChunk(BufferManager &buffer_manager);

	std::mutex rc_lock;

	//! BufferManager
	BufferManager &buffer_manager;
	//! The number of bytes per block
	idx_t block_capacity;
	//! The blocks holding the main data
	vector<RowDataBlock> blocks;

	idx_t Size() {
		return blocks.size();
	}

public:
	void SerializeVectorData(VectorData &vdata, PhysicalType type, const SelectionVector &sel, idx_t ser_count,
	                         idx_t col_idx, data_ptr_t key_locations[], data_ptr_t nullmask_locations[]);
	void SerializeVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count, idx_t col_idx,
	                     data_ptr_t key_locations[], data_ptr_t nullmask_locations[]);
	idx_t AppendToBlock(RowDataBlock &block, BufferHandle &handle, vector<BlockAppendEntry> &append_entries,
	                    idx_t added_count, idx_t starting_entry, idx_t offsets[]);
	void Build(idx_t added_count, idx_t offsets[], data_ptr_t key_locations[]);

	static void DeserializeIntoVectorData(Vector &v, VectorData &vdata, PhysicalType type, idx_t vcount, idx_t col_idx,
	                                      data_ptr_t key_locations[], data_ptr_t nullmask_locations[]);
	static void DeserializeIntoVector(Vector &v, idx_t vcount, idx_t col_idx, data_ptr_t key_locations[],
	                                  data_ptr_t nullmask_locations[]);
};

} // namespace duckdb
