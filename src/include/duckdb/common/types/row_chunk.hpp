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
	RowDataBlock(BufferManager &buffer_manager, const idx_t &byte_capacity, const idx_t &constant_entry_size,
	             const idx_t &positions_blocksize)
	    : count(0), byte_offset(0), byte_capacity(byte_capacity), constant_entry_size(constant_entry_size),
	      entry_capacity(constant_entry_size ? byte_capacity / constant_entry_size : 0) {
		block = buffer_manager.RegisterMemory(byte_capacity, false);
		if (!constant_entry_size) {
			entry_positions = buffer_manager.RegisterMemory(positions_blocksize, false);
		}
	}
	shared_ptr<BlockHandle> block;
	idx_t count;

	idx_t byte_offset;
	idx_t byte_capacity;
	shared_ptr<BlockHandle> entry_positions = nullptr;

	idx_t constant_entry_size;
	idx_t entry_capacity;
};

struct BlockAppendEntry {
	BlockAppendEntry(data_ptr_t baseptr, idx_t count, idx_t *entry_positions)
	    : baseptr(baseptr), count(count), entry_positions(entry_positions) {
	}
	data_ptr_t baseptr;
	idx_t count;

	idx_t *entry_positions;
};

class RowChunk {
public:
	explicit RowChunk(BufferManager &buffer_manager);

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
	                    idx_t remaining, idx_t entry_sizes[], BufferHandle *endings_handle);
	void Build(idx_t added_count, data_ptr_t key_locations[], idx_t entry_sizes[], const idx_t &constant_entry_size,
	           const idx_t &positions_blocksize);

	static void DeserializeIntoVectorData(Vector &v, VectorData &vdata, PhysicalType type, idx_t vcount, idx_t col_idx,
	                                      data_ptr_t key_locations[], data_ptr_t nullmask_locations[]);
	static void DeserializeIntoVector(Vector &v, idx_t vcount, idx_t col_idx, data_ptr_t key_locations[],
	                                  data_ptr_t nullmask_locations[]);
};

} // namespace duckdb
