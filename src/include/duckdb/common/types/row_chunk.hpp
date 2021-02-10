//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row_chunk.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

struct RowDataBlock {
	RowDataBlock(BufferManager &buffer_manager, idx_t capacity_, idx_t entry_size) : count(0), capacity(capacity_) {
		block = buffer_manager.RegisterMemory(capacity * entry_size, false);
	}
	idx_t count;
	idx_t capacity;
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

	//! The stringheap of the RowChunk
	StringHeap string_heap;
	//! BufferManager
	BufferManager &buffer_manager;
	//! Size of the nullmask bitset
	idx_t nullmask_size;
	//! The size of an entry as stored in the HashTable
	idx_t entry_size;
	//! The amount of entries stored per block
	idx_t block_capacity;
	//! The amount of entries stored in the HT currently
	idx_t count;
	//! The blocks holding the main data
	vector<RowDataBlock> blocks;

public:
	void SerializeVectorData(VectorData &vdata, PhysicalType type, const SelectionVector &sel, idx_t ser_count,
	                         idx_t col_idx, data_ptr_t key_locations[], data_ptr_t nullmask_locations[]);
	void SerializeVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count, idx_t col_idx,
	                     data_ptr_t key_locations[], data_ptr_t nullmask_locations[]);
	idx_t AppendToBlock(RowDataBlock &block, BufferHandle &handle, vector<BlockAppendEntry> &append_entries,
	                    idx_t remaining);
	void Build(idx_t added_count, data_ptr_t key_locations[], data_ptr_t nullmask_locations[]);

	void DeserializeIntoVectorData(VectorData &vdata, PhysicalType type, idx_t vcount, idx_t col_idx,
	                               data_ptr_t key_locations[], data_ptr_t nullmask_locations[]);
	void DeserializeIntoVector(Vector &v, idx_t vcount, idx_t col_idx, data_ptr_t key_locations[],
	                           data_ptr_t nullmask_locations[]);
	void DeserializeRowBlock(DataChunk &chunk, RowDataBlock &block, idx_t entry);

	void Append(RowChunk &chunk);
};

} // namespace duckdb
