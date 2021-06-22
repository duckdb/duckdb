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
	RowDataBlock(BufferManager &buffer_manager, idx_t capacity, idx_t entry_size)
	    : capacity(capacity), entry_size(entry_size), count(0), byte_offset(0) {
		block = buffer_manager.RegisterMemory(capacity * entry_size, false);
	}
	shared_ptr<BlockHandle> block;
	const idx_t capacity;
	const idx_t entry_size;
	idx_t count;
	idx_t byte_offset;

	RowDataBlock(const RowDataBlock &other)
	    : block(other.block), capacity(other.capacity), entry_size(other.entry_size), count(other.count),
	      byte_offset(other.byte_offset) {
	}
};

struct BlockAppendEntry {
	BlockAppendEntry(data_ptr_t baseptr, idx_t count, uint16_t block_index)
	    : baseptr(baseptr), count(count), block_index(block_index) {
	}
	data_ptr_t baseptr;
	idx_t count;
	uint16_t block_index;
};

class RowDataCollection {
public:
	RowDataCollection(BufferManager &buffer_manager, idx_t block_capacity, idx_t entry_size);

	mutex rc_lock;

	//! BufferManager
	BufferManager &buffer_manager;
	//! The total number of stored entries
	idx_t count;
	//! The number of entries per block
	idx_t block_capacity;
	//! Size of entries in the blocks
	idx_t entry_size;
	//! The blocks holding the main data
	vector<RowDataBlock> blocks;
	//! The index of this collection
	uint16_t collection_index;
	//! Mapping from block index to the corresponding block handle
	unordered_map<uint32_t, shared_ptr<BlockHandle>> block_map;

public:
	void SerializeVectorSortable(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count,
	                             data_ptr_t key_locations[], bool desc, bool has_null, bool invert, idx_t prefix_len);

	static void ComputeEntrySizes(Vector &v, VectorData &vdata, idx_t entry_sizes[], idx_t vcount, idx_t ser_count,
	                              const SelectionVector &sel, idx_t offset = 0);
	static void ComputeEntrySizes(Vector &v, idx_t entry_sizes[], idx_t vcount, idx_t ser_count,
	                              const SelectionVector &sel, idx_t offset = 0);
	static void ComputeEntrySizes(DataChunk &input, idx_t entry_sizes[], idx_t entry_size, const SelectionVector &sel,
	                              idx_t ser_count);

	static void SerializeVectorData(VectorData &vdata, PhysicalType type, const SelectionVector &sel, idx_t ser_count,
	                                idx_t col_idx, data_ptr_t key_locations[], data_ptr_t validitymask_locations[],
	                                idx_t offset = 0);
	static void SerializeVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count, idx_t col_idx,
	                            data_ptr_t key_locations[], data_ptr_t validitymask_locations[], idx_t offset = 0);
	idx_t AppendToBlock(RowDataBlock &block, BufferHandle &handle, vector<BlockAppendEntry> &append_entries,
	                    idx_t remaining, idx_t entry_sizes[]);
	vector<BlockAppendEntry> Build(idx_t added_count, data_ptr_t key_locations[], idx_t entry_sizes[]);

	void Merge(RowDataCollection &other);

	static void DeserializeIntoVector(Vector &v, const idx_t &vcount, const SelectionVector &sel, const idx_t &col_idx,
	                                  data_ptr_t key_locations[], data_ptr_t validitymask_locations[]);

private:
	template <class T>
	void TemplatedSerializeVectorSortable(VectorData &vdata, const SelectionVector &sel, idx_t count,
	                                      data_ptr_t key_locations[], bool desc, bool has_null, bool invert);
	void SerializeStringVectorSortable(VectorData &vdata, const SelectionVector &sel, idx_t add_count,
	                                   data_ptr_t key_locations[], const bool desc, const bool has_null,
	                                   const bool nulls_first, const idx_t prefix_len);

	static void ComputeStringEntrySizes(VectorData &col, idx_t entry_sizes[], const idx_t ser_count,
	                                    const SelectionVector &sel, const idx_t offset);
	static void ComputeStructEntrySizes(Vector &v, idx_t entry_sizes[], idx_t vcount, idx_t ser_count,
	                                    const SelectionVector &sel, idx_t offset);
	static void ComputeListEntrySizes(Vector &v, VectorData &vdata, idx_t entry_sizes[], idx_t ser_count,
	                                  const SelectionVector &sel, idx_t offset);

	static void SerializeStringVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count,
	                                  idx_t col_idx, data_ptr_t key_locations[], data_ptr_t validitymask_locations[],
	                                  idx_t offset);
	static void SerializeStructVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count,
	                                  idx_t col_idx, data_ptr_t key_locations[], data_ptr_t validitymask_locations[],
	                                  idx_t offset);
	static void SerializeListVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count, idx_t col_idx,
	                                data_ptr_t key_locations[], data_ptr_t validitymask_locations[], idx_t offset);

	//! Whether the system is little endian
	const bool is_little_endian;
	//! The index of the next block
	uint16_t current_block_index;

	void AddBlockToMap(shared_ptr<BlockHandle> block) {
		data_t block_index_data[sizeof(uint32_t)];
		Store<uint16_t>(collection_index, block_index_data);
		Store<uint16_t>(current_block_index, block_index_data + sizeof(uint16_t));
		uint32_t &block_index = (uint32_t &)*block_index_data;
		block_map[block_index] = move(block);
	}
};

} // namespace duckdb
