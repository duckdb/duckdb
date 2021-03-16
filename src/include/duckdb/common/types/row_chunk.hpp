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
	RowDataBlock(BufferManager &buffer_manager, const idx_t &capacity, const idx_t &entry_size)
	    : CAPACITY(capacity), count(0), byte_offset(0) {
		block = buffer_manager.RegisterMemory(capacity * entry_size, false);
	}
	shared_ptr<BlockHandle> block;
	const idx_t CAPACITY;
	idx_t count;
	idx_t byte_offset;
};

struct BlockAppendEntry {
	BlockAppendEntry(data_ptr_t baseptr, idx_t count) : baseptr(baseptr), count(count) {
	}
	data_ptr_t baseptr;
	idx_t count;
};

class RowChunk {
public:
	RowChunk(BufferManager &buffer_manager, idx_t block_capacity, idx_t entry_size);

	RowChunk(RowChunk &other);

	std::mutex rc_lock;

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

	idx_t Size() {
		return blocks.size();
	}

public:
	void SerializeVectorSortable(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count,
	                             data_ptr_t key_locations[], bool desc, bool has_null, bool invert, idx_t prefix_len);

	void SerializeVectorData(VectorData &vdata, PhysicalType type, const SelectionVector &sel, idx_t ser_count,
	                         idx_t col_idx, data_ptr_t key_locations[], data_ptr_t validitymask_locations[]);
	void SerializeVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count, idx_t col_idx,
	                     data_ptr_t key_locations[], data_ptr_t validitymask_locations[]);
	idx_t AppendToBlock(RowDataBlock &block, BufferHandle &handle, vector<BlockAppendEntry> &append_entries,
	                    idx_t remaining, idx_t entry_sizes[]);
	void Build(idx_t added_count, data_ptr_t key_locations[], idx_t entry_sizes[]);

	static void DeserializeIntoVectorData(Vector &v, PhysicalType type, idx_t vcount, idx_t col_idx,
	                                      data_ptr_t key_locations[], data_ptr_t validitymask_locations[]);
	static void DeserializeIntoVector(Vector &v, const idx_t &vcount, const idx_t &col_idx, data_ptr_t key_locations[],
	                                  data_ptr_t validitymask_locations[]);

private:
	template <class T>
	void EncodeData(data_t *data, T value) {
		throw NotImplementedException("Cannot create data from this type");
	}
	void EncodeStringData(data_ptr_t dataptr, string_t value, idx_t prefix_len);

	template <class T>
	void TemplatedSerializeVectorSortable(VectorData &vdata, const SelectionVector &sel, idx_t count,
	                                      data_ptr_t key_locations[], bool desc, bool has_null, bool invert);
	void SerializeStringVectorSortable(VectorData &vdata, const SelectionVector &sel, idx_t add_count,
	                                   data_ptr_t key_locations[], const bool desc, const bool has_null,
	                                   const bool nulls_first, const idx_t prefix_len);

	//! Whether the system is little endian
	bool is_little_endian;
};

template <>
void RowChunk::EncodeData(data_ptr_t dataptr, bool value);
template <>
void RowChunk::EncodeData(data_ptr_t dataptr, int8_t value);
template <>
void RowChunk::EncodeData(data_ptr_t dataptr, int16_t value);
template <>
void RowChunk::EncodeData(data_ptr_t dataptr, int32_t value);
template <>
void RowChunk::EncodeData(data_ptr_t dataptr, int64_t value);
template <>
void RowChunk::EncodeData(data_ptr_t dataptr, uint8_t value);
template <>
void RowChunk::EncodeData(data_ptr_t dataptr, uint16_t value);
template <>
void RowChunk::EncodeData(data_ptr_t dataptr, uint32_t value);
template <>
void RowChunk::EncodeData(data_ptr_t dataptr, uint64_t value);
template <>
void RowChunk::EncodeData(data_ptr_t dataptr, hugeint_t value);
template <>
void RowChunk::EncodeData(data_ptr_t dataptr, float value);
template <>
void RowChunk::EncodeData(data_ptr_t dataptr, double value);

} // namespace duckdb
