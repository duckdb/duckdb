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
	    : CAPACITY(capacity), count(0) {
		block = buffer_manager.RegisterMemory(capacity * entry_size, false);
	}
	shared_ptr<BlockHandle> block;
	const idx_t CAPACITY;
	idx_t count;
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

	std::mutex rc_lock;

	//! BufferManager
	BufferManager &buffer_manager;
	//! The total number of stored entries
	idx_t count;
	//! The number of entries per block
	idx_t block_capacity;
	//! Size of entries in the blocks (0 if variable length)
	idx_t entry_size;
	//! The blocks holding the main data
	vector<RowDataBlock> blocks;

	//! Whether the system is little endian
	bool is_little_endian;

	idx_t Size() {
		return blocks.size();
	}

public:
	void SerializeVectorSortable(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count,
	                             data_ptr_t key_locations[], bool has_null, bool invert);
    static void SerializeIndices(data_ptr_t key_locations[], idx_t start, idx_t added_count);

	void SerializeVectorData(VectorData &vdata, PhysicalType type, const SelectionVector &sel, idx_t ser_count,
	                         idx_t col_idx, data_ptr_t key_locations[], data_ptr_t nullmask_locations[]);
	void SerializeVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count, idx_t col_idx,
	                     data_ptr_t key_locations[], data_ptr_t nullmask_locations[]);
	idx_t AppendToBlock(RowDataBlock &block, BufferHandle &handle, vector<BlockAppendEntry> &append_entries,
	                    idx_t remaining);
	idx_t Build(idx_t added_count, data_ptr_t key_locations[]);

	static void DeserializeIntoVectorData(Vector &v, PhysicalType type, idx_t vcount, idx_t col_idx,
	                                      data_ptr_t key_locations[], data_ptr_t nullmask_locations[]);
	static void DeserializeIntoVector(Vector &v, const idx_t &vcount, const idx_t &col_idx, data_ptr_t key_locations[],
	                                  data_ptr_t validitymask_locations[]);
	static void SkipOverType(PhysicalType &type, idx_t &vcount, data_ptr_t key_locations[]);

private:
	template <class T>
	void EncodeData(data_t *data, T value) {
		throw NotImplementedException("Cannot create data from this type");
	}

	template <class T>
	void TemplatedSerializeVectorSortable(VectorData &vdata, const SelectionVector &sel, idx_t count,
	                                      data_ptr_t key_locations[], bool has_null, bool invert);
};

template <>
void RowChunk::EncodeData(data_t *data, bool value);
template <>
void RowChunk::EncodeData(data_t *data, int8_t value);
template <>
void RowChunk::EncodeData(data_t *data, int16_t value);
template <>
void RowChunk::EncodeData(data_t *data, int32_t value);
template <>
void RowChunk::EncodeData(data_t *data, int64_t value);
template <>
void RowChunk::EncodeData(data_t *data, uint8_t value);
template <>
void RowChunk::EncodeData(data_t *data, uint16_t value);
template <>
void RowChunk::EncodeData(data_t *data, uint32_t value);
template <>
void RowChunk::EncodeData(data_t *data, uint64_t value);
template <>
void RowChunk::EncodeData(data_t *data, hugeint_t value);
template <>
void RowChunk::EncodeData(data_t *data, float value);
template <>
void RowChunk::EncodeData(data_t *data, double value);
template <>
void RowChunk::EncodeData(data_t *data, string_t value);
template <>
void RowChunk::EncodeData(data_t *data, const char *value);

} // namespace duckdb
