//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/index_storage_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

//! Information to serialize a FixedSizeAllocator, which holds the index data
struct FixedSizeAllocatorInfo {
	idx_t segment_size;
	vector<idx_t> buffer_ids;
	vector<BlockPointer> block_pointers;
	vector<idx_t> segment_counts;
	vector<idx_t> allocation_sizes;
	vector<idx_t> buffers_with_free_space;

	void Serialize(Serializer &serializer) const;
	static FixedSizeAllocatorInfo Deserialize(Deserializer &deserializer);
};

//! Information to serialize an index buffer to the WAL
struct IndexBufferInfo {
	IndexBufferInfo(data_ptr_t buffer_ptr, const idx_t allocation_size)
	    : buffer_ptr(buffer_ptr), allocation_size(allocation_size) {
	}

	data_ptr_t buffer_ptr;
	idx_t allocation_size;
};

//! Index (de)serialization information.
struct IndexStorageInfo {
	IndexStorageInfo() {};
	explicit IndexStorageInfo(const string &name) : name(name) {};

	//! The name.
	string name;
	//! The storage root.
	idx_t root;
	//! True, if the index uses the storage format of v1.0.0. Defaults to true during deserialization.
	//! We write this to throw a meaningful error message w.r.t. forwards compatibility when reading
	//! a newer file format with a deprecated duckdb version. It is not necessary for the program's logic.
	bool v1_0_0_storage;
	//! Serialization information for fixed-size allocator memory.
	vector<FixedSizeAllocatorInfo> allocator_infos;

	//! Contains all buffer pointers and their allocation size for serializing to the WAL.
	//! First dimension: All fixed-size allocators.
	//! Second dimension: The buffers of each fixed-size allocator.
	vector<vector<IndexBufferInfo>> buffers;

	//! The root block pointer of the index. Necessary to support older storage files.
	BlockPointer root_block_ptr;

	//! Returns true, if IndexStorageInfo holds information to deserialize an index.
	bool IsValid() const {
		return root_block_ptr.IsValid() || !allocator_infos.empty();
	}

	void Serialize(Serializer &serializer) const;
	static IndexStorageInfo Deserialize(Deserializer &deserializer);
};

//! Additional index information for tables
struct IndexInfo {
	bool is_unique;
	bool is_primary;
	bool is_foreign;
	unordered_set<column_t> column_set;
};

} // namespace duckdb
