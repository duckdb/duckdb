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

//! Information to serialize an index
struct IndexStorageInfoo {
	//! Default constructor. We overwrite the deprecated_storage field during deserialization.
	IndexStorageInfoo() : deprecated_storage(true) {};
	explicit IndexStorageInfoo(bool deprecated_storage) : deprecated_storage(deprecated_storage) {};
	IndexStorageInfoo(string name, bool deprecated_storage)
	    : name(std::move(name)), deprecated_storage(deprecated_storage) {};

	//! The name of the index
	string name;
	//! The root of the index
	idx_t root;
	//! Whether the ART uses deprecated storage or nested leaf storage.
	bool deprecated_storage;
	//! Information to serialize the index memory held by the fixed-size allocators
	vector<FixedSizeAllocatorInfo> allocator_infos;

	//! Contains all buffer pointers and their allocation size for serializing to the WAL
	//! First dimension: all fixed-size allocators, second dimension: the buffers of each allocator
	vector<vector<IndexBufferInfo>> buffers;

	//! The root block pointer of the index, which is necessary to support older storage files
	BlockPointer root_block_ptr;

	//! Returns true, if the struct contains index information
	bool IsValid() const {
		return root_block_ptr.IsValid() || !allocator_infos.empty();
	}

	void Serialize(Serializer &serializer) const;
	static IndexStorageInfoo Deserialize(Deserializer &deserializer);
};

//! Additional index information for tables
struct IndexInfo {
	bool is_unique;
	bool is_primary;
	bool is_foreign;
	unordered_set<column_t> column_set;
};

} // namespace duckdb
