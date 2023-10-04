//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table_storage_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

struct ColumnSegmentInfo {
	idx_t row_group_index;
	idx_t column_id;
	string column_path;
	idx_t segment_idx;
	string segment_type;
	idx_t segment_start;
	idx_t segment_count;
	string compression_type;
	string segment_stats;
	bool has_updates;
	bool persistent;
	block_id_t block_id;
	idx_t block_offset;
	string segment_info;
};

//! Information to serialize the underlying data of an index
struct IndexDataStorageInfo {
	idx_t segment_size;
	vector<idx_t> buffer_ids;
	vector<BlockPointer> buffer_block_pointers;
	vector<idx_t> buffer_segment_counts;
	vector<idx_t> buffer_allocation_sizes;
	vector<idx_t> buffers_with_free_space_vec;

	void Serialize(Serializer &serializer) const;
	static IndexDataStorageInfo Deserialize(Deserializer &deserializer);
};

//! Information to serialize an index
struct IndexStorageInfo {
	//! The name of the index
	string name;
	//! Arbitrary index properties
	unordered_map<string, Value> properties;
	//! Information to serialize the index memory
	vector<IndexDataStorageInfo> index_data_storage_infos;

	//! Returns true, if the struct contains index information
	bool IsValid() const {
		return !name.empty() && (!properties.empty() || !index_data_storage_infos.empty());
	}

	void Serialize(Serializer &serializer) const;
	static IndexStorageInfo Deserialize(Deserializer &deserializer);
};

struct IndexInfo {
	bool is_unique;
	bool is_primary;
	bool is_foreign;
	unordered_set<column_t> column_set;
};

class TableStorageInfo {
public:
	//! The (estimated) cardinality of the table
	idx_t cardinality = DConstants::INVALID_INDEX;
	//! Info of the indexes of a table
	vector<IndexInfo> index_info;
};

} // namespace duckdb
