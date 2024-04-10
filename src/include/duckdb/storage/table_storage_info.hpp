//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table_storage_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/index_storage_info.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

//! Column segment information
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

//! Table storage information
class TableStorageInfo {
public:
	//! The (estimated) cardinality of the table
	optional_idx cardinality;
	//! Info of the indexes of a table
	vector<IndexInfo> index_info;
};

} // namespace duckdb
