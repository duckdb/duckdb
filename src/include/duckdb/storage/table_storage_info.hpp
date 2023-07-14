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
