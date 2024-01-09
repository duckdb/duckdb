//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/database_size.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

struct DatabaseSize {
	idx_t total_blocks = 0;
	idx_t block_size = 0;
	idx_t free_blocks = 0;
	idx_t used_blocks = 0;
	idx_t bytes = 0;
	idx_t wal_size = 0;
};

struct MetadataBlockInfo {
	block_id_t block_id;
	idx_t total_blocks;
	vector<idx_t> free_list;
};

} // namespace duckdb
