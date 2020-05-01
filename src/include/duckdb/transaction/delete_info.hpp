//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/delete_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {
class ChunkInfo;
class DataTable;

struct DeleteInfo {
	DataTable *table;
	ChunkInfo *vinfo;
	idx_t count;
	idx_t base_row;
	row_t rows[1];
};

} // namespace duckdb
