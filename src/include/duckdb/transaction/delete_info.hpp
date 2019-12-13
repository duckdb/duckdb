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
	ChunkInfo *vinfo;
	index_t count;
	index_t base_row;
	row_t rows[1];

	DataTable &GetTable();
};

} // namespace duckdb
