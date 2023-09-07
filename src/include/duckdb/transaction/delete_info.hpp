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
class DataTable;
class RowVersionManager;

struct DeleteInfo {
	DataTable *table;
	RowVersionManager *version_info;
	idx_t vector_idx;
	idx_t count;
	idx_t base_row;
	row_t rows[1];
};

} // namespace duckdb
