//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/append_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {
class DataTable;

struct AppendInfo {
	DataTable *table;
	idx_t start_row;
	idx_t count;
};

} // namespace duckdb
