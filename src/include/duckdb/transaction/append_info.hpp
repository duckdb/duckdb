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
class DuckTableEntry;

struct AppendInfo {
	DuckTableEntry *table;
	idx_t start_row;
	idx_t count;
};

} // namespace duckdb
