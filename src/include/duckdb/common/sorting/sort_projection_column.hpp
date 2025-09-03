//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sorting/sort_projection_column.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"

namespace duckdb {

struct SortProjectionColumn {
	bool is_payload;
	idx_t layout_col_idx;
	idx_t output_col_idx;
};

} // namespace duckdb
