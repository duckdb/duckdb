//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/column_binding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include <functional>

namespace duckdb {

struct ColumnBinding {
	idx_t table_index;
	idx_t column_index;
	idx_t lambda_index;

	ColumnBinding()
	    : table_index(DConstants::INVALID_INDEX), column_index(DConstants::INVALID_INDEX),
	      lambda_index(DConstants::INVALID_INDEX) {
	}
	ColumnBinding(idx_t table, idx_t column, idx_t lambda = DConstants::INVALID_INDEX)
	    : table_index(table), column_index(column), lambda_index(lambda) {
	}

	bool operator==(const ColumnBinding &rhs) const {
		return table_index == rhs.table_index && column_index == rhs.column_index && lambda_index == rhs.lambda_index;
	}
};

} // namespace duckdb
