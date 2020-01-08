//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/column_binding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

struct ColumnBinding {
	index_t table_index;
	index_t column_index;

	ColumnBinding() : table_index(INVALID_INDEX), column_index(INVALID_INDEX) {
	}
	ColumnBinding(index_t table, index_t column) : table_index(table), column_index(column) {
	}

	bool operator==(const ColumnBinding &rhs) const {
		return table_index == rhs.table_index && column_index == rhs.column_index;
	}
};

} // namespace duckdb
