//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/column_binding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"

namespace duckdb {

struct ColumnBinding {
	uint32_t table_index;
	uint32_t column_index;

	ColumnBinding() : table_index((uint32_t)-1), column_index((uint32_t)-1) {
	}
	ColumnBinding(uint32_t table, uint32_t column) : table_index(table), column_index(column) {
	}

	// these constructors are only there to check assertions
	ColumnBinding(uint64_t table, uint64_t column) : ColumnBinding((uint32_t)table, (uint32_t)column) {
		assert(table <= std::numeric_limits<uint32_t>::max());
		assert(column <= std::numeric_limits<uint32_t>::max());
	}

	bool operator==(const ColumnBinding &rhs) const {
		return table_index == rhs.table_index && column_index == rhs.column_index;
	}
};

} // namespace duckdb
