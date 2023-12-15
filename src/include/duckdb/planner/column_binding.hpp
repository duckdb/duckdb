//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/column_binding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/to_string.hpp"

#include <functional>

namespace duckdb {
class Serializer;
class Deserializer;

struct ColumnBinding {
	idx_t table_index;
	// This index is local to a Binding, and has no meaning outside of the context of the Binding that created it
	idx_t column_index;

	ColumnBinding() : table_index(DConstants::INVALID_INDEX), column_index(DConstants::INVALID_INDEX) {
	}
	ColumnBinding(idx_t table, idx_t column) : table_index(table), column_index(column) {
	}

	string ToString() const {
		return "#[" + to_string(table_index) + "." + to_string(column_index) + "]";
	}

	bool operator==(const ColumnBinding &rhs) const {
		return table_index == rhs.table_index && column_index == rhs.column_index;
	}

	bool operator!=(const ColumnBinding &rhs) const {
		return !(*this == rhs);
	}

	void Serialize(Serializer &serializer) const;
	static ColumnBinding Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
