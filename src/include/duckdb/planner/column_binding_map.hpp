//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/column_binding_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

struct ColumnBindingHashFunction {
	uint64_t operator()(const ColumnBinding &a) const {
		return CombineHash(Hash<idx_t>(a.table_index), Hash<idx_t>(a.column_index));
	}
};

struct ColumnBindingEquality {
	bool operator()(const ColumnBinding &a, const ColumnBinding &b) const {
		return a == b;
	}
};

template <typename T>
using column_binding_map_t = unordered_map<ColumnBinding, T, ColumnBindingHashFunction, ColumnBindingEquality>;

using column_binding_set_t = unordered_set<ColumnBinding, ColumnBindingHashFunction, ColumnBindingEquality>;

} // namespace duckdb
