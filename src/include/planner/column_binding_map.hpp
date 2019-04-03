//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/column_binding_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/hash.hpp"
#include "planner/column_binding.hpp"
#include "common/unordered_map.hpp"

namespace duckdb {

struct ColumnBindingHashFunction {
	size_t operator()(const ColumnBinding &a) const {
		return CombineHash(Hash<uint32_t>(a.table_index), Hash<uint32_t>(a.column_index));
	}
};

struct ColumnBindingEquality {
	bool operator()(const ColumnBinding &a, const ColumnBinding &b) const {
		return a == b;
	}
};

template <typename T>
using column_binding_map_t = unordered_map<ColumnBinding, T, ColumnBindingHashFunction, ColumnBindingEquality>;

}
