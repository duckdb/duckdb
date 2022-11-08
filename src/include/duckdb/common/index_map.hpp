//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/index_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {
class Expression;

struct LogicalIndexHashFunction {
	uint64_t operator()(const LogicalIndex &index) const {
		return std::hash<idx_t>()(index.index);
	}
};

template <typename T>
using logical_index_map_t = unordered_map<LogicalIndex, T, LogicalIndexHashFunction>;

using logical_index_set_t = unordered_set<LogicalIndex, LogicalIndexHashFunction>;

} // namespace duckdb
