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

struct LogicalIndexHashFunction {
	uint64_t operator()(const LogicalIndex &index) const {
		return std::hash<idx_t>()(index.index);
	}
};

struct PhysicalIndexHashFunction {
	uint64_t operator()(const PhysicalIndex &index) const {
		return std::hash<idx_t>()(index.index);
	}
};

template <typename T>
using logical_index_map_t = unordered_map<LogicalIndex, T, LogicalIndexHashFunction>;

using logical_index_set_t = unordered_set<LogicalIndex, LogicalIndexHashFunction>;

template <typename T>
using physical_index_map_t = unordered_map<PhysicalIndex, T, PhysicalIndexHashFunction>;

using physical_index_set_t = unordered_set<PhysicalIndex, PhysicalIndexHashFunction>;

} // namespace duckdb
