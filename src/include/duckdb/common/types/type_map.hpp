//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/type_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

struct LogicalTypeHashFunction {
	uint64_t operator()(const LogicalType &type) const {
		return (uint64_t)type.Hash();
	}
};

struct LogicalTypeEquality {
	bool operator()(const LogicalType &a, const LogicalType &b) const {
		return a == b;
	}
};

template <typename T>
using type_map_t = unordered_map<LogicalType, T, LogicalTypeHashFunction, LogicalTypeEquality>;

using type_set_t = unordered_set<LogicalType, LogicalTypeHashFunction, LogicalTypeEquality>;

} // namespace duckdb
