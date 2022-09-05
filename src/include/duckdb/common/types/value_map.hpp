//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct ValueHashFunction {
	uint64_t operator()(const Value &value) const {
		return (uint64_t)value.Hash();
	}
};

struct ValueEquality {
	bool operator()(const Value &a, const Value &b) const {
		return Value::NotDistinctFrom(a, b);
	}
};

template <typename T>
using value_map_t = unordered_map<Value, T, ValueHashFunction, ValueEquality>;

using value_set_t = unordered_set<Value, ValueHashFunction, ValueEquality>;

} // namespace duckdb
