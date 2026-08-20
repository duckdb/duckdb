//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/sql_value_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/map.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

//! Container policies that match DuckDB comparison semantics, including NaN equality and ordering.
template <class T>
struct SQLValueHashFunction {
	hash_t operator()(const T &value) const {
		return duckdb::Hash(value);
	}
};

template <class T>
struct SQLValueEquality {
	bool operator()(const T &left, const T &right) const {
		return Equals::Operation(left, right);
	}
};

template <class T>
struct SQLValueLessThan {
	bool operator()(const T &left, const T &right) const {
		return LessThan::Operation(left, right);
	}
};

template <class KEY, class VALUE>
using sql_value_map_t = unordered_map<KEY, VALUE, SQLValueHashFunction<KEY>, SQLValueEquality<KEY>>;

template <class KEY, class VALUE>
using sql_value_ordered_map_t = map<KEY, VALUE, SQLValueLessThan<KEY>>;

} // namespace duckdb
