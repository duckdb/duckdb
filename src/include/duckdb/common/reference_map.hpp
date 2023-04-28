//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/reference_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {
class Expression;

template <class T>
struct ReferenceHashFunction {
	uint64_t operator()(const reference<T> &ref) const {
		return std::hash<void *>()((void *)&ref.get());
	}
};

template <class T>
struct ReferenceEquality {
	bool operator()(const reference<T> &a, const reference<T> &b) const {
		return &a.get() == &b.get();
	}
};

template <typename T, typename TGT>
using reference_map_t = unordered_map<reference<T>, TGT, ReferenceHashFunction<T>, ReferenceEquality<T>>;

template <typename T>
using reference_set_t = unordered_set<reference<T>, ReferenceHashFunction<T>, ReferenceEquality<T>>;

} // namespace duckdb
