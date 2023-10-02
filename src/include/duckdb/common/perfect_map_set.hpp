//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/perfect_map_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

struct PerfectHash {
	inline std::size_t operator()(const idx_t &h) const {
		return h;
	}
};

struct PerfectEquality {
	inline bool operator()(const idx_t &a, const idx_t &b) const {
		return a == b;
	}
};

template <typename T>
using perfect_map_t = unordered_map<idx_t, T, PerfectHash, PerfectEquality>;

using perfect_set_t = unordered_set<idx_t, PerfectHash, PerfectEquality>;

} // namespace duckdb
