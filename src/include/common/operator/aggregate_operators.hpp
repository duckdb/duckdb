//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/operator/aggregate_operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>

namespace duckdb {

struct Min {
	template <class T> static inline T Operation(T left, T right) {
		return std::min(left, right);
	}
};

struct Max {
	template <class T> static inline T Operation(T left, T right) {
		return std::max(left, right);
	}
};

struct MaximumStringLength {
	static inline uint64_t Operation(const char *str, uint64_t right) {
		return std::max((uint64_t)strlen(str), right);
	}
};

} // namespace duckdb
