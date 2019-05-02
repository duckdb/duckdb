//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/operator/numeric_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <math.h>

namespace duckdb {

struct Abs {
	template <class T> static inline T Operation(T left) {
		return left < 0 ? left * -1 : left;
	}
};

struct Round {
	template <class T> static inline T Operation(T input, int8_t precision) {
		if (precision < 0) {
			precision = 0;
		}
		T modifier = pow(10, precision);
		return ((int64_t)(input * modifier)) / modifier;
	}
};

} // namespace duckdb
