//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/operator/numeric_functions.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <math.h>

namespace operators {

struct Abs {
	template <class T> static inline T Operation(T left) {
		return left < 0 ? left * -1 : left;
	}
};

struct Round {
	static inline double Operation(double input, int8_t precision) {
		if (precision < 0) {
			precision = 0;
		}
		double modifier = pow(10, precision);
		return ((int64_t)(input * modifier)) / modifier;
	}
};

} // namespace operators
