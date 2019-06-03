//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/operator/numeric_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <cmath>

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

struct Ceil {
	template <class T> static inline T Operation(T left) {
		return ceil(left);
	}
};

struct Floor {
	template <class T> static inline T Operation(T left) {
		return floor(left);
	}
};

struct Sin {
	template <class T> static inline double Operation(T left) {
		return sin(left);
	}
};

struct Cos {
	template <class T> static inline double Operation(T left) {
		return (double)cos(left);
	}
};

struct Tan {
	template <class T> static inline double Operation(T left) {
		return (double)tan(left);
	}
};

struct ASin {
	template <class T> static inline double Operation(T left) {
		if (left < -1 || left > 1) {
			throw Exception("ASIN is undefined outside [-1,1]");
		}
		return (double)asin(left);
	}
};

struct ACos {
	template <class T> static inline double Operation(T left) {
		if (left < -1 || left > 1) {
			throw Exception("ACOS is undefined outside [-1,1]");
		}
		return (double)acos(left);
	}
};

struct ATan {
	template <class T> static inline double Operation(T left) {
		return (double)atan(left);
	}
};

struct ATan2 {
	template <class T> static inline double Operation(T left, T right) {
		return (double)atan2(left, right);
	}
};

} // namespace duckdb
