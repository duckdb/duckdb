//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/numeric_functions.hpp
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

struct CbRt {
	template <class T> static inline double Operation(T left) {
		return cbrt(left);
	}
};

struct Degrees {
	template <class T> static inline double Operation(T left) {
		return left * (180 / PI);
	}
};

struct Radians {
	template <class T> static inline double Operation(T left) {
		return left * (PI / 180);
	}
};

struct Exp {
	template <class T> static inline double Operation(T left) {
		return exp(left);
	}
};

struct Sqrt {
	template <class T> static inline T Operation(T left) {
		return sqrt(left);
	}
};

struct Ln {
	template <class T> static inline T Operation(T left) {
		return log(left);
	}
};

struct Log10 {
	template <class T> static inline T Operation(T left) {
		return log10(left);
	}
};

struct Log2 {
	template <class T> static inline T Operation(T left) {
		return log2(left);
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

struct Sign {
	template <class T> static inline int8_t Operation(T left) {
		if (left == T(0))
			return 0;
		else if (left > T(0))
			return 1;
		else
			return -1;
	}
};

struct Pow {
	template <class T> static inline T Operation(T base, T exponent) {
		return pow(base, exponent);
	}
};

} // namespace duckdb
