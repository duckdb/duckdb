//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/numeric_inplace_operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

struct AddInPlace {
	template <class T> static inline void Operation(T &left, T right) {
		left += right;
	}
};

struct SubtractInPlace {
	template <class T> static inline void Operation(T &left, T right) {
		left -= right;
	}
};

struct MultiplyInPlace {
	template <class T> static inline void Operation(T &left, T right) {
		left *= right;
	}
};

struct DivideInPlace {
	template <class T> static inline void Operation(T &left, T right) {
		assert(right != 0); // this should be checked before!
		left /= right;
	}
};

struct ModuloIntInPlace {
	template <class T> static inline void Operation(T &left, T right) {
		assert(right != 0);
		left %= right;
	}
};

struct ModuloRealInPlace {
	template <class T> static inline void Operation(T &left, T right) {
		assert(right != 0);
		left = fmod(left, right);
	}
};

} // namespace duckdb
