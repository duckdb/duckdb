//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/numeric_binary_operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include <cmath>

namespace duckdb {

struct Add {
	template <class T> static inline T Operation(T left, T right) {
		return left + right;
	}
};

struct Subtract {
	template <class T> static inline T Operation(T left, T right) {
		return left - right;
	}
};

struct Multiply {
	template <class T> static inline T Operation(T left, T right) {
		return left * right;
	}
};

struct Divide {
	template <class T> static inline T Operation(T left, T right) {
		assert(right != 0); // this should be checked before!
		return left / right;
	}
};

struct ModuloInt {
	template <class T> static inline T Operation(T left, T right) {
		assert(right != 0);
		return left % right;
	}
};

struct ModuloReal {
	template <class T> static inline T Operation(T left, T right) {
		assert(right != 0);
		return fmod(left, right);
	}
};

} // namespace duckdb
