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

struct Modulo {
	template <class T> static inline T Operation(T left, T right) {
		assert(right != 0);
		return left % right;
	}
};

template <> float Modulo::Operation(float left, float right);
template <> double Modulo::Operation(double left, double right);

} // namespace duckdb
