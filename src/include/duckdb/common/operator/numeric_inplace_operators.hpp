//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/operator/numeric_inplace_operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

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

struct ModuloInPlace {
	template <class T> static inline void Operation(T &left, T right) {
		assert(right != 0);
		left %= right;
	}
};

} // namespace duckdb
