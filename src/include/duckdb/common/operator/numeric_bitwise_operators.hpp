//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/numeric_bitwise_operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

struct BitwiseXOR {
	template <class T> static inline T Operation(T left, T right) {
		return left ^ right;
	}
};

struct BitwiseAND {
	template <class T> static inline T Operation(T left, T right) {
		return left & right;
	}
};

struct BitwiseOR {
	template <class T> static inline T Operation(T left, T right) {
		return left | right;
	}
};

struct BitwiseShiftLeft {
	template <class T> static inline T Operation(T left, T right) {
		return left << right;
	}
};

struct BitwiseShiftRight {
	template <class T> static inline T Operation(T left, T right) {
		return left >> right;
	}
};

struct BitwiseNOT {
	template <class T> static inline T Operation(T input) {
		return ~input;
	}
};

} // namespace duckdb
