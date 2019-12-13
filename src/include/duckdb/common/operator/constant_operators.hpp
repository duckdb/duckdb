//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/constant_operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

struct PickLeft {
	template <class T> static inline T Operation(T left, T right) {
		return left;
	}
};

struct PickRight {
	template <class T> static inline T Operation(T left, T right) {
		return right;
	}
};

struct NOP {
	template <class T> static inline T Operation(T left) {
		return left;
	}
};

struct ConstantZero {
	template <class T> static inline T Operation(T left, T right) {
		return 0;
	}
};

struct ConstantOne {
	template <class T> static inline T Operation(T left, T right) {
		return 1;
	}
};

struct AddOne {
	template <class T> static inline T Operation(T left, T right) {
		return right + 1;
	}
};

} // namespace duckdb
