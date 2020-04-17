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

struct AddOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return left + right;
	}
};

struct SubtractOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return left - right;
	}
};

struct NegateOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return -input;
	}
};

struct MultiplyOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return left * right;
	}
};

struct DivideOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		assert(right != 0); // this should be checked before!
		return left / right;
	}
};

struct ModuloOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		assert(right != 0);
		return left % right;
	}
};

template <> float AddOperator::Operation(float left, float right);
template <> double AddOperator::Operation(double left, double right);
template <> float SubtractOperator::Operation(float left, float right);
template <> double SubtractOperator::Operation(double left, double right);
template <> float MultiplyOperator::Operation(float left, float right);
template <> double MultiplyOperator::Operation(double left, double right);
template <> float DivideOperator::Operation(float left, float right);
template <> double DivideOperator::Operation(double left, double right);
template <> float ModuloOperator::Operation(float left, float right);
template <> double ModuloOperator::Operation(double left, double right);

} // namespace duckdb
