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

struct DivideOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		D_ASSERT(right != 0); // this should be checked before!
		return left / right;
	}
};

struct ModuloOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		D_ASSERT(right != 0);
		return left % right;
	}
};

template <>
float DivideOperator::Operation(float left, float right);
template <>
double DivideOperator::Operation(double left, double right);
template <>
hugeint_t DivideOperator::Operation(hugeint_t left, hugeint_t right);
template <>
interval_t DivideOperator::Operation(interval_t left, int64_t right);

template <>
float ModuloOperator::Operation(float left, float right);
template <>
double ModuloOperator::Operation(double left, double right);
template <>
hugeint_t ModuloOperator::Operation(hugeint_t left, hugeint_t right);

} // namespace duckdb
