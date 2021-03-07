//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/numeric_binary_operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arch.h"
#include "duckdb/common/assert.hpp"
#include <cmath>

namespace duckdb {

struct SET_ARCH(NegateOperator) {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return -input;
	}
};

struct SET_ARCH(DivideOperator) {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		D_ASSERT(right != 0); // this should be checked before!
		return left / right;
	}
};

struct SET_ARCH(ModuloOperator) {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		D_ASSERT(right != 0);
		return left % right;
	}
};

template <>
float SET_ARCH(DivideOperator)::Operation(float left, float right);
template <>
double SET_ARCH(DivideOperator)::Operation(double left, double right);
template <>
hugeint_t SET_ARCH(DivideOperator)::Operation(hugeint_t left, hugeint_t right);
template <>
interval_t SET_ARCH(DivideOperator)::Operation(interval_t left, int64_t right);

template <>
float SET_ARCH(ModuloOperator)::Operation(float left, float right);
template <>
double SET_ARCH(ModuloOperator)::Operation(double left, double right);
template <>
hugeint_t SET_ARCH(ModuloOperator)::Operation(hugeint_t left, hugeint_t right);

} // namespace duckdb
