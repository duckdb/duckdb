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
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right)
#if defined(__has_feature)
    #if __has_feature(__address_sanitizer__)
        __attribute__((__no_sanitize__("signed-integer-overflow")))
    #endif
#endif
	{
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
template <> interval_t AddOperator::Operation(interval_t left, interval_t right);
template <> date_t AddOperator::Operation(date_t left, interval_t right);
template <> date_t AddOperator::Operation(interval_t left, date_t right);
template <> timestamp_t AddOperator::Operation(timestamp_t left, interval_t right);
template <> timestamp_t AddOperator::Operation(interval_t left, timestamp_t right);
template <> hugeint_t AddOperator::Operation(hugeint_t left, hugeint_t right);

template <> float SubtractOperator::Operation(float left, float right);
template <> double SubtractOperator::Operation(double left, double right);
template <> interval_t SubtractOperator::Operation(interval_t left, interval_t right);
template <> date_t SubtractOperator::Operation(date_t left, interval_t right);
template <> timestamp_t SubtractOperator::Operation(timestamp_t left, interval_t right);
template <> interval_t SubtractOperator::Operation(timestamp_t left, timestamp_t right);
template <> hugeint_t SubtractOperator::Operation(hugeint_t left, hugeint_t right);

template <> float MultiplyOperator::Operation(float left, float right);
template <> double MultiplyOperator::Operation(double left, double right);
template <> interval_t MultiplyOperator::Operation(interval_t left, int64_t right);
template <> interval_t MultiplyOperator::Operation(int64_t left, interval_t right);
template <> hugeint_t MultiplyOperator::Operation(hugeint_t left, hugeint_t right);

template <> float DivideOperator::Operation(float left, float right);
template <> double DivideOperator::Operation(double left, double right);
template <> hugeint_t DivideOperator::Operation(hugeint_t left, hugeint_t right);
template <> interval_t DivideOperator::Operation(interval_t left, int64_t right);

template <> float ModuloOperator::Operation(float left, float right);
template <> double ModuloOperator::Operation(double left, double right);
template <> hugeint_t ModuloOperator::Operation(hugeint_t left, hugeint_t right);

template <> hugeint_t NegateOperator::Operation(hugeint_t input);

} // namespace duckdb
