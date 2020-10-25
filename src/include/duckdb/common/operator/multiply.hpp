//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/multiply.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

struct MultiplyOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return left * right;
	}
};

template <> float MultiplyOperator::Operation(float left, float right);
template <> double MultiplyOperator::Operation(double left, double right);
template <> interval_t MultiplyOperator::Operation(interval_t left, int64_t right);
template <> interval_t MultiplyOperator::Operation(int64_t left, interval_t right);

struct MultiplyOperatorOverflowCheck {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		throw InternalException("Unimplemented type for MultiplyOperatorOverflowCheck");
	}
};

template <> int8_t MultiplyOperatorOverflowCheck::Operation(int8_t left, int8_t right);
template <> int16_t MultiplyOperatorOverflowCheck::Operation(int16_t left, int16_t right);
template <> int32_t MultiplyOperatorOverflowCheck::Operation(int32_t left, int32_t right);
template <> int64_t MultiplyOperatorOverflowCheck::Operation(int64_t left, int64_t right);

}
