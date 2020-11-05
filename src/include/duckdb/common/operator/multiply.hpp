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

struct TryMultiplyOperator {
	template <class TA, class TB, class TR> static inline bool Operation(TA left, TB right, TR &result) {
		throw InternalException("Unimplemented type for TryMultiplyOperator");
	}
};

template <> bool TryMultiplyOperator::Operation(int8_t left, int8_t right, int8_t &result);
template <> bool TryMultiplyOperator::Operation(int16_t left, int16_t right, int16_t &result);
template <> bool TryMultiplyOperator::Operation(int32_t left, int32_t right, int32_t &result);
template <> bool TryMultiplyOperator::Operation(int64_t left, int64_t right, int64_t &result);

struct MultiplyOperatorOverflowCheck {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		TR result;
		if (!TryMultiplyOperator::Operation(left, right, result)) {
			throw OutOfRangeException("Overflow in multiplication of %s (%d * %d)!", TypeIdToString(GetTypeId<TA>()), left, right);
		}
		return result;
	}
};

struct DecimalMultiplyOperatorOverflowCheck {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		throw InternalException("Unimplemented type for DecimalMultiplyOperatorOverflowCheck");
	}
};

template <> int64_t DecimalMultiplyOperatorOverflowCheck::Operation(int64_t left, int64_t right);
template <> hugeint_t DecimalMultiplyOperatorOverflowCheck::Operation(hugeint_t left, hugeint_t right);

}
