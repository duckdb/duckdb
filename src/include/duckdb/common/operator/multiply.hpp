//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/multiply.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arch.h"
#include "duckdb/common/types.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

struct SET_ARCH(MultiplyOperator) {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return left * right;
	}
};

template <>
float SET_ARCH(MultiplyOperator)::Operation(float left, float right);
template <>
double SET_ARCH(MultiplyOperator)::Operation(double left, double right);
template <>
interval_t SET_ARCH(MultiplyOperator)::Operation(interval_t left, int64_t right);
template <>
interval_t SET_ARCH(MultiplyOperator)::Operation(int64_t left, interval_t right);

struct SET_ARCH(TryMultiplyOperator) {
	template <class TA, class TB, class TR>
	static inline bool Operation(TA left, TB right, TR &result) {
		throw InternalException("Unimplemented type for TryMultiplyOperator");
	}
};

template <>
bool SET_ARCH(TryMultiplyOperator)::Operation(uint8_t left, uint8_t right, uint8_t &result);
template <>
bool SET_ARCH(TryMultiplyOperator)::Operation(uint16_t left, uint16_t right, uint16_t &result);
template <>
bool SET_ARCH(TryMultiplyOperator)::Operation(uint32_t left, uint32_t right, uint32_t &result);
template <>
bool SET_ARCH(TryMultiplyOperator)::Operation(uint64_t left, uint64_t right, uint64_t &result);

template <>
bool SET_ARCH(TryMultiplyOperator)::Operation(int8_t left, int8_t right, int8_t &result);
template <>
bool SET_ARCH(TryMultiplyOperator)::Operation(int16_t left, int16_t right, int16_t &result);
template <>
bool SET_ARCH(TryMultiplyOperator)::Operation(int32_t left, int32_t right, int32_t &result);
template <>
bool SET_ARCH(TryMultiplyOperator)::Operation(int64_t left, int64_t right, int64_t &result);

struct MultiplyOperatorOverflowCheck {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		TR result;
		if (!SET_ARCH(TryMultiplyOperator)::Operation(left, right, result)) {
			throw OutOfRangeException("Overflow in multiplication of %s (%d * %d)!", TypeIdToString(GetTypeId<TA>()),
			                          left, right);
		}
		return result;
	}
};

struct SET_ARCH(TryDecimalMultiply) {
	template <class TA, class TB, class TR>
	static inline bool Operation(TA left, TB right, TR &result) {
		throw InternalException("Unimplemented type for TryDecimalMultiply");
	}
};

template <>
bool SET_ARCH(TryDecimalMultiply)::Operation(int16_t left, int16_t right, int16_t &result);
template <>
bool SET_ARCH(TryDecimalMultiply)::Operation(int32_t left, int32_t right, int32_t &result);
template <>
bool SET_ARCH(TryDecimalMultiply)::Operation(int64_t left, int64_t right, int64_t &result);
template <>
bool SET_ARCH(TryDecimalMultiply)::Operation(hugeint_t left, hugeint_t right, hugeint_t &result);

struct SET_ARCH(DecimalMultiplyOverflowCheck) {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		TR result;
		if (!SET_ARCH(TryDecimalMultiply)::Operation<TA, TB, TR>(left, right, result)) {
			throw OutOfRangeException("Overflow in multiplication of DECIMAL(18) (%d * %d). You might want to add an "
			                          "explicit cast to a bigger decimal.",
			                          left, right);
		}
		return result;
	}
};

template <>
hugeint_t SET_ARCH(DecimalMultiplyOverflowCheck)::Operation(hugeint_t left, hugeint_t right);

} // namespace duckdb
