//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/numeric_cast.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {

template <class SRC, class DST>
static bool TryCastWithOverflowCheck(SRC value, DST &result) {
	if (NumericLimits<SRC>::IsSigned() != NumericLimits<DST>::IsSigned()) {
		if (NumericLimits<SRC>::IsSigned()) {
			// signed to unsigned conversion
			if (NumericLimits<SRC>::Digits() > NumericLimits<DST>::Digits()) {
				if (value < 0 || value > (SRC)NumericLimits<DST>::Maximum()) {
					return false;
				}
			} else {
				if (value < 0) {
					return false;
				}
			}
			result = (DST)value;
			return true;
		} else {
			// unsigned to signed conversion
			if (NumericLimits<SRC>::Digits() >= NumericLimits<DST>::Digits()) {
				if (value <= (SRC)NumericLimits<DST>::Maximum()) {
					result = (DST)value;
					return true;
				}
				return false;
			} else {
				result = (DST)value;
				return true;
			}
		}
	} else {
		// same sign conversion
		if (NumericLimits<DST>::Digits() >= NumericLimits<SRC>::Digits()) {
			result = (DST) value;
			return true;
		} else {
			if (value < SRC(NumericLimits<DST>::Minimum()) || value > SRC(NumericLimits<DST>::Maximum())) {
				return false;
			}
			result = (DST)value;
			return true;
		}
	}
}

template <>
bool TryCastWithOverflowCheck(float value, int32_t &result) {
	if (!(value >= -2147483648.0f && value < 2147483648.0f)) {
		return false;
	}
	result = int32_t(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(float value, int64_t &result) {
	if (!(value >= -9223372036854775808.0f && value < 9223372036854775808.0f)) {
		return false;
	}
	result = int64_t(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(double value, int64_t &result) {
	if (!(value >= -9223372036854775808.0 && value < 9223372036854775808.0)) {
		return false;
	}
	result = int64_t(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(bool value, bool &result) {
	result = value;
	return true;
}

template <>
bool TryCastWithOverflowCheck(hugeint_t value, hugeint_t &result) {
	result = value;
	return true;
}

#define TRY_CAST_BOOL(SOURCE_TYPE) \
template <> \
bool TryCastWithOverflowCheck(SOURCE_TYPE value, bool &result) { \
	result = bool(value); \
	return true; \
} \
template <> \
bool TryCastWithOverflowCheck(bool value, SOURCE_TYPE &result) { \
	result = SOURCE_TYPE(value); \
	return true; \
}

#define TRY_CAST_HUGEINT(SOURCE_TYPE) \
template <> \
bool TryCastWithOverflowCheck(SOURCE_TYPE value, hugeint_t &result) { \
	return Hugeint::TryConvert(value, result); \
} \
template <> \
bool TryCastWithOverflowCheck(hugeint_t value, SOURCE_TYPE &result) { \
	return Hugeint::TryCast(value, result); \
}

TRY_CAST_BOOL(int8_t)
TRY_CAST_BOOL(int16_t)
TRY_CAST_BOOL(int32_t)
TRY_CAST_BOOL(int64_t)
TRY_CAST_BOOL(uint8_t)
TRY_CAST_BOOL(uint16_t)
TRY_CAST_BOOL(uint32_t)
TRY_CAST_BOOL(uint64_t)
TRY_CAST_BOOL(float)
TRY_CAST_BOOL(double)

template <>
bool TryCastWithOverflowCheck(hugeint_t input, bool &result) {
	result = input.upper != 0 || input.lower != 0;
	return true;
}
template <>
bool TryCastWithOverflowCheck(bool input, hugeint_t &result) {
	result.upper = 0;
	result.lower = input ? 1 : 0;
	return true;
}

TRY_CAST_HUGEINT(int8_t)
TRY_CAST_HUGEINT(int16_t)
TRY_CAST_HUGEINT(int32_t)
TRY_CAST_HUGEINT(int64_t)
TRY_CAST_HUGEINT(uint8_t)
TRY_CAST_HUGEINT(uint16_t)
TRY_CAST_HUGEINT(uint32_t)
TRY_CAST_HUGEINT(uint64_t)
TRY_CAST_HUGEINT(float)
TRY_CAST_HUGEINT(double)

template <>
bool TryCastWithOverflowCheck(double input, float &result) {
	if (input < (double)NumericLimits<float>::Minimum() || input > (double)NumericLimits<float>::Maximum()) {
		return false;
	}
	auto res = (float)input;
	if (std::isnan(res) || std::isinf(res)) {
		return false;
	}
	result = res;
	return true;
}

struct NumericTryCast {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, bool strict = false) {
		return TryCastWithOverflowCheck(input, result);
	}
};

struct NumericCast {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		DST result;
		if (!NumericTryCast::Operation(input, result)) {
			throw InvalidInputException(CastExceptionText<SRC, DST>(input));
		}
		return result;
	}
};

}

