//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/numeric_cast.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include <cmath>

namespace duckdb {

template <class SRC, class DST>
static bool TryCastWithOverflowCheck(SRC value, DST &result) {
	if (!Value::IsFinite<SRC>(value)) {
		return false;
	}
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
			result = (DST)value;
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

template <class SRC, class T>
bool TryCastWithOverflowCheckFloat(SRC value, T &result, SRC min, SRC max) {
	if (!Value::IsFinite<SRC>(value)) {
		return false;
	}
	if (!(value >= min && value < max)) {
		return false;
	}
	// PG FLOAT => INT casts use statistical rounding.
	result = std::nearbyint(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(float value, int8_t &result) {
	return TryCastWithOverflowCheckFloat<float, int8_t>(value, result, -128.0f, 128.0f);
}

template <>
bool TryCastWithOverflowCheck(float value, int16_t &result) {
	return TryCastWithOverflowCheckFloat<float, int16_t>(value, result, -32768.0f, 32768.0f);
}

template <>
bool TryCastWithOverflowCheck(float value, int32_t &result) {
	return TryCastWithOverflowCheckFloat<float, int32_t>(value, result, -2147483648.0f, 2147483648.0f);
}

template <>
bool TryCastWithOverflowCheck(float value, int64_t &result) {
	return TryCastWithOverflowCheckFloat<float, int64_t>(value, result, -9223372036854775808.0f,
	                                                     9223372036854775808.0f);
}

template <>
bool TryCastWithOverflowCheck(float value, uint8_t &result) {
	return TryCastWithOverflowCheckFloat<float, uint8_t>(value, result, 0.0f, 256.0f);
}

template <>
bool TryCastWithOverflowCheck(float value, uint16_t &result) {
	return TryCastWithOverflowCheckFloat<float, uint16_t>(value, result, 0.0f, 65536.0f);
}

template <>
bool TryCastWithOverflowCheck(float value, uint32_t &result) {
	return TryCastWithOverflowCheckFloat<float, uint32_t>(value, result, 0.0f, 4294967296.0f);
}

template <>
bool TryCastWithOverflowCheck(float value, uint64_t &result) {
	return TryCastWithOverflowCheckFloat<float, uint64_t>(value, result, 0.0f, 18446744073709551616.0f);
}

template <>
bool TryCastWithOverflowCheck(double value, int8_t &result) {
	return TryCastWithOverflowCheckFloat<double, int8_t>(value, result, -128.0, 128.0);
}

template <>
bool TryCastWithOverflowCheck(double value, int16_t &result) {
	return TryCastWithOverflowCheckFloat<double, int16_t>(value, result, -32768.0, 32768.0);
}

template <>
bool TryCastWithOverflowCheck(double value, int32_t &result) {
	return TryCastWithOverflowCheckFloat<double, int32_t>(value, result, -2147483648.0, 2147483648.0);
}

template <>
bool TryCastWithOverflowCheck(double value, int64_t &result) {
	return TryCastWithOverflowCheckFloat<double, int64_t>(value, result, -9223372036854775808.0, 9223372036854775808.0);
}

template <>
bool TryCastWithOverflowCheck(double value, uint8_t &result) {
	return TryCastWithOverflowCheckFloat<double, uint8_t>(value, result, 0.0, 256.0);
}

template <>
bool TryCastWithOverflowCheck(double value, uint16_t &result) {
	return TryCastWithOverflowCheckFloat<double, uint16_t>(value, result, 0.0, 65536.0);
}

template <>
bool TryCastWithOverflowCheck(double value, uint32_t &result) {
	return TryCastWithOverflowCheckFloat<double, uint32_t>(value, result, 0.0, 4294967296.0);
}

template <>
bool TryCastWithOverflowCheck(double value, uint64_t &result) {
	return TryCastWithOverflowCheckFloat<double, uint64_t>(value, result, 0.0, 18446744073709551615.0);
}
template <>
bool TryCastWithOverflowCheck(float input, float &result) {
	result = input;
	return true;
}
template <>
bool TryCastWithOverflowCheck(float input, double &result) {
	result = double(input);
	return true;
}
template <>
bool TryCastWithOverflowCheck(double input, double &result) {
	result = input;
	return true;
}

template <>
bool TryCastWithOverflowCheck(double input, float &result) {
	if (!Value::IsFinite(input)) {
		result = float(input);
		return true;
	}
	auto res = float(input);
	if (!Value::FloatIsFinite(input)) {
		return false;
	}
	result = res;
	return true;
}

//===--------------------------------------------------------------------===//
// Cast Numeric -> bool
//===--------------------------------------------------------------------===//
template <>
bool TryCastWithOverflowCheck(bool value, bool &result) {
	result = bool(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(int8_t value, bool &result) {
	result = bool(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(int16_t value, bool &result) {
	result = bool(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(int32_t value, bool &result) {
	result = bool(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(int64_t value, bool &result) {
	result = bool(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(uint8_t value, bool &result) {
	result = bool(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(uint16_t value, bool &result) {
	result = bool(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(uint32_t value, bool &result) {
	result = bool(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(uint64_t value, bool &result) {
	result = bool(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(float value, bool &result) {
	result = bool(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(double value, bool &result) {
	result = bool(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(hugeint_t input, bool &result) {
	result = input.upper != 0 || input.lower != 0;
	return true;
}

//===--------------------------------------------------------------------===//
// Cast bool -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCastWithOverflowCheck(bool value, int8_t &result) {
	result = int8_t(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(bool value, int16_t &result) {
	result = int16_t(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(bool value, int32_t &result) {
	result = int32_t(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(bool value, int64_t &result) {
	result = int64_t(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(bool value, uint8_t &result) {
	result = uint8_t(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(bool value, uint16_t &result) {
	result = uint16_t(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(bool value, uint32_t &result) {
	result = uint32_t(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(bool value, uint64_t &result) {
	result = uint64_t(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(bool value, float &result) {
	result = float(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(bool value, double &result) {
	result = double(value);
	return true;
}

template <>
bool TryCastWithOverflowCheck(bool input, hugeint_t &result) {
	result.upper = 0;
	result.lower = input ? 1 : 0;
	return true;
}

//===--------------------------------------------------------------------===//
// Cast Numeric -> hugeint
//===--------------------------------------------------------------------===//
template <>
bool TryCastWithOverflowCheck(int8_t value, hugeint_t &result) {
	return Hugeint::TryConvert(value, result);
}

template <>
bool TryCastWithOverflowCheck(int16_t value, hugeint_t &result) {
	return Hugeint::TryConvert(value, result);
}

template <>
bool TryCastWithOverflowCheck(int32_t value, hugeint_t &result) {
	return Hugeint::TryConvert(value, result);
}

template <>
bool TryCastWithOverflowCheck(int64_t value, hugeint_t &result) {
	return Hugeint::TryConvert(value, result);
}

template <>
bool TryCastWithOverflowCheck(uint8_t value, hugeint_t &result) {
	return Hugeint::TryConvert(value, result);
}

template <>
bool TryCastWithOverflowCheck(uint16_t value, hugeint_t &result) {
	return Hugeint::TryConvert(value, result);
}

template <>
bool TryCastWithOverflowCheck(uint32_t value, hugeint_t &result) {
	return Hugeint::TryConvert(value, result);
}

template <>
bool TryCastWithOverflowCheck(uint64_t value, hugeint_t &result) {
	return Hugeint::TryConvert(value, result);
}

template <>
bool TryCastWithOverflowCheck(float value, hugeint_t &result) {
	return Hugeint::TryConvert(std::nearbyintf(value), result);
}

template <>
bool TryCastWithOverflowCheck(double value, hugeint_t &result) {
	return Hugeint::TryConvert(std::nearbyint(value), result);
}

template <>
bool TryCastWithOverflowCheck(hugeint_t value, hugeint_t &result) {
	result = value;
	return true;
}

//===--------------------------------------------------------------------===//
// Cast Hugeint -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCastWithOverflowCheck(hugeint_t value, int8_t &result) {
	return Hugeint::TryCast(value, result);
}

template <>
bool TryCastWithOverflowCheck(hugeint_t value, int16_t &result) {
	return Hugeint::TryCast(value, result);
}

template <>
bool TryCastWithOverflowCheck(hugeint_t value, int32_t &result) {
	return Hugeint::TryCast(value, result);
}

template <>
bool TryCastWithOverflowCheck(hugeint_t value, int64_t &result) {
	return Hugeint::TryCast(value, result);
}

template <>
bool TryCastWithOverflowCheck(hugeint_t value, uint8_t &result) {
	return Hugeint::TryCast(value, result);
}

template <>
bool TryCastWithOverflowCheck(hugeint_t value, uint16_t &result) {
	return Hugeint::TryCast(value, result);
}

template <>
bool TryCastWithOverflowCheck(hugeint_t value, uint32_t &result) {
	return Hugeint::TryCast(value, result);
}

template <>
bool TryCastWithOverflowCheck(hugeint_t value, uint64_t &result) {
	return Hugeint::TryCast(value, result);
}

template <>
bool TryCastWithOverflowCheck(hugeint_t value, float &result) {
	return Hugeint::TryCast(value, result);
}

template <>
bool TryCastWithOverflowCheck(hugeint_t value, double &result) {
	return Hugeint::TryCast(value, result);
}

struct NumericTryCastToBit {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		return StringVector::AddStringOrBlob(result, Bit::NumericToBit(input));
	}
};

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

} // namespace duckdb
