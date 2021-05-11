#include "duckdb/common/operator/cast_operators.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/vector.hpp"
#include "fmt/format.h"

#include <cctype>
#include <cmath>
#include <cstdlib>

namespace duckdb {

template <class SRC, class DST>
static bool TryCastWithOverflowCheck(SRC value, DST &result) {
	if (std::numeric_limits<SRC>::is_signed != std::numeric_limits<DST>::is_signed) {
		if (std::numeric_limits<SRC>::is_signed) {
			// signed to unsigned conversion
			if (std::numeric_limits<SRC>::digits > std::numeric_limits<DST>::digits) {
				if (value < 0 || value > (SRC)NumericLimits<DST>::Maximum()) {
					return false;
				}
			} else {
				if (value < 0 || (DST)value > NumericLimits<DST>::Maximum()) {
					return false;
				}
			}

			result = (DST)value;
			return true;
		} else {
			// unsigned to signed conversion
			if (std::numeric_limits<SRC>::digits > std::numeric_limits<DST>::digits) {
				if (value <= (SRC)NumericLimits<DST>::Maximum()) {
					result = (DST)value;
					return true;
				}
			} else {
				if ((DST)value <= NumericLimits<DST>::Maximum()) {
					result = (DST)value;
					return true;
				}
			}

			return false;
		}
	} else {
		if (value < NumericLimits<DST>::Minimum() || value > NumericLimits<DST>::Maximum()) {
			return false;
		}
		result = (DST)value;
		return true;
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

template <class SRC, class DST>
static DST CastWithOverflowCheck(SRC value) {
	DST result;
	if (!TryCastWithOverflowCheck<SRC, DST>(value, result)) {
		throw ValueOutOfRangeException((double)value, GetTypeId<SRC>(), GetTypeId<DST>());
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Numeric -> int8_t casts
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(uint8_t input, int8_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(uint16_t input, int8_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(uint32_t input, int8_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(uint64_t input, int8_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(int16_t input, int8_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(int32_t input, int8_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(int64_t input, int8_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(float input, int8_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(double input, int8_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}

template <>
int8_t Cast::Operation(uint8_t input) {
	return CastWithOverflowCheck<uint8_t, int8_t>(input);
}
template <>
int8_t Cast::Operation(uint16_t input) {
	return CastWithOverflowCheck<uint16_t, int8_t>(input);
}
template <>
int8_t Cast::Operation(uint32_t input) {
	return CastWithOverflowCheck<uint32_t, int8_t>(input);
}
template <>
int8_t Cast::Operation(uint64_t input) {
	return CastWithOverflowCheck<uint64_t, int8_t>(input);
}
template <>
int8_t Cast::Operation(int16_t input) {
	return CastWithOverflowCheck<int16_t, int8_t>(input);
}
template <>
int8_t Cast::Operation(int32_t input) {
	return CastWithOverflowCheck<int32_t, int8_t>(input);
}
template <>
int8_t Cast::Operation(int64_t input) {
	return CastWithOverflowCheck<int64_t, int8_t>(input);
}
template <>
int8_t Cast::Operation(float input) {
	return CastWithOverflowCheck<float, int8_t>(input);
}
template <>
int8_t Cast::Operation(double input) {
	return CastWithOverflowCheck<double, int8_t>(input);
}

//===--------------------------------------------------------------------===//
// Numeric -> uint8_t casts
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(uint16_t input, uint8_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(uint32_t input, uint8_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(uint64_t input, uint8_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(int8_t input, uint8_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(int16_t input, uint8_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(int32_t input, uint8_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(int64_t input, uint8_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(float input, uint8_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(double input, uint8_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
uint8_t Cast::Operation(uint16_t input) {
	return CastWithOverflowCheck<uint16_t, uint8_t>(input);
}
template <>
uint8_t Cast::Operation(uint32_t input) {
	return CastWithOverflowCheck<uint32_t, uint8_t>(input);
}
template <>
uint8_t Cast::Operation(uint64_t input) {
	return CastWithOverflowCheck<uint64_t, uint8_t>(input);
}
template <>
uint8_t Cast::Operation(int8_t input) {
	return CastWithOverflowCheck<int8_t, uint8_t>(input);
}
template <>
uint8_t Cast::Operation(int16_t input) {
	return CastWithOverflowCheck<int16_t, uint8_t>(input);
}
template <>
uint8_t Cast::Operation(int32_t input) {
	return CastWithOverflowCheck<int32_t, uint8_t>(input);
}
template <>
uint8_t Cast::Operation(int64_t input) {
	return CastWithOverflowCheck<int64_t, uint8_t>(input);
}
template <>
uint8_t Cast::Operation(float input) {
	return CastWithOverflowCheck<float, uint8_t>(input);
}
template <>
uint8_t Cast::Operation(double input) {
	return CastWithOverflowCheck<double, uint8_t>(input);
}

//===--------------------------------------------------------------------===//
// Numeric -> int16_t casts
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(uint16_t input, int16_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(uint32_t input, int16_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(uint64_t input, int16_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(int32_t input, int16_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(int64_t input, int16_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(float input, int16_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(double input, int16_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}

template <>
int16_t Cast::Operation(uint16_t input) {
	return CastWithOverflowCheck<uint16_t, int16_t>(input);
}
template <>
int16_t Cast::Operation(uint32_t input) {
	return CastWithOverflowCheck<uint32_t, int16_t>(input);
}
template <>
int16_t Cast::Operation(uint64_t input) {
	return CastWithOverflowCheck<uint64_t, int16_t>(input);
}

template <>
int16_t Cast::Operation(int32_t input) {
	return CastWithOverflowCheck<int32_t, int16_t>(input);
}
template <>
int16_t Cast::Operation(int64_t input) {
	return CastWithOverflowCheck<int64_t, int16_t>(input);
}
template <>
int16_t Cast::Operation(float input) {
	return CastWithOverflowCheck<float, int16_t>(input);
}
template <>
int16_t Cast::Operation(double input) {
	return CastWithOverflowCheck<double, int16_t>(input);
}

//===--------------------------------------------------------------------===//
// Numeric -> uint16_t casts
//===--------------------------------------------------------------------===//

template <>
bool TryCast::Operation(uint32_t input, uint16_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(uint64_t input, uint16_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(int8_t input, uint16_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}

template <>
bool TryCast::Operation(int16_t input, uint16_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(int32_t input, uint16_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(int64_t input, uint16_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(float input, uint16_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(double input, uint16_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}

template <>
uint16_t Cast::Operation(uint32_t input) {
	return CastWithOverflowCheck<uint32_t, uint16_t>(input);
}
template <>
uint16_t Cast::Operation(uint64_t input) {
	return CastWithOverflowCheck<uint64_t, uint16_t>(input);
}

template <>
uint16_t Cast::Operation(int8_t input) {
	return CastWithOverflowCheck<int8_t, uint16_t>(input);
}

template <>
uint16_t Cast::Operation(int16_t input) {
	return CastWithOverflowCheck<int16_t, uint16_t>(input);
}
template <>
uint16_t Cast::Operation(int32_t input) {
	return CastWithOverflowCheck<int32_t, uint16_t>(input);
}
template <>
uint16_t Cast::Operation(int64_t input) {
	return CastWithOverflowCheck<int64_t, uint16_t>(input);
}
template <>
uint16_t Cast::Operation(float input) {
	return CastWithOverflowCheck<float, uint16_t>(input);
}
template <>
uint16_t Cast::Operation(double input) {
	return CastWithOverflowCheck<double, uint16_t>(input);
}
//===--------------------------------------------------------------------===//
// Numeric -> int32_t casts
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(uint32_t input, int32_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(uint64_t input, int32_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(int64_t input, int32_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(float input, int32_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(double input, int32_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}

template <>
int32_t Cast::Operation(uint32_t input) {
	return CastWithOverflowCheck<uint32_t, int32_t>(input);
}
template <>
int32_t Cast::Operation(uint64_t input) {
	return CastWithOverflowCheck<uint64_t, int32_t>(input);
}

template <>
int32_t Cast::Operation(int64_t input) {
	return CastWithOverflowCheck<int64_t, int32_t>(input);
}
template <>
int32_t Cast::Operation(float input) {
	return CastWithOverflowCheck<float, int32_t>(input);
}
template <>
int32_t Cast::Operation(double input) {
	return CastWithOverflowCheck<double, int32_t>(input);
}

//===--------------------------------------------------------------------===//
// Numeric -> uint32_t casts
//===--------------------------------------------------------------------===//

template <>
bool TryCast::Operation(uint64_t input, uint32_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(int8_t input, uint32_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(int16_t input, uint32_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(int32_t input, uint32_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(int64_t input, uint32_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(float input, uint32_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(double input, uint32_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}

template <>
uint32_t Cast::Operation(uint64_t input) {
	return CastWithOverflowCheck<uint64_t, uint32_t>(input);
}
template <>
uint32_t Cast::Operation(int8_t input) {
	return CastWithOverflowCheck<int8_t, uint32_t>(input);
}
template <>
uint32_t Cast::Operation(int16_t input) {
	return CastWithOverflowCheck<int16_t, uint32_t>(input);
}

template <>
uint32_t Cast::Operation(int32_t input) {
	return CastWithOverflowCheck<int32_t, uint32_t>(input);
}
template <>
uint32_t Cast::Operation(int64_t input) {
	return CastWithOverflowCheck<int64_t, uint32_t>(input);
}
template <>
uint32_t Cast::Operation(float input) {
	return CastWithOverflowCheck<float, uint32_t>(input);
}
template <>
uint32_t Cast::Operation(double input) {
	return CastWithOverflowCheck<double, uint32_t>(input);
}
//===--------------------------------------------------------------------===//
// Numeric -> int64_t casts
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(uint64_t input, int64_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(float input, int64_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(double input, int64_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}

template <>
int64_t Cast::Operation(uint64_t input) {
	return CastWithOverflowCheck<uint64_t, int64_t>(input);
}
template <>
int64_t Cast::Operation(float input) {
	return CastWithOverflowCheck<float, int64_t>(input);
}
template <>
int64_t Cast::Operation(double input) {
	return CastWithOverflowCheck<double, int64_t>(input);
}

//===--------------------------------------------------------------------===//
// Numeric -> uint64_t casts
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(int8_t input, uint64_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(int16_t input, uint64_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(int32_t input, uint64_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}

template <>
bool TryCast::Operation(int64_t input, uint64_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(float input, uint64_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
bool TryCast::Operation(double input, uint64_t &result, bool strict) {
	return TryCastWithOverflowCheck(input, result);
}
template <>
uint64_t Cast::Operation(int8_t input) {
	return CastWithOverflowCheck<int8_t, uint64_t>(input);
}
template <>
uint64_t Cast::Operation(int16_t input) {
	return CastWithOverflowCheck<int16_t, uint64_t>(input);
}
template <>
uint64_t Cast::Operation(int32_t input) {
	return CastWithOverflowCheck<int32_t, uint64_t>(input);
}

template <>
uint64_t Cast::Operation(int64_t input) {
	return CastWithOverflowCheck<int64_t, uint64_t>(input);
}
template <>
uint64_t Cast::Operation(float input) {
	return CastWithOverflowCheck<float, uint64_t>(input);
}
template <>
uint64_t Cast::Operation(double input) {
	return CastWithOverflowCheck<double, uint64_t>(input);
}

//===--------------------------------------------------------------------===//
// Double -> float casts
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(double input, float &result, bool strict) {
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

template <>
float Cast::Operation(double input) {
	float result;
	bool strict = false;
	if (!TryCast::Operation(input, result, strict)) {
		throw ValueOutOfRangeException(input, GetTypeId<double>(), GetTypeId<float>());
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Cast String -> Numeric
//===--------------------------------------------------------------------===//
template <class T>
static T TryCastString(string_t input) {
	T result;
	if (!TryCast::Operation<string_t, T>(input, result)) {
		throw ConversionException("Could not convert string '%s' to %s", input.GetString(),
		                          TypeIdToString(GetTypeId<T>()));
	}
	return result;
}

template <class T>
static T TryStrictCastString(string_t input) {
	T result;
	if (!TryCast::Operation<string_t, T>(input, result, true)) {
		throw ConversionException("Could not convert string '%s' to %s", input.GetString(),
		                          TypeIdToString(GetTypeId<T>()));
	}
	return result;
}

struct IntegerCastOperation {
	template <class T, bool NEGATIVE>
	static bool HandleDigit(T &result, uint8_t digit) {
		if (NEGATIVE) {
			if (result < (NumericLimits<T>::Minimum() + digit) / 10) {
				return false;
			}
			result = result * 10 - digit;
		} else {
			if (result > (NumericLimits<T>::Maximum() - digit) / 10) {
				return false;
			}
			result = result * 10 + digit;
		}
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool HandleExponent(T &result, int64_t exponent) {
		double dbl_res = result * std::pow(10.0L, exponent);
		if (dbl_res < NumericLimits<T>::Minimum() || dbl_res > NumericLimits<T>::Maximum()) {
			return false;
		}
		result = (T)dbl_res;
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool HandleDecimal(T &result, uint8_t digit) {
		return true;
	}

	template <class T>
	static bool Finalize(T &result) {
		return true;
	}
};

template <class T, bool NEGATIVE, bool ALLOW_EXPONENT, class OP = IntegerCastOperation>
static bool IntegerCastLoop(const char *buf, idx_t len, T &result, bool strict) {
	idx_t start_pos = NEGATIVE || *buf == '+' ? 1 : 0;
	idx_t pos = start_pos;
	while (pos < len) {
		if (!StringUtil::CharacterIsDigit(buf[pos])) {
			// not a digit!
			if (buf[pos] == '.') {
				if (strict) {
					return false;
				}
				bool number_before_period = pos > start_pos;
				// decimal point: we accept decimal values for integers as well
				// we just truncate them
				// make sure everything after the period is a number
				pos++;
				idx_t start_digit = pos;
				while (pos < len) {
					if (!StringUtil::CharacterIsDigit(buf[pos])) {
						break;
					}
					if (!OP::template HandleDecimal<T, NEGATIVE>(result, buf[pos] - '0')) {
						return false;
					}
					pos++;
				}
				// make sure there is either (1) one number after the period, or (2) one number before the period
				// i.e. we accept "1." and ".1" as valid numbers, but not "."
				if (!(number_before_period || pos > start_digit)) {
					return false;
				}
				if (pos >= len) {
					break;
				}
			}
			if (StringUtil::CharacterIsSpace(buf[pos])) {
				// skip any trailing spaces
				while (++pos < len) {
					if (!StringUtil::CharacterIsSpace(buf[pos])) {
						return false;
					}
				}
				break;
			}
			if (ALLOW_EXPONENT) {
				if (buf[pos] == 'e' || buf[pos] == 'E') {
					if (pos == start_pos) {
						return false;
					}
					pos++;
					int64_t exponent = 0;
					int negative = buf[pos] == '-';
					if (negative) {
						if (!IntegerCastLoop<int64_t, true, false>(buf + pos, len - pos, exponent, strict)) {
							return false;
						}
					} else {
						if (!IntegerCastLoop<int64_t, false, false>(buf + pos, len - pos, exponent, strict)) {
							return false;
						}
					}
					return OP::template HandleExponent<T, NEGATIVE>(result, exponent);
				}
			}
			return false;
		}
		uint8_t digit = buf[pos++] - '0';
		if (!OP::template HandleDigit<T, NEGATIVE>(result, digit)) {
			return false;
		}
	}
	if (!OP::template Finalize<T>(result)) {
		return false;
	}
	return pos > start_pos;
}

template <class T, bool ALLOW_EXPONENT = true, class OP = IntegerCastOperation, bool ZERO_INITIALIZE = true>
static bool TryIntegerCast(const char *buf, idx_t len, T &result, bool strict, bool unsigned_int = false) {
	// skip any spaces at the start
	while (len > 0 && StringUtil::CharacterIsSpace(*buf)) {
		buf++;
		len--;
	}
	if (len == 0) {
		return false;
	}
	int negative = *buf == '-';

	if (ZERO_INITIALIZE) {
		memset(&result, 0, sizeof(T));
	}
	if (!negative) {
		return IntegerCastLoop<T, false, ALLOW_EXPONENT, OP>(buf, len, result, strict);
	} else {
		if (unsigned_int) {
			// Need to check if its not -0
			idx_t pos = 1;
			while (pos < len) {
				if (buf[pos++] != '0') {
					return false;
				}
			}
		}
		return IntegerCastLoop<T, true, ALLOW_EXPONENT, OP>(buf, len, result, strict);
	}
}

template <>
bool TryCast::Operation(string_t input, bool &result, bool strict) {
	auto input_data = input.GetDataUnsafe();
	auto input_size = input.GetSize();

	switch (input_size) {
	case 1: {
		char c = std::tolower(*input_data);
		if (c == 't' || (!strict && c == '1')) {
			result = true;
			return true;
		} else if (c == 'f' || (!strict && c == '0')) {
			result = false;
			return true;
		}
		return false;
	}
	case 4: {
		char t = std::tolower(input_data[0]);
		char r = std::tolower(input_data[1]);
		char u = std::tolower(input_data[2]);
		char e = std::tolower(input_data[3]);
		if (t == 't' && r == 'r' && u == 'u' && e == 'e') {
			result = true;
			return true;
		}
		return false;
	}
	case 5: {
		char f = std::tolower(input_data[0]);
		char a = std::tolower(input_data[1]);
		char l = std::tolower(input_data[2]);
		char s = std::tolower(input_data[3]);
		char e = std::tolower(input_data[4]);
		if (f == 'f' && a == 'a' && l == 'l' && s == 's' && e == 'e') {
			result = false;
			return true;
		}
		return false;
	}
	default:
		return false;
	}
}
template <>
bool TryCast::Operation(string_t input, int8_t &result, bool strict) {
	return TryIntegerCast<int8_t>(input.GetDataUnsafe(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, int16_t &result, bool strict) {
	return TryIntegerCast<int16_t>(input.GetDataUnsafe(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, int32_t &result, bool strict) {
	return TryIntegerCast<int32_t>(input.GetDataUnsafe(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, int64_t &result, bool strict) {
	return TryIntegerCast<int64_t>(input.GetDataUnsafe(), input.GetSize(), result, strict);
}

template <>
bool TryCast::Operation(string_t input, uint8_t &result, bool strict) {
	return TryIntegerCast<uint8_t>(input.GetDataUnsafe(), input.GetSize(), result, strict, true);
}
template <>
bool TryCast::Operation(string_t input, uint16_t &result, bool strict) {
	return TryIntegerCast<uint16_t>(input.GetDataUnsafe(), input.GetSize(), result, strict, true);
}
template <>
bool TryCast::Operation(string_t input, uint32_t &result, bool strict) {
	return TryIntegerCast<uint32_t>(input.GetDataUnsafe(), input.GetSize(), result, strict, true);
}
template <>
bool TryCast::Operation(string_t input, uint64_t &result, bool strict) {
	return TryIntegerCast<uint64_t>(input.GetDataUnsafe(), input.GetSize(), result, strict, true);
}

template <class T, bool NEGATIVE>
static void ComputeDoubleResult(T &result, idx_t decimal, idx_t decimal_factor) {
	if (decimal_factor > 1) {
		if (NEGATIVE) {
			result -= (T)decimal / (T)decimal_factor;
		} else {
			result += (T)decimal / (T)decimal_factor;
		}
	}
}

template <class T, bool NEGATIVE>
static bool DoubleCastLoop(const char *buf, idx_t len, T &result, bool strict) {
	idx_t start_pos = NEGATIVE || *buf == '+' ? 1 : 0;
	idx_t pos = start_pos;
	idx_t decimal = 0;
	idx_t decimal_factor = 0;
	while (pos < len) {
		if (!StringUtil::CharacterIsDigit(buf[pos])) {
			// not a digit!
			if (buf[pos] == '.') {
				// decimal point
				if (decimal_factor != 0) {
					// nested periods
					return false;
				}
				decimal_factor = 1;
				pos++;
				continue;
			} else if (StringUtil::CharacterIsSpace(buf[pos])) {
				// skip any trailing spaces
				while (++pos < len) {
					if (!StringUtil::CharacterIsSpace(buf[pos])) {
						return false;
					}
				}
				ComputeDoubleResult<T, NEGATIVE>(result, decimal, decimal_factor);
				return true;
			} else if (buf[pos] == 'e' || buf[pos] == 'E') {
				if (pos == start_pos) {
					return false;
				}
				// E power
				// parse an integer, this time not allowing another exponent
				pos++;
				int64_t exponent;
				if (!TryIntegerCast<int64_t, false>(buf + pos, len - pos, exponent, strict)) {
					return false;
				}
				ComputeDoubleResult<T, NEGATIVE>(result, decimal, decimal_factor);
				if (result > NumericLimits<T>::Maximum() / std::pow(10.0L, exponent)) {
					return false;
				}
				result = result * std::pow(10.0L, exponent);

				return true;
			} else {
				return false;
			}
		}
		T digit = buf[pos++] - '0';
		if (decimal_factor == 0) {
			result = result * 10 + (NEGATIVE ? -digit : digit);
		} else {
			if (decimal_factor >= 1000000000000000000) {
				// decimal value will overflow if we parse more, ignore any subsequent numbers
				continue;
			}
			decimal = decimal * 10 + digit;
			decimal_factor *= 10;
		}
	}
	ComputeDoubleResult<T, NEGATIVE>(result, decimal, decimal_factor);
	return pos > start_pos;
}

template <class T>
bool CheckDoubleValidity(T value);

template <>
bool CheckDoubleValidity(float value) {
	return Value::FloatIsValid(value);
}

template <>
bool CheckDoubleValidity(double value) {
	return Value::DoubleIsValid(value);
}

template <class T>
static bool TryDoubleCast(const char *buf, idx_t len, T &result, bool strict) {
	// skip any spaces at the start
	while (len > 0 && StringUtil::CharacterIsSpace(*buf)) {
		buf++;
		len--;
	}
	if (len == 0) {
		return false;
	}
	int negative = *buf == '-';

	result = 0;
	if (!negative) {
		if (!DoubleCastLoop<T, false>(buf, len, result, strict)) {
			return false;
		}
	} else {
		if (!DoubleCastLoop<T, true>(buf, len, result, strict)) {
			return false;
		}
	}
	if (!CheckDoubleValidity<T>(result)) {
		return false;
	}
	return true;
}

template <>
bool TryCast::Operation(string_t input, float &result, bool strict) {
	return TryDoubleCast<float>(input.GetDataUnsafe(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, double &result, bool strict) {
	return TryDoubleCast<double>(input.GetDataUnsafe(), input.GetSize(), result, strict);
}

template <>
bool Cast::Operation(string_t input) {
	return TryCastString<bool>(input);
}
template <>
int8_t Cast::Operation(string_t input) {
	return TryCastString<int8_t>(input);
}
template <>
int16_t Cast::Operation(string_t input) {
	return TryCastString<int16_t>(input);
}
template <>
int32_t Cast::Operation(string_t input) {
	return TryCastString<int32_t>(input);
}
template <>
int64_t Cast::Operation(string_t input) {
	return TryCastString<int64_t>(input);
}
template <>
uint8_t Cast::Operation(string_t input) {
	return TryCastString<uint8_t>(input);
}
template <>
uint16_t Cast::Operation(string_t input) {
	return TryCastString<uint16_t>(input);
}
template <>
uint32_t Cast::Operation(string_t input) {
	return TryCastString<uint32_t>(input);
}
template <>
uint64_t Cast::Operation(string_t input) {
	return TryCastString<uint64_t>(input);
}

template <>
float Cast::Operation(string_t input) {
	return TryCastString<float>(input);
}
template <>
double Cast::Operation(string_t input) {
	return TryCastString<double>(input);
}

template <>
bool StrictCast::Operation(string_t input) {
	return TryStrictCastString<bool>(input);
}
template <>
int8_t StrictCast::Operation(string_t input) {
	return TryStrictCastString<int8_t>(input);
}
template <>
int16_t StrictCast::Operation(string_t input) {
	return TryStrictCastString<int16_t>(input);
}
template <>
int32_t StrictCast::Operation(string_t input) {
	return TryStrictCastString<int32_t>(input);
}
template <>
int64_t StrictCast::Operation(string_t input) {
	return TryStrictCastString<int64_t>(input);
}
template <>
uint8_t StrictCast::Operation(string_t input) {
	return TryStrictCastString<uint8_t>(input);
}
template <>
uint16_t StrictCast::Operation(string_t input) {
	return TryStrictCastString<uint16_t>(input);
}
template <>
uint32_t StrictCast::Operation(string_t input) {
	return TryStrictCastString<uint32_t>(input);
}
template <>
uint64_t StrictCast::Operation(string_t input) {
	return TryStrictCastString<uint64_t>(input);
}
template <>
float StrictCast::Operation(string_t input) {
	return TryStrictCastString<float>(input);
}
template <>
double StrictCast::Operation(string_t input) {
	return TryStrictCastString<double>(input);
}

//===--------------------------------------------------------------------===//
// Cast Numeric -> String
//===--------------------------------------------------------------------===//
template <class T>
string CastToStandardString(T input) {
	Vector v(LogicalType::VARCHAR);
	return StringCast::Operation(input, v).GetString();
}

template <>
string Cast::Operation(bool input) {
	return CastToStandardString(input);
}
template <>
string Cast::Operation(int8_t input) {
	return CastToStandardString(input);
}
template <>
string Cast::Operation(int16_t input) {
	return CastToStandardString(input);
}
template <>
string Cast::Operation(int32_t input) {
	return CastToStandardString(input);
}
template <>
string Cast::Operation(int64_t input) {
	return CastToStandardString(input);
}
template <>
string Cast::Operation(uint8_t input) {
	return CastToStandardString(input);
}
template <>
string Cast::Operation(uint16_t input) {
	return CastToStandardString(input);
}
template <>
string Cast::Operation(uint32_t input) {
	return CastToStandardString(input);
}
template <>
string Cast::Operation(uint64_t input) {
	return CastToStandardString(input);
}
template <>
string Cast::Operation(hugeint_t input) {
	return Hugeint::ToString(input);
}
template <>
string Cast::Operation(float input) {
	return CastToStandardString(input);
}
template <>
string Cast::Operation(double input) {
	return CastToStandardString(input);
}
template <>
string Cast::Operation(string_t input) {
	return input.GetString();
}

template <>
string_t StringCast::Operation(bool input, Vector &vector) {
	if (input) {
		return StringVector::AddString(vector, "true", 4);
	} else {
		return StringVector::AddString(vector, "false", 5);
	}
}

template <>
string_t StringCast::Operation(int8_t input, Vector &vector) {
	return NumericHelper::FormatSigned<int8_t, uint8_t>(input, vector);
}

template <>
string_t StringCast::Operation(int16_t input, Vector &vector) {
	return NumericHelper::FormatSigned<int16_t, uint16_t>(input, vector);
}
template <>
string_t StringCast::Operation(int32_t input, Vector &vector) {
	return NumericHelper::FormatSigned<int32_t, uint32_t>(input, vector);
}

template <>
string_t StringCast::Operation(int64_t input, Vector &vector) {
	return NumericHelper::FormatSigned<int64_t, uint64_t>(input, vector);
}
template <>
duckdb::string_t StringCast::Operation(uint8_t input, Vector &vector) {
	return NumericHelper::FormatSigned<uint8_t, uint64_t>(input, vector);
}
template <>
duckdb::string_t StringCast::Operation(uint16_t input, Vector &vector) {
	return NumericHelper::FormatSigned<uint16_t, uint64_t>(input, vector);
}
template <>
duckdb::string_t StringCast::Operation(uint32_t input, Vector &vector) {
	return NumericHelper::FormatSigned<uint32_t, uint64_t>(input, vector);
}
template <>
duckdb::string_t StringCast::Operation(uint64_t input, Vector &vector) {
	return NumericHelper::FormatSigned<uint64_t, uint64_t>(input, vector);
}

template <>
string_t StringCast::Operation(float input, Vector &vector) {
	std::string s = duckdb_fmt::format("{}", input);
	return StringVector::AddString(vector, s);
}

template <>
string_t StringCast::Operation(double input, Vector &vector) {
	std::string s = duckdb_fmt::format("{}", input);
	return StringVector::AddString(vector, s);
}

template <>
string_t StringCast::Operation(interval_t input, Vector &vector) {
	char buffer[70];
	idx_t length = IntervalToStringCast::Format(input, buffer);
	return StringVector::AddString(vector, buffer, length);
}

template <>
duckdb::string_t StringCast::Operation(hugeint_t input, Vector &vector) {
	return HugeintToStringCast::FormatSigned(input, vector);
}

//===--------------------------------------------------------------------===//
// Cast From Date
//===--------------------------------------------------------------------===//
template <>
string_t CastFromDate::Operation(date_t input, Vector &vector) {
	int32_t date[3];
	Date::Convert(input, date[0], date[1], date[2]);

	idx_t year_length;
	bool add_bc;
	idx_t length = DateToStringCast::Length(date, year_length, add_bc);

	string_t result = StringVector::EmptyString(vector, length);
	auto data = result.GetDataWriteable();

	DateToStringCast::Format(data, date, year_length, add_bc);

	result.Finalize();
	return result;
}

//===--------------------------------------------------------------------===//
// Cast To Date
//===--------------------------------------------------------------------===//
template <>
date_t CastToDate::Operation(string_t input) {
	return Date::FromCString(input.GetDataUnsafe(), input.GetSize());
}

template <>
date_t StrictCastToDate::Operation(string_t input) {
	return Date::FromCString(input.GetDataUnsafe(), input.GetSize(), true);
}

//===--------------------------------------------------------------------===//
// Cast From Time
//===--------------------------------------------------------------------===//
template <>
string_t CastFromTime::Operation(dtime_t input, Vector &vector) {
	int32_t time[4];
	Time::Convert(input, time[0], time[1], time[2], time[3]);

	char micro_buffer[10];
	idx_t length = TimeToStringCast::Length(time, micro_buffer);

	string_t result = StringVector::EmptyString(vector, length);
	auto data = result.GetDataWriteable();

	TimeToStringCast::Format(data, length, time, micro_buffer);

	result.Finalize();
	return result;
}

//===--------------------------------------------------------------------===//
// Cast To Time
//===--------------------------------------------------------------------===//
template <>
dtime_t CastToTime::Operation(string_t input) {
	return Time::FromCString(input.GetDataUnsafe(), input.GetSize());
}

template <>
dtime_t StrictCastToTime::Operation(string_t input) {
	return Time::FromCString(input.GetDataUnsafe(), input.GetSize(), true);
}

template <>
timestamp_t CastDateToTimestamp::Operation(date_t input) {
	return Timestamp::FromDatetime(input, Time::FromTime(0, 0, 0, 0));
}

//===--------------------------------------------------------------------===//
// Cast From Timestamps
//===--------------------------------------------------------------------===//
template <>
string_t CastFromTimestamp::Operation(timestamp_t input, Vector &vector) {
	date_t date_entry;
	dtime_t time_entry;
	Timestamp::Convert(input, date_entry, time_entry);

	int32_t date[3], time[4];
	Date::Convert(date_entry, date[0], date[1], date[2]);
	Time::Convert(time_entry, time[0], time[1], time[2], time[3]);

	// format for timestamp is DATE TIME (separated by space)
	idx_t year_length;
	bool add_bc;
	char micro_buffer[6];
	idx_t date_length = DateToStringCast::Length(date, year_length, add_bc);
	idx_t time_length = TimeToStringCast::Length(time, micro_buffer);
	idx_t length = date_length + time_length + 1;

	string_t result = StringVector::EmptyString(vector, length);
	auto data = result.GetDataWriteable();

	DateToStringCast::Format(data, date, year_length, add_bc);
	data[date_length] = ' ';
	TimeToStringCast::Format(data + date_length + 1, time_length, time, micro_buffer);

	result.Finalize();
	return result;
}
template <>
duckdb::string_t CastFromTimestampNS::Operation(duckdb::timestamp_t input, Vector &result) {
	return CastFromTimestamp::Operation(Timestamp::FromEpochNanoSeconds(input.value), result);
}
template <>
duckdb::string_t CastFromTimestampMS::Operation(duckdb::timestamp_t input, Vector &result) {
	return CastFromTimestamp::Operation(Timestamp::FromEpochMs(input.value), result);
}
template <>
duckdb::string_t CastFromTimestampSec::Operation(duckdb::timestamp_t input, Vector &result) {
	return CastFromTimestamp::Operation(Timestamp::FromEpochSeconds(input.value), result);
}

template <>
date_t CastTimestampToDate::Operation(timestamp_t input) {
	return Timestamp::GetDate(input);
}

template <>
dtime_t CastTimestampToTime::Operation(timestamp_t input) {
	return Timestamp::GetTime(input);
}

template <>
timestamp_t CastTimestampUsToMs::Operation(timestamp_t input) {
	timestamp_t cast_timestamp(Timestamp::GetEpochMs(input));
	return cast_timestamp;
}
template <>
timestamp_t CastTimestampUsToNs::Operation(timestamp_t input) {
	timestamp_t cast_timestamp(Timestamp::GetEpochNanoSeconds(input));
	return cast_timestamp;
}
template <>
timestamp_t CastTimestampUsToSec::Operation(timestamp_t input) {
	timestamp_t cast_timestamp(Timestamp::GetEpochSeconds(input));
	return cast_timestamp;
}
template <>
timestamp_t CastTimestampMsToUs::Operation(timestamp_t input) {
	return Timestamp::FromEpochMs(input.value);
}
template <>
timestamp_t CastTimestampNsToUs::Operation(timestamp_t input) {
	return Timestamp::FromEpochNanoSeconds(input.value);
}
template <>
timestamp_t CastTimestampSecToUs::Operation(timestamp_t input) {
	return Timestamp::FromEpochSeconds(input.value);
}
//===--------------------------------------------------------------------===//
// Cast To Timestamp
//===--------------------------------------------------------------------===//
template <>
timestamp_t CastToTimestamp::Operation(string_t input) {
	return Timestamp::FromCString(input.GetDataUnsafe(), input.GetSize());
}

template <>
timestamp_t CastToTimestampNS::Operation(string_t input) {
	timestamp_t cast_timestamp(
	    Timestamp::GetEpochNanoSeconds(Timestamp::FromCString(input.GetDataUnsafe(), input.GetSize())));
	return cast_timestamp;
}

template <>
timestamp_t CastToTimestampMS::Operation(string_t input) {
	timestamp_t cast_timestamp(Timestamp::GetEpochMs(Timestamp::FromCString(input.GetDataUnsafe(), input.GetSize())));
	return cast_timestamp;
}

template <>
timestamp_t CastToTimestampSec::Operation(string_t input) {
	timestamp_t cast_timestamp(
	    Timestamp::GetEpochSeconds(Timestamp::FromCString(input.GetDataUnsafe(), input.GetSize())));
	return cast_timestamp;
}

//===--------------------------------------------------------------------===//
// Cast From Blob
//===--------------------------------------------------------------------===//
template <>
string_t CastFromBlob::Operation(string_t input, Vector &vector) {
	idx_t result_size = Blob::GetStringSize(input);

	string_t result = StringVector::EmptyString(vector, result_size);
	Blob::ToString(input, result.GetDataWriteable());
	result.Finalize();
	return result;
}

//===--------------------------------------------------------------------===//
// Cast To Blob
//===--------------------------------------------------------------------===//
template <>
string_t CastToBlob::Operation(string_t input, Vector &vector) {
	idx_t result_size = Blob::GetBlobSize(input);

	string_t result = StringVector::EmptyString(vector, result_size);
	Blob::ToBlob(input, (data_ptr_t)result.GetDataWriteable());
	result.Finalize();
	return result;
}

//===--------------------------------------------------------------------===//
// Cast To Date
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(string_t input, date_t &result, bool strict) {
	idx_t pos;
	return Date::TryConvertDate(input.GetDataUnsafe(), input.GetSize(), pos, result, strict);
}

template <>
date_t StrictCast::Operation(string_t input) {
	return TryStrictCastString<date_t>(input);
}

template <>
date_t Cast::Operation(string_t input) {
	return TryCastString<date_t>(input);
}

//===--------------------------------------------------------------------===//
// Cast To Time
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(string_t input, dtime_t &result, bool strict) {
	idx_t pos;
	return Time::TryConvertTime(input.GetDataUnsafe(), input.GetSize(), pos, result, strict);
}

template <>
dtime_t StrictCast::Operation(string_t input) {
	return TryStrictCastString<dtime_t>(input);
}

template <>
dtime_t Cast::Operation(string_t input) {
	return TryCastString<dtime_t>(input);
}

//===--------------------------------------------------------------------===//
// Cast To Timestamp
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(string_t input, timestamp_t &result, bool strict) {
	try {
		result = Timestamp::FromCString(input.GetDataUnsafe(), input.GetSize());
		return true;
	} catch (const ConversionException &) {
		return false;
	}
}

template <>
timestamp_t StrictCast::Operation(string_t input) {
	return TryStrictCastString<timestamp_t>(input);
}

template <>
timestamp_t Cast::Operation(string_t input) {
	return Timestamp::FromCString(input.GetDataUnsafe(), input.GetSize());
}

//===--------------------------------------------------------------------===//
// Cast From Interval
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(string_t input, interval_t &result, bool strict) {
	return Interval::FromCString(input.GetDataUnsafe(), input.GetSize(), result);
}

template <>
interval_t StrictCast::Operation(string_t input) {
	return TryStrictCastString<interval_t>(input);
}

template <>
interval_t Cast::Operation(string_t input) {
	return TryCastString<interval_t>(input);
}

//===--------------------------------------------------------------------===//
// Cast From Hugeint
//===--------------------------------------------------------------------===//
// parsing hugeint from string is done a bit differently for performance reasons
// for other integer types we keep track of a single value
// and multiply that value by 10 for every digit we read
// however, for hugeints, multiplication is very expensive (>20X as expensive as for int64)
// for that reason, we parse numbers first into an int64 value
// when that value is full, we perform a HUGEINT multiplication to flush it into the hugeint
// this takes the number of HUGEINT multiplications down from [0-38] to [0-2]
struct HugeIntCastData {
	hugeint_t hugeint;
	int64_t intermediate;
	uint8_t digits;

	bool Flush() {
		if (digits == 0 && intermediate == 0) {
			return true;
		}
		if (hugeint.lower != 0 || hugeint.upper != 0) {
			if (digits > 38) {
				return false;
			}
			if (!Hugeint::TryMultiply(hugeint, Hugeint::POWERS_OF_TEN[digits], hugeint)) {
				return false;
			}
		}
		if (!Hugeint::AddInPlace(hugeint, hugeint_t(intermediate))) {
			return false;
		}
		digits = 0;
		intermediate = 0;
		return true;
	}
};

struct HugeIntegerCastOperation {
	template <class T, bool NEGATIVE>
	static bool HandleDigit(T &result, uint8_t digit) {
		if (NEGATIVE) {
			if (result.intermediate < (NumericLimits<int64_t>::Minimum() + digit) / 10) {
				// intermediate is full: need to flush it
				if (!result.Flush()) {
					return false;
				}
			}
			result.intermediate = result.intermediate * 10 - digit;
		} else {
			if (result.intermediate > (NumericLimits<int64_t>::Maximum() - digit) / 10) {
				if (!result.Flush()) {
					return false;
				}
			}
			result.intermediate = result.intermediate * 10 + digit;
		}
		result.digits++;
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool HandleExponent(T &result, int64_t exponent) {
		result.Flush();
		if (exponent < -38 || exponent > 38) {
			// out of range for exact exponent: use double and convert
			double dbl_res = Hugeint::Cast<double>(result.hugeint) * std::pow(10.0L, exponent);
			if (dbl_res < Hugeint::Cast<double>(NumericLimits<hugeint_t>::Minimum()) ||
			    dbl_res > Hugeint::Cast<double>(NumericLimits<hugeint_t>::Maximum())) {
				return false;
			}
			result.hugeint = Hugeint::Convert(dbl_res);
			return true;
		}
		if (exponent < 0) {
			// negative exponent: divide by power of 10
			result.hugeint = Hugeint::Divide(result.hugeint, Hugeint::POWERS_OF_TEN[-exponent]);
			return true;
		} else {
			// positive exponent: multiply by power of 10
			return Hugeint::TryMultiply(result.hugeint, Hugeint::POWERS_OF_TEN[exponent], result.hugeint);
		}
	}

	template <class T, bool NEGATIVE>
	static bool HandleDecimal(T &result, uint8_t digit) {
		return true;
	}

	template <class T>
	static bool Finalize(T &result) {
		return result.Flush();
	}
};

template <>
bool TryCast::Operation(string_t input, hugeint_t &result, bool strict) {
	HugeIntCastData data;
	if (!TryIntegerCast<HugeIntCastData, true, HugeIntegerCastOperation>(input.GetDataUnsafe(), input.GetSize(), data,
	                                                                     strict)) {
		return false;
	}
	result = data.hugeint;
	return true;
}

template <>
hugeint_t Cast::Operation(string_t input) {
	return TryCastString<hugeint_t>(input);
}

template <>
hugeint_t StrictCast::Operation(string_t input) {
	return TryStrictCastString<hugeint_t>(input);
}

//===--------------------------------------------------------------------===//
// Numeric -> Hugeint
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(bool input, hugeint_t &result, bool strict) {
	result = Cast::Operation<bool, hugeint_t>(input);
	return true;
}

template <>
bool TryCast::Operation(int8_t input, hugeint_t &result, bool strict) {
	result = Cast::Operation<int8_t, hugeint_t>(input);
	return true;
}

template <>
bool TryCast::Operation(int16_t input, hugeint_t &result, bool strict) {
	result = Cast::Operation<int16_t, hugeint_t>(input);
	return true;
}

template <>
bool TryCast::Operation(int32_t input, hugeint_t &result, bool strict) {
	result = Cast::Operation<int32_t, hugeint_t>(input);
	return true;
}

template <>
bool TryCast::Operation(int64_t input, hugeint_t &result, bool strict) {
	result = Cast::Operation<int64_t, hugeint_t>(input);
	return true;
}

template <>
bool TryCast::Operation(uint8_t input, hugeint_t &result, bool strict) {
	result = Cast::Operation<uint8_t, hugeint_t>(input);
	return true;
}
template <>
bool TryCast::Operation(uint16_t input, hugeint_t &result, bool strict) {
	result = Cast::Operation<uint16_t, hugeint_t>(input);
	return true;
}
template <>
bool TryCast::Operation(uint32_t input, hugeint_t &result, bool strict) {
	result = Cast::Operation<uint32_t, hugeint_t>(input);
	return true;
}
template <>
bool TryCast::Operation(uint64_t input, hugeint_t &result, bool strict) {
	result = Cast::Operation<uint64_t, hugeint_t>(input);
	return true;
}

template <>
bool TryCast::Operation(float input, hugeint_t &result, bool strict) {
	result = Cast::Operation<float, hugeint_t>(input);
	return true;
}

template <>
bool TryCast::Operation(double input, hugeint_t &result, bool strict) {
	result = Cast::Operation<double, hugeint_t>(input);
	return true;
}

template <>
hugeint_t Cast::Operation(bool input) {
	hugeint_t result;
	result.upper = 0;
	result.lower = input ? 1 : 0;
	return result;
}

template <>
hugeint_t Cast::Operation(uint8_t input) {
	return Hugeint::Convert<uint8_t>(input);
}
template <>
hugeint_t Cast::Operation(uint16_t input) {
	return Hugeint::Convert<uint16_t>(input);
}
template <>
hugeint_t Cast::Operation(uint32_t input) {
	return Hugeint::Convert<uint32_t>(input);
}
template <>
hugeint_t Cast::Operation(uint64_t input) {
	return Hugeint::Convert<uint64_t>(input);
}

template <>
hugeint_t Cast::Operation(int8_t input) {
	return Hugeint::Convert<int8_t>(input);
}
template <>
hugeint_t Cast::Operation(int16_t input) {
	return Hugeint::Convert<int16_t>(input);
}
template <>
hugeint_t Cast::Operation(int32_t input) {
	return Hugeint::Convert<int32_t>(input);
}
template <>
hugeint_t Cast::Operation(int64_t input) {
	return Hugeint::Convert<int64_t>(input);
}
template <>
hugeint_t Cast::Operation(float input) {
	return Hugeint::Convert<float>(input);
}
template <>
hugeint_t Cast::Operation(double input) {
	return Hugeint::Convert<double>(input);
}

//===--------------------------------------------------------------------===//
// Hugeint -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(hugeint_t input, bool &result, bool strict) {
	// any positive number converts to true
	result = input.upper > 0 || (input.upper == 0 && input.lower > 0);
	return true;
}

template <>
bool TryCast::Operation(hugeint_t input, int8_t &result, bool strict) {
	return Hugeint::TryCast<int8_t>(input, result);
}

template <>
bool TryCast::Operation(hugeint_t input, int16_t &result, bool strict) {
	return Hugeint::TryCast<int16_t>(input, result);
}

template <>
bool TryCast::Operation(hugeint_t input, int32_t &result, bool strict) {
	return Hugeint::TryCast<int32_t>(input, result);
}

template <>
bool TryCast::Operation(hugeint_t input, date_t &result, bool strict) {
	int32_t days;
	auto success = Hugeint::TryCast<int32_t>(input, days);
	result = date_t(days);
	return success;
}

template <>
bool TryCast::Operation(hugeint_t input, dtime_t &result, bool strict) {
	int64_t micros;
	auto success = Hugeint::TryCast<int64_t>(input, micros);
	result = dtime_t(micros);
	return success;
}

template <>
bool TryCast::Operation(hugeint_t input, timestamp_t &result, bool strict) {
	int64_t micros;
	auto success = Hugeint::TryCast<int64_t>(input, micros);
	result = timestamp_t(micros);
	return success;
}

template <>
bool TryCast::Operation(hugeint_t input, int64_t &result, bool strict) {
	return Hugeint::TryCast<int64_t>(input, result);
}

template <>
bool TryCast::Operation(hugeint_t input, uint8_t &result, bool strict) {
	return Hugeint::TryCast<uint8_t>(input, result);
}
template <>
bool TryCast::Operation(hugeint_t input, uint16_t &result, bool strict) {
	return Hugeint::TryCast<uint16_t>(input, result);
}
template <>
bool TryCast::Operation(hugeint_t input, uint32_t &result, bool strict) {
	return Hugeint::TryCast<uint32_t>(input, result);
}
template <>
bool TryCast::Operation(hugeint_t input, uint64_t &result, bool strict) {
	return Hugeint::TryCast<uint64_t>(input, result);
}

template <>
bool TryCast::Operation(hugeint_t input, float &result, bool strict) {
	return Hugeint::TryCast<float>(input, result);
}

template <>
bool TryCast::Operation(hugeint_t input, double &result, bool strict) {
	return Hugeint::TryCast<double>(input, result);
}

template <>
bool Cast::Operation(hugeint_t input) {
	bool result;
	TryCast::Operation(input, result);
	return result;
}

template <class T>
static T HugeintCastToNumeric(hugeint_t input) {
	T result;
	if (!TryCast::Operation<hugeint_t, T>(input, result)) {
		throw ValueOutOfRangeException(input, PhysicalType::INT128, GetTypeId<T>());
	}
	return result;
}

template <>
int8_t Cast::Operation(hugeint_t input) {
	return HugeintCastToNumeric<int8_t>(input);
}

template <>
int16_t Cast::Operation(hugeint_t input) {
	return HugeintCastToNumeric<int16_t>(input);
}

template <>
int32_t Cast::Operation(hugeint_t input) {
	return HugeintCastToNumeric<int32_t>(input);
}

template <>
int64_t Cast::Operation(hugeint_t input) {
	return HugeintCastToNumeric<int64_t>(input);
}

template <>
uint8_t Cast::Operation(hugeint_t input) {
	return HugeintCastToNumeric<uint8_t>(input);
}
template <>
uint16_t Cast::Operation(hugeint_t input) {
	return HugeintCastToNumeric<uint16_t>(input);
}
template <>
uint32_t Cast::Operation(hugeint_t input) {
	return HugeintCastToNumeric<uint32_t>(input);
}
template <>
uint64_t Cast::Operation(hugeint_t input) {
	return HugeintCastToNumeric<uint64_t>(input);
}

template <>
float Cast::Operation(hugeint_t input) {
	return HugeintCastToNumeric<float>(input);
}

template <>
double Cast::Operation(hugeint_t input) {
	return HugeintCastToNumeric<double>(input);
}

template <>
date_t Cast::Operation(hugeint_t input) {
	return date_t(HugeintCastToNumeric<int32_t>(input));
}

template <>
dtime_t Cast::Operation(hugeint_t input) {
	return dtime_t(HugeintCastToNumeric<int64_t>(input));
}

template <>
timestamp_t Cast::Operation(hugeint_t input) {
	return timestamp_t(HugeintCastToNumeric<int64_t>(input));
}

template <>
bool TryCast::Operation(hugeint_t input, hugeint_t &result, bool strict) {
	result = input;
	return true;
}

template <>
hugeint_t Cast::Operation(hugeint_t input) {
	return input;
}

//===--------------------------------------------------------------------===//
// Decimal String Cast
//===--------------------------------------------------------------------===//
template <class T>
struct DecimalCastData {
	T result;
	uint8_t width;
	uint8_t scale;
	uint8_t digit_count;
	uint8_t decimal_count;
};

struct DecimalCastOperation {
	template <class T, bool NEGATIVE>
	static bool HandleDigit(T &state, uint8_t digit) {
		if (state.result == 0 && digit == 0) {
			// leading zero's don't count towards the digit count
			return true;
		}
		if (state.digit_count == state.width - state.scale) {
			// width of decimal type is exceeded!
			return false;
		}
		state.digit_count++;
		if (NEGATIVE) {
			state.result = state.result * 10 - digit;
		} else {
			state.result = state.result * 10 + digit;
		}
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool HandleExponent(T &state, int64_t exponent) {
		Finalize<T>(state);
		if (exponent < 0) {
			for(idx_t i = 0; i < idx_t(-exponent); i++) {
				state.result /= 10;
				if (state.result == 0) {
					break;
				}
			}
			return true;
		} else {
			// positive exponent: append 0's
			for(idx_t i = 0; i < idx_t(exponent); i++) {
				if (!HandleDigit<T, NEGATIVE>(state, 0)) {
					return false;
				}
			}
			return true;
		}
	}

	template <class T, bool NEGATIVE>
	static bool HandleDecimal(T &state, uint8_t digit) {
		if (state.decimal_count == state.scale) {
			// we exceeded the amount of supported decimals
			// however, we don't throw an error here
			// we just truncate the decimal
			return true;
		}
		state.decimal_count++;
		if (NEGATIVE) {
			state.result = state.result * 10 - digit;
		} else {
			state.result = state.result * 10 + digit;
		}
		return true;
	}

	template <class T>
	static bool Finalize(T &state) {
		// if we have not gotten exactly "scale" decimals, we need to multiply the result
		// e.g. if we have a string "1.0" that is cast to a DECIMAL(9,3), the value needs to be 1000
		// but we have only gotten the value "10" so far, so we multiply by 1000
		for (uint8_t i = state.decimal_count; i < state.scale; i++) {
			state.result *= 10;
		}
		return true;
	}
};

template <class T>
T DecimalStringCast(string_t input, uint8_t width, uint8_t scale) {
	DecimalCastData<T> state;
	state.result = 0;
	state.width = width;
	state.scale = scale;
	state.digit_count = 0;
	state.decimal_count = 0;
	if (!TryIntegerCast<DecimalCastData<T>, true, DecimalCastOperation, false>(input.GetDataUnsafe(), input.GetSize(),
	                                                                            state, false)) {
		throw ConversionException("Could not convert string \"%s\" to DECIMAL(%d,%d)", input.GetString(), (int)width,
		                          (int)scale);
	}
	return state.result;
}

template <>
int16_t CastToDecimal::Operation(string_t input, uint8_t width, uint8_t scale) {
	return DecimalStringCast<int16_t>(input, width, scale);
}

template <>
int32_t CastToDecimal::Operation(string_t input, uint8_t width, uint8_t scale) {
	return DecimalStringCast<int32_t>(input, width, scale);
}

template <>
int64_t CastToDecimal::Operation(string_t input, uint8_t width, uint8_t scale) {
	return DecimalStringCast<int64_t>(input, width, scale);
}

template <>
hugeint_t CastToDecimal::Operation(string_t input, uint8_t width, uint8_t scale) {
	return DecimalStringCast<hugeint_t>(input, width, scale);
}

template <>
string_t StringCastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale, Vector &result) {
	return DecimalToString::Format<int16_t, uint16_t>(input, scale, result);
}

template <>
string_t StringCastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale, Vector &result) {
	return DecimalToString::Format<int32_t, uint32_t>(input, scale, result);
}

template <>
string_t StringCastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale, Vector &result) {
	return DecimalToString::Format<int64_t, uint64_t>(input, scale, result);
}

template <>
string_t StringCastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale, Vector &result) {
	return HugeintToStringCast::FormatDecimal(input, scale, result);
}

template <>
int16_t CastToDecimal::Operation(bool input, uint8_t width, uint8_t scale) {
	return input ? NumericHelper::POWERS_OF_TEN[scale] : 0;
}

template <>
int32_t CastToDecimal::Operation(bool input, uint8_t width, uint8_t scale) {
	return input ? NumericHelper::POWERS_OF_TEN[scale] : 0;
}

template <>
int64_t CastToDecimal::Operation(bool input, uint8_t width, uint8_t scale) {
	return input ? NumericHelper::POWERS_OF_TEN[scale] : 0;
}

template <>
hugeint_t CastToDecimal::Operation(bool input, uint8_t width, uint8_t scale) {
	return input ? Hugeint::POWERS_OF_TEN[scale] : 0;
}

template <>
bool CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale) {
	return Cast::Operation<int16_t, bool>(input);
}

template <>
bool CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale) {
	return Cast::Operation<int32_t, bool>(input);
}

template <>
bool CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale) {
	return Cast::Operation<int64_t, bool>(input);
}

template <>
bool CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale) {
	return Cast::Operation<hugeint_t, bool>(input);
}

//===--------------------------------------------------------------------===//
// Numeric -> Decimal Cast
//===--------------------------------------------------------------------===//
template <class SRC, class DST>
DST StandardNumericToDecimalCast(SRC input, uint8_t width, uint8_t scale) {
	// check for overflow
	DST max_width = NumericHelper::POWERS_OF_TEN[width - scale];
	if (int64_t(input) >= max_width || int64_t(input) <= -max_width) {
		throw OutOfRangeException("Could not cast value %d to DECIMAL(%d,%d)", input, width, scale);
	}
	return DST(input) * NumericHelper::POWERS_OF_TEN[scale];
}

template <class SRC>
hugeint_t NumericToHugeDecimalCast(SRC input, uint8_t width, uint8_t scale) {
	// check for overflow
	hugeint_t max_width = Hugeint::POWERS_OF_TEN[width - scale];
	hugeint_t hinput = hugeint_t(input);
	if (hinput >= max_width || hinput <= -max_width) {
		throw OutOfRangeException("Could not cast value %s to DECIMAL(%d,%d)", hinput.ToString(), width, scale);
	}
	return hinput * Hugeint::POWERS_OF_TEN[scale];
}

// TINYINT -> DECIMAL
template <>
int16_t CastToDecimal::Operation(int8_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int8_t, int16_t>(input, width, scale);
}

template <>
int32_t CastToDecimal::Operation(int8_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int8_t, int32_t>(input, width, scale);
}

template <>
int64_t CastToDecimal::Operation(int8_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int8_t, int64_t>(input, width, scale);
}

template <>
hugeint_t CastToDecimal::Operation(int8_t input, uint8_t width, uint8_t scale) {
	return NumericToHugeDecimalCast<int8_t>(input, width, scale);
}

// UTINYINT -> DECIMAL
template <>
int16_t CastToDecimal::Operation(uint8_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint8_t, int16_t>(input, width, scale);
}
template <>
int32_t CastToDecimal::Operation(uint8_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint8_t, int32_t>(input, width, scale);
}
template <>
int64_t CastToDecimal::Operation(uint8_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint8_t, int64_t>(input, width, scale);
}
template <>
hugeint_t CastToDecimal::Operation(uint8_t input, uint8_t width, uint8_t scale) {
	return NumericToHugeDecimalCast<uint8_t>(input, width, scale);
}

// SMALLINT -> DECIMAL
template <>
int16_t CastToDecimal::Operation(int16_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int16_t, int16_t>(input, width, scale);
}

template <>
int32_t CastToDecimal::Operation(int16_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int16_t, int32_t>(input, width, scale);
}

template <>
int64_t CastToDecimal::Operation(int16_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int16_t, int64_t>(input, width, scale);
}

template <>
hugeint_t CastToDecimal::Operation(int16_t input, uint8_t width, uint8_t scale) {
	return NumericToHugeDecimalCast<int16_t>(input, width, scale);
}

// USMALLINT -> DECIMAL
template <>
int16_t CastToDecimal::Operation(uint16_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint16_t, int16_t>(input, width, scale);
}
template <>
int32_t CastToDecimal::Operation(uint16_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint16_t, int32_t>(input, width, scale);
}

template <>
int64_t CastToDecimal::Operation(uint16_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint16_t, int64_t>(input, width, scale);
}
template <>
hugeint_t CastToDecimal::Operation(uint16_t input, uint8_t width, uint8_t scale) {
	return NumericToHugeDecimalCast<uint16_t>(input, width, scale);
}

// INTEGER -> DECIMAL
template <>
int16_t CastToDecimal::Operation(int32_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int32_t, int16_t>(input, width, scale);
}

template <>
int32_t CastToDecimal::Operation(int32_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int32_t, int32_t>(input, width, scale);
}

template <>
int64_t CastToDecimal::Operation(int32_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int32_t, int64_t>(input, width, scale);
}

template <>
hugeint_t CastToDecimal::Operation(int32_t input, uint8_t width, uint8_t scale) {
	return NumericToHugeDecimalCast<int32_t>(input, width, scale);
}

// UINTEGER -> DECIMAL

template <>
int16_t CastToDecimal::Operation(uint32_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint32_t, int16_t>(input, width, scale);
}
template <>
int32_t CastToDecimal::Operation(uint32_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint32_t, int32_t>(input, width, scale);
}
template <>
int64_t CastToDecimal::Operation(uint32_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint32_t, int64_t>(input, width, scale);
}
template <>
hugeint_t CastToDecimal::Operation(uint32_t input, uint8_t width, uint8_t scale) {
	return NumericToHugeDecimalCast<uint32_t>(input, width, scale);
}

// BIGINT -> DECIMAL
template <>
int16_t CastToDecimal::Operation(int64_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int64_t, int16_t>(input, width, scale);
}

template <>
int32_t CastToDecimal::Operation(int64_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int64_t, int32_t>(input, width, scale);
}

template <>
int64_t CastToDecimal::Operation(int64_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int64_t, int64_t>(input, width, scale);
}

template <>
hugeint_t CastToDecimal::Operation(int64_t input, uint8_t width, uint8_t scale) {
	return NumericToHugeDecimalCast<int64_t>(input, width, scale);
}

// BIGINT -> DECIMAL

template <>
int16_t CastToDecimal::Operation(uint64_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint64_t, int16_t>(input, width, scale);
}

template <>
int32_t CastToDecimal::Operation(uint64_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint64_t, int32_t>(input, width, scale);
}

template <>
int64_t CastToDecimal::Operation(uint64_t input, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint64_t, int64_t>(input, width, scale);
}

template <>
hugeint_t CastToDecimal::Operation(uint64_t input, uint8_t width, uint8_t scale) {
	return NumericToHugeDecimalCast<uint64_t>(input, width, scale);
}

template <class DST>
DST HugeintToDecimalCast(hugeint_t input, uint8_t width, uint8_t scale) {
	// check for overflow
	hugeint_t max_width = Hugeint::POWERS_OF_TEN[width - scale];
	if (input >= max_width || input <= -max_width) {
		throw OutOfRangeException("Could not cast value %s to DECIMAL(%d,%d)", input.ToString(), width, scale);
	}
	return Hugeint::Cast<DST>(input * Hugeint::POWERS_OF_TEN[scale]);
}

// HUGEINT -> DECIMAL
template <>
int16_t CastToDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale) {
	return HugeintToDecimalCast<int16_t>(input, width, scale);
}

template <>
int32_t CastToDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale) {
	return HugeintToDecimalCast<int32_t>(input, width, scale);
}

template <>
int64_t CastToDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale) {
	return HugeintToDecimalCast<int64_t>(input, width, scale);
}

template <>
hugeint_t CastToDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale) {
	return HugeintToDecimalCast<hugeint_t>(input, width, scale);
}

template <class SRC, class DST>
DST DoubleToDecimalCast(SRC input, uint8_t width, uint8_t scale) {
	double value = input * NumericHelper::DOUBLE_POWERS_OF_TEN[scale];
	if (value <= -NumericHelper::DOUBLE_POWERS_OF_TEN[width] || value >= NumericHelper::DOUBLE_POWERS_OF_TEN[width]) {
		throw OutOfRangeException("Could not cast value %f to DECIMAL(%d,%d)", value, width, scale);
	}
	return Cast::Operation<SRC, DST>(value);
}

// FLOAT -> DECIMAL
template <>
int16_t CastToDecimal::Operation(float input, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<float, int16_t>(input, width, scale);
}

template <>
int32_t CastToDecimal::Operation(float input, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<float, int32_t>(input, width, scale);
}

template <>
int64_t CastToDecimal::Operation(float input, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<float, int64_t>(input, width, scale);
}

template <>
hugeint_t CastToDecimal::Operation(float input, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<float, hugeint_t>(input, width, scale);
}

// DOUBLE -> DECIMAL
template <>
int16_t CastToDecimal::Operation(double input, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<double, int16_t>(input, width, scale);
}

template <>
int32_t CastToDecimal::Operation(double input, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<double, int32_t>(input, width, scale);
}

template <>
int64_t CastToDecimal::Operation(double input, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<double, int64_t>(input, width, scale);
}

template <>
hugeint_t CastToDecimal::Operation(double input, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<double, hugeint_t>(input, width, scale);
}

//===--------------------------------------------------------------------===//
// Decimal -> Numeric Cast
//===--------------------------------------------------------------------===//
template <class SRC, class DST>
DST CastDecimalToNumeric(SRC input, uint8_t scale) {
	return Cast::Operation<SRC, DST>(input / NumericHelper::POWERS_OF_TEN[scale]);
}

template <class DST>
DST CastHugeDecimalToNumeric(hugeint_t input, uint8_t scale) {
	return Cast::Operation<hugeint_t, DST>(input / Hugeint::POWERS_OF_TEN[scale]);
}

// DECIMAL -> TINYINT
template <>
int8_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int16_t, int8_t>(input, scale);
}

template <>
int8_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int32_t, int8_t>(input, scale);
}

template <>
int8_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int64_t, int8_t>(input, scale);
}

template <>
int8_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale) {
	return CastHugeDecimalToNumeric<int8_t>(input, scale);
}

// DECIMAL -> UTINYINT
template <>
uint8_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int16_t, uint8_t>(input, scale);
}
template <>
uint8_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int32_t, uint8_t>(input, scale);
}
template <>
uint8_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int64_t, uint8_t>(input, scale);
}
template <>
uint8_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale) {
	return CastHugeDecimalToNumeric<uint8_t>(input, scale);
}

// DECIMAL -> SMALLINT
template <>
int16_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int16_t, int16_t>(input, scale);
}

template <>
int16_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int32_t, int16_t>(input, scale);
}

template <>
int16_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int64_t, int16_t>(input, scale);
}

template <>
int16_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale) {
	return CastHugeDecimalToNumeric<int16_t>(input, scale);
}

// DECIMAL -> USMALLINT
template <>
uint16_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int16_t, uint16_t>(input, scale);
}

template <>
uint16_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int32_t, uint16_t>(input, scale);
}

template <>
uint16_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int64_t, uint16_t>(input, scale);
}

template <>
uint16_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale) {
	return CastHugeDecimalToNumeric<uint16_t>(input, scale);
}

// DECIMAL -> INTEGER
template <>
int32_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int16_t, int32_t>(input, scale);
}

template <>
int32_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int32_t, int32_t>(input, scale);
}

template <>
int32_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int64_t, int32_t>(input, scale);
}

template <>
int32_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale) {
	return CastHugeDecimalToNumeric<int32_t>(input, scale);
}

// DECIMAL -> UINTEGER
template <>
uint32_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int16_t, uint32_t>(input, scale);
}
template <>
uint32_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int32_t, uint32_t>(input, scale);
}
template <>
uint32_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int64_t, uint32_t>(input, scale);
}
template <>
uint32_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale) {
	return CastHugeDecimalToNumeric<uint32_t>(input, scale);
}

// DECIMAL -> BIGINT
template <>
int64_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int16_t, int64_t>(input, scale);
}

template <>
int64_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int32_t, int64_t>(input, scale);
}

template <>
int64_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int64_t, int64_t>(input, scale);
}

template <>
int64_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale) {
	return CastHugeDecimalToNumeric<int64_t>(input, scale);
}

// DECIMAL -> UBIGINT
template <>
uint64_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int16_t, uint64_t>(input, scale);
}
template <>
uint64_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int32_t, uint64_t>(input, scale);
}
template <>
uint64_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int64_t, uint64_t>(input, scale);
}
template <>
uint64_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale) {
	return CastHugeDecimalToNumeric<uint64_t>(input, scale);
}

// DECIMAL -> HUGEINT
template <>
hugeint_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int16_t, hugeint_t>(input, scale);
}

template <>
hugeint_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int32_t, hugeint_t>(input, scale);
}

template <>
hugeint_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToNumeric<int64_t, hugeint_t>(input, scale);
}

template <>
hugeint_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale) {
	return CastHugeDecimalToNumeric<hugeint_t>(input, scale);
}

template <class SRC, class DST>
DST CastDecimalToFloatingPoint(SRC input, uint8_t scale) {
	return Cast::Operation<SRC, DST>(input) / DST(NumericHelper::DOUBLE_POWERS_OF_TEN[scale]);
}

// DECIMAL -> FLOAT
template <>
float CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToFloatingPoint<int16_t, float>(input, scale);
}

template <>
float CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToFloatingPoint<int32_t, float>(input, scale);
}

template <>
float CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToFloatingPoint<int64_t, float>(input, scale);
}

template <>
float CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToFloatingPoint<hugeint_t, float>(input, scale);
}

// DECIMAL -> DOUBLE
template <>
double CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToFloatingPoint<int16_t, double>(input, scale);
}

template <>
double CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToFloatingPoint<int32_t, double>(input, scale);
}

template <>
double CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToFloatingPoint<int64_t, double>(input, scale);
}

template <>
double CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale) {
	return CastDecimalToFloatingPoint<hugeint_t, double>(input, scale);
}

} // namespace duckdb
