#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/string_cast.hpp"
#include "duckdb/common/operator/numeric_cast.hpp"

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

//===--------------------------------------------------------------------===//
// bool casts
//===--------------------------------------------------------------------===//
#define BOOL_NUMERIC_TRY_CAST_IMPL(TARGET_TYPE) \
template <> \
bool TryCast::Operation(bool input, TARGET_TYPE &result, bool strict) { \
	result = input ? 1 : 0; \
	return true; \
}

#define TO_BOOL_NUMERIC_TRY_CAST_IMPL(SOURCE_TYPE) \
template <> \
bool TryCast::Operation(SOURCE_TYPE input, bool &result, bool strict) { \
	result = input ? true : false; \
	return true; \
}


TO_BOOL_NUMERIC_TRY_CAST_IMPL(bool)
BOOL_NUMERIC_TRY_CAST_IMPL(int8_t)
BOOL_NUMERIC_TRY_CAST_IMPL(int16_t)
BOOL_NUMERIC_TRY_CAST_IMPL(int32_t)
BOOL_NUMERIC_TRY_CAST_IMPL(int64_t)
BOOL_NUMERIC_TRY_CAST_IMPL(uint8_t)
BOOL_NUMERIC_TRY_CAST_IMPL(uint16_t)
BOOL_NUMERIC_TRY_CAST_IMPL(uint32_t)
BOOL_NUMERIC_TRY_CAST_IMPL(uint64_t)
BOOL_NUMERIC_TRY_CAST_IMPL(float)
BOOL_NUMERIC_TRY_CAST_IMPL(double)

//===--------------------------------------------------------------------===//
// numeric casts
//===--------------------------------------------------------------------===//
#define NUMERIC_TRY_CAST_IMPL(SOURCE_TYPE, TARGET_TYPE) \
template <> \
bool TryCast::Operation(SOURCE_TYPE input, TARGET_TYPE &result, bool strict) { \
	return TryCastWithOverflowCheck(input, result); \
}

//===--------------------------------------------------------------------===//
// int8_t casts
//===--------------------------------------------------------------------===//
TO_BOOL_NUMERIC_TRY_CAST_IMPL(int8_t)
NUMERIC_TRY_CAST_IMPL(int8_t, int8_t)
NUMERIC_TRY_CAST_IMPL(int8_t, int16_t)
NUMERIC_TRY_CAST_IMPL(int8_t, int32_t)
NUMERIC_TRY_CAST_IMPL(int8_t, int64_t)
NUMERIC_TRY_CAST_IMPL(int8_t, uint8_t)
NUMERIC_TRY_CAST_IMPL(int8_t, uint16_t)
NUMERIC_TRY_CAST_IMPL(int8_t, uint32_t)
NUMERIC_TRY_CAST_IMPL(int8_t, uint64_t)
NUMERIC_TRY_CAST_IMPL(int8_t, float)
NUMERIC_TRY_CAST_IMPL(int8_t, double)

//===--------------------------------------------------------------------===//
// int16_t casts
//===--------------------------------------------------------------------===//
TO_BOOL_NUMERIC_TRY_CAST_IMPL(int16_t)
NUMERIC_TRY_CAST_IMPL(int16_t, int8_t)
NUMERIC_TRY_CAST_IMPL(int16_t, int16_t)
NUMERIC_TRY_CAST_IMPL(int16_t, int32_t)
NUMERIC_TRY_CAST_IMPL(int16_t, int64_t)
NUMERIC_TRY_CAST_IMPL(int16_t, uint8_t)
NUMERIC_TRY_CAST_IMPL(int16_t, uint16_t)
NUMERIC_TRY_CAST_IMPL(int16_t, uint32_t)
NUMERIC_TRY_CAST_IMPL(int16_t, uint64_t)
NUMERIC_TRY_CAST_IMPL(int16_t, float)
NUMERIC_TRY_CAST_IMPL(int16_t, double)

//===--------------------------------------------------------------------===//
// int32_t casts
//===--------------------------------------------------------------------===//
TO_BOOL_NUMERIC_TRY_CAST_IMPL(int32_t)
NUMERIC_TRY_CAST_IMPL(int32_t, int8_t)
NUMERIC_TRY_CAST_IMPL(int32_t, int16_t)
NUMERIC_TRY_CAST_IMPL(int32_t, int32_t)
NUMERIC_TRY_CAST_IMPL(int32_t, int64_t)
NUMERIC_TRY_CAST_IMPL(int32_t, uint8_t)
NUMERIC_TRY_CAST_IMPL(int32_t, uint16_t)
NUMERIC_TRY_CAST_IMPL(int32_t, uint32_t)
NUMERIC_TRY_CAST_IMPL(int32_t, uint64_t)
NUMERIC_TRY_CAST_IMPL(int32_t, float)
NUMERIC_TRY_CAST_IMPL(int32_t, double)

//===--------------------------------------------------------------------===//
// int64_t casts
//===--------------------------------------------------------------------===//
TO_BOOL_NUMERIC_TRY_CAST_IMPL(int64_t)
NUMERIC_TRY_CAST_IMPL(int64_t, int8_t)
NUMERIC_TRY_CAST_IMPL(int64_t, int16_t)
NUMERIC_TRY_CAST_IMPL(int64_t, int32_t)
NUMERIC_TRY_CAST_IMPL(int64_t, int64_t)
NUMERIC_TRY_CAST_IMPL(int64_t, uint8_t)
NUMERIC_TRY_CAST_IMPL(int64_t, uint16_t)
NUMERIC_TRY_CAST_IMPL(int64_t, uint32_t)
NUMERIC_TRY_CAST_IMPL(int64_t, uint64_t)
NUMERIC_TRY_CAST_IMPL(int64_t, float)
NUMERIC_TRY_CAST_IMPL(int64_t, double)

//===--------------------------------------------------------------------===//
// uint8_t casts
//===--------------------------------------------------------------------===//
TO_BOOL_NUMERIC_TRY_CAST_IMPL(uint8_t)
NUMERIC_TRY_CAST_IMPL(uint8_t, int8_t)
NUMERIC_TRY_CAST_IMPL(uint8_t, int16_t)
NUMERIC_TRY_CAST_IMPL(uint8_t, int32_t)
NUMERIC_TRY_CAST_IMPL(uint8_t, int64_t)
NUMERIC_TRY_CAST_IMPL(uint8_t, uint8_t)
NUMERIC_TRY_CAST_IMPL(uint8_t, uint16_t)
NUMERIC_TRY_CAST_IMPL(uint8_t, uint32_t)
NUMERIC_TRY_CAST_IMPL(uint8_t, uint64_t)
NUMERIC_TRY_CAST_IMPL(uint8_t, float)
NUMERIC_TRY_CAST_IMPL(uint8_t, double)

//===--------------------------------------------------------------------===//
// uint16_t casts
//===--------------------------------------------------------------------===//
TO_BOOL_NUMERIC_TRY_CAST_IMPL(uint16_t)
NUMERIC_TRY_CAST_IMPL(uint16_t, int8_t)
NUMERIC_TRY_CAST_IMPL(uint16_t, int16_t)
NUMERIC_TRY_CAST_IMPL(uint16_t, int32_t)
NUMERIC_TRY_CAST_IMPL(uint16_t, int64_t)
NUMERIC_TRY_CAST_IMPL(uint16_t, uint8_t)
NUMERIC_TRY_CAST_IMPL(uint16_t, uint16_t)
NUMERIC_TRY_CAST_IMPL(uint16_t, uint32_t)
NUMERIC_TRY_CAST_IMPL(uint16_t, uint64_t)
NUMERIC_TRY_CAST_IMPL(uint16_t, float)
NUMERIC_TRY_CAST_IMPL(uint16_t, double)

//===--------------------------------------------------------------------===//
// uint32_t casts
//===--------------------------------------------------------------------===//
TO_BOOL_NUMERIC_TRY_CAST_IMPL(uint32_t)
NUMERIC_TRY_CAST_IMPL(uint32_t, int8_t)
NUMERIC_TRY_CAST_IMPL(uint32_t, int16_t)
NUMERIC_TRY_CAST_IMPL(uint32_t, int32_t)
NUMERIC_TRY_CAST_IMPL(uint32_t, int64_t)
NUMERIC_TRY_CAST_IMPL(uint32_t, uint8_t)
NUMERIC_TRY_CAST_IMPL(uint32_t, uint16_t)
NUMERIC_TRY_CAST_IMPL(uint32_t, uint32_t)
NUMERIC_TRY_CAST_IMPL(uint32_t, uint64_t)
NUMERIC_TRY_CAST_IMPL(uint32_t, float)
NUMERIC_TRY_CAST_IMPL(uint32_t, double)

//===--------------------------------------------------------------------===//
// uint64_t casts
//===--------------------------------------------------------------------===//
TO_BOOL_NUMERIC_TRY_CAST_IMPL(uint64_t)
NUMERIC_TRY_CAST_IMPL(uint64_t, int8_t)
NUMERIC_TRY_CAST_IMPL(uint64_t, int16_t)
NUMERIC_TRY_CAST_IMPL(uint64_t, int32_t)
NUMERIC_TRY_CAST_IMPL(uint64_t, int64_t)
NUMERIC_TRY_CAST_IMPL(uint64_t, uint8_t)
NUMERIC_TRY_CAST_IMPL(uint64_t, uint16_t)
NUMERIC_TRY_CAST_IMPL(uint64_t, uint32_t)
NUMERIC_TRY_CAST_IMPL(uint64_t, uint64_t)
NUMERIC_TRY_CAST_IMPL(uint64_t, float)
NUMERIC_TRY_CAST_IMPL(uint64_t, double)

//===--------------------------------------------------------------------===//
// float casts
//===--------------------------------------------------------------------===//
TO_BOOL_NUMERIC_TRY_CAST_IMPL(float)
NUMERIC_TRY_CAST_IMPL(float, int8_t)
NUMERIC_TRY_CAST_IMPL(float, int16_t)
NUMERIC_TRY_CAST_IMPL(float, int32_t)
NUMERIC_TRY_CAST_IMPL(float, int64_t)
NUMERIC_TRY_CAST_IMPL(float, uint8_t)
NUMERIC_TRY_CAST_IMPL(float, uint16_t)
NUMERIC_TRY_CAST_IMPL(float, uint32_t)
NUMERIC_TRY_CAST_IMPL(float, uint64_t)
NUMERIC_TRY_CAST_IMPL(float, float)
NUMERIC_TRY_CAST_IMPL(float, double)

//===--------------------------------------------------------------------===//
// double casts
//===--------------------------------------------------------------------===//
TO_BOOL_NUMERIC_TRY_CAST_IMPL(double)
NUMERIC_TRY_CAST_IMPL(double, int8_t)
NUMERIC_TRY_CAST_IMPL(double, int16_t)
NUMERIC_TRY_CAST_IMPL(double, int32_t)
NUMERIC_TRY_CAST_IMPL(double, int64_t)
NUMERIC_TRY_CAST_IMPL(double, uint8_t)
NUMERIC_TRY_CAST_IMPL(double, uint16_t)
NUMERIC_TRY_CAST_IMPL(double, uint32_t)
NUMERIC_TRY_CAST_IMPL(double, uint64_t)
NUMERIC_TRY_CAST_IMPL(double, double)

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

//===--------------------------------------------------------------------===//
// Cast String -> Numeric
//===--------------------------------------------------------------------===//
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

template <class T, bool IS_SIGNED = true, bool ALLOW_EXPONENT = true, class OP = IntegerCastOperation, bool ZERO_INITIALIZE = true>
static bool TryIntegerCast(const char *buf, idx_t len, T &result, bool strict) {
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
		if (!IS_SIGNED) {
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
	return TryIntegerCast<uint8_t, false>(input.GetDataUnsafe(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, uint16_t &result, bool strict) {
	return TryIntegerCast<uint16_t, false>(input.GetDataUnsafe(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, uint32_t &result, bool strict) {
	return TryIntegerCast<uint32_t, false>(input.GetDataUnsafe(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, uint64_t &result, bool strict) {
	return TryIntegerCast<uint64_t, false>(input.GetDataUnsafe(), input.GetSize(), result, strict);
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
				if (!TryIntegerCast<int64_t, true, false>(buf + pos, len - pos, exponent, strict)) {
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

//===--------------------------------------------------------------------===//
// Cast From Date
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(date_t input, date_t &result, bool strict) {
	result = input;
	return true;
}

template <>
bool TryCast::Operation(date_t input, timestamp_t &result, bool strict) {
	result = Timestamp::FromDatetime(input, Time::FromTime(0, 0, 0));
	return true;
}

//===--------------------------------------------------------------------===//
// Cast From Time
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(dtime_t input, dtime_t &result, bool strict) {
	result = input;
	return true;
}

//===--------------------------------------------------------------------===//
// Cast From Timestamps
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(timestamp_t input, date_t &result, bool strict) {
	result = Timestamp::GetDate(input);
	return true;
}

template <>
bool TryCast::Operation(timestamp_t input, dtime_t &result, bool strict) {
	result = Timestamp::GetTime(input);
	return true;
}

template <>
bool TryCast::Operation(timestamp_t input, timestamp_t &result, bool strict) {
	result = input;
	return true;
}

//===--------------------------------------------------------------------===//
// Cast from Interval
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(interval_t input, interval_t &result, bool strict) {
	result = input;
	return true;
}

//===--------------------------------------------------------------------===//
// Non-Standard Timestamps
//===--------------------------------------------------------------------===//
template <>
duckdb::string_t CastFromTimestampNS::Operation(duckdb::timestamp_t input, Vector &result) {
	return StringCast::Operation<timestamp_t>(Timestamp::FromEpochNanoSeconds(input.value), result);
}
template <>
duckdb::string_t CastFromTimestampMS::Operation(duckdb::timestamp_t input, Vector &result) {
	return StringCast::Operation<timestamp_t>(Timestamp::FromEpochMs(input.value), result);
}
template <>
duckdb::string_t CastFromTimestampSec::Operation(duckdb::timestamp_t input, Vector &result) {
	return StringCast::Operation<timestamp_t>(Timestamp::FromEpochSeconds(input.value), result);
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
bool TryCastToTimestampNS::Operation(string_t input, timestamp_t &result, bool strict) {
	if (!TryCast::Operation<string_t, timestamp_t>(input, result, strict)) {
		return false;
	}
	result = Timestamp::GetEpochNanoSeconds(result);
	return true;
}

template <>
bool TryCastToTimestampMS::Operation(string_t input, timestamp_t &result, bool strict) {
	if (!TryCast::Operation<string_t, timestamp_t>(input, result, strict)) {
		return false;
	}
	result = Timestamp::GetEpochMs(result);
	return true;
}

template <>
bool TryCastToTimestampSec::Operation(string_t input, timestamp_t &result, bool strict) {
	if (!TryCast::Operation<string_t, timestamp_t>(input, result, strict)) {
		return false;
	}
	result = Timestamp::GetEpochSeconds(result);
	return true;
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
bool TryCastToBlob::Operation(string_t input, string_t &result, Vector &result_vector, string *error_message, bool strict) {
	idx_t result_size;
	if (!Blob::TryGetBlobSize(input, result_size, error_message)) {
		return false;
	}

	result = StringVector::EmptyString(result_vector, result_size);
	Blob::ToBlob(input, (data_ptr_t)result.GetDataWriteable());
	result.Finalize();
	return true;
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
date_t Cast::Operation(string_t input) {
	return Date::FromCString(input.GetDataUnsafe(), input.GetSize());
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
dtime_t Cast::Operation(string_t input) {
	return Time::FromCString(input.GetDataUnsafe(), input.GetSize());
}

//===--------------------------------------------------------------------===//
// Cast To Timestamp
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(string_t input, timestamp_t &result, bool strict) {
	return Timestamp::TryConvertTimestamp(input.GetDataUnsafe(), input.GetSize(), result);
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
	if (!TryIntegerCast<HugeIntCastData, true, true, HugeIntegerCastOperation>(input.GetDataUnsafe(), input.GetSize(), data,
	                                                                     strict)) {
		return false;
	}
	result = data.hugeint;
	return true;
}

//===--------------------------------------------------------------------===//
// Numeric -> Hugeint
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(bool input, hugeint_t &result, bool strict) {
	result.upper = 0;
	result.lower = input ? 1 : 0;
	return true;
}

template <>
bool TryCast::Operation(int8_t input, hugeint_t &result, bool strict) {
	result = Hugeint::Convert<int8_t>(input);
	return true;
}

template <>
bool TryCast::Operation(int16_t input, hugeint_t &result, bool strict) {
	result = Hugeint::Convert<int16_t>(input);
	return true;
}

template <>
bool TryCast::Operation(int32_t input, hugeint_t &result, bool strict) {
	result = Hugeint::Convert<int32_t>(input);
	return true;
}

template <>
bool TryCast::Operation(int64_t input, hugeint_t &result, bool strict) {
	result = Hugeint::Convert<int64_t>(input);
	return true;
}

template <>
bool TryCast::Operation(uint8_t input, hugeint_t &result, bool strict) {
	result = Hugeint::Convert<uint8_t>(input);
	return true;
}
template <>
bool TryCast::Operation(uint16_t input, hugeint_t &result, bool strict) {
	result = Hugeint::Convert<uint16_t>(input);
	return true;
}
template <>
bool TryCast::Operation(uint32_t input, hugeint_t &result, bool strict) {
	result = Hugeint::Convert<uint32_t>(input);
	return true;
}
template <>
bool TryCast::Operation(uint64_t input, hugeint_t &result, bool strict) {
	result = Hugeint::Convert<uint64_t>(input);
	return true;
}

template <>
bool TryCast::Operation(float input, hugeint_t &result, bool strict) {
	return Hugeint::TryConvert(input, result);
}

template <>
bool TryCast::Operation(double input, hugeint_t &result, bool strict) {
	return Hugeint::TryConvert(input, result);
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
			for (idx_t i = 0; i < idx_t(-exponent); i++) {
				state.result /= 10;
				if (state.result == 0) {
					break;
				}
			}
			return true;
		} else {
			// positive exponent: append 0's
			for (idx_t i = 0; i < idx_t(exponent); i++) {
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
bool TryDecimalStringCast(string_t input, T &result, string *error_message, uint8_t width, uint8_t scale) {
	DecimalCastData<T> state;
	state.result = 0;
	state.width = width;
	state.scale = scale;
	state.digit_count = 0;
	state.decimal_count = 0;
	if (!TryIntegerCast<DecimalCastData<T>, true, true, DecimalCastOperation, false>(input.GetDataUnsafe(), input.GetSize(),
	                                                                           state, false)) {
		string error = StringUtil::Format("Could not convert string \"%s\" to DECIMAL(%d,%d)", input.GetString(), (int)width,
		                          (int)scale);
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	result = state.result;
	return true;
}

template <>
bool TryCastToDecimal::Operation(string_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryDecimalStringCast<int16_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(string_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryDecimalStringCast<int32_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(string_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryDecimalStringCast<int64_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(string_t input, hugeint_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryDecimalStringCast<hugeint_t>(input, result, error_message, width, scale);
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


//===--------------------------------------------------------------------===//
// Decimal Casts
//===--------------------------------------------------------------------===//
// Decimal <-> Bool
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(bool input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCast::Operation<bool, int16_t>(input, result);
}

template <>
bool TryCastToDecimal::Operation(bool input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCast::Operation<bool, int32_t>(input, result);
}

template <>
bool TryCastToDecimal::Operation(bool input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCast::Operation<bool, int64_t>(input, result);
}

template <>
bool TryCastToDecimal::Operation(bool input, hugeint_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCast::Operation<bool, hugeint_t>(input, result);
}

template <>
bool TryCastFromDecimal::Operation(int16_t input, bool &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCast::Operation<int16_t, bool>(input, result);
}

template <>
bool TryCastFromDecimal::Operation(int32_t input, bool &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCast::Operation<int32_t, bool>(input, result);
}

template <>
bool TryCastFromDecimal::Operation(int64_t input, bool &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCast::Operation<int64_t, bool>(input, result);
}

template <>
bool TryCastFromDecimal::Operation(hugeint_t input, bool &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCast::Operation<hugeint_t, bool>(input, result);
}

//===--------------------------------------------------------------------===//
// Numeric -> Decimal Cast
//===--------------------------------------------------------------------===//
template <class SRC, class DST>
bool StandardNumericToDecimalCast(SRC input, DST &result, string *error_message, uint8_t width, uint8_t scale) {
	// check for overflow
	DST max_width = NumericHelper::POWERS_OF_TEN[width - scale];
	if (int64_t(input) >= max_width || int64_t(input) <= -max_width) {
		string error = StringUtil::Format("Could not cast value %d to DECIMAL(%d,%d)", input, width, scale);
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	result = DST(input) * NumericHelper::POWERS_OF_TEN[scale];
	return true;
}

template <class SRC>
bool NumericToHugeDecimalCast(SRC input, hugeint_t &result, string *error_message, uint8_t width, uint8_t scale) {
	// check for overflow
	hugeint_t max_width = Hugeint::POWERS_OF_TEN[width - scale];
	hugeint_t hinput = hugeint_t(input);
	if (hinput >= max_width || hinput <= -max_width) {
		string error = StringUtil::Format("Could not cast value %s to DECIMAL(%d,%d)", hinput.ToString(), width, scale);
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	result = hinput * Hugeint::POWERS_OF_TEN[scale];
	return true;
}

#define TO_DECIMAL_TRY_CAST_IMPL(SOURCE_TYPE) \
template <> \
bool TryCastToDecimal::Operation(SOURCE_TYPE input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) { \
	return StandardNumericToDecimalCast<SOURCE_TYPE, int16_t>(input, result, error_message, width, scale); \
} \
template <> \
bool TryCastToDecimal::Operation(SOURCE_TYPE input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) { \
	return StandardNumericToDecimalCast<SOURCE_TYPE, int32_t>(input, result, error_message, width, scale); \
} \
template <> \
bool TryCastToDecimal::Operation(SOURCE_TYPE input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) { \
	return StandardNumericToDecimalCast<SOURCE_TYPE, int64_t>(input, result, error_message, width, scale); \
} \
template <> \
bool TryCastToDecimal::Operation(SOURCE_TYPE input, hugeint_t &result, string *error_message, uint8_t width, uint8_t scale) { \
	return NumericToHugeDecimalCast<SOURCE_TYPE>(input, result, error_message, width, scale); \
}

TO_DECIMAL_TRY_CAST_IMPL(int8_t);
TO_DECIMAL_TRY_CAST_IMPL(int16_t);
TO_DECIMAL_TRY_CAST_IMPL(int32_t);
TO_DECIMAL_TRY_CAST_IMPL(int64_t);
TO_DECIMAL_TRY_CAST_IMPL(uint8_t);
TO_DECIMAL_TRY_CAST_IMPL(uint16_t);
TO_DECIMAL_TRY_CAST_IMPL(uint32_t);
TO_DECIMAL_TRY_CAST_IMPL(uint64_t);

//===--------------------------------------------------------------------===//
// Hugeint -> Decimal Cast
//===--------------------------------------------------------------------===//
template <class DST>
bool HugeintToDecimalCast(hugeint_t input, DST &result, string *error_message, uint8_t width, uint8_t scale) {
	// check for overflow
	hugeint_t max_width = Hugeint::POWERS_OF_TEN[width - scale];
	if (input >= max_width || input <= -max_width) {
		string error = StringUtil::Format("Could not cast value %s to DECIMAL(%d,%d)", input.ToString(), width, scale);
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	result = Hugeint::Cast<DST>(input * Hugeint::POWERS_OF_TEN[scale]);
	return true;
}

template <>
bool TryCastToDecimal::Operation(hugeint_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return HugeintToDecimalCast<int16_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(hugeint_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return HugeintToDecimalCast<int32_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(hugeint_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return HugeintToDecimalCast<int64_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(hugeint_t input, hugeint_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return HugeintToDecimalCast<hugeint_t>(input, result, error_message, width, scale);
}

//===--------------------------------------------------------------------===//
// Float/Double -> Decimal Cast
//===--------------------------------------------------------------------===//
template <class SRC, class DST>
bool DoubleToDecimalCast(SRC input, DST &result, string *error_message, uint8_t width, uint8_t scale) {
	double value = input * NumericHelper::DOUBLE_POWERS_OF_TEN[scale];
	if (value <= -NumericHelper::DOUBLE_POWERS_OF_TEN[width] || value >= NumericHelper::DOUBLE_POWERS_OF_TEN[width]) {
		string error = StringUtil::Format("Could not cast value %f to DECIMAL(%d,%d)", value, width, scale);;
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	result = Cast::Operation<SRC, DST>(value);
	return true;
}

template <>
bool TryCastToDecimal::Operation(float input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<float, int16_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(float input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<float, int32_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(float input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<float, int64_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(float input, hugeint_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<float, hugeint_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(double input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<double, int16_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(double input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<double, int32_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(double input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<double, int64_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(double input, hugeint_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<double, hugeint_t>(input, result, error_message, width, scale);
}

//===--------------------------------------------------------------------===//
// Decimal -> Numeric Cast
//===--------------------------------------------------------------------===//
template <class SRC, class DST>
bool TryCastDecimalToNumeric(SRC input, DST &result, string *error_message, uint8_t scale) {
	auto scaled_value = input / NumericHelper::POWERS_OF_TEN[scale];
	if (!TryCast::Operation<SRC, DST>(scaled_value, result)) {
		string error = StringUtil::Format("Failed to cast decimal value %d to type %s", scaled_value, GetTypeId<DST>());
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	return true;
}

template <class DST>
bool TryCastHugeDecimalToNumeric(hugeint_t input, DST &result, string *error_message, uint8_t scale) {
	auto scaled_value = input / Hugeint::POWERS_OF_TEN[scale];
	if (!TryCast::Operation<hugeint_t, DST>(scaled_value, result)) {
		string error = StringUtil::Format("Failed to cast decimal value %s to type %s", ConvertToString::Operation(scaled_value), GetTypeId<DST>());
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	return true;
}

#define FROM_DECIMAL_TRY_CAST_IMPL(TARGET_TYPE) \
template <> \
bool TryCastFromDecimal::Operation(int16_t input, TARGET_TYPE &result, string *error_message, uint8_t width, uint8_t scale) { \
	return TryCastDecimalToNumeric<int16_t, TARGET_TYPE>(input, result, error_message, scale); \
} \
template <> \
bool TryCastFromDecimal::Operation(int32_t input, TARGET_TYPE &result, string *error_message, uint8_t width, uint8_t scale) { \
	return TryCastDecimalToNumeric<int32_t, TARGET_TYPE>(input, result, error_message, scale); \
} \
template <> \
bool TryCastFromDecimal::Operation(int64_t input, TARGET_TYPE &result, string *error_message, uint8_t width, uint8_t scale) { \
	return TryCastDecimalToNumeric<int64_t, TARGET_TYPE>(input, result, error_message, scale); \
} \
template <> \
bool TryCastFromDecimal::Operation(hugeint_t input, TARGET_TYPE &result, string *error_message, uint8_t width, uint8_t scale) { \
	return TryCastHugeDecimalToNumeric<TARGET_TYPE>(input, result, error_message, scale); \
}

FROM_DECIMAL_TRY_CAST_IMPL(int8_t);
FROM_DECIMAL_TRY_CAST_IMPL(int16_t);
FROM_DECIMAL_TRY_CAST_IMPL(int32_t);
FROM_DECIMAL_TRY_CAST_IMPL(int64_t);
FROM_DECIMAL_TRY_CAST_IMPL(uint8_t);
FROM_DECIMAL_TRY_CAST_IMPL(uint16_t);
FROM_DECIMAL_TRY_CAST_IMPL(uint32_t);
FROM_DECIMAL_TRY_CAST_IMPL(uint64_t);
FROM_DECIMAL_TRY_CAST_IMPL(hugeint_t);

//===--------------------------------------------------------------------===//
// Decimal -> Float/Double Cast
//===--------------------------------------------------------------------===//
template <class SRC, class DST>
bool TryCastDecimalToFloatingPoint(SRC input, DST &result, uint8_t scale) {
	result = Cast::Operation<SRC, DST>(input) / DST(NumericHelper::DOUBLE_POWERS_OF_TEN[scale]);
	return true;
}

// DECIMAL -> FLOAT
template <>
bool TryCastFromDecimal::Operation(int16_t input, float &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int16_t, float>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(int32_t input, float &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int32_t, float>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(int64_t input, float &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int64_t, float>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(hugeint_t input, float &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToFloatingPoint<hugeint_t, float>(input, result, scale);
}

// DECIMAL -> DOUBLE
template <>
bool TryCastFromDecimal::Operation(int16_t input, double &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int16_t, double>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(int32_t input, double &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int32_t, double>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(int64_t input, double &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int64_t, double>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(hugeint_t input, double &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToFloatingPoint<hugeint_t, double>(input, result, scale);
}

} // namespace duckdb
