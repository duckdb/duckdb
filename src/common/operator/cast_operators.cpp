#include "common/operator/cast_operators.hpp"

#include "common/exception.hpp"
#include "common/limits.hpp"
#include "common/types/date.hpp"
#include "common/types/timestamp.hpp"

#include <cstdlib>
#include <cctype>
#include <cmath>

using namespace duckdb;
using namespace std;

namespace duckdb {

template <class SRC, class DST> static bool try_cast_with_overflow_check(SRC value, DST &result) {
	if (value < MinimumValue<DST>() || value > MaximumValue<DST>()) {
		return false;
	}
	result = (DST)value;
	return true;
}

template <class SRC, class DST> static DST cast_with_overflow_check(SRC value) {
	DST result;
	if (!try_cast_with_overflow_check<SRC, DST>(value, result)) {
		throw ValueOutOfRangeException((int64_t)value, GetTypeId<SRC>(), GetTypeId<DST>());
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Numeric -> int8_t casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(int16_t left, int8_t &result) {
	return try_cast_with_overflow_check(left, result);
}
template <> bool TryCast::Operation(int32_t left, int8_t &result) {
	return try_cast_with_overflow_check(left, result);
}
template <> bool TryCast::Operation(int64_t left, int8_t &result) {
	return try_cast_with_overflow_check(left, result);
}
template <> bool TryCast::Operation(float left, int8_t &result) {
	return try_cast_with_overflow_check(left, result);
}
template <> bool TryCast::Operation(double left, int8_t &result) {
	return try_cast_with_overflow_check(left, result);
}

template <> int8_t Cast::Operation(int16_t left) {
	return cast_with_overflow_check<int16_t, int8_t>(left);
}
template <> int8_t Cast::Operation(int32_t left) {
	return cast_with_overflow_check<int32_t, int8_t>(left);
}
template <> int8_t Cast::Operation(int64_t left) {
	return cast_with_overflow_check<int64_t, int8_t>(left);
}
template <> int8_t Cast::Operation(float left) {
	return cast_with_overflow_check<float, int8_t>(left);
}
template <> int8_t Cast::Operation(double left) {
	return cast_with_overflow_check<double, int8_t>(left);
}
//===--------------------------------------------------------------------===//
// Numeric -> int16_t casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(int32_t left, int16_t &result) {
	return try_cast_with_overflow_check(left, result);
}
template <> bool TryCast::Operation(int64_t left, int16_t &result) {
	return try_cast_with_overflow_check(left, result);
}
template <> bool TryCast::Operation(float left, int16_t &result) {
	return try_cast_with_overflow_check(left, result);
}
template <> bool TryCast::Operation(double left, int16_t &result) {
	return try_cast_with_overflow_check(left, result);
}

template <> int16_t Cast::Operation(int32_t left) {
	return cast_with_overflow_check<int32_t, int16_t>(left);
}
template <> int16_t Cast::Operation(int64_t left) {
	return cast_with_overflow_check<int64_t, int16_t>(left);
}
template <> int16_t Cast::Operation(float left) {
	return cast_with_overflow_check<float, int16_t>(left);
}
template <> int16_t Cast::Operation(double left) {
	return cast_with_overflow_check<double, int16_t>(left);
}
//===--------------------------------------------------------------------===//
// Numeric -> int32_t casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(int64_t left, int32_t &result) {
	return try_cast_with_overflow_check(left, result);
}
template <> bool TryCast::Operation(float left, int32_t &result) {
	return try_cast_with_overflow_check(left, result);
}
template <> bool TryCast::Operation(double left, int32_t &result) {
	return try_cast_with_overflow_check(left, result);
}

template <> int32_t Cast::Operation(int64_t left) {
	return cast_with_overflow_check<int64_t, int32_t>(left);
}
template <> int32_t Cast::Operation(float left) {
	return cast_with_overflow_check<float, int32_t>(left);
}
template <> int32_t Cast::Operation(double left) {
	return cast_with_overflow_check<double, int32_t>(left);
}
//===--------------------------------------------------------------------===//
// Numeric -> int64_t casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(float left, int64_t &result) {
	return try_cast_with_overflow_check(left, result);
}
template <> bool TryCast::Operation(double left, int64_t &result) {
	return try_cast_with_overflow_check(left, result);
}

template <> int64_t Cast::Operation(float left) {
	return cast_with_overflow_check<float, int64_t>(left);
}
template <> int64_t Cast::Operation(double left) {
	return cast_with_overflow_check<double, int64_t>(left);
}

//===--------------------------------------------------------------------===//
// Cast String -> Numeric
//===--------------------------------------------------------------------===//
template <class T> static T try_cast_string(const char *left) {
	T result;
	if (!TryCast::Operation<const char *, T>(left, result)) {
		throw ConversionException("Could not convert string '%s' to numeric", left);
	}
	return result;
}

template <class T, bool NEGATIVE, bool ALLOW_EXPONENT> static bool IntegerCastLoop(const char *buf, T &result) {
	index_t pos = NEGATIVE ? 1 : 0;
	while (buf[pos]) {
		if (!std::isdigit(buf[pos])) {
			// not a digit!
			if (buf[pos] == '.') {
				// decimal point: we accept decimal values for integers as well
				// we just truncate them
				// make sure everything after the period is a number
				pos++;
				while (buf[pos]) {
					if (!std::isdigit(buf[pos++])) {
						return false;
					}
				}
				return true;
			}
			if (std::isspace(buf[pos])) {
				// skip any trailing spaces
				while (buf[++pos]) {
					if (!std::isspace(buf[pos])) {
						return false;
					}
				}
				return true;
			}
			if (ALLOW_EXPONENT) {
				if (buf[pos] == 'e' || buf[pos] == 'E') {
					pos++;
					int64_t exponent = 0;
					int negative = buf[pos] == '-';
					if (negative) {
						if (!IntegerCastLoop<int64_t, true, false>(buf + pos, exponent)) {
							return false;
						}
					} else {
						if (!IntegerCastLoop<int64_t, false, false>(buf + pos, exponent)) {
							return false;
						}
					}
					double dbl_res = result * pow(10, exponent);
					if (dbl_res < MinimumValue<T>() || dbl_res > MaximumValue<T>()) {
						return false;
					}
					result = (T)dbl_res;
					return true;
				}
			}
			return false;
		}
		T digit = buf[pos++] - '0';
		if (NEGATIVE) {
			if (result < (MinimumValue<T>() + digit) / 10) {
				return false;
			}
			result = result * 10 - digit;
		} else {
			if (result > (MaximumValue<T>() - digit) / 10) {
				return false;
			}
			result = result * 10 + digit;
		}
	}
	return pos > (NEGATIVE ? 1 : 0);
}

template <class T, bool ALLOW_EXPONENT = true> static bool TryIntegerCast(const char *buf, T &result) {
	if (!*buf) {
		return false;
	}
	// skip any spaces at the start
	while (std::isspace(*buf)) {
		buf++;
	}
	int negative = *buf == '-';

	result = 0;
	if (!negative) {
		return IntegerCastLoop<T, false, ALLOW_EXPONENT>(buf, result);
	} else {
		return IntegerCastLoop<T, true, ALLOW_EXPONENT>(buf, result);
	}
}

template <> bool TryCast::Operation(const char *left, bool &result) {
	if (left[0] == 't' || left[0] == 'T') {
		result = true;
	} else if (left[0] == 'f' || left[0] == 'F') {
		result = false;
	} else {
		return false;
	}
	return true;
}
template <> bool TryCast::Operation(const char *left, int8_t &result) {
	return TryIntegerCast<int8_t>(left, result);
}
template <> bool TryCast::Operation(const char *left, int16_t &result) {
	return TryIntegerCast<int16_t>(left, result);
}
template <> bool TryCast::Operation(const char *left, int32_t &result) {
	return TryIntegerCast<int32_t>(left, result);
}
template <> bool TryCast::Operation(const char *left, int64_t &result) {
	return TryIntegerCast<int64_t>(left, result);
}

template <class T, bool NEGATIVE> static void ComputeDoubleResult(T &result, index_t decimal, index_t decimal_factor) {
	if (decimal_factor > 1) {
		if (NEGATIVE) {
			result -= (T)decimal / (T)decimal_factor;
		} else {
			result += (T)decimal / (T)decimal_factor;
		}
	}
}

template <class T, bool NEGATIVE> static bool DoubleCastLoop(const char *buf, T &result) {
	index_t pos = NEGATIVE ? 1 : 0;
	index_t decimal = 0;
	index_t decimal_factor = 0;
	while (buf[pos]) {
		if (!std::isdigit(buf[pos])) {
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
			} else if (std::isspace(buf[pos])) {
				// skip any trailing spaces
				while (buf[++pos]) {
					if (!std::isspace(buf[pos])) {
						return false;
					}
				}
				ComputeDoubleResult<T, NEGATIVE>(result, decimal, decimal_factor);
				return true;
			} else if (buf[pos] == 'e' || buf[pos] == 'E') {
				// E power
				// parse an integer, this time not allowing another exponent
				pos++;
				int64_t exponent;
				if (!TryIntegerCast<int64_t, false>(buf + pos, exponent)) {
					return false;
				}
				ComputeDoubleResult<T, NEGATIVE>(result, decimal, decimal_factor);
				result = result * pow(10, exponent);
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
	return pos > (NEGATIVE ? 1 : 0);
}

template <class T> static bool TryDoubleCast(const char *buf, T &result) {
	if (!*buf) {
		return false;
	}
	// skip any spaces at the start
	while (std::isspace(*buf)) {
		buf++;
	}
	int negative = *buf == '-';

	result = 0;
	if (!negative) {
		return DoubleCastLoop<T, false>(buf, result);
	} else {
		return DoubleCastLoop<T, true>(buf, result);
	}
}

template <> bool TryCast::Operation(const char *left, float &result) {
	return TryDoubleCast<float>(left, result);
}
template <> bool TryCast::Operation(const char *left, double &result) {
	return TryDoubleCast<double>(left, result);
}

template <> bool Cast::Operation(const char *left) {
	return try_cast_string<bool>(left);
}
template <> int8_t Cast::Operation(const char *left) {
	return try_cast_string<int8_t>(left);
}
template <> int16_t Cast::Operation(const char *left) {
	return try_cast_string<int16_t>(left);
}
template <> int32_t Cast::Operation(const char *left) {
	return try_cast_string<int32_t>(left);
}
template <> int64_t Cast::Operation(const char *left) {
	return try_cast_string<int64_t>(left);
}
template <> float Cast::Operation(const char *left) {
	return try_cast_string<float>(left);
}
template <> double Cast::Operation(const char *left) {
	return try_cast_string<double>(left);
}

//===--------------------------------------------------------------------===//
// Cast Numeric -> String
//===--------------------------------------------------------------------===//
template <> string Cast::Operation(bool left) {
	if (left) {
		return "true";
	} else {
		return "false";
	}
}

template <> string Cast::Operation(int8_t left) {
	return to_string(left);
}

template <> string Cast::Operation(int16_t left) {
	return to_string(left);
}

template <> string Cast::Operation(int left) {
	return to_string(left);
}

template <> string Cast::Operation(int64_t left) {
	return to_string(left);
}

template <> string Cast::Operation(uint64_t left) {
	return to_string(left);
}

template <> string Cast::Operation(float left) {
	return to_string(left);
}

template <> string Cast::Operation(double left) {
	return to_string(left);
}

//===--------------------------------------------------------------------===//
// Cast From Date
//===--------------------------------------------------------------------===//
template <> string CastFromDate::Operation(date_t left) {
	return Date::ToString(left);
}

template <> int32_t CastFromDate::Operation(date_t left) {
	return (int32_t)left;
}

template <> int64_t CastFromDate::Operation(date_t left) {
	return (int64_t)left;
}

//===--------------------------------------------------------------------===//
// Cast To Date
//===--------------------------------------------------------------------===//
template <> date_t CastToDate::Operation(const char *left) {
	return Date::FromCString(left);
}

template <> date_t CastToDate::Operation(int32_t left) {
	return (date_t)left;
}

template <> date_t CastToDate::Operation(int64_t left) {
	return (date_t)left;
}

//===--------------------------------------------------------------------===//
// Cast From Timestamps
//===--------------------------------------------------------------------===//

template <> string CastFromTimestamp::Operation(timestamp_t left) {
	return Timestamp::ToString(left);
}

template <> int64_t CastFromTimestamp::Operation(timestamp_t left) {
	return (int64_t)left;
}

//===--------------------------------------------------------------------===//
// Cast To Timestamp
//===--------------------------------------------------------------------===//
template <> timestamp_t CastToTimestamp::Operation(const char *left) {
	return Timestamp::FromString(left);
}

template <> timestamp_t CastToTimestamp::Operation(int64_t left) {
	return (timestamp_t)left;
}

} // namespace duckdb
