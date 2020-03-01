#include "duckdb/common/operator/cast_operators.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/vector.hpp"

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
template <> bool TryCast::Operation(int16_t input, int8_t &result) {
	return try_cast_with_overflow_check(input, result);
}
template <> bool TryCast::Operation(int32_t input, int8_t &result) {
	return try_cast_with_overflow_check(input, result);
}
template <> bool TryCast::Operation(int64_t input, int8_t &result) {
	return try_cast_with_overflow_check(input, result);
}
template <> bool TryCast::Operation(float input, int8_t &result) {
	return try_cast_with_overflow_check(input, result);
}
template <> bool TryCast::Operation(double input, int8_t &result) {
	return try_cast_with_overflow_check(input, result);
}

template <> int8_t Cast::Operation(int16_t input) {
	return cast_with_overflow_check<int16_t, int8_t>(input);
}
template <> int8_t Cast::Operation(int32_t input) {
	return cast_with_overflow_check<int32_t, int8_t>(input);
}
template <> int8_t Cast::Operation(int64_t input) {
	return cast_with_overflow_check<int64_t, int8_t>(input);
}
template <> int8_t Cast::Operation(float input) {
	return cast_with_overflow_check<float, int8_t>(input);
}
template <> int8_t Cast::Operation(double input) {
	return cast_with_overflow_check<double, int8_t>(input);
}
//===--------------------------------------------------------------------===//
// Numeric -> int16_t casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(int32_t input, int16_t &result) {
	return try_cast_with_overflow_check(input, result);
}
template <> bool TryCast::Operation(int64_t input, int16_t &result) {
	return try_cast_with_overflow_check(input, result);
}
template <> bool TryCast::Operation(float input, int16_t &result) {
	return try_cast_with_overflow_check(input, result);
}
template <> bool TryCast::Operation(double input, int16_t &result) {
	return try_cast_with_overflow_check(input, result);
}

template <> int16_t Cast::Operation(int32_t input) {
	return cast_with_overflow_check<int32_t, int16_t>(input);
}
template <> int16_t Cast::Operation(int64_t input) {
	return cast_with_overflow_check<int64_t, int16_t>(input);
}
template <> int16_t Cast::Operation(float input) {
	return cast_with_overflow_check<float, int16_t>(input);
}
template <> int16_t Cast::Operation(double input) {
	return cast_with_overflow_check<double, int16_t>(input);
}
//===--------------------------------------------------------------------===//
// Numeric -> int32_t casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(int64_t input, int32_t &result) {
	return try_cast_with_overflow_check(input, result);
}
template <> bool TryCast::Operation(float input, int32_t &result) {
	return try_cast_with_overflow_check(input, result);
}
template <> bool TryCast::Operation(double input, int32_t &result) {
	return try_cast_with_overflow_check(input, result);
}

template <> int32_t Cast::Operation(int64_t input) {
	return cast_with_overflow_check<int64_t, int32_t>(input);
}
template <> int32_t Cast::Operation(float input) {
	return cast_with_overflow_check<float, int32_t>(input);
}
template <> int32_t Cast::Operation(double input) {
	return cast_with_overflow_check<double, int32_t>(input);
}
//===--------------------------------------------------------------------===//
// Numeric -> int64_t casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(float input, int64_t &result) {
	return try_cast_with_overflow_check(input, result);
}
template <> bool TryCast::Operation(double input, int64_t &result) {
	return try_cast_with_overflow_check(input, result);
}

template <> int64_t Cast::Operation(float input) {
	return cast_with_overflow_check<float, int64_t>(input);
}
template <> int64_t Cast::Operation(double input) {
	return cast_with_overflow_check<double, int64_t>(input);
}

//===--------------------------------------------------------------------===//
// Cast String -> Numeric
//===--------------------------------------------------------------------===//
template <class T> static T try_cast_string(string_t input) {
	T result;
	if (!TryCast::Operation<string_t, T>(input, result)) {
		throw ConversionException("Could not convert string '%s' to numeric", input.GetData());
	}
	return result;
}

template <class T, bool NEGATIVE, bool ALLOW_EXPONENT> static bool IntegerCastLoop(const char *buf, T &result) {
	idx_t pos = NEGATIVE ? 1 : 0;
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

template <> bool TryCast::Operation(string_t input, bool &result) {
	auto input_data = input.GetData();
	if (input_data[0] == 't' || input_data[0] == 'T') {
		result = true;
	} else if (input_data[0] == 'f' || input_data[0] == 'F') {
		result = false;
	} else {
		return false;
	}
	return true;
}
template <> bool TryCast::Operation(string_t input, int8_t &result) {
	return TryIntegerCast<int8_t>(input.GetData(), result);
}
template <> bool TryCast::Operation(string_t input, int16_t &result) {
	return TryIntegerCast<int16_t>(input.GetData(), result);
}
template <> bool TryCast::Operation(string_t input, int32_t &result) {
	return TryIntegerCast<int32_t>(input.GetData(), result);
}
template <> bool TryCast::Operation(string_t input, int64_t &result) {
	return TryIntegerCast<int64_t>(input.GetData(), result);
}

template <class T, bool NEGATIVE> static void ComputeDoubleResult(T &result, idx_t decimal, idx_t decimal_factor) {
	if (decimal_factor > 1) {
		if (NEGATIVE) {
			result -= (T)decimal / (T)decimal_factor;
		} else {
			result += (T)decimal / (T)decimal_factor;
		}
	}
}

template <class T, bool NEGATIVE> static bool DoubleCastLoop(const char *buf, T &result) {
	idx_t pos = NEGATIVE ? 1 : 0;
	idx_t decimal = 0;
	idx_t decimal_factor = 0;
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

template <> bool TryCast::Operation(string_t input, float &result) {
	return TryDoubleCast<float>(input.GetData(), result);
}
template <> bool TryCast::Operation(string_t input, double &result) {
	return TryDoubleCast<double>(input.GetData(), result);
}

template <> bool Cast::Operation(string_t input) {
	return try_cast_string<bool>(input);
}
template <> int8_t Cast::Operation(string_t input) {
	return try_cast_string<int8_t>(input);
}
template <> int16_t Cast::Operation(string_t input) {
	return try_cast_string<int16_t>(input);
}
template <> int32_t Cast::Operation(string_t input) {
	return try_cast_string<int32_t>(input);
}
template <> int64_t Cast::Operation(string_t input) {
	return try_cast_string<int64_t>(input);
}
template <> float Cast::Operation(string_t input) {
	return try_cast_string<float>(input);
}
template <> double Cast::Operation(string_t input) {
	return try_cast_string<double>(input);
}

//===--------------------------------------------------------------------===//
// Cast Numeric -> String
//===--------------------------------------------------------------------===//
template <> string_t StringCast::Operation(bool input, Vector &vector) {
	if (input) {
		return vector.AddString("true", 4);
	} else {
		return vector.AddString("false", 5);
	}
}

struct StringToIntegerCast {
	static constexpr const char *string_digits = "0001020304050607080910111213141516171819"
	"2021222324252627282930313233343536373839"
	"4041424344454647484950515253545556575859"
	"6061626364656667686970717273747576777879"
	"8081828384858687888990919293949596979899";


	template<class T>
	static int UnsignedLength(T value);

	// Formats value in reverse and returns a pointer to the beginning.
	template<class T>
	static char* FormatUnsigned(T value, char *ptr) {
		while (value >= 100) {
			// Integer division is slow so do it for a group of two digits instead
			// of for every digit. The idea comes from the talk by Alexandrescu
			// "Three Optimization Tips for C++". See speed-test for a comparison.
			auto index = static_cast<unsigned>((value % 100) * 2);
			value /= 100;
			*--ptr = string_digits[index + 1];
			*--ptr = string_digits[index];
		}
		if (value < 10) {
			*--ptr = static_cast<char>('0' + value);
			return ptr;
		}
		auto index = static_cast<unsigned>(value * 2);
		*--ptr = string_digits[index + 1];
		*--ptr = string_digits[index];
		return ptr;
	}

	template<class SIGNED, class UNSIGNED>
	static string_t FormatSigned(SIGNED value, Vector &vector) {
		int sign = -(value < 0);
		UNSIGNED unsigned_value = (value ^ sign) - sign;
		int length = UnsignedLength<UNSIGNED>(unsigned_value) - sign;
		string_t result = vector.EmptyString(length);
		auto dataptr = result.GetData();
		auto endptr = dataptr + length;
		endptr = FormatUnsigned(unsigned_value, endptr);
		if (sign) {
			*--endptr = '-';
		}
		result.Finalize();
		return result;
	}
};

template <> int StringToIntegerCast::UnsignedLength(uint8_t value) {
	int length = 1;
	length += value >= 10;
	length += value >= 100;
	return length;
}

template <> int StringToIntegerCast::UnsignedLength(uint16_t value) {
	int length = 1;
	length += value >= 10;
	length += value >= 100;
	length += value >= 1000;
	length += value >= 10000;
	return length;
}

template <> int StringToIntegerCast::UnsignedLength(uint32_t value) {
	if (value >= 10000) {
		int length = 5;
		length += value >= 100000;
		length += value >= 1000000;
		length += value >= 10000000;
		length += value >= 100000000;
		length += value >= 1000000000;
		return length;
	} else {
		int length = 1;
		length += value >= 10;
		length += value >= 100;
		length += value >= 1000;
		return length;
	}
}

template <> int StringToIntegerCast::UnsignedLength(uint64_t value) {
	if (value >= 10000000000ULL) {
		if (value >= 1000000000000000ULL) {
			int length = 16;
			length += value >= 10000000000000000ULL;
			length += value >= 100000000000000000ULL;
			length += value >= 1000000000000000000ULL;
			length += value >= 10000000000000000000ULL;
			return length;
		} else {
			int length = 11;
			length += value >= 100000000000ULL;
			length += value >= 1000000000000ULL;
			length += value >= 10000000000000ULL;
			length += value >= 100000000000000ULL;
			return length;
		}
	} else {
		if (value >= 100000ULL) {
			int length = 6;
			length += value >= 1000000ULL;
			length += value >= 10000000ULL;
			length += value >= 100000000ULL;
			length += value >= 1000000000ULL;
			return length;
		} else {
			int length = 1;
			length += value >= 10ULL;
			length += value >= 100ULL;
			length += value >= 1000ULL;
			length += value >= 10000ULL;
			return length;
		}
	}
}

template <> string_t StringCast::Operation(int8_t input, Vector &vector) {
	return StringToIntegerCast::FormatSigned<int8_t, uint8_t>(input, vector);
}

template <> string_t StringCast::Operation(int16_t input, Vector &vector) {
	return StringToIntegerCast::FormatSigned<int16_t, uint16_t>(input, vector);
}
template <> string_t StringCast::Operation(int32_t input, Vector &vector) {
	return StringToIntegerCast::FormatSigned<int32_t, uint32_t>(input, vector);
}

template <> string_t StringCast::Operation(int64_t input, Vector &vector) {
	return StringToIntegerCast::FormatSigned<int64_t, uint64_t>(input, vector);
}

template <> string_t StringCast::Operation(float input, Vector &vector) {
	string str = to_string(input);
	return vector.AddString(str);
}

template <> string_t StringCast::Operation(double input, Vector &vector) {
	string str = to_string(input);
	return vector.AddString(str);
}

//===--------------------------------------------------------------------===//
// Cast From Date
//===--------------------------------------------------------------------===//
template <> string_t CastFromDate::Operation(date_t input, Vector &vector) {
	string str = Date::ToString(input);
	return vector.AddString(str);
}

//===--------------------------------------------------------------------===//
// Cast To Date
//===--------------------------------------------------------------------===//
template <> date_t CastToDate::Operation(string_t input) {
	return Date::FromCString(input.GetData());
}

//===--------------------------------------------------------------------===//
// Cast From Time
//===--------------------------------------------------------------------===//
template <> string_t CastFromTime::Operation(dtime_t input, Vector &vector) {
	string str = Time::ToString(input);
	return vector.AddString(str);
}

//===--------------------------------------------------------------------===//
// Cast To Time
//===--------------------------------------------------------------------===//
template <> dtime_t CastToTime::Operation(string_t input) {
	return Time::FromCString(input.GetData());
}

template <> timestamp_t CastDateToTimestamp::Operation(date_t input) {
	return Timestamp::FromDatetime(input, Time::FromTime(0, 0, 0, 0));
}

//===--------------------------------------------------------------------===//
// Cast From Timestamps
//===--------------------------------------------------------------------===//
template <> string_t CastFromTimestamp::Operation(timestamp_t input, Vector &vector) {
	string str = Timestamp::ToString(input);
	return vector.AddString(str);
}

template <> date_t CastTimestampToDate::Operation(timestamp_t input) {
	return Timestamp::GetDate(input);
}

template <> dtime_t CastTimestampToTime::Operation(timestamp_t input) {
	return Timestamp::GetTime(input);
}

//===--------------------------------------------------------------------===//
// Cast To Timestamp
//===--------------------------------------------------------------------===//
template <> timestamp_t CastToTimestamp::Operation(string_t input) {
	return Timestamp::FromString(input.GetData());
}

} // namespace duckdb
