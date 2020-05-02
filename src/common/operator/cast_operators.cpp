#include "duckdb/common/operator/cast_operators.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/vector.hpp"
#include "fmt/format.h"

#include <cctype>
#include <cmath>
#include <cstdlib>

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
template <> bool TryCast::Operation(int16_t input, int8_t &result, bool strict) {
	return try_cast_with_overflow_check(input, result);
}
template <> bool TryCast::Operation(int32_t input, int8_t &result, bool strict) {
	return try_cast_with_overflow_check(input, result);
}
template <> bool TryCast::Operation(int64_t input, int8_t &result, bool strict) {
	return try_cast_with_overflow_check(input, result);
}
template <> bool TryCast::Operation(float input, int8_t &result, bool strict) {
	return try_cast_with_overflow_check(input, result);
}
template <> bool TryCast::Operation(double input, int8_t &result, bool strict) {
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
template <> bool TryCast::Operation(int32_t input, int16_t &result, bool strict) {
	return try_cast_with_overflow_check(input, result);
}
template <> bool TryCast::Operation(int64_t input, int16_t &result, bool strict) {
	return try_cast_with_overflow_check(input, result);
}
template <> bool TryCast::Operation(float input, int16_t &result, bool strict) {
	return try_cast_with_overflow_check(input, result);
}
template <> bool TryCast::Operation(double input, int16_t &result, bool strict) {
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
template <> bool TryCast::Operation(int64_t input, int32_t &result, bool strict) {
	return try_cast_with_overflow_check(input, result);
}
template <> bool TryCast::Operation(float input, int32_t &result, bool strict) {
	return try_cast_with_overflow_check(input, result);
}
template <> bool TryCast::Operation(double input, int32_t &result, bool strict) {
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
template <> bool TryCast::Operation(float input, int64_t &result, bool strict) {
	return try_cast_with_overflow_check(input, result);
}
template <> bool TryCast::Operation(double input, int64_t &result, bool strict) {
	return try_cast_with_overflow_check(input, result);
}

template <> int64_t Cast::Operation(float input) {
	return cast_with_overflow_check<float, int64_t>(input);
}
template <> int64_t Cast::Operation(double input) {
	return cast_with_overflow_check<double, int64_t>(input);
}

//===--------------------------------------------------------------------===//
// Double -> float casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(double input, float &result, bool strict) {
	auto res = (float)input;
	if (std::isnan(res) || std::isinf(res)) {
		return false;
	}
	result = res;
	return true;
}

template <> float Cast::Operation(double input) {
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
template <class T> static T try_cast_string(string_t input) {
	T result;
	if (!TryCast::Operation<string_t, T>(input, result)) {
		throw ConversionException("Could not convert string '%s' to numeric", input.GetData());
	}
	return result;
}

template <class T> static T try_strict_cast_string(string_t input) {
	T result;
	if (!TryCast::Operation<string_t, T>(input, result, true)) {
		throw ConversionException("Could not convert string '%s' to numeric", input.GetData());
	}
	return result;
}

template <class T, bool NEGATIVE, bool ALLOW_EXPONENT>
static bool IntegerCastLoop(const char *buf, T &result, bool strict) {
	idx_t pos = NEGATIVE ? 1 : 0;
	while (buf[pos]) {
		if (!std::isdigit((unsigned char)buf[pos])) {
			// not a digit!
			if (buf[pos] == '.') {
				if (strict) {
					return false;
				}
				// decimal point: we accept decimal values for integers as well
				// we just truncate them
				// make sure everything after the period is a number
				pos++;
				while (buf[pos]) {
					if (!std::isdigit((unsigned char)buf[pos++])) {
						return false;
					}
				}
				return true;
			}
			if (std::isspace((unsigned char)buf[pos])) {
				// skip any trailing spaces
				while (buf[++pos]) {
					if (!std::isspace((unsigned char)buf[pos])) {
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
						if (!IntegerCastLoop<int64_t, true, false>(buf + pos, exponent, strict)) {
							return false;
						}
					} else {
						if (!IntegerCastLoop<int64_t, false, false>(buf + pos, exponent, strict)) {
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

template <class T, bool ALLOW_EXPONENT = true> static bool TryIntegerCast(const char *buf, T &result, bool strict) {
	if (!*buf) {
		return false;
	}
	// skip any spaces at the start
	while (std::isspace((unsigned char)*buf)) {
		buf++;
	}
	int negative = *buf == '-';

	result = 0;
	if (!negative) {
		return IntegerCastLoop<T, false, ALLOW_EXPONENT>(buf, result, strict);
	} else {
		return IntegerCastLoop<T, true, ALLOW_EXPONENT>(buf, result, strict);
	}
}

template <> bool TryCast::Operation(string_t input, bool &result, bool strict) {
	auto input_data = input.GetData();
	// TODO: add support for '0' and '1' as boolean
	if (strict) {
		if (strcmp(input_data, "true") == 0 || strcmp(input_data, "True") == 0 || strcmp(input_data, "TRUE") == 0) {
			result = true;
		} else if (strcmp(input_data, "false") == 0 || strcmp(input_data, "False") == 0 ||
		           strcmp(input_data, "FALSE") == 0) {
			result = false;
		} else {
			return false;
		}
	} else {
		if (input_data[0] == 't' || input_data[0] == 'T') {
			result = true;
		} else if (input_data[0] == 'f' || input_data[0] == 'F') {
			result = false;
		} else {
			return false;
		}
	}

	return true;
}
template <> bool TryCast::Operation(string_t input, int8_t &result, bool strict) {
	return TryIntegerCast<int8_t>(input.GetData(), result, strict);
}
template <> bool TryCast::Operation(string_t input, int16_t &result, bool strict) {
	return TryIntegerCast<int16_t>(input.GetData(), result, strict);
}
template <> bool TryCast::Operation(string_t input, int32_t &result, bool strict) {
	return TryIntegerCast<int32_t>(input.GetData(), result, strict);
}
template <> bool TryCast::Operation(string_t input, int64_t &result, bool strict) {
	return TryIntegerCast<int64_t>(input.GetData(), result, strict);
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

template <class T, bool NEGATIVE> static bool DoubleCastLoop(const char *buf, T &result, bool strict) {
	idx_t pos = NEGATIVE ? 1 : 0;
	idx_t decimal = 0;
	idx_t decimal_factor = 0;
	while (buf[pos]) {
		if (!std::isdigit((unsigned char)buf[pos])) {
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
			} else if (std::isspace((unsigned char)buf[pos])) {
				// skip any trailing spaces
				while (buf[++pos]) {
					if (!std::isspace((unsigned char)buf[pos])) {
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
				if (!TryIntegerCast<int64_t, false>(buf + pos, exponent, strict)) {
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

template <class T> bool CheckDoubleValidity(T value);

template <> bool CheckDoubleValidity(float value) {
	return Value::FloatIsValid(value);
}

template <> bool CheckDoubleValidity(double value) {
	return Value::DoubleIsValid(value);
}

template <class T> static bool TryDoubleCast(const char *buf, T &result, bool strict) {
	if (!*buf) {
		return false;
	}
	// skip any spaces at the start
	while (std::isspace((unsigned char)*buf)) {
		buf++;
	}
	int negative = *buf == '-';

	result = 0;
	if (!negative) {
		if (!DoubleCastLoop<T, false>(buf, result, strict)) {
			return false;
		}
	} else {
		if (!DoubleCastLoop<T, true>(buf, result, strict)) {
			return false;
		}
	}
	if (!CheckDoubleValidity<T>(result)) {
		return false;
	}
	return true;
}

template <> bool TryCast::Operation(string_t input, float &result, bool strict) {
	return TryDoubleCast<float>(input.GetData(), result, strict);
}
template <> bool TryCast::Operation(string_t input, double &result, bool strict) {
	return TryDoubleCast<double>(input.GetData(), result, strict);
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

template <> bool StrictCast::Operation(string_t input) {
	return try_strict_cast_string<bool>(input);
}
template <> int8_t StrictCast::Operation(string_t input) {
	return try_strict_cast_string<int8_t>(input);
}
template <> int16_t StrictCast::Operation(string_t input) {
	return try_strict_cast_string<int16_t>(input);
}
template <> int32_t StrictCast::Operation(string_t input) {
	return try_strict_cast_string<int32_t>(input);
}
template <> int64_t StrictCast::Operation(string_t input) {
	return try_strict_cast_string<int64_t>(input);
}
template <> float StrictCast::Operation(string_t input) {
	return try_strict_cast_string<float>(input);
}
template <> double StrictCast::Operation(string_t input) {
	return try_strict_cast_string<double>(input);
}

//===--------------------------------------------------------------------===//
// Cast Numeric -> String
//===--------------------------------------------------------------------===//
template <class T> string CastToStandardString(T input) {
	Vector v(TypeId::VARCHAR);
	return StringCast::Operation(input, v).GetString();
}

template <> string Cast::Operation(bool input) {
	return CastToStandardString(input);
}
template <> string Cast::Operation(int8_t input) {
	return CastToStandardString(input);
}
template <> string Cast::Operation(int16_t input) {
	return CastToStandardString(input);
}
template <> string Cast::Operation(int32_t input) {
	return CastToStandardString(input);
}
template <> string Cast::Operation(int64_t input) {
	return CastToStandardString(input);
}
template <> string Cast::Operation(float input) {
	return CastToStandardString(input);
}
template <> string Cast::Operation(double input) {
	return CastToStandardString(input);
}
template <> string Cast::Operation(string_t input) {
	return input.GetString();
}

template <> string_t StringCast::Operation(bool input, Vector &vector) {
	if (input) {
		return StringVector::AddString(vector, "true", 4);
	} else {
		return StringVector::AddString(vector, "false", 5);
	}
}

struct StringToIntegerCast {
	template <class T> static int UnsignedLength(T value);

	// Formats value in reverse and returns a pointer to the beginning.
	template <class T> static char *FormatUnsigned(T value, char *ptr) {
		while (value >= 100) {
			// Integer division is slow so do it for a group of two digits instead
			// of for every digit. The idea comes from the talk by Alexandrescu
			// "Three Optimization Tips for C++". See speed-test for a comparison.
			auto index = static_cast<unsigned>((value % 100) * 2);
			value /= 100;
			*--ptr = fmt::internal::data::digits[index + 1];
			*--ptr = fmt::internal::data::digits[index];
		}
		if (value < 10) {
			*--ptr = static_cast<char>('0' + value);
			return ptr;
		}
		auto index = static_cast<unsigned>(value * 2);
		*--ptr = fmt::internal::data::digits[index + 1];
		*--ptr = fmt::internal::data::digits[index];
		return ptr;
	}

	template <class SIGNED, class UNSIGNED> static string_t FormatSigned(SIGNED value, Vector &vector) {
		int sign = -(value < 0);
		UNSIGNED unsigned_value = (value ^ sign) - sign;
		int length = UnsignedLength<UNSIGNED>(unsigned_value) - sign;
		string_t result = StringVector::EmptyString(vector, length);
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
	std::string s = fmt::format("{}", input);
	return StringVector::AddString(vector, s);
}

template <> string_t StringCast::Operation(double input, Vector &vector) {
	std::string s = fmt::format("{}", input);
	return StringVector::AddString(vector, s);
}

//===--------------------------------------------------------------------===//
// Cast From Date
//===--------------------------------------------------------------------===//
struct DateToStringCast {
	static idx_t Length(int32_t date[], idx_t &year_length, bool &add_bc) {
		// format is YYYY-MM-DD with optional (BC) at the end
		// regular length is 10
		idx_t length = 6;
		year_length = 4;
		add_bc = false;
		if (date[0] <= 0) {
			// add (BC) suffix
			length += 5;
			date[0] = -date[0];
			add_bc = true;
		}

		// potentially add extra characters depending on length of year
		year_length += date[0] >= 10000;
		year_length += date[0] >= 100000;
		year_length += date[0] >= 1000000;
		year_length += date[0] >= 10000000;
		length += year_length;
		return length;
	}

	static void Format(char *data, int32_t date[], idx_t year_length, bool add_bc) {
		// now we write the string, first write the year
		auto endptr = data + year_length;
		endptr = StringToIntegerCast::FormatUnsigned(date[0], endptr);
		// add optional leading zeros
		while (endptr > data) {
			*--endptr = '0';
		}
		// now write the month and day
		auto ptr = data + year_length;
		for (int i = 1; i <= 2; i++) {
			ptr[0] = '-';
			if (date[i] < 10) {
				ptr[1] = '0';
				ptr[2] = '0' + date[i];
			} else {
				auto index = static_cast<unsigned>(date[i] * 2);
				ptr[1] = fmt::internal::data::digits[index];
				ptr[2] = fmt::internal::data::digits[index + 1];
			}
			ptr += 3;
		}
		// optionally add BC to the end of the date
		if (add_bc) {
			memcpy(ptr, " (BC)", 5);
		}
	}
};

template <> string_t CastFromDate::Operation(date_t input, Vector &vector) {
	int32_t date[3];
	Date::Convert(input, date[0], date[1], date[2]);

	idx_t year_length;
	bool add_bc;
	idx_t length = DateToStringCast::Length(date, year_length, add_bc);

	string_t result = StringVector::EmptyString(vector, length);
	auto data = result.GetData();

	DateToStringCast::Format(data, date, year_length, add_bc);

	result.Finalize();
	return result;
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
struct TimeToStringCast {
	static idx_t Length(int32_t time[]) {
		// format is HH:MM:DD
		// regular length is 8
		idx_t length = 8;
		if (time[3] > 0) {
			// if there are msecs, we add the miliseconds after the time with a period separator
			// i.e. the format becomes HH:MM:DD.msec
			length += 4;
		}
		return length;
	}

	static void Format(char *data, idx_t length, int32_t time[]) {
		// first write hour, month and day
		auto ptr = data;
		for (int i = 0; i <= 2; i++) {
			if (time[i] < 10) {
				ptr[0] = '0';
				ptr[1] = '0' + time[i];
			} else {
				auto index = static_cast<unsigned>(time[i] * 2);
				ptr[0] = fmt::internal::data::digits[index];
				ptr[1] = fmt::internal::data::digits[index + 1];
			}
			ptr[2] = ':';
			ptr += 3;
		}
		// now optionally write ms at the end
		if (time[3] > 0) {
			auto start = ptr;
			ptr = StringToIntegerCast::FormatUnsigned(time[3], data + length);
			while (ptr > start) {
				*--ptr = '0';
			}
			*--ptr = '.';
		}
	}
};

template <> string_t CastFromTime::Operation(dtime_t input, Vector &vector) {
	int32_t time[4];
	Time::Convert(input, time[0], time[1], time[2], time[3]);

	idx_t length = TimeToStringCast::Length(time);

	string_t result = StringVector::EmptyString(vector, length);
	auto data = result.GetData();

	TimeToStringCast::Format(data, length, time);

	result.Finalize();
	return result;
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
	date_t date_entry;
	dtime_t time_entry;
	Timestamp::Convert(input, date_entry, time_entry);

	int32_t date[3], time[4];
	Date::Convert(date_entry, date[0], date[1], date[2]);
	Time::Convert(time_entry, time[0], time[1], time[2], time[3]);

	// format for timestamp is DATE TIME (separated by space)
	idx_t year_length;
	bool add_bc;
	idx_t date_length = DateToStringCast::Length(date, year_length, add_bc);
	idx_t time_length = TimeToStringCast::Length(time);
	idx_t length = date_length + time_length + 1;

	string_t result = StringVector::EmptyString(vector, length);
	auto data = result.GetData();

	DateToStringCast::Format(data, date, year_length, add_bc);
	data[date_length] = ' ';
	TimeToStringCast::Format(data + date_length + 1, time_length, time);

	result.Finalize();
	return result;
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
