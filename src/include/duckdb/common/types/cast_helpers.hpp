//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/cast_helpers.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/vector.hpp"
#include "fmt/format.h"

namespace duckdb {

//! NumericHelper is a static class that holds helper functions for integers/doubles
class NumericHelper {
public:
	static constexpr uint8_t CACHED_POWERS_OF_TEN = 20;
	static const int64_t POWERS_OF_TEN[CACHED_POWERS_OF_TEN];
	static const double DOUBLE_POWERS_OF_TEN[40];

public:
	template <class T>
	static int UnsignedLength(T value);
	template <class SIGNED, class UNSIGNED>
	static int SignedLength(SIGNED value) {
		int sign = -(value < 0);
		UNSIGNED unsigned_value = (value ^ sign) - sign;
		return UnsignedLength(unsigned_value) - sign;
	}

	// Formats value in reverse and returns a pointer to the beginning.
	template <class T>
	static char *FormatUnsigned(T value, char *ptr) {
		while (value >= 100) {
			// Integer division is slow so do it for a group of two digits instead
			// of for every digit. The idea comes from the talk by Alexandrescu
			// "Three Optimization Tips for C++".
			auto index = static_cast<unsigned>((value % 100) * 2);
			value /= 100;
			*--ptr = duckdb_fmt::internal::data::digits[index + 1];
			*--ptr = duckdb_fmt::internal::data::digits[index];
		}
		if (value < 10) {
			*--ptr = static_cast<char>('0' + value);
			return ptr;
		}
		auto index = static_cast<unsigned>(value * 2);
		*--ptr = duckdb_fmt::internal::data::digits[index + 1];
		*--ptr = duckdb_fmt::internal::data::digits[index];
		return ptr;
	}

	template <class SIGNED, class UNSIGNED>
	static string_t FormatSigned(SIGNED value, Vector &vector) {
		int sign = -(value < 0);
		UNSIGNED unsigned_value = UNSIGNED(value ^ sign) - sign;
		int length = UnsignedLength<UNSIGNED>(unsigned_value) - sign;
		string_t result = StringVector::EmptyString(vector, length);
		auto dataptr = result.GetDataWriteable();
		auto endptr = dataptr + length;
		endptr = FormatUnsigned(unsigned_value, endptr);
		if (sign) {
			*--endptr = '-';
		}
		result.Finalize();
		return result;
	}

	template <class T>
	static std::string ToString(T value) {
		return std::to_string(value);
	}
};

template <>
int NumericHelper::UnsignedLength(uint8_t value);
template <>
int NumericHelper::UnsignedLength(uint16_t value);
template <>
int NumericHelper::UnsignedLength(uint32_t value);
template <>
int NumericHelper::UnsignedLength(uint64_t value);

template <>
std::string NumericHelper::ToString(hugeint_t value);

struct DecimalToString {
	template <class SIGNED, class UNSIGNED>
	static int DecimalLength(SIGNED value, uint8_t width, uint8_t scale) {
		if (scale == 0) {
			// scale is 0: regular number
			return NumericHelper::SignedLength<SIGNED, UNSIGNED>(value);
		}
		// length is max of either:
		// scale + 2 OR
		// integer length + 1
		// scale + 2 happens when the number is in the range of (-1, 1)
		// in that case we print "0.XXX", which is the scale, plus "0." (2 chars)
		// integer length + 1 happens when the number is outside of that range
		// in that case we print the integer number, but with one extra character ('.')
		auto extra_characters = width > scale ? 2 : 1;
		return MaxValue(scale + extra_characters + (value < 0 ? 1 : 0),
		                NumericHelper::SignedLength<SIGNED, UNSIGNED>(value) + 1);
	}

	template <class SIGNED, class UNSIGNED>
	static void FormatDecimal(SIGNED value, uint8_t width, uint8_t scale, char *dst, idx_t len) {
		char *end = dst + len;
		if (value < 0) {
			value = -value;
			*dst = '-';
		}
		if (scale == 0) {
			NumericHelper::FormatUnsigned<UNSIGNED>(value, end);
			return;
		}
		// we write two numbers:
		// the numbers BEFORE the decimal (major)
		// and the numbers AFTER the decimal (minor)
		UNSIGNED minor = value % (UNSIGNED)NumericHelper::POWERS_OF_TEN[scale];
		UNSIGNED major = value / (UNSIGNED)NumericHelper::POWERS_OF_TEN[scale];
		// write the number after the decimal
		dst = NumericHelper::FormatUnsigned<UNSIGNED>(minor, end);
		// (optionally) pad with zeros and add the decimal point
		while (dst > (end - scale)) {
			*--dst = '0';
		}
		*--dst = '.';
		// now write the part before the decimal
		D_ASSERT(width > scale || major == 0);
		if (width > scale) {
			// there are numbers after the comma
			dst = NumericHelper::FormatUnsigned<UNSIGNED>(major, dst);
		}
	}

	template <class SIGNED, class UNSIGNED>
	static string_t Format(SIGNED value, uint8_t width, uint8_t scale, Vector &vector) {
		int len = DecimalLength<SIGNED, UNSIGNED>(value, width, scale);
		string_t result = StringVector::EmptyString(vector, len);
		FormatDecimal<SIGNED, UNSIGNED>(value, width, scale, result.GetDataWriteable(), len);
		result.Finalize();
		return result;
	}
};

struct HugeintToStringCast {
	static int UnsignedLength(hugeint_t value) {
		D_ASSERT(value.upper >= 0);
		if (value.upper == 0) {
			return NumericHelper::UnsignedLength<uint64_t>(value.lower);
		}
		// search the length using the POWERS_OF_TEN array
		// the length has to be between [17] and [38], because the hugeint is bigger than 2^63
		// we use the same approach as above, but split a bit more because comparisons for hugeints are more expensive
		if (value >= Hugeint::POWERS_OF_TEN[27]) {
			// [27..38]
			if (value >= Hugeint::POWERS_OF_TEN[32]) {
				if (value >= Hugeint::POWERS_OF_TEN[36]) {
					int length = 37;
					length += value >= Hugeint::POWERS_OF_TEN[37];
					length += value >= Hugeint::POWERS_OF_TEN[38];
					return length;
				} else {
					int length = 33;
					length += value >= Hugeint::POWERS_OF_TEN[33];
					length += value >= Hugeint::POWERS_OF_TEN[34];
					length += value >= Hugeint::POWERS_OF_TEN[35];
					return length;
				}
			} else {
				if (value >= Hugeint::POWERS_OF_TEN[30]) {
					int length = 31;
					length += value >= Hugeint::POWERS_OF_TEN[31];
					length += value >= Hugeint::POWERS_OF_TEN[32];
					return length;
				} else {
					int length = 28;
					length += value >= Hugeint::POWERS_OF_TEN[28];
					length += value >= Hugeint::POWERS_OF_TEN[29];
					return length;
				}
			}
		} else {
			// [17..27]
			if (value >= Hugeint::POWERS_OF_TEN[22]) {
				// [22..27]
				if (value >= Hugeint::POWERS_OF_TEN[25]) {
					int length = 26;
					length += value >= Hugeint::POWERS_OF_TEN[26];
					return length;
				} else {
					int length = 23;
					length += value >= Hugeint::POWERS_OF_TEN[23];
					length += value >= Hugeint::POWERS_OF_TEN[24];
					return length;
				}
			} else {
				// [17..22]
				if (value >= Hugeint::POWERS_OF_TEN[20]) {
					int length = 21;
					length += value >= Hugeint::POWERS_OF_TEN[21];
					return length;
				} else {
					int length = 18;
					length += value >= Hugeint::POWERS_OF_TEN[18];
					length += value >= Hugeint::POWERS_OF_TEN[19];
					return length;
				}
			}
		}
	}

	// Formats value in reverse and returns a pointer to the beginning.
	static char *FormatUnsigned(hugeint_t value, char *ptr) {
		while (value.upper > 0) {
			// while integer division is slow, hugeint division is MEGA slow
			// we want to avoid doing as many divisions as possible
			// for that reason we start off doing a division by a large power of ten that uint64_t can hold
			// (100000000000000000) - this is the third largest
			// the reason we don't use the largest is because that can result in an overflow inside the division
			// function
			uint64_t remainder;
			value = Hugeint::DivModPositive(value, 100000000000000000ULL, remainder);

			auto startptr = ptr;
			// now we format the remainder: note that we need to pad with zero's in case
			// the remainder is small (i.e. less than 10000000000000000)
			ptr = NumericHelper::FormatUnsigned<uint64_t>(remainder, ptr);

			int format_length = startptr - ptr;
			// pad with zero
			for (int i = format_length; i < 17; i++) {
				*--ptr = '0';
			}
		}
		// once the value falls in the range of a uint64_t, fallback to formatting as uint64_t to avoid hugeint division
		return NumericHelper::FormatUnsigned<uint64_t>(value.lower, ptr);
	}

	static string_t FormatSigned(hugeint_t value, Vector &vector) {
		int negative = value.upper < 0;
		if (negative) {
			Hugeint::NegateInPlace(value);
		}
		int length = UnsignedLength(value) + negative;
		string_t result = StringVector::EmptyString(vector, length);
		auto dataptr = result.GetDataWriteable();
		auto endptr = dataptr + length;
		if (value.upper == 0) {
			// small value: format as uint64_t
			endptr = NumericHelper::FormatUnsigned<uint64_t>(value.lower, endptr);
		} else {
			endptr = FormatUnsigned(value, endptr);
		}
		if (negative) {
			*--endptr = '-';
		}
		D_ASSERT(endptr == dataptr);
		result.Finalize();
		return result;
	}

	static int DecimalLength(hugeint_t value, uint8_t width, uint8_t scale) {
		int negative;
		if (value.upper < 0) {
			Hugeint::NegateInPlace(value);
			negative = 1;
		} else {
			negative = 0;
		}
		if (scale == 0) {
			// scale is 0: regular number
			return UnsignedLength(value) + negative;
		}
		// length is max of either:
		// scale + 2 OR
		// integer length + 1
		// scale + 2 happens when the number is in the range of (-1, 1)
		// in that case we print "0.XXX", which is the scale, plus "0." (2 chars)
		// integer length + 1 happens when the number is outside of that range
		// in that case we print the integer number, but with one extra character ('.')
		auto extra_numbers = width > scale ? 2 : 1;
		return MaxValue(scale + extra_numbers, UnsignedLength(value) + 1) + negative;
	}

	static void FormatDecimal(hugeint_t value, uint8_t width, uint8_t scale, char *dst, int len) {
		auto endptr = dst + len;

		int negative = value.upper < 0;
		if (negative) {
			Hugeint::NegateInPlace(value);
			*dst = '-';
			dst++;
		}
		if (scale == 0) {
			// with scale=0 we format the number as a regular number
			FormatUnsigned(value, endptr);
			return;
		}

		// we write two numbers:
		// the numbers BEFORE the decimal (major)
		// and the numbers AFTER the decimal (minor)
		hugeint_t minor;
		hugeint_t major = Hugeint::DivMod(value, Hugeint::POWERS_OF_TEN[scale], minor);

		// write the number after the decimal
		dst = FormatUnsigned(minor, endptr);
		// (optionally) pad with zeros and add the decimal point
		while (dst > (endptr - scale)) {
			*--dst = '0';
		}
		*--dst = '.';
		// now write the part before the decimal
		D_ASSERT(width > scale || major == 0);
		if (width > scale) {
			dst = FormatUnsigned(major, dst);
		}
	}

	static string_t FormatDecimal(hugeint_t value, uint8_t width, uint8_t scale, Vector &vector) {
		int length = DecimalLength(value, width, scale);
		string_t result = StringVector::EmptyString(vector, length);

		auto dst = result.GetDataWriteable();

		FormatDecimal(value, width, scale, dst, length);

		result.Finalize();
		return result;
	}
};

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
			date[0] = -date[0] + 1;
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
		endptr = NumericHelper::FormatUnsigned(date[0], endptr);
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
				ptr[1] = duckdb_fmt::internal::data::digits[index];
				ptr[2] = duckdb_fmt::internal::data::digits[index + 1];
			}
			ptr += 3;
		}
		// optionally add BC to the end of the date
		if (add_bc) {
			memcpy(ptr, " (BC)", 5);
		}
	}
};

struct TimeToStringCast {
	//! Format microseconds to a buffer of length 6. Returns the number of trailing zeros
	static int32_t FormatMicros(uint32_t microseconds, char micro_buffer[]) {
		char *endptr = micro_buffer + 6;
		endptr = NumericHelper::FormatUnsigned<uint32_t>(microseconds, endptr);
		while (endptr > micro_buffer) {
			*--endptr = '0';
		}
		idx_t trailing_zeros = 0;
		for (idx_t i = 5; i > 0; i--) {
			if (micro_buffer[i] != '0') {
				break;
			}
			trailing_zeros++;
		}
		return trailing_zeros;
	}

	static idx_t Length(int32_t time[], char micro_buffer[]) {
		// format is HH:MM:DD.MS
		// microseconds come after the time with a period separator
		idx_t length;
		if (time[3] == 0) {
			// no microseconds
			// format is HH:MM:DD
			length = 8;
		} else {
			length = 15;
			// for microseconds, we truncate any trailing zeros (i.e. "90000" becomes ".9")
			// first write the microseconds to the microsecond buffer
			// we write backwards and pad with zeros to the left
			// now we figure out how many digits we need to include by looking backwards
			// and checking how many zeros we encounter
			length -= FormatMicros(time[3], micro_buffer);
		}
		return length;
	}

	static void FormatTwoDigits(char *ptr, int32_t value) {
		D_ASSERT(value >= 0 && value <= 99);
		if (value < 10) {
			ptr[0] = '0';
			ptr[1] = '0' + value;
		} else {
			auto index = static_cast<unsigned>(value * 2);
			ptr[0] = duckdb_fmt::internal::data::digits[index];
			ptr[1] = duckdb_fmt::internal::data::digits[index + 1];
		}
	}

	static void Format(char *data, idx_t length, int32_t time[], char micro_buffer[]) {
		// first write hour, month and day
		auto ptr = data;
		ptr[2] = ':';
		ptr[5] = ':';
		for (int i = 0; i <= 2; i++) {
			FormatTwoDigits(ptr, time[i]);
			ptr += 3;
		}
		if (length > 8) {
			// write the micro seconds at the end
			data[8] = '.';
			memcpy(data + 9, micro_buffer, length - 9);
		}
	}
};

struct IntervalToStringCast {
	static void FormatSignedNumber(int64_t value, char buffer[], idx_t &length) {
		int sign = -(value < 0);
		uint64_t unsigned_value = (value ^ sign) - sign;
		length += NumericHelper::UnsignedLength<uint64_t>(unsigned_value) - sign;
		auto endptr = buffer + length;
		endptr = NumericHelper::FormatUnsigned<uint64_t>(unsigned_value, endptr);
		if (sign) {
			*--endptr = '-';
		}
	}

	static void FormatTwoDigits(int64_t value, char buffer[], idx_t &length) {
		TimeToStringCast::FormatTwoDigits(buffer + length, value);
		length += 2;
	}

	static void FormatIntervalValue(int32_t value, char buffer[], idx_t &length, const char *name, idx_t name_len) {
		if (value == 0) {
			return;
		}
		if (length != 0) {
			// space if there is already something in the buffer
			buffer[length++] = ' ';
		}
		FormatSignedNumber(value, buffer, length);
		// append the name together with a potential "s" (for plurals)
		memcpy(buffer + length, name, name_len);
		length += name_len;
		if (value != 1) {
			buffer[length++] = 's';
		}
	}

	//! Formats an interval to a buffer, the buffer should be >=70 characters
	//! years: 17 characters (max value: "-2147483647 years")
	//! months: 9 (max value: "12 months")
	//! days: 16 characters (max value: "-2147483647 days")
	//! time: 24 characters (max value: -2562047788:00:00.123456)
	//! spaces between all characters (+3 characters)
	//! Total: 70 characters
	//! Returns the length of the interval
	static idx_t Format(interval_t interval, char buffer[]) {
		idx_t length = 0;
		if (interval.months != 0) {
			int32_t years = interval.months / 12;
			int32_t months = interval.months - years * 12;
			// format the years and months
			FormatIntervalValue(years, buffer, length, " year", 5);
			FormatIntervalValue(months, buffer, length, " month", 6);
		}
		if (interval.days != 0) {
			// format the days
			FormatIntervalValue(interval.days, buffer, length, " day", 4);
		}
		if (interval.micros != 0) {
			if (length != 0) {
				// space if there is already something in the buffer
				buffer[length++] = ' ';
			}
			int64_t micros = interval.micros;
			if (micros < 0) {
				// negative time: append negative sign
				buffer[length++] = '-';
			} else {
				micros = -micros;
			}
			int64_t hour = -(micros / Interval::MICROS_PER_HOUR);
			micros += hour * Interval::MICROS_PER_HOUR;
			int64_t min = -(micros / Interval::MICROS_PER_MINUTE);
			micros += min * Interval::MICROS_PER_MINUTE;
			int64_t sec = -(micros / Interval::MICROS_PER_SEC);
			micros += sec * Interval::MICROS_PER_SEC;
			micros = -micros;

			if (hour < 10) {
				buffer[length++] = '0';
			}
			FormatSignedNumber(hour, buffer, length);
			buffer[length++] = ':';
			FormatTwoDigits(min, buffer, length);
			buffer[length++] = ':';
			FormatTwoDigits(sec, buffer, length);
			if (micros != 0) {
				buffer[length++] = '.';
				auto trailing_zeros = TimeToStringCast::FormatMicros(micros, buffer + length);
				length += 6 - trailing_zeros;
			}
		} else if (length == 0) {
			// empty interval: default to 00:00:00
			memcpy(buffer, "00:00:00", 8);
			return 8;
		}
		return length;
	}
};

} // namespace duckdb
