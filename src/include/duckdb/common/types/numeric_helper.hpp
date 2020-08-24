//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/numeric_helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/vector.hpp"
#include "fmt/format.h"

namespace duckdb {

//! NumericHelper is a static class that holds helper functions for integers/doubles
class NumericHelper {
public:
	static int64_t PowersOfTen[20];
	static double DoublePowersOfTen[40];

public:
	template <class T> static int UnsignedLength(T value);
	template <class SIGNED, class UNSIGNED> static int SignedLength(SIGNED value) {
		int sign = -(value < 0);
		UNSIGNED unsigned_value = (value ^ sign) - sign;
		return UnsignedLength(unsigned_value) - sign;
	}

	// Formats value in reverse and returns a pointer to the beginning.
	template <class T> static char *FormatUnsigned(T value, char *ptr) {
		while (value >= 100) {
			// Integer division is slow so do it for a group of two digits instead
			// of for every digit. The idea comes from the talk by Alexandrescu
			// "Three Optimization Tips for C++". See speed-test for a comparison.
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

template <> int NumericHelper::UnsignedLength(uint8_t value);
template <> int NumericHelper::UnsignedLength(uint16_t value);
template <> int NumericHelper::UnsignedLength(uint32_t value);
template <> int NumericHelper::UnsignedLength(uint64_t value);

struct DecimalToString {
	template <class SIGNED, class UNSIGNED> static int DecimalLength(SIGNED value, uint8_t scale) {
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
		return MaxValue(scale + 2 + (value < 0 ? 1 : 0), NumericHelper::SignedLength<SIGNED, UNSIGNED>(value) + 1);
	}

	template <class SIGNED, class UNSIGNED>
	static void FormatDecimal(SIGNED value, uint8_t scale, char *dst, idx_t len) {
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
		UNSIGNED minor = value % (UNSIGNED)NumericHelper::PowersOfTen[scale];
		UNSIGNED major = value / (UNSIGNED)NumericHelper::PowersOfTen[scale];
		// write the number after the decimal
		dst = NumericHelper::FormatUnsigned<UNSIGNED>(minor, end);
		// (optionally) pad with zeros and add the decimal point
		while (dst > (end - scale)) {
			*--dst = '0';
		}
		*--dst = '.';
		// now write the part before the decimal
		dst = NumericHelper::FormatUnsigned<UNSIGNED>(major, dst);
	}

	template <class SIGNED, class UNSIGNED> static string_t Format(SIGNED value, uint8_t scale, Vector &vector) {
		int len = DecimalLength<SIGNED, UNSIGNED>(value, scale);
		string_t result = StringVector::EmptyString(vector, len);
		FormatDecimal<SIGNED, UNSIGNED>(value, scale, result.GetData(), len);
		result.Finalize();
		return result;
	}
};

struct HugeintToStringCast {
	static int UnsignedLength(hugeint_t value) {
		assert(value.upper >= 0);
		if (value.upper == 0) {
			return NumericHelper::UnsignedLength<uint64_t>(value.lower);
		}
		// search the length using the PowersOfTen array
		// the length has to be between [17] and [38], because the hugeint is bigger than 2^63
		// we use the same approach as above, but split a bit more because comparisons for hugeints are more expensive
		if (value >= Hugeint::PowersOfTen[27]) {
			// [27..38]
			if (value >= Hugeint::PowersOfTen[32]) {
				if (value >= Hugeint::PowersOfTen[36]) {
					int length = 37;
					length += value >= Hugeint::PowersOfTen[37];
					length += value >= Hugeint::PowersOfTen[38];
					return length;
				} else {
					int length = 33;
					length += value >= Hugeint::PowersOfTen[33];
					length += value >= Hugeint::PowersOfTen[34];
					length += value >= Hugeint::PowersOfTen[35];
					return length;
				}
			} else {
				if (value >= Hugeint::PowersOfTen[30]) {
					int length = 31;
					length += value >= Hugeint::PowersOfTen[31];
					length += value >= Hugeint::PowersOfTen[32];
					return length;
				} else {
					int length = 28;
					length += value >= Hugeint::PowersOfTen[28];
					length += value >= Hugeint::PowersOfTen[29];
					return length;
				}
			}
		} else {
			// [17..27]
			if (value >= Hugeint::PowersOfTen[22]) {
				// [22..27]
				if (value >= Hugeint::PowersOfTen[25]) {
					int length = 26;
					length += value >= Hugeint::PowersOfTen[26];
					return length;
				} else {
					int length = 23;
					length += value >= Hugeint::PowersOfTen[23];
					length += value >= Hugeint::PowersOfTen[24];
					return length;
				}
			} else {
				// [17..22]
				if (value >= Hugeint::PowersOfTen[20]) {
					int length = 21;
					length += value >= Hugeint::PowersOfTen[21];
					return length;
				} else {
					int length = 18;
					length += value >= Hugeint::PowersOfTen[18];
					length += value >= Hugeint::PowersOfTen[19];
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
		auto dataptr = result.GetData();
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
		assert(endptr == dataptr);
		result.Finalize();
		return result;
	}

	static int DecimalLength(hugeint_t value, uint8_t scale) {
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
		return MaxValue(scale + 2, UnsignedLength(value) + 1) + negative;
	}

	static void FormatDecimal(hugeint_t value, uint8_t scale, char *dst, int len) {
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
		hugeint_t major = Hugeint::DivMod(value, Hugeint::PowersOfTen[scale], minor);

		// write the number after the decimal
		dst = FormatUnsigned(minor, endptr);
		// (optionally) pad with zeros and add the decimal point
		while (dst > (endptr - scale)) {
			*--dst = '0';
		}
		*--dst = '.';
		// now write the part before the decimal
		dst = FormatUnsigned(major, dst);
	}

	static string_t FormatDecimal(hugeint_t value, uint8_t scale, Vector &vector) {
		int length = DecimalLength(value, scale);
		string_t result = StringVector::EmptyString(vector, length);

		auto dst = result.GetData();

		FormatDecimal(value, scale, dst, length);

		result.Finalize();
		return result;
	}
};

} // namespace duckdb
