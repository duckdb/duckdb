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
#include "duckdb/common/types/vector.hpp"
#include "fmt/format.h"

namespace duckdb {

//! NumericHelper is a static class that holds helper functions for integers/doubles
class NumericHelper {
public:
	static int64_t PowersOfTen[20];
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
	template<class SIGNED, class UNSIGNED>
	static int DecimalLength(SIGNED value, uint8_t scale) {
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
		return std::max(scale + 2 + (value < 0 ? 1 : 0), NumericHelper::SignedLength<SIGNED, UNSIGNED>(value) + 1);
	}

	template<class SIGNED, class UNSIGNED>
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
		UNSIGNED minor = value % (UNSIGNED) NumericHelper::PowersOfTen[scale];
		UNSIGNED major = value / (UNSIGNED) NumericHelper::PowersOfTen[scale];
		// write the number after the decimal
		dst = NumericHelper::FormatUnsigned<UNSIGNED>(minor, end);
		// (optionally) pad with zeros and add the decimal point
		while(dst > (end - scale)) {
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

}
