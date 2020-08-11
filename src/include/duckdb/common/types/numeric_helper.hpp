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
#include "duckdb/common/types/vector.hpp"
#include "fmt/format.h"

namespace duckdb {

//! NumericHelper is a static class that holds helper functions for integers/doubles
class NumericHelper {
public:
	template <class T> static int UnsignedLength(T value);
	template <class SIGNED, class UNSIGNED> static int SignedLength(SIGNED value) {
		int sign = -(value < 0);
		UNSIGNED unsigned_value = (value ^ sign) - sign;
		return UnsignedLength(unsigned_value);
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

}
