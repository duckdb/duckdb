//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/decimal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

template <class PHYSICAL_TYPE>
struct DecimalWidth {};

template <>
struct DecimalWidth<int16_t> {
	static constexpr uint8_t max = 4;
};

template <>
struct DecimalWidth<int32_t> {
	static constexpr uint8_t max = 9;
};

template <>
struct DecimalWidth<int64_t> {
	static constexpr uint8_t max = 18;
};

template <>
struct DecimalWidth<hugeint_t> {
	static constexpr uint8_t max = 38;
};

//! The Decimal class is a static class that holds helper functions for the Decimal type
class Decimal {
public:
	static constexpr uint8_t MAX_WIDTH_INT16 = DecimalWidth<int16_t>::max;
	static constexpr uint8_t MAX_WIDTH_INT32 = DecimalWidth<int32_t>::max;
	static constexpr uint8_t MAX_WIDTH_INT64 = DecimalWidth<int64_t>::max;
	static constexpr uint8_t MAX_WIDTH_INT128 = DecimalWidth<hugeint_t>::max;
	//! The maximum width of a decimal that is stored in a fixed-size integer (INT16/INT32/INT64/INT128)
	static constexpr uint8_t MAX_WIDTH_DECIMAL = MAX_WIDTH_INT128;
	//! Decimals wider than MAX_WIDTH_DECIMAL are stored as BIGNUM blobs holding the unscaled value, up to this width
	static constexpr uint8_t MAX_WIDTH_BIGNUM = 255;

public:
	//! Whether width/scale form a valid DECIMAL type: width in [1, MAX_WIDTH_BIGNUM] and scale not exceeding width.
	static bool IsValidWidthScale(uint8_t width, uint8_t scale) {
		return width >= 1 && width <= MAX_WIDTH_BIGNUM && scale <= width;
	}
	//! Whether a decimal of this width is stored as a BIGNUM blob instead of a fixed-size integer
	static bool IsWide(uint8_t width) {
		return width > MAX_WIDTH_DECIMAL;
	}
	//! The widest decimal that may result from combining decimals of the given widths: results stay within the
	//! fixed-size range unless one of the inputs is already a wide (BIGNUM-backed) decimal
	static uint8_t MaxCombinedWidth(uint8_t left_width, uint8_t right_width) {
		return IsWide(left_width) || IsWide(right_width) ? MAX_WIDTH_BIGNUM : MAX_WIDTH_DECIMAL;
	}
	static string ToString(int16_t value, uint8_t width, uint8_t scale);
	static string ToString(int32_t value, uint8_t width, uint8_t scale);
	static string ToString(int64_t value, uint8_t width, uint8_t scale);
	static string ToString(hugeint_t value, uint8_t width, uint8_t scale);
	//! Formats the unscaled value of a wide decimal (a BIGNUM blob)
	static string ToString(string_t value, uint8_t width, uint8_t scale);
};
} // namespace duckdb
