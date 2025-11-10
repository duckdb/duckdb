//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/arrow_format_version.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class ArrowOffsetSize : uint8_t { REGULAR, LARGE };

enum class ArrowFormatVersion : uint8_t {
	//! Base Version
	V1_0 = 10,
	//! Added 256-bit Decimal type.
	V1_1 = 11,
	//! Added MonthDayNano interval type.
	V1_2 = 12,
	//! Added Run-End Encoded Layout.
	V1_3 = 13,
	//! Added Variable-size Binary View Layout and the associated BinaryView and Utf8View types.
	//! Added ListView Layout and the associated ListView and LargeListView types. Added Variadic buffers.
	V1_4 = 14,
	//! Expanded Decimal type bit widths to allow 32-bit and 64-bit types.
	V1_5 = 15
};

} // namespace duckdb
