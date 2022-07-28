//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/decimal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {

//! The Decimal class is a static class that holds helper functions for the Decimal type
class Decimal {
public:
	static constexpr uint8_t MAX_WIDTH_INT16 = 4;
	static constexpr uint8_t MAX_WIDTH_INT32 = 9;
	static constexpr uint8_t MAX_WIDTH_INT64 = 18;
	static constexpr uint8_t MAX_WIDTH_INT128 = 38;
	static constexpr uint8_t MAX_WIDTH_DECIMAL = MAX_WIDTH_INT128;

public:
	static string ToString(int16_t value, uint8_t width, uint8_t scale);
	static string ToString(int32_t value, uint8_t width, uint8_t scale);
	static string ToString(int64_t value, uint8_t width, uint8_t scale);
	static string ToString(hugeint_t value, uint8_t width, uint8_t scale);
};
} // namespace duckdb
