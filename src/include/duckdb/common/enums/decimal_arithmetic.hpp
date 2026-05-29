//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/decimal_arithmetic.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class DecimalArithmetic : uint8_t { DECIMAL = 0, DOUBLE = 1 };

} // namespace duckdb
