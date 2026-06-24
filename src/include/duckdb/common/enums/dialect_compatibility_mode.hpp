//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/dialect_compatibility_mode.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class DialectCompatibilityMode : uint8_t { NONE = 0, SPARK = 1 };

} // namespace duckdb
