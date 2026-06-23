//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/mark_null_strategy.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class MarkNullStrategy : uint8_t { NONE, SIMPLE_HAS_NULL, CORRELATED_COUNTS, NULL_REMAINDER, FULL_SCAN };

} // namespace duckdb
