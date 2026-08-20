//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/show_behavior.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//! Controls how "SHOW name" resolves an identifier - as a table to describe or as a setting value.
enum class ShowBehaviorType : uint8_t { AUTO = 0, SETTING = 1, TABLE = 2 };

} // namespace duckdb
