//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/debug_initialize.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class DebugInitialize : uint8_t { NO_INITIALIZE = 0, DEBUG_ZERO_INITIALIZE = 1, DEBUG_ONE_INITIALIZE = 2 };

} // namespace duckdb
