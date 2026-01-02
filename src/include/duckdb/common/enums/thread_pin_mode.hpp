//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/thread_pin_mode.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class ThreadPinMode : uint8_t { OFF = 0, ON = 1, AUTO = 2 };

} // namespace duckdb
