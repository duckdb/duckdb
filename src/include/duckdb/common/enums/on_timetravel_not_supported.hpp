//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/on_timetravel_not_supported.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class OnTimetravelNotSupported : uint8_t { THROW_EXCEPTION = 0, IGNORE_TIMETRAVEL = 1 };

} // namespace duckdb
