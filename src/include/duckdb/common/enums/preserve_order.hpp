//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/preserve_order.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class PreserveOrderType : uint8_t { AUTOMATIC = 0, PRESERVE_ORDER = 1, DONT_PRESERVE_ORDER = 2 };

} // namespace duckdb
