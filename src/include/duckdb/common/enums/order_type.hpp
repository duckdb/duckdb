//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/order_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class OrderType : uint8_t { INVALID = 0, ORDER_DEFAULT = 1, ASCENDING = 2, DESCENDING = 3 };
enum class OrderByNullType : uint8_t { INVALID = 0, ORDER_DEFAULT = 1, NULLS_FIRST = 2, NULLS_LAST = 3 };

} // namespace duckdb
