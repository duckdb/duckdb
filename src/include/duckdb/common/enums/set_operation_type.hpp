//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/set_operation_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class SetOperationType : uint8_t { NONE = 0, UNION = 1, EXCEPT = 2, INTERSECT = 3, UNION_BY_NAME = 4 };

} // namespace duckdb
