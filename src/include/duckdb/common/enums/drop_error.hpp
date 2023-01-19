//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/drop_error.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Catalog Types
//===--------------------------------------------------------------------===//
enum class DropErrorType : uint8_t { SUCCESS = 0, ENTRY_DOES_NOT_EXIST = 1, ENTRY_TYPE_MISMATCH = 2 };

} // namespace duckdb
