//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/table_column_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class TableColumnType : uint8_t { STANDARD = 0, GENERATED = 1 };

} // namespace duckdb
