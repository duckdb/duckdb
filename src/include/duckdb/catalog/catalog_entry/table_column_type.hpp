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

enum class TableColumnType : uint8_t { STANDARD = 0, GENERATED_VIRTUAL = 1, GENERATED_STORED = 2 };

} // namespace duckdb
