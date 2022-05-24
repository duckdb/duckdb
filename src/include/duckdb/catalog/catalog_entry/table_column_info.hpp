//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/table_column_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class TableColumnType : uint8_t { STANDARD = 0, GENERATED = 1 };

struct TableColumnInfo {
	column_t index;
	TableColumnType column_type;
	TableColumnInfo(column_t index = 0, TableColumnType column_type = TableColumnType::STANDARD)
	    : index(index), column_type(column_type) {
	}
};

} // namespace duckdb
