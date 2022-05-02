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

enum class TableColumnType { STANDARD, GENERATED };

struct TableColumnInfo {
	column_t index;
	TableColumnType column_type;
	TableColumnInfo(column_t index = 0, TableColumnType column_type = TableColumnType::STANDARD);
	TableColumnInfo(const TableColumnInfo &table_column_info);
	TableColumnInfo &operator=(const TableColumnInfo &table_column_info);
};

} // namespace duckdb
