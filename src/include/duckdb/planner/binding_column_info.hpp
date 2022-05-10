//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/binding_column_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_column_info.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

struct BindingColumnInfo {
	BindingColumnInfo(string name, LogicalType type, TableColumnInfo info) : name(name), type(type), info(info) {}
	string name;
	LogicalType	type;
	TableColumnInfo info;
};

} // namespace duckdb
