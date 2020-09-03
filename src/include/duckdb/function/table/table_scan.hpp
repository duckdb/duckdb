//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/table_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {
class TableCatalogEntry;

struct TableScanBindData : public FunctionData {
	TableScanBindData(TableCatalogEntry *table) : table(table) {}

	//! The table to scan
	TableCatalogEntry *table;

	unique_ptr<FunctionData> Copy() override {
		return make_unique<TableScanBindData>(table);
	}
};

//! The table scan function represents a sequential scan over one of DuckDB's base tables.
struct TableScanFunction {
	static TableFunction GetFunction();
};

} // namespace duckdb
