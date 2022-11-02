//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/table_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {
class TableCatalogEntry;

struct TableScanBindData : public TableFunctionData {
	explicit TableScanBindData(TableCatalogEntry *table) : table(table), is_index_scan(false), is_create_index(false) {
	}

	//! The table to scan
	TableCatalogEntry *table;

	//! Whether or not the table scan is an index scan
	bool is_index_scan;
	//! Whether or not the table scan is for index creation
	bool is_create_index;
	//! The row ids to fetch (in case of an index scan)
	vector<row_t> result_ids;

public:
	bool Equals(const FunctionData &other_p) const override {
		auto &other = (const TableScanBindData &)other_p;
		return other.table == table && result_ids == other.result_ids;
	}
};

//! The table scan function represents a sequential scan over one of DuckDB's base tables.
struct TableScanFunction {
	static void RegisterFunction(BuiltinFunctions &set);
	static TableFunction GetFunction();
	static TableFunction GetIndexScanFunction();
	static TableCatalogEntry *GetTableEntry(const TableFunction &function, const FunctionData *bind_data);
};

} // namespace duckdb
