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
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {
class DuckTableEntry;
class TableCatalogEntry;

struct TableScanBindData : public TableFunctionData {
	explicit TableScanBindData(TableCatalogEntry &table) : table(table), is_index_scan(false), is_create_index(false) {
	}

	//! The table to scan.
	TableCatalogEntry &table;
	//! The old purpose of this field has been deprecated.
	//! We now use it to express an index scan in the ANALYZE call.
	//! I.e., we const-cast the bind data and set this to true, if we opt for an index scan.
	bool is_index_scan;
	//! Whether or not the table scan is for index creation.
	bool is_create_index;

public:
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<TableScanBindData>();
		return &other.table == &table;
	}
	unique_ptr<FunctionData> Copy() const override {
		auto bind_data = make_uniq<TableScanBindData>(table);
		bind_data->is_index_scan = is_index_scan;
		bind_data->is_create_index = is_create_index;
		bind_data->column_ids = column_ids;
		return std::move(bind_data);
	}
};

//! The table scan function represents a sequential or index scan over one of DuckDB's base tables.
struct TableScanFunction {
	static void RegisterFunction(BuiltinFunctions &set);
	static TableFunction GetFunction();
};

} // namespace duckdb
