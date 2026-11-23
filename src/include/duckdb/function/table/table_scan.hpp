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
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/function/built_in_functions.hpp"

#include "duckdb/storage/table/row_group_order_options.hpp"

namespace duckdb {
class DuckTableEntry;
class TableCatalogEntry;

//! Whether a table scan is for index creation
enum class CreateIndexScanType : uint8_t {
	//! A regular table scan
	NONE,
	//! A CREATE INDEX scan
	CREATE_INDEX,
	//! A CREATE INDEX scan that registers an index build, which applies the commits made during the build
	ONLINE_INDEX_BUILD
};

struct TableScanBindData : public TableFunctionData {
	explicit TableScanBindData(TableCatalogEntry &table)
	    : table(table), is_index_scan(false), create_index(CreateIndexScanType::NONE) {
	}

	//! The table to scan.
	TableCatalogEntry &table;
	//! The old purpose of this field has been deprecated.
	//! We now use it to express an index scan in the ANALYZE call.
	//! I.e., we const-cast the bind data and set this to true, if we opt for an index scan.
	bool is_index_scan;
	//! Whether and how the table scan is for index creation.
	CreateIndexScanType create_index;
	//! In what order to scan the row groups
	unique_ptr<RowGroupOrderOptions> order_options;
	//! Subset of partition indices to scan, if null, scan all
	unique_ptr<unordered_set<idx_t>> partitions_to_scan;

public:
	bool IsCreateIndex() const {
		return create_index != CreateIndexScanType::NONE;
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<TableScanBindData>();
		return &other.table == &table;
	}
	unique_ptr<FunctionData> Copy() const override {
		auto bind_data = make_uniq<TableScanBindData>(table);
		bind_data->is_index_scan = is_index_scan;
		bind_data->create_index = create_index;
		bind_data->column_ids = column_ids;
		bind_data->order_options = order_options ? make_uniq<RowGroupOrderOptions>(*order_options) : nullptr;
		bind_data->partitions_to_scan =
		    partitions_to_scan ? make_uniq<unordered_set<idx_t>>(*partitions_to_scan) : nullptr;
		return std::move(bind_data);
	}
};

//! The table scan function represents a sequential or index scan over one of DuckDB's base tables.
struct TableScanFunction {
	static void RegisterFunction(BuiltinFunctions &set);
	static TableFunction GetFunction();
};

} // namespace duckdb
