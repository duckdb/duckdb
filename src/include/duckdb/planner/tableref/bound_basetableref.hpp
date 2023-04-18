//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/tableref/bound_basetableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class TableCatalogEntry;

//! Represents a TableReference to a base table in the schema
class BoundBaseTableRef : public BoundTableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::BASE_TABLE;

public:
	BoundBaseTableRef(TableCatalogEntry &table, unique_ptr<LogicalOperator> get)
	    : BoundTableRef(TableReferenceType::BASE_TABLE), table(table), get(std::move(get)) {
	}

	TableCatalogEntry &table;
	unique_ptr<LogicalOperator> get;
};
} // namespace duckdb
