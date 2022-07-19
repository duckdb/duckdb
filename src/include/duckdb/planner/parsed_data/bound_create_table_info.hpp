//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/parsed_data/bound_create_table_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/bound_constraint.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/storage/table/persistent_table_data.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/catalog/catalog_entry/table_column_type.hpp"
#include "duckdb/catalog/catalog_entry/column_dependency_manager.hpp"
#include "duckdb/storage/table_index.hpp"

namespace duckdb {
class CatalogEntry;

struct BoundCreateTableInfo {
	explicit BoundCreateTableInfo(unique_ptr<CreateInfo> base_p) : base(move(base_p)) {
		D_ASSERT(base);
	}

	//! The schema to create the table in
	SchemaCatalogEntry *schema;
	//! The base CreateInfo object
	unique_ptr<CreateInfo> base;
	//! The map of column names -> column index, used during binding
	case_insensitive_map_t<column_t> name_map;
	//! Column dependency manager of the table
	ColumnDependencyManager column_dependency_manager;
	//! List of constraints on the table
	vector<unique_ptr<Constraint>> constraints;
	//! List of bound constraints on the table
	vector<unique_ptr<BoundConstraint>> bound_constraints;
	//! Bound default values
	vector<unique_ptr<Expression>> bound_defaults;
	//! Dependents of the table (in e.g. default values)
	unordered_set<CatalogEntry *> dependencies;
	//! The existing table data on disk (if any)
	unique_ptr<PersistentTableData> data;
	//! CREATE TABLE from QUERY
	unique_ptr<LogicalOperator> query;
	//! Indexes created by this table <Block_ID, Offset>
	vector<BlockPointer> indexes;

	CreateTableInfo &Base() {
		D_ASSERT(base);
		return (CreateTableInfo &)*base;
	}
};

} // namespace duckdb
