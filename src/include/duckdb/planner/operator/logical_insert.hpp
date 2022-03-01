//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalInsert represents an insertion of data into a base table
class LogicalInsert : public LogicalOperator {
public:
	explicit LogicalInsert(TableCatalogEntry *table)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_INSERT), table(table) {
	}

	vector<vector<unique_ptr<Expression>>> insert_values;
	//! The insertion map ([table_index -> index in result, or DConstants::INVALID_INDEX if not specified])
	vector<idx_t> column_index_map;
	//! The expected types for the INSERT statement (obtained from the column types)
	vector<LogicalType> expected_types;
	//! The base table to insert into
	TableCatalogEntry *table;
	//! The default statements used by the table
	vector<unique_ptr<Expression>> bound_defaults;
	//! The list of returning expressions
	vector<unique_ptr<Expression>> returning_list;

protected:
	vector<ColumnBinding> GetColumnBindings() override {
		vector<ColumnBinding> bindings;
		// TODO: what if it isn't a column but rather a literal (i.e RETURNING 50, "duckdb")?
		if (returning_list.empty()) {
			bindings.push_back({0, 0});
		} else {
			for (idx_t i = 0; i != returning_list.size(); i++) {
				bindings.push_back({0, i});
			}
		}
		return bindings;
	}

	void ResolveTypes() override {
		if (returning_list.empty()) {
			types.emplace_back(LogicalType::BIGINT);
		}
		else {
			for(idx_t i = 0; i != returning_list.size(); i++) {
				types.emplace_back(returning_list[i]->return_type);
			}
		}
	}
};
} // namespace duckdb
