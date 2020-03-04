//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_table_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalTableFunction represents a call to a table-producing function
class LogicalTableFunction : public LogicalOperator {
public:
	LogicalTableFunction(TableFunctionCatalogEntry *function, idx_t table_index,
	                     vector<unique_ptr<Expression>> parameters, vector<SQLType> return_types, vector<string> names)
	    : LogicalOperator(LogicalOperatorType::TABLE_FUNCTION), function(function), table_index(table_index),
	      return_types(move(return_types)), names(move(names)) {
		expressions = move(parameters);
	}

	//! The function
	TableFunctionCatalogEntry *function;
	//! The table index of the table-producing function
	idx_t table_index;
	//! The set of returned sql types
	vector<SQLType> return_types;
	//! The set of returned column names
	vector<string> names;

public:
	vector<ColumnBinding> GetColumnBindings() override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
