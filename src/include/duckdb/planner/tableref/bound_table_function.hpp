//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/tableref/bound_table_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/bound_tableref.hpp"

namespace duckdb {
class TableFunctionCatalogEntry;

//! Represents a reference to a table-producing function call
class BoundTableFunction : public BoundTableRef {
public:
	BoundTableFunction(TableFunctionCatalogEntry *function, idx_t bind_index)
	    : BoundTableRef(TableReferenceType::TABLE_FUNCTION), function(function), bind_index(bind_index) {
	}

	//! The function that is called
	TableFunctionCatalogEntry *function;
	//! The set of parameters to use as input to the table-producing function
	vector<unique_ptr<Expression>> parameters;
	//! The index in the bind context
	idx_t bind_index;
};
} // namespace duckdb
