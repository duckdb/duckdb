//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/operator/logical_table_function.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalTableFunction represents a call to a table-producing function
class LogicalTableFunction : public LogicalOperator {
  public:
	LogicalTableFunction(TableFunctionCatalogEntry *function,
	                     size_t table_index,
	                     std::unique_ptr<Expression> function_call)
	    : LogicalOperator(LogicalOperatorType::TABLE_FUNCTION),
	      function(function), function_call(std::move(function_call)),
	      table_index(table_index) {
		referenced_tables.insert(table_index);
	}

	//! The function
	TableFunctionCatalogEntry *function;
	//! The function call
	std::unique_ptr<Expression> function_call;
	//! The table index of the table-producing function
	size_t table_index;

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }
};
} // namespace duckdb
