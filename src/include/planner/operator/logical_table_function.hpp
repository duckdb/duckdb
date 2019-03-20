//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_table_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalTableFunction represents a call to a table-producing function
class LogicalTableFunction : public LogicalOperator {
public:
	LogicalTableFunction(TableFunctionCatalogEntry *function, size_t table_index, vector<unique_ptr<Expression>> parameters)
	    : LogicalOperator(LogicalOperatorType::TABLE_FUNCTION), function(function),
	      parameters(move(parameters)), table_index(table_index) {
	}

	//! The function
	TableFunctionCatalogEntry *function;
	//! The function call
	vector<unique_ptr<Expression>> parameters;
	//! The table index of the table-producing function
	size_t table_index;

	vector<string> GetNames() override;

	size_t ExpressionCount() override;
	Expression *GetExpression(size_t index) override;
	void ReplaceExpression(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
	                       size_t index) override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
