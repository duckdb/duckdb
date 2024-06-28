//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/table_function_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

//! The table function binder can bind standard table function parameters (i.e., non-table-in-out functions)
class TableFunctionBinder : public ExpressionBinder {
public:
	TableFunctionBinder(Binder &binder, ClientContext &context, string table_function_name = string());

protected:
	BindResult BindLambdaReference(LambdaRefExpression &expr, idx_t depth);
	BindResult BindColumnReference(unique_ptr<ParsedExpression> &expr, idx_t depth, bool root_expression);
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr, idx_t depth, bool root_expression = false) override;

	string UnsupportedAggregateMessage() override;

private:
	string table_function_name;
};

} // namespace duckdb
