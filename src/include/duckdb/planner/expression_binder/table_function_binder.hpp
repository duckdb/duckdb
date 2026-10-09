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
	enum class IdentifierConversionPolicy : uint8_t { FOLLOW_SETTING, ALLOW };

	TableFunctionBinder(
	    Binder &binder, ClientContext &context, string table_function_name = string(), string clause = "Table function",
	    IdentifierConversionPolicy identifier_conversion_policy = IdentifierConversionPolicy::FOLLOW_SETTING);

public:
	void DisableSQLValueFunctions() {
		accept_sql_value_functions = false;
	}
	void EnableSQLValueFunctions() {
		accept_sql_value_functions = true;
	}

protected:
	BindResult BindLambdaReference(LambdaRefExpression &expr, idx_t depth);
	BindResult BindColumnReference(unique_ptr<ParsedExpression> &expr, idx_t depth, bool root_expression);
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr, idx_t depth, bool root_expression = false) override;

	string UnsupportedAggregateMessage() override;

private:
	string table_function_name;
	string clause;
	IdentifierConversionPolicy identifier_conversion_policy;
	//! Whether sql_value_functions (GetSQLValueFunctionName) are considered when binding column refs
	bool accept_sql_value_functions = true;
};

} // namespace duckdb
