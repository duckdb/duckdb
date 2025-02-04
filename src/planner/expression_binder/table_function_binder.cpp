#include "duckdb/planner/expression_binder/table_function_binder.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/table_binding.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

TableFunctionBinder::TableFunctionBinder(Binder &binder, ClientContext &context, string table_function_name_p)
    : ExpressionBinder(binder, context), table_function_name(std::move(table_function_name_p)) {
}

BindResult TableFunctionBinder::BindLambdaReference(LambdaRefExpression &expr, idx_t depth) {
	D_ASSERT(lambda_bindings && expr.lambda_idx < lambda_bindings->size());
	auto &lambda_ref = expr.Cast<LambdaRefExpression>();
	return (*lambda_bindings)[expr.lambda_idx].Bind(lambda_ref, depth);
}

BindResult TableFunctionBinder::BindColumnReference(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
                                                    bool root_expression) {
	// try binding as a lambda parameter
	auto &col_ref = expr_ptr->Cast<ColumnRefExpression>();
	if (!col_ref.IsQualified()) {
		auto column_name = col_ref.GetName();
		auto lambda_ref = LambdaRefExpression::FindMatchingBinding(lambda_bindings, column_name);
		if (lambda_ref) {
			return BindLambdaReference(lambda_ref->Cast<LambdaRefExpression>(), depth);
		}
		if (binder.macro_binding && binder.macro_binding->HasMatchingBinding(column_name)) {
			throw ParameterNotResolvedException();
		}
	}
	auto query_location = col_ref.GetQueryLocation();
	auto column_names = col_ref.column_names;
	auto result_name = StringUtil::Join(column_names, ".");
	if (!table_function_name.empty()) {
		// check if this is a lateral join column/parameter
		auto result = BindCorrelatedColumns(expr_ptr, ErrorData("error"));
		if (!result.HasError()) {
			// it is a lateral join parameter - this is not supported in this type of table function
			throw BinderException(query_location,
			                      "Table function \"%s\" does not support lateral join column parameters - cannot use "
			                      "column \"%s\" in this context.\nThe function only supports literals as parameters.",
			                      table_function_name, result_name);
		}
	}

	auto value_function = ExpressionBinder::GetSQLValueFunction(column_names.back());
	if (value_function) {
		return BindExpression(value_function, depth, root_expression);
	}

	return BindResult(make_uniq<BoundConstantExpression>(Value(result_name)));
}

BindResult TableFunctionBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
                                               bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::LAMBDA_REF:
		return BindLambdaReference(expr.Cast<LambdaRefExpression>(), depth);
	case ExpressionClass::COLUMN_REF:
		return BindColumnReference(expr_ptr, depth, root_expression);
	case ExpressionClass::SUBQUERY:
		throw BinderException("Table function cannot contain subqueries");
	case ExpressionClass::DEFAULT:
		return BindResult("Table function cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindResult("Table function cannot contain window functions!");
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string TableFunctionBinder::UnsupportedAggregateMessage() {
	return "Table function cannot contain aggregates!";
}

} // namespace duckdb
