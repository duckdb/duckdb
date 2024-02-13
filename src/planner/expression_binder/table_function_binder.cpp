#include "duckdb/planner/expression_binder/table_function_binder.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/table_binding.hpp"

namespace duckdb {

TableFunctionBinder::TableFunctionBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

BindResult TableFunctionBinder::BindLambdaReference(LambdaRefExpression &expr, idx_t depth) {
	D_ASSERT(lambda_bindings && expr.lambda_idx < lambda_bindings->size());
	auto &lambda_ref = expr.Cast<LambdaRefExpression>();
	return (*lambda_bindings)[expr.lambda_idx].Bind(lambda_ref, depth);
}

BindResult TableFunctionBinder::BindColumnReference(ColumnRefExpression &expr, idx_t depth, bool root_expression) {

	// try binding as a lambda parameter
	auto &col_ref = expr.Cast<ColumnRefExpression>();
	if (!col_ref.IsQualified()) {
		auto lambda_ref = LambdaRefExpression::FindMatchingBinding(lambda_bindings, col_ref.GetName());
		if (lambda_ref) {
			return BindLambdaReference(lambda_ref->Cast<LambdaRefExpression>(), depth);
		}
	}

	auto value_function = ExpressionBinder::GetSQLValueFunction(expr.GetColumnName());
	if (value_function) {
		return BindExpression(value_function, depth, root_expression);
	}

	auto result_name = StringUtil::Join(expr.column_names, ".");
	return BindResult(make_uniq<BoundConstantExpression>(Value(result_name)));
}

BindResult TableFunctionBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
                                               bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::LAMBDA_REF:
		return BindLambdaReference(expr.Cast<LambdaRefExpression>(), depth);
	case ExpressionClass::COLUMN_REF:
		return BindColumnReference(expr.Cast<ColumnRefExpression>(), depth, root_expression);
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
