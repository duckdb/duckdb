#include "duckdb/planner/expression_binder/table_function_binder.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/table_binding.hpp"

namespace duckdb {

TableFunctionBinder::TableFunctionBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

BindResult TableFunctionBinder::BindColumnReference(ColumnRefExpression &expr, idx_t depth, bool root_expression) {

	// if this is a lambda parameters, then we temporarily add a BoundLambdaRef,
	// which we capture and remove later
	if (lambda_bindings) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		for (idx_t i = 0; i < lambda_bindings->size(); i++) {
			if (colref.GetColumnName() == (*lambda_bindings)[i].dummy_name) {
				return (*lambda_bindings)[i].Bind(colref, i, depth);
			}
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
