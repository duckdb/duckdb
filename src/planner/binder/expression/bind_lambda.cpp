#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/bind_context.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(LambdaExpression &expr, idx_t depth, const bool is_lambda,
                                            const LogicalType &list_child_type) {

	string error;

	if (is_lambda) {

		D_ASSERT(expr.lhs);

		if (expr.lhs->expression_class != ExpressionClass::FUNCTION &&
		    expr.lhs->expression_class != ExpressionClass::COLUMN_REF) {
			throw BinderException(
			    "Invalid parameter list! Parameters must be comma-separated column names, e.g. x or (x, y).");
		}

		// move the lambda parameters to the params vector
		if (expr.lhs->expression_class == ExpressionClass::COLUMN_REF) {
			expr.params.push_back(move(expr.lhs));
		} else {
			auto &func_expr = (FunctionExpression &)*expr.lhs;
			for (idx_t i = 0; i < func_expr.children.size(); i++) {
				expr.params.push_back(move(func_expr.children[i]));
			}
		}
		if (expr.params.empty()) {
			throw BinderException("No parameters provided!");
		}

		// create dummy columns for the lambda parameters (lhs)
		vector<LogicalType> column_types;
		vector<string> column_names;
		vector<string> params_strings;

		// positional parameters as column references
		for (idx_t i = 0; i < expr.params.size(); i++) {

			if (expr.params[i]->GetExpressionClass() != ExpressionClass::COLUMN_REF) {
				throw BinderException("Parameter must be a column name.");
			}

			auto column_ref = (ColumnRefExpression &)*expr.params[i];
			if (column_ref.IsQualified()) {
				throw BinderException("Invalid parameter name '%s': must be unqualified", column_ref.ToString());
			}

			column_types.emplace_back(list_child_type);
			column_names.push_back(column_ref.GetColumnName());
			params_strings.push_back(expr.params[i]->ToString());
		}

		// base table alias
		auto params_alias = StringUtil::Join(params_strings, ", ");
		if (params_strings.size() > 1) {
			params_alias = "(" + params_alias + ")";
		}

		// create a lambda binding and push it to the lambda bindings vector
		vector<DummyBinding> local_bindings;
		if (!lambda_bindings) {
			lambda_bindings = &local_bindings;
		}
		DummyBinding new_lambda_binding(column_types, column_names, params_alias);
		lambda_bindings->push_back(new_lambda_binding);

		// bind the parameter expressions
		for (idx_t i = 0; i < expr.params.size(); i++) {
			auto result = BindExpression(&expr.params[i], depth, false);
			D_ASSERT(!result.HasError());
		}

		auto result = BindExpression(&expr.expr, depth, false);
		lambda_bindings->pop_back();

		// successfully bound a subtree of nested lambdas, set this to nullptr in case other parts of the
		// query also contain lambdas
		if (lambda_bindings->empty()) {
			lambda_bindings = nullptr;
		}

		// now bind the rhs as a normal expression
		return result;
	}

	// this is for binding macros
	D_ASSERT(expr.params.size() == 1);
	auto lhs_expr = expr.params[0]->Copy();
	OperatorExpression arrow_expr(ExpressionType::ARROW, move(lhs_expr), move(expr.expr));
	return BindExpression(arrow_expr, depth);
}

} // namespace duckdb
