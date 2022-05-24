#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/bind_context.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(LambdaExpression &expr, idx_t depth, const idx_t lambda_params_count,
                                            const LogicalType &list_child_type) {

	string error;

	if (lambda_params_count != 0) {

		if (expr.params.size() != lambda_params_count) {
			throw BinderException(
			    "Invalid number of left-hand side parameters for this lambda function (params -> expr). "
			    "Expected " +
			    std::to_string(lambda_params_count) + ", got " + std::to_string(expr.params.size()) + ".");
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
		vector<LambdaBinding> local_bindings;
		if (!lambda_bindings) {
			lambda_bindings = &local_bindings;
		}
		LambdaBinding new_lambda_binding(column_types, column_names, params_alias);
		lambda_bindings->push_back(new_lambda_binding);

		// bind the parameter expressions
		for (idx_t i = 0; i < expr.params.size(); i++) {
			auto result = BindExpression(&expr.params[i], depth, false);
			if (result.HasError()) {
				return BindResult(error);
			}
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

	D_ASSERT(expr.params.size() == 1);
	auto lhs_expr = expr.params[0]->Copy();
	OperatorExpression arrow_expr(ExpressionType::ARROW, move(lhs_expr), move(expr.expr));
	return BindExpression(arrow_expr, depth);
}

} // namespace duckdb
