#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/bind_context.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(LambdaExpression &expr, idx_t depth, const idx_t lambda_param_count,
                                            const LogicalType &list_child_type) {

	string error;

	if (lambda_param_count != 0) {

		D_ASSERT(expr.lhs.size() != 0);

		if (expr.lhs.size() != lambda_param_count) {
			throw BinderException("Invalid number of left-hand side parameters for this lambda function.");
		}

		// create dummy columns for the lambda parameters (lhs)
		vector<LogicalType> column_types;
		vector<string> column_names;
		vector<string> lhs_strings;

		// positional parameters as column references
		for (idx_t i = 0; i < expr.lhs.size(); i++) {

			auto column_ref = (ColumnRefExpression &)*expr.lhs[i];
			if (column_ref.IsQualified()) {
				throw BinderException("Invalid parameter name '%s': must be unqualified", column_ref.ToString());
			}

			column_types.emplace_back(list_child_type);
			column_names.push_back(column_ref.GetColumnName());
			lhs_strings.push_back(expr.lhs[i]->ToString());
		}

		// base table alias
		auto lhs_alias = StringUtil::Join(lhs_strings, ", ");
		if (lhs_strings.size() > 1) {
			lhs_alias = "(" + lhs_alias + ")";
		}

		// create a lambda binding and push it to the lambda bindings vector
		vector<LambdaBinding> local_bindings;
		if (!lambda_bindings) {
			lambda_bindings = &local_bindings;
		}
		LambdaBinding new_lambda_binding(column_types, column_names, lhs_alias);
		lambda_bindings->push_back(new_lambda_binding);

		// bind the lhs expressions
		for (idx_t i = 0; i < expr.lhs.size(); i++) {
			auto result = BindExpression(&expr.lhs[i], depth, false);
			if (result.HasError()) {
				return BindResult(error);
			}
		}

		auto result = BindExpression(&expr.rhs, depth, false);
		(*lambda_bindings).pop_back();

		// now bind the rhs as a normal expression
		return result;
	}

	D_ASSERT(expr.lhs.size() == 1);
	auto lhs_expr = expr.lhs[0]->Copy();
	OperatorExpression arrow_expr(ExpressionType::ARROW, move(lhs_expr), move(expr.rhs));
	return BindExpression(arrow_expr, depth);
}

} // namespace duckdb
