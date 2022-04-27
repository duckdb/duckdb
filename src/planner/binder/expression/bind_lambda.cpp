#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/bind_context.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(LambdaExpression &expr, idx_t depth, bool is_lambda,
                                            LogicalType list_child_type) {

	string error;

	if (is_lambda) {

		D_ASSERT(expr.lhs.size() != 0);

		// create dummy columns for the lambda parameters (lhs)
		vector<LogicalType> column_types;
		vector<string> dummy_column_names;
		vector<string> lhs_strings;

		// positional parameters as column references
		for (idx_t i = 0; i < expr.lhs.size(); i++) {

			auto column_ref = (ColumnRefExpression &)*expr.lhs[i];
			if (column_ref.IsQualified()) {
				throw BinderException("Invalid parameter name '%s': must be unqualified", column_ref.ToString());
			}

			column_types.emplace_back(list_child_type);
			dummy_column_names.push_back(column_ref.GetColumnName());
			lhs_strings.push_back(expr.lhs[i]->ToString());
		}

		// base table alias
		auto lhs_alias = StringUtil::Join(lhs_strings, ", ");
		if (lhs_strings.size() > 1) {
			lhs_alias = "(" + lhs_alias + ")";
		}

		// create bindings for the lambda parameters
		binder.bind_context.AddGenericBinding(-1, lhs_alias, dummy_column_names, column_types);

		// now bind the rhs as a normal expression
		return ExpressionBinder::BindExpression(&expr.rhs, depth, false);
	}

	D_ASSERT(expr.lhs.size() == 1);
	auto lhs_expr = expr.lhs[0]->Copy();
	OperatorExpression arrow_expr(ExpressionType::ARROW, move(lhs_expr), move(expr.rhs));
	return ExpressionBinder::BindExpression(arrow_expr, depth);
}

} // namespace duckdb
